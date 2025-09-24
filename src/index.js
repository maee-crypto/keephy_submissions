import express from 'express';
import mongoose from 'mongoose';
import pino from 'pino';
import pinoHttp from 'pino-http';

const PORT = process.env.PORT || 3005;
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017/keephy_submissions';

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

// Mongo setup
mongoose.set('strictQuery', true);
mongoose
  .connect(MONGO_URL, { autoIndex: true })
  .then(() => logger.info({ msg: 'Connected to MongoDB', url: MONGO_URL }))
  .catch((err) => {
    logger.error({ err }, 'MongoDB connection error');
    process.exit(1);
  });

// Schema & Model
const submissionSchema = new mongoose.Schema(
  {
    businessId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true },
    franchiseId: { type: mongoose.Schema.Types.ObjectId, required: false, index: true },
    formId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true },
    rating: { type: Number, required: true, min: 1, max: 5 },
    categories: [{ key: String, score: Number }],
    comment: { type: String },
    staffId: { type: mongoose.Schema.Types.ObjectId, required: false, index: true },
    deviceId: { type: String, required: false, index: true },
    ip: { type: String, required: false },
    // dedupe window key to prevent rapid duplicate submissions
    dedupeKey: { type: String, index: true },
    createdBy: { type: String },
  },
  { timestamps: true }
);

submissionSchema.index({ dedupeKey: 1 }, { unique: false });

const Submission = mongoose.model('Submission', submissionSchema);

// Simple Outbox for events
const outboxSchema = new mongoose.Schema(
  {
    type: { type: String, required: true },
    payload: { type: Object, required: true },
    status: { type: String, enum: ['pending', 'sent', 'failed'], default: 'pending', index: true },
    attempts: { type: Number, default: 0 },
    lastError: { type: String },
  },
  { timestamps: true }
);
const Outbox = mongoose.model('SubmissionOutbox', outboxSchema);

// App
const app = express();
app.use(express.json());
app.use(
  pinoHttp({
    logger,
    customLogLevel: (_req, res, err) => {
      if (res.statusCode >= 500 || err) return 'error';
      if (res.statusCode >= 400) return 'warn';
      return 'info';
    },
  })
);

// Helpers
const buildDedupeKey = ({ formId, deviceId, ip }) => {
  const basis = deviceId || ip || 'anonymous';
  // Round to 15-min windows to reduce accidental dupes while allowing later submissions
  const windowMs = 15 * 60 * 1000;
  const bucket = Math.floor(Date.now() / windowMs);
  return `${formId}:${basis}:${bucket}`;
};

const emitEvent = async (type, payload) => {
  await Outbox.create({ type, payload, status: 'pending' });
  logger.info({ type }, 'Queued event in outbox');
};

// Routes
app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok', service: 'submissions-service' });
});
app.get('/ready', (_req, res) => {
  const state = mongoose.connection.readyState; // 1 = connected
  res.status(state === 1 ? 200 : 503).json({ ready: state === 1 });
});

// Create submission with simple dedupe
app.post('/submissions', async (req, res) => {
  try {
    const {
      businessId,
      franchiseId,
      formId,
      rating,
      categories,
      comment,
      staffId,
      deviceId,
    } = req.body || {};

    if (!businessId || !formId || !rating) {
      return res.status(400).json({ message: 'businessId, formId, rating are required' });
    }

    const dedupeKey = buildDedupeKey({ formId, deviceId, ip: req.ip });

    // Check recent duplicate in last 15 minutes
    const existing = await Submission.findOne({ dedupeKey }).lean();
    if (existing) {
      return res.status(429).json({ message: 'Duplicate submission detected. Please try later.' });
    }

    const submission = await Submission.create({
      businessId,
      franchiseId,
      formId,
      rating,
      categories,
      comment,
      staffId,
      deviceId,
      ip: req.ip,
      dedupeKey,
      createdBy: req.headers['x-user-id'] || undefined,
    });

    // Emit event stub
    await emitEvent('FormSubmitted', {
      submissionId: submission._id.toString(),
      businessId,
      franchiseId,
      formId,
      rating,
      at: submission.createdAt,
    });

    res.status(201).json(submission);
  } catch (err) {
    req.log.error({ err }, 'Failed to create submission');
    res.status(500).json({ message: 'Internal server error' });
  }
});

// List by business (basic pagination)
app.get('/submissions/by-business/:businessId', async (req, res) => {
  try {
    const { businessId } = req.params;
    const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.min(parseInt(req.query.limit, 10) || 20, 100);
    const skip = (page - 1) * limit;

    const [items, total] = await Promise.all([
      Submission.find({ businessId }).sort({ createdAt: -1 }).skip(skip).limit(limit).lean(),
      Submission.countDocuments({ businessId }),
    ]);

    res.status(200).json({ items, total, page, limit });
  } catch (err) {
    req.log.error({ err }, 'Failed to list submissions');
    res.status(500).json({ message: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  logger.info(`submissions-service listening on ${PORT}`);
});

// Minimal pending endpoint for debugging outbox
app.get('/outbox/pending', async (_req, res) => {
  const items = await Outbox.find({ status: 'pending' }).sort({ createdAt: 1 }).limit(50).lean();
  res.json({ items });
});

// Manual consume endpoint (stub)
app.post('/internal/consume-outbox', async (req, res) => {
  const limit = Math.min(Number(req.body?.limit || 10), 100);
  const processed = [];
  for (let i = 0; i < limit; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    const doc = await Outbox.findOneAndUpdate(
      { status: 'pending' },
      { $set: { status: 'sent' }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, new: true }
    ).lean();
    if (!doc) break;
    processed.push(doc._id);
  }
  res.json({ processedCount: processed.length, processed });
});

// Background dispatcher stub (would publish to Kafka/Bus)
const DISPATCH_INTERVAL_MS = Number(process.env.DISPATCH_INTERVAL_MS || 5000);
setInterval(async () => {
  try {
    const doc = await Outbox.findOneAndUpdate(
      { status: 'pending' },
      { $set: { status: 'sent' }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, new: true }
    ).lean();
    if (doc) {
      logger.info({ type: doc.type, id: doc._id }, 'Dispatched event (stub)');
    }
  } catch (err) {
    logger.error({ err }, 'Outbox dispatcher error');
  }
}, DISPATCH_INTERVAL_MS);


