/* eslint-disable no-console */
require("dotenv").config();

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { MongoClient } = require("mongodb");
const notificationapi = require("notificationapi-node-server-sdk").default;

const app = express();
app.use(cors());
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ----------------- Config -----------------
const PORT = Number(process.env.PORT || 3001);

const NOTIF_CLIENT_ID = process.env.NOTIF_CLIENT_ID;
const NOTIF_CLIENT_SECRET = process.env.NOTIF_CLIENT_SECRET;
const NOTIF_BASE_URL = process.env.NOTIF_BASE_URL; // optional
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL; // not required for sending, but ok to keep
const NOTIF_INBOUND_WEBHOOK_SECRET = process.env.NOTIF_INBOUND_WEBHOOK_SECRET;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || "llrchatbot";

if (!NOTIF_CLIENT_ID || !NOTIF_CLIENT_SECRET) throw new Error("Missing NOTIF_CLIENT_ID / NOTIF_CLIENT_SECRET");
if (!NOTIF_INBOUND_WEBHOOK_SECRET) throw new Error("Missing NOTIF_INBOUND_WEBHOOK_SECRET");
if (!OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");
if (!MONGODB_URI) throw new Error("Missing MONGODB_URI");

// Init NotificationAPI AFTER env is validated
const initConfig = NOTIF_BASE_URL ? { baseURL: NOTIF_BASE_URL } : undefined;
notificationapi.init(NOTIF_CLIENT_ID, NOTIF_CLIENT_SECRET, initConfig);

// ----------------- Mongo -----------------
const mongo = new MongoClient(MONGODB_URI);
let sessions;
let webhookEvents;

async function initDb() {
  await mongo.connect();
  const db = mongo.db(MONGODB_DB);

  sessions = db.collection("SMS_AI_Sessions");
  webhookEvents = db.collection("Webhook_Events");

  await sessions.createIndex({ userId: 1 }, { unique: true });
  await sessions.createIndex({ updatedAt: -1 });

  // Dedupe key: lastTrackingId per inbound event
  await webhookEvents.createIndex({ trackingId: 1 }, { unique: true });
  // Optional TTL to auto-clean dedupe records (7 days)
  await webhookEvents.createIndex({ createdAt: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });

  console.log("✅ Mongo connected");
}

// ----------------- Helpers -----------------
function isEmojiOnly(text) {
  const t = String(text || "").trim();
  if (!t) return true;
  const stripped = t
    .replace(/[\u{1F300}-\u{1FAFF}]/gu, "")
    .replace(/[\u{2600}-\u{27BF}]/gu, "")
    .replace(/\s/g, "");
  return stripped.length === 0;
}

function normalizeUserIdFromPhone(phoneE164) {
  return String(phoneE164 || "").replace(/\D/g, "");
}

function assertE164(number) {
  if (!/^\+[1-9]\d{1,14}$/.test(number)) {
    throw new Error(`Phone must be E.164 like +16175551212. Got: ${number}`);
  }
}

function takeLastMessages(history = [], max = 12) {
  if (!Array.isArray(history)) return [];
  return history.slice(Math.max(0, history.length - max));
}

async function openaiReply({ userText, history }) {
  const system = `
You are an AI SMS assistant. Be concise, helpful, and human.
Rules:
- Keep replies under ~320 characters unless absolutely necessary.
- No markdown, no bullet spam.
- If user asks multiple questions, answer in 1-2 tight sentences.
- If they ask for sensitive personal data, refuse.
`;

  const messages = [
    { role: "system", content: system.trim() },
    ...takeLastMessages(history, 12),
    { role: "user", content: userText },
  ];

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      model: OPENAI_MODEL,
      messages,
      temperature: 0.4,
      max_tokens: 140,
    },
    {
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
      timeout: 60000,
    }
  );

  return resp.data?.choices?.[0]?.message?.content?.trim() || "";
}

// ----------------- OUTBOUND-FIRST -----------------
app.post("/outbound/start", async (req, res) => {
  try {
    const { number, firstMessage, userId } = req.body || {};
    if (!number || !firstMessage) {
      return res.status(400).json({ error: "number and firstMessage are required" });
    }

    assertE164(number);
    const uid = userId || normalizeUserIdFromPhone(number);

    // Create / update session doc
    await sessions.updateOne(
      { userId: uid },
      {
        $setOnInsert: {
          userId: uid,
          createdAt: new Date(),
        },
        $set: { number, updatedAt: new Date() },
        $push: {
          history: {
            $each: [{ role: "assistant", content: String(firstMessage) }],
            $slice: -30,
          },
        },
      },
      { upsert: true }
    );

    // Send outbound SMS (NO webhookUrl here; webhooks are configured in dashboard)
    await notificationapi.send({
      type: "ai_sms_chat",
      to: { id: uid, number },
      sms: { message: String(firstMessage) },
    });

    return res.json({ ok: true, userId: uid, number });
  } catch (err) {
    console.error("outbound/start error:", err?.response?.data || err?.message || err);
    return res.status(500).json({ error: err?.message || "failed" });
  }
});

// ----------------- INBOUND WEBHOOK -----------------
app.post("/webhook/notificationapi/sms", async (req, res) => {
  const debug = req.query.debug === "1";

  try {
    // Don’t log tokens or full URLs
    console.log("📩 webhook HIT", {
      eventType: req.body?.eventType,
      userId: req.body?.userId,
      from: req.body?.from,
      hasText: Boolean(req.body?.text),
      lastTrackingId: req.body?.lastTrackingId,
    });

    // Basic auth via query param secret (simple)
    if (req.query.token !== NOTIF_INBOUND_WEBHOOK_SECRET) {
      return res.status(401).send("unauthorized");
    }

    const payload = req.body || {};

    // Ignore delivery receipts etc.
    if (payload.eventType !== "SMS_INBOUND") {
      return res.status(200).json({ ok: true, ignored: payload.eventType || true });
    }

    const { from, text, lastTrackingId } = payload;
    let { userId } = payload;

    if (!userId && from) userId = normalizeUserIdFromPhone(from);

    if (!from || typeof text !== "string" || !userId) {
      return res.status(200).json({ ok: true, ignored: "missing_fields" });
    }

    // Dedupe webhook retries using lastTrackingId
    if (lastTrackingId) {
      try {
        await webhookEvents.insertOne({ trackingId: lastTrackingId, createdAt: new Date() });
      } catch (e) {
        // Duplicate key means we already processed it
        if (String(e?.code) === "11000") {
          return res.status(200).json({ ok: true, deduped: true });
        }
        throw e;
      }
    }

    const upper = text.trim().toUpperCase();
    if (["STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"].includes(upper)) {
      return res.status(200).json({ ok: true, opted_out: true });
    }

    if (isEmojiOnly(text)) {
      return res.status(200).json({ ok: true, emoji_only: true });
    }

    // Load session history (for conversational context)
    const existing = await sessions.findOne({ userId });
    const history = existing?.history || [];

    // Save inbound message (cap history)
    await sessions.updateOne(
      { userId },
      {
        $setOnInsert: { userId, createdAt: new Date() },
        $set: { number: from, updatedAt: new Date() },
        $push: {
          history: {
            $each: [{ role: "user", content: text }],
            $slice: -30,
          },
        },
      },
      { upsert: true }
    );

    // Generate AI response
    let reply = "";
    try {
      reply = await openaiReply({ userText: text, history });
    } catch (aiErr) {
      console.error("🤖 OpenAI error:", aiErr?.response?.data || aiErr?.message || aiErr);
      reply = "I’m having trouble replying right now. Please try again in a minute.";
    }

    reply = String(reply || "").trim();
    if (!reply) {
      return res.status(200).json({ ok: true, empty_reply: true });
    }

    // Send reply (NO webhookUrl)
    try {
      await notificationapi.send({
        type: "ai_sms_chat",
        to: { id: userId, number: from },
        sms: { message: reply },
      });
    } catch (sendErr) {
      console.error("📤 NotificationAPI send error:", sendErr?.response?.data || sendErr?.message || sendErr);
      if (debug) {
        return res.status(200).json({
          ok: false,
          where: "notificationapi.send",
          error: sendErr?.message || "send failed",
          details: sendErr?.response?.data || null,
        });
      }
      return res.status(200).json({ ok: false });
    }

    // Save assistant message (cap history)
    await sessions.updateOne(
      { userId },
      {
        $set: { updatedAt: new Date() },
        $push: {
          history: {
            $each: [{ role: "assistant", content: reply }],
            $slice: -30,
          },
        },
      }
    );

    return res.status(200).json({ ok: true, replyPreview: debug ? reply : undefined });
  } catch (err) {
    console.error("🔥 webhook fatal:", err?.response?.data || err?.message || err);
    if (debug) {
      return res.status(200).json({
        ok: false,
        where: "webhook_fatal",
        error: err?.message || "unknown error",
        details: err?.response?.data || null,
      });
    }
    return res.status(200).json({ ok: false });
  }
});

// ----------------- Health -----------------
app.get("/health", (req, res) => res.json({ ok: true }));

// ----------------- Boot -----------------
initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`✅ Server listening on :${PORT}`));
  })
  .catch((e) => {
    console.error("DB init failed:", e);
    process.exit(1);
  });
