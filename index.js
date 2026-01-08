/* eslint-disable no-console */
require("dotenv").config();

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { MongoClient, ObjectId } = require("mongodb");
const notificationapi = require("notificationapi-node-server-sdk").default;

const app = express();

app.use(
  cors({
    origin: true,
    allowedHeaders: ["Content-Type", "x-admin-key"],
  })
);
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ----------------- Config -----------------
const PORT = Number(process.env.PORT || 3001);

const NOTIF_CLIENT_ID = process.env.NOTIF_CLIENT_ID;
const NOTIF_CLIENT_SECRET = process.env.NOTIF_CLIENT_SECRET;
const NOTIF_BASE_URL = process.env.NOTIF_BASE_URL;
const NOTIF_INBOUND_WEBHOOK_SECRET = process.env.NOTIF_INBOUND_WEBHOOK_SECRET;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || "llrchatbot";

const ADMIN_API_KEY = process.env.ADMIN_API_KEY || ""; // optional
const REPLY_DELAY_MS = Number(process.env.REPLY_DELAY_MS || 15000);
const FAQ_DIRECT_THRESHOLD = Number(process.env.FAQ_DIRECT_THRESHOLD || 2.5);

// Bulk outbound config
const OUTBOUND_SPACING_MS = Number(process.env.OUTBOUND_SPACING_MS || 5000); // 5 seconds default
const OUTBOUND_WORKER_POLL_MS = Number(process.env.OUTBOUND_WORKER_POLL_MS || 1000);

if (!NOTIF_CLIENT_ID || !NOTIF_CLIENT_SECRET) throw new Error("Missing NOTIF_CLIENT_ID / NOTIF_CLIENT_SECRET");
if (!NOTIF_INBOUND_WEBHOOK_SECRET) throw new Error("Missing NOTIF_INBOUND_WEBHOOK_SECRET");
if (!OPENAI_API_KEY) throw new Error("Missing OPENAI_API_KEY");
if (!MONGODB_URI) throw new Error("Missing MONGODB_URI");

// Init NotificationAPI
const initConfig = NOTIF_BASE_URL ? { baseURL: NOTIF_BASE_URL } : undefined;
notificationapi.init(NOTIF_CLIENT_ID, NOTIF_CLIENT_SECRET, initConfig);

// ----------------- Mongo -----------------
const mongo = new MongoClient(MONGODB_URI);

let sessions;
let webhookEvents;
let replyJobs;
let botSettings;
let escalationKeywords;
let escalationTargets;
let escalationEvents;
let faqCollection;
let dncKeywords;
let dncEvents;
// NEW:
let outboundSettings;
let outboundBatches;
let outboundQueue;

async function initDb() {
  await mongo.connect();
  const db = mongo.db(MONGODB_DB);

  sessions = db.collection("SMS_AI_Sessions");
  webhookEvents = db.collection("Webhook_Events");
  replyJobs = db.collection("SMS_AI_ReplyJobs");
  botSettings = db.collection("Bot_Settings");
  escalationKeywords = db.collection("Escalation_Keywords");
  escalationTargets = db.collection("Escalation_Targets");
  escalationEvents = db.collection("Escalation_Events");
  faqCollection = db.collection("FAQ_KB");
  dncKeywords = db.collection("DNC_Keywords");
  dncEvents = db.collection("DNC_Events");


  outboundSettings = db.collection("Outbound_Settings");
  outboundBatches = db.collection("Outbound_Batches");
  outboundQueue = db.collection("Outbound_Queue");

  // sessions
  await sessions.createIndex({ userId: 1 }, { unique: true });
  await sessions.createIndex({ number: 1 });
  await sessions.createIndex({ updatedAt: -1 });
  await botSettings.createIndex({ key: 1 }, { unique: true });
  // webhook dedupe
  await webhookEvents.createIndex({ trackingId: 1 }, { unique: true });
  await webhookEvents.createIndex({ createdAt: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });

  // reply jobs
  await replyJobs.createIndex({ trackingId: 1 }, { unique: true });
  await replyJobs.createIndex({ status: 1, runAt: 1 });

  // escalation
  await escalationKeywords.createIndex({ keyword: 1 }, { unique: true });
  await escalationTargets.createIndex({ number: 1 }, { unique: true });

  await escalationEvents.createIndex({ createdAt: -1 });
  await escalationEvents.createIndex({ from: 1, createdAt: -1 });
  await escalationEvents.createIndex({ matchedKeywords: 1 });


  // FAQ KB
  await faqCollection.createIndex({ enabled: 1 });
  await faqCollection.createIndex({ updatedAt: -1 });
  await faqCollection.createIndex(
    { question: "text", answer: "text", keywords: "text" },
    { default_language: "english" }
  );

  // Outbound settings: single doc per "key"
  await outboundSettings.createIndex({ key: 1 }, { unique: true });

  // Outbound batches/queue
  await outboundBatches.createIndex({ createdAt: -1 });
  await outboundQueue.createIndex({ batchId: 1, runAt: 1 });
  await outboundQueue.createIndex({ status: 1, runAt: 1 });
  await outboundQueue.createIndex({ trackingId: 1 }, { unique: true });

  await dncKeywords.createIndex({ keyword: 1 }, { unique: true });
  await dncKeywords.createIndex({ enabled: 1 });

  await dncEvents.createIndex({ userId: 1, createdAt: -1 });

  console.log("✅ Mongo connected");

  await botSettings.updateOne(
  { key: "overall_goal" },
  {
    $setOnInsert: {
      key: "overall_goal",
      goal: "Qualify buying intent and route hot leads to a human.",
      enabled: true,
      createdAt: new Date(),
    },
    $set: { updatedAt: new Date() },
  },
  { upsert: true }
);

  // Ensure default template exists
  await outboundSettings.updateOne(
    { key: "first_message_template" },
    {
      $setOnInsert: {
        key: "first_message_template",
        template:
          "Hey {{firstName}}, this is the LLRChatbot delayed version. Please reply if you get it in your inbox.",
        defaultCountryCode: "1",
        enabled: true,
        createdAt: new Date(),
      },
      $set: { updatedAt: new Date() },
    },
    { upsert: true }
  );
}

// ----------------- Helpers -----------------
function requireAdmin(req, res, next) {
  if (!ADMIN_API_KEY) return next(); // public if unset (your preference right now)
  const key = req.header("x-admin-key");
  if (key !== ADMIN_API_KEY) return res.status(401).json({ error: "unauthorized" });
  next();
}

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
    throw new Error(`Phone must be E.164 like +12025550123. Got: ${number}`);
  }
}

function normalizeToE164(input, defaultCountryCode = "1") {
  const raw = String(input || "").trim();
  if (!raw) return null;

  // if already +E164
  if (/^\+[1-9]\d{1,14}$/.test(raw)) return raw;

  const digits = raw.replace(/\D/g, "");

  // US: 10 digits -> +1
  if (digits.length === 10) return `+${defaultCountryCode}${digits}`;

  // 11 digits, US leading 1
  if (digits.length === 11 && digits.startsWith("1")) return `+${digits}`;

  // if someone pasted full country code digits (e.g. 8801...)
  if (digits.length >= 11 && digits.length <= 15) return `+${digits}`;

  return null;
}

function renderTemplate(template, vars) {
  let out = String(template || "");
  const firstName = String(vars.firstName || "").trim();
  const lastName = String(vars.lastName || "").trim();
  const fullName = `${firstName} ${lastName}`.trim();

  const map = {
    firstName,
    lastName,
    fullName,
  };

  // support {{var}} and {var}
  for (const k of Object.keys(map)) {
    out = out.replaceAll(`{{${k}}}`, map[k]);
    out = out.replaceAll(`{${k}}`, map[k]);
  }

  // cleanup double spaces
  out = out.replace(/\s{2,}/g, " ").trim();
  return out;
}

function takeLastMessages(history = [], max = 12) {
  if (!Array.isArray(history)) return [];
  return history.slice(Math.max(0, history.length - max));
}

function normalizeTextForMatching(text = "") {
  return text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ") // remove punctuation (commas, dots, etc)
    .replace(/\s+/g, " ")             // collapse spaces
    .trim();
}


async function openaiReply({ userText, history, faqContext = "" }) {
  const goalCfg = await getOverallGoal();
const overallGoalText = goalCfg.enabled && goalCfg.goal ? goalCfg.goal : "(none)";

const system = `
You are an AI SMS assistant. Be concise, helpful, and human.

OVERALL GOAL (follow this strongly):
${overallGoalText}

Use the FAQ knowledge base below as the source of truth when relevant.
If the answer is not in the FAQs, answer normally.

FAQ KB:
${faqContext || "(none)"}

Rules:
- Keep replies under ~320 characters unless absolutely necessary.
- No markdown, no bullet spam.
- If user asks multiple questions, answer in 1-2 tight sentences.
- If they ask for sensitive personal data, refuse.
`;


  function normalizeRole(role) {
  if (role === "agent") return "assistant";
  if (role === "customer") return "user";
  if (role === "assistant" || role === "user" || role === "system" || role === "developer") return role;
  return "assistant";
}


  const messages = [
    { role: "system", content: system.trim() },
    ...takeLastMessages(history, 12)
      .filter(m => m && m.content)
      .map(m => ({ role: normalizeRole(m.role), content: String(m.content) })),
    { role: "user", content: userText },
  ];


  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    { model: OPENAI_MODEL, messages, temperature: 0.4, max_tokens: 140 },
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` }, timeout: 60000 }
  );

  return resp.data?.choices?.[0]?.message?.content?.trim() || "";
}

// ---------- Escalation cache ----------
let escalationCache = { loadedAt: 0, keywords: [], targets: [] };
const ESCALATION_CACHE_MS = 10_000;

async function getEscalationConfig() {
  const now = Date.now();
  if (now - escalationCache.loadedAt < ESCALATION_CACHE_MS) return escalationCache;

  const [kwDocs, targetDocs] = await Promise.all([
    escalationKeywords.find({ enabled: true }).toArray(),
    escalationTargets.find({ enabled: true }).toArray(),
  ]);

  escalationCache = {
    loadedAt: now,
    keywords: kwDocs.map((d) => String(d.keyword || "").trim().toLowerCase()).filter(Boolean),
    targets: targetDocs.map((d) => String(d.number || "").trim()).filter(Boolean),
  };

  return escalationCache;
}

let goalCache = { loadedAt: 0, goal: "", enabled: true };
const GOAL_CACHE_MS = 10_000;

async function getOverallGoal() {
  const now = Date.now();
  if (now - goalCache.loadedAt < GOAL_CACHE_MS) return goalCache;

  const doc = await botSettings.findOne({ key: "overall_goal" });
  goalCache = {
    loadedAt: now,
    goal: String(doc?.goal || "").trim(),
    enabled: doc?.enabled !== false,
  };
  return goalCache;
}


// async function sendEscalationAlerts({ from, userId, text, matchedKeywords }) {
//   const cfg = await getEscalationConfig();
//   if (!matchedKeywords.length || !cfg.targets.length) return;

//   const msg =
//     `🚨 Keyword alert: ${matchedKeywords.join(", ")}\n` +
//     `From: ${from}\n` +
//     `UserId: ${userId}\n` +
//     `Message: ${String(text).slice(0, 500)}`;

//   await Promise.all(
//     cfg.targets.map((targetNumber) =>
//       notificationapi.send({
//         type: "ai_sms_chat",
//         to: { id: normalizeUserIdFromPhone(targetNumber), number: targetNumber },
//         sms: { message: msg },
//       })
//     )
//   );
// }

async function sendEscalationAlerts({ userId, from, text, matchedKeywords }) {
  const cfg = await getEscalationConfig();
  if (!matchedKeywords.length || !cfg.targets.length) return;

  const session = await sessions.findOne(
    { userId },
    { projection: { firstName: 1, lastName: 1, history: { $slice: -30 },  } }
  );

  const firstName = String(session?.firstName || "").trim();
  const lastName = String(session?.lastName || "").trim();
  const fullName = `${firstName} ${lastName}`.trim() || "Unknown";

  const history = session?.history || [];
  const lastOutbound = [...history].reverse().find((m) => m?.role === "assistant" || m?.role === "agent");
  const previousMessage = lastOutbound?.content
    ? String(lastOutbound.content).slice(0, 500)
    : "(no previous outbound message found)";

  const msg =
    `🚨 Keyword alert: ${matchedKeywords.join(", ")}\n` +
    `From: ${from}\n` +
    `Name: ${fullName}\n` +
    `Previous: ${previousMessage}\n` +
    `Reply: ${String(text).slice(0, 500)}`;

  await Promise.all(
    cfg.targets.map((targetNumber) =>
      notificationapi.send({
        type: "ai_sms_chat",
        to: { id: normalizeUserIdFromPhone(targetNumber), number: targetNumber },
        sms: { message: msg },
      })
    )
  );
}



// ---------- FAQ helpers ----------
async function fetchRelevantFaqs(queryText, limit = 5) {
  try {
    return await faqCollection
      .find(
        { $text: { $search: queryText }, enabled: true },
        { projection: { score: { $meta: "textScore" }, question: 1, answer: 1, keywords: 1 } }
      )
      .sort({ score: { $meta: "textScore" } })
      .limit(limit)
      .toArray();
  } catch (e) {
    console.error("FAQ text search error:", e?.message || e);
    return [];
  }
}

function buildFaqContext(faqHits) {
  if (!faqHits || !faqHits.length) return "";
  return faqHits.map((f, idx) => `FAQ ${idx + 1}\nQ: ${f.question}\nA: ${f.answer}`).join("\n\n");
}

// ---------- Outbound template ----------
async function getFirstMessageTemplate() {
  const doc = await outboundSettings.findOne({ key: "first_message_template" });
  return (
    doc || {
      key: "first_message_template",
      template: "Hey {{firstName}}, this is the LLRChatbot delayed version. Please reply if you get it in your inbox.",
      defaultCountryCode: "1",
      enabled: true,
    }
  );
}

// ----------------- OUTBOUND (single) -----------------
// If firstMessage omitted, it uses the template from Mongo.
// You can pass firstName/lastName for personalization.
app.post("/outbound/start", async (req, res) => {
  try {
    const { number, firstName, lastName, firstMessage } = req.body || {};
    if (!number) return res.status(400).json({ error: "number_required" });

    const settings = await getFirstMessageTemplate();
    const normalized = normalizeToE164(number, settings.defaultCountryCode || "1");
    if (!normalized) return res.status(400).json({ error: "invalid_phone_number" });
    assertE164(normalized);

    const uid = normalizeUserIdFromPhone(normalized);

    const msg = firstMessage
      ? String(firstMessage)
      : renderTemplate(settings.template, { firstName, lastName });

    await sessions.updateOne(
      { userId: uid },
      {
        $setOnInsert: { userId: uid, createdAt: new Date() },
        $set: {
          number: normalized,
          firstName: String(firstName || "").trim(),
          lastName: String(lastName || "").trim(),
          updatedAt: new Date()
        },
        $push: {
          history: { $each: [{ role: "assistant", content: msg, createdAt: new Date() }], $slice: -30 },
        },
      },
      { upsert: true }
    );

    await notificationapi.send({
      type: "ai_sms_chat",
      to: { id: uid, number: normalized },
      sms: { message: msg },
    });

    return res.json({ ok: true, userId: uid, number: normalized, message: msg });
  } catch (err) {
    console.error("outbound/start error:", err?.response?.data || err?.message || err);
    return res.status(500).json({ error: err?.message || "failed" });
  }
});

// ----------------- INBOUND WEBHOOK -----------------
app.post("/webhook/notificationapi/sms", async (req, res) => {
  try {
    // 0. Auth
    if (req.query.token !== NOTIF_INBOUND_WEBHOOK_SECRET) {
      return res.status(401).send("unauthorized");
    }

    const payload = req.body || {};
    if (payload.eventType !== "SMS_INBOUND") {
      return res.status(200).json({ ok: true, ignored: payload.eventType || true });
    }

    // 1. Parse payload FIRST
    const { from, firstName,lastName, text, lastTrackingId } = payload;
    if (!from || typeof text !== "string") {
      return res.status(200).json({ ok: true, ignored: "missing_fields" });
    }

    const userId =
      String(payload.userId || "").trim() ||
      normalizeUserIdFromPhone(from);

    const textLower = text.toLowerCase();
    const upper = text.trim().toUpperCase();

    // 2. Load existing session (for permanent opt-out)
    const existingSession = await sessions.findOne({ userId });

    // 🔒 HARD GUARD: already opted out → never reply again
    if (existingSession?.optedOut) {
      return res.status(200).json({ ok: true, opted_out: true });
    }

    // 3. Keyword-based DNC (admin-defined)
    const dnc = await dncKeywords.find({ enabled: true }).toArray();
    const matchedDnc = dnc.find(k => textLower.includes(k.keyword));

    if (matchedDnc) {
      await sessions.updateOne(
        { userId },
        {
          $set: {
            userId,
            number: from,
            firstName,
            lastName,
            optedOut: true,
            optedOutAt: new Date(),
            updatedAt: new Date()
          },
          $push: {
            history: { role: "user", content: text, createdAt: new Date() }
          }
        },
        { upsert: true }
      );

      await dncEvents.insertOne({
        userId,
        from,
        keyword: matchedDnc.keyword,
        text,
        createdAt: new Date()
      });

      // optional carrier-safe confirmation (still commented)
      // await notificationapi.send({ ... });

      return res.status(200).json({ ok: true, opted_out: true });
    }

    // 4. Carrier STOP words (persist them too)
    if (["STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"].includes(upper)) {
      await sessions.updateOne(
        { userId },
        {
          $set: {
            userId,
            number: from,
            firstName,
            lastName,
            optedOut: true,
            optedOutAt: new Date(),
            updatedAt: new Date()
          },
          $push: {
            history: { role: "user", content: text, createdAt: new Date() }
          }
        },
        { upsert: true }
      );

      return res.status(200).json({ ok: true, opted_out: true });
    }

    // 5. Emoji-only ignore
    if (isEmojiOnly(text)) {
      return res.status(200).json({ ok: true, emoji_only: true });
    }

    // 6. Dedupe retries
    if (lastTrackingId) {
      try {
        await webhookEvents.insertOne({
          trackingId: lastTrackingId,
          createdAt: new Date()
        });
      } catch (e) {
        if (String(e?.code) === "11000") {
          return res.status(200).json({ ok: true, deduped: true });
        }
        throw e;
      }
    }

    // 7. Save inbound message
    await sessions.updateOne(
      { userId },
      {
        $setOnInsert: { userId, createdAt: new Date() },
        $set: {
          number: from,
          firstName,
          lastName,
          updatedAt: new Date(),
          lastInboundAt: new Date()
        },
        $push: {
          history: {
            $each: [{ role: "user", content: text, createdAt: new Date() }],
            $slice: -30
          }
        }
      },
      { upsert: true }
    );

    // 8. Escalation logic (unchanged)
    const cfg = await getEscalationConfig();
    const normalizedText = normalizeTextForMatching(text);

    const matched = cfg.keywords.filter(k => {
      if (!k) return false;
      const keyword = normalizeTextForMatching(k);

      // match whole words only
      const regex = new RegExp(`\\b${keyword}\\b`, "i");
      return regex.test(normalizedText);
    });


    if (matched.length) {
      try {
        const session = await sessions.findOne(
          { userId },
          { projection: { firstName: 1, lastName: 1, history: { $slice: -30 } } }
        );

        const firstName = String(session?.firstName || "").trim();
        const lastName = String(session?.lastName || "").trim();

        const history = session?.history || [];
        const lastOutbound = [...history].reverse().find(
          m => m?.role === "assistant" || m?.role === "agent"
        );

        const previousAssistantMessage = lastOutbound?.content
          ? String(lastOutbound.content).slice(0, 500)
          : "";

        await escalationEvents.insertOne({
          createdAt: new Date(),
          from,
          firstName,
          lastName,
          matchedKeywords: matched,
          triggerText: String(text).slice(0, 800),
          previousAssistantMessage,
          alertedTargets: cfg.targets || []
        });

        await sendEscalationAlerts({
          userId,
          from,
          text,
          matchedKeywords: matched
        });
      } catch (error) {
        console.error("Escalation alert error:", error?.message || error);
      }

      // 🔕 IMPORTANT: no AI reply after escalation
      return res.status(200).json({
        ok: true,
        escalated: true,
        matchedKeywords: matched
      });
    }

    // 9. No escalation → enqueue AI reply
    const trackingId = lastTrackingId || `${userId}:${Date.now()}`;
    try {
      await replyJobs.insertOne({
        trackingId,
        userId,
        from,
        text,
        status: "queued",
        runAt: new Date(Date.now() + REPLY_DELAY_MS),
        createdAt: new Date()
      });
    } catch (e) {
      if (String(e?.code) !== "11000") throw e;
    }

    return res.status(200).json({
      ok: true,
      scheduled: true,
      delayMs: REPLY_DELAY_MS,
      userId
    });

  } catch (err) {
    console.error("🔥 webhook fatal:", err?.response?.data || err?.message || err);
    return res.status(200).json({ ok: false });
  }
});

// ----------------- Reply Worker -----------------
async function processOneDueJob() {
  const now = new Date();

  const claimed = await replyJobs.findOneAndUpdate(
    { status: "queued", runAt: { $lte: now } },
    { $set: { status: "processing", processingAt: now } },
    { sort: { runAt: 1 }, returnDocument: "after" }
  );

  const job = claimed?.value ?? claimed ?? null;
  if (!job || !job._id) return false;

  try {
    const { userId, from, text } = job;

    const existing = await sessions.findOne({ userId });
    const history = existing?.history || [];

    const faqHits = await fetchRelevantFaqs(text, 5);
    const topFaq = faqHits[0];

    let reply = "";

    if (topFaq && typeof topFaq.score === "number" && topFaq.score >= FAQ_DIRECT_THRESHOLD) {
      reply = String(topFaq.answer || "").trim();
    } else {
      const faqContext = buildFaqContext(faqHits);
      try {
        reply = await openaiReply({ userText: text, history, faqContext });
      } catch (aiErr) {
        console.error("🤖 OpenAI error:", aiErr?.response?.data || aiErr?.message || aiErr);
        reply = "I’m having trouble replying right now. Please try again in a minute.";
      }
    }

    reply = String(reply || "").trim();
    if (!reply) {
      await replyJobs.updateOne({ _id: job._id }, { $set: { status: "done", doneAt: new Date(), note: "empty_reply" } });
      return true;
    }

    await notificationapi.send({
      type: "ai_sms_chat",
      to: { id: userId, number: from },
      sms: { message: reply },
    });

    await sessions.updateOne(
      { userId },
      { $set: { updatedAt: new Date() }, $push: { history: { $each: [{ role: "assistant", content: reply, createdAt: new Date() }], $slice: -30 } } },
      { upsert: true }
    );

    await replyJobs.updateOne({ _id: job._id }, { $set: { status: "done", doneAt: new Date() } });
    return true;
  } catch (err) {
    console.error("🔥 Job processing error:", err?.response?.data || err?.message || err);
    await replyJobs.updateOne(
      { _id: job._id },
      { $set: { status: "failed", failedAt: new Date(), error: String(err?.message || err) } }
    );
    return true;
  }
}

function startReplyWorker() {
  setInterval(async () => {
    try {
      for (let i = 0; i < 5; i++) {
        const did = await processOneDueJob();
        if (!did) break;
      }
    } catch (e) {
      console.error("Reply worker tick error:", e?.message || e);
    }
  }, 2000);

  console.log("✅ Reply worker started", { delayMs: REPLY_DELAY_MS });
}

// ----------------- Bulk Outbound API + Worker -----------------

// GET /api/inbox/unread-count
// GET /api/inbox/unread-count
app.get("/api/inbox/unread-count", async (req, res) => {
  try {
    const count = await sessions.countDocuments({
      lastInboundAt: { $exists: true },
      $expr: {
        $or: [
          { $not: ["$lastAgentAt"] },
          { $gt: ["$lastInboundAt", "$lastAgentAt"] }
        ]
      }
    });

    res.json({ unreadCount: count });
  } catch (e) {
    console.error("unread-count error:", e);
    res.status(500).json({ unreadCount: 0 });
  }
});



// Get/set template
app.get("/api/outbound/template", requireAdmin, async (req, res) => {
  const doc = await getFirstMessageTemplate();
  res.json({ template: doc.template, defaultCountryCode: doc.defaultCountryCode || "1", enabled: !!doc.enabled });
});

app.post("/api/outbound/template", requireAdmin, async (req, res) => {
  const template = String(req.body?.template || "").trim();
  const defaultCountryCode = String(req.body?.defaultCountryCode || "1").trim();
  const enabled = req.body?.enabled;
  if (!template) return res.status(400).json({ error: "template_required" });

  await outboundSettings.updateOne(
    { key: "first_message_template" },
    {
      $set: {
        template,
        defaultCountryCode,
        enabled: typeof enabled === "boolean" ? enabled : true,
        updatedAt: new Date(),
      },
      $setOnInsert: { key: "first_message_template", createdAt: new Date() },
    },
    { upsert: true }
  );

  res.json({ ok: true });
});

// GET /api/inbox/stats
app.get("/api/inbox/stats", async (req, res) => {
  try {
    const agg = await sessions.aggregate([
      {
        $project: {
          outboundCount: {
            $size: {
              $filter: {
                input: "$history",
                as: "m",
                cond: { $eq: ["$$m.role", "assistant"] }
              }
            }
          },
          inboundCount: {
            $size: {
              $filter: {
                input: "$history",
                as: "m",
                cond: { $eq: ["$$m.role", "user"] }
              }
            }
          }
        }
      },
      {
        $group: {
          _id: null,
          outbound: { $sum: "$outboundCount" },
          inbound: { $sum: "$inboundCount" }
        }
      }
    ]).toArray();

    const row = agg[0] || { outbound: 0, inbound: 0 };

    res.json({
      outbound: row.outbound,
      inbound: row.inbound
    });
  } catch (e) {
    console.error("stats error:", e);
    res.json({ outbound: 0, inbound: 0 });
  }
});


// Create batch: frontend posts rows JSON parsed from CSV
// Body: { rows: [{ firstName, lastName, phone }], spacingMs?: 5000, defaultCountryCode?: "1" }
app.post("/api/outbound/batch", requireAdmin, async (req, res) => {
  try {
    const { rows, spacingMs, assignedTarget } = req.body;

    if (!rows.length) return res.status(400).json({ error: "rows_required" });
    if (rows.length > 5000) return res.status(400).json({ error: "too_many_rows" });

    const settings = await getFirstMessageTemplate();

    const defaultCountryCode = String(req.body?.defaultCountryCode || settings.defaultCountryCode || "1");

    const batchId = new ObjectId().toString();
    const now = Date.now();

    let accepted = 0;
    let rejected = 0;
    const rejects = [];

    // Create batch record
    await outboundBatches.insertOne({
      _id: batchId,
      status: "queued",
      total: rows.length,
      accepted: 0,
      rejected: 0,
      sent: 0,
      failed: 0,
      spacingMs,
      assignedTarget: assignedTarget || null,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    const queueDocs = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};
      const firstName = String(r.firstName || r.firstname || "").trim();
      const lastName = String(r.lastName || r.lastname || "").trim();
      const phoneRaw = String(r.phone || r.Phone || r.number || "").trim();

      const e164 = normalizeToE164(phoneRaw, defaultCountryCode);
      if (!e164) {
        rejected++;
        rejects.push({ index: i, phone: phoneRaw, reason: "invalid_phone" });
        continue;
      }

      const uid = normalizeUserIdFromPhone(e164);
      const message = renderTemplate(settings.template, { firstName, lastName });

      const runAt = new Date(now + accepted * spacingMs); // 5s between accepted sends
      const trackingId = `${batchId}:${uid}:${accepted}`;

      queueDocs.push({
        trackingId,
        batchId,
        userId: uid,
        number: e164,
        firstName,
        lastName,
        message,
        status: "queued",
        runAt,
        createdAt: new Date(),
      });

      accepted++;
    }

    if (queueDocs.length) {
      // insertMany with ordered:false so one duplicate won't kill everything
      await outboundQueue.insertMany(queueDocs, { ordered: false });
    }

    await outboundBatches.updateOne(
      { _id: batchId },
      {
        $set: { accepted, rejected, status: "queued", updatedAt: new Date() },
      }
    );

    res.json({
      ok: true,
      batchId,
      total: rows.length,
      accepted,
      rejected,
      rejects: rejects.slice(0, 50),
      spacingMs,
    });
  } catch (err) {
    console.error("outbound/batch error:", err?.message || err);
    res.status(500).json({ error: err?.message || "failed" });
  }
});

app.get("/api/escalation/events", async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 200), 500);

  const phone = String(req.query.phone || "").trim();
  const keyword = String(req.query.keyword || "").trim().toLowerCase();

  const q = {};
  if (phone) q.from = phone;
  if (keyword) q.matchedKeywords = keyword;

  const items = await escalationEvents
    .find(q)
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();

  res.json({ items });
});

// Mark conversation as read when opened
app.post("/api/conversations/:userId/read", async (req, res) => {
  try {
    await sessions.updateOne(
      { userId: req.params.userId },
      {
        $set: {
          lastViewedAt: new Date(),
          updatedAt: new Date()
        }
      }
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("mark-read error:", e);
    res.status(500).json({ ok: false });
  }
});




// Batch progress
app.get("/api/outbound/batch/:batchId", requireAdmin, async (req, res) => {
  const batchId = req.params.batchId;
  const doc = await outboundBatches.findOne({ _id: batchId });
  if (!doc) return res.status(404).json({ error: "not_found" });
  res.json(doc);
});

// Worker sends one queued message at a time (5s spacing already encoded in runAt)
async function processOneOutboundJob() {
  const now = new Date();

  const claimed = await outboundQueue.findOneAndUpdate(
    { status: "queued", runAt: { $lte: now } },
    { $set: { status: "processing", processingAt: now } },
    { sort: { runAt: 1 }, returnDocument: "after" }
  );

  const job = claimed?.value ?? claimed ?? null;
  if (!job || !job._id) return false;

  const { batchId, userId, number, message } = job;

  try {
    // Create session + save first message
    const batch = await outboundBatches.findOne(
      { _id: batchId },
      { projection: { assignedTarget: 1 } }
    );

    await sessions.updateOne(
      { userId },
      {
        $setOnInsert: { userId, createdAt: new Date() },
        $set: {
        number,
        firstName: String(job.firstName || "").trim(),
        lastName: String(job.lastName || "").trim(),
        assignedTarget: batch?.assignedTarget || null,
        updatedAt: new Date()
      },
        $push: { history: { $each: [{ role: "assistant", content: message, createdAt: new Date() }], $slice: -30 } },
      },
      { upsert: true }
    );

    await notificationapi.send({
      type: "ai_sms_chat",
      to: { id: userId, number },
      sms: { message },
    });

    await outboundQueue.updateOne({ _id: job._id }, { $set: { status: "sent", sentAt: new Date() } });

    await outboundBatches.updateOne(
      { _id: batchId },
      { $inc: { sent: 1 }, $set: { updatedAt: new Date() } }
    );

    // If all done, close batch (best-effort)
    const b = await outboundBatches.findOne({ _id: batchId });
    if (b && b.sent + b.failed >= b.accepted) {
      await outboundBatches.updateOne({ _id: batchId }, { $set: { status: "done", updatedAt: new Date() } });
    }

    return true;
  } catch (err) {
    console.error("Outbound job error:", err?.response?.data || err?.message || err);

    await outboundQueue.updateOne(
      { _id: job._id },
      { $set: { status: "failed", failedAt: new Date(), error: String(err?.message || err) } }
    );

    await outboundBatches.updateOne(
      { _id: batchId },
      { $inc: { failed: 1 }, $set: { updatedAt: new Date() } }
    );

    const b = await outboundBatches.findOne({ _id: batchId });
    if (b && b.sent + b.failed >= b.accepted) {
      await outboundBatches.updateOne({ _id: batchId }, { $set: { status: "done", updatedAt: new Date() } });
    }

    return true;
  }
}

function startOutboundWorker() {
  setInterval(async () => {
    try {
      // send max 1 per tick (runAt already enforces 5s spacing), but we keep it safe
      await processOneOutboundJob();
    } catch (e) {
      console.error("Outbound worker tick error:", e?.message || e);
    }
  }, OUTBOUND_WORKER_POLL_MS);

  console.log("✅ Outbound worker started", { spacingMs: OUTBOUND_SPACING_MS, pollMs: OUTBOUND_WORKER_POLL_MS });
}

// ----------------- Existing APIs (conversations, escalation, faq) -----------------
app.get("/api/conversations", async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);

  const items = await sessions
    .find({}, {
      projection: {
      userId: 1,
      number: 1,
      firstName: 1,
      lastName: 1,
      updatedAt: 1,
      lastInboundAt: 1,
      lastAgentAt: 1,
      history: { $slice: -1 }
    }
    }).sort({ updatedAt: -1 })
    .limit(limit)
    .toArray();

  res.json({
    items: items.map((d) => {
 const lastSeen = d.lastViewedAt || d.lastAgentAt;

const unread =
  d.lastInboundAt &&
  (!lastSeen || new Date(d.lastInboundAt) > new Date(lastSeen));


  return {
  userId: d.userId,
  number: d.number,
  firstName: d.firstName || "",
  lastName: d.lastName || "",
  updatedAt: d.updatedAt,
  lastMessage: d.history?.[0]?.content || "",
  lastRole: d.history?.[0]?.role || "",
  unread
};
  }),
  });
});

app.get("/api/conversations/:userId", async (req, res) => {
  const doc = await sessions.findOne({ userId: req.params.userId });
  if (!doc) return res.status(404).json({ error: "not_found" });

  res.json({
    userId: doc.userId,
    number: doc.number,
    firstName: doc.firstName || "",
    lastName: doc.lastName || "",
    updatedAt: doc.updatedAt,
    history: doc.history || [],
  });
});

app.post("/api/conversations/:userId/send", async (req, res) => {
  const { message } = req.body || {};
  if (!message) return res.status(400).json({ error: "message_required" });

  const doc = await sessions.findOne({ userId: req.params.userId });
  if (!doc?.number) return res.status(404).json({ error: "conversation_not_found" });

  await notificationapi.send({
    type: "ai_sms_chat",
    to: { id: doc.userId, number: doc.number },
    sms: { message: String(message) },
  });

  await sessions.updateOne(
    { userId: doc.userId },
    {
      $set: {
      updatedAt: new Date(),
      lastAgentAt: new Date()
    },

      $push: { history: { $each: [{ role: "agent", content: String(message) , createdAt: new Date()}], $slice: -50 } },
    }
  );

  res.json({ ok: true });
});

// Escalation endpoints (kept)
app.get("/api/escalation/keywords", requireAdmin, async (req, res) => {
  const items = await escalationKeywords.find({}).sort({ keyword: 1 }).toArray();
  res.json({ items });
});

app.post("/api/escalation/keywords", requireAdmin, async (req, res) => {
  const keyword = String(req.body?.keyword || "").trim().toLowerCase();
  if (!keyword) return res.status(400).json({ error: "keyword_required" });

  await escalationKeywords.updateOne(
    { keyword },
    { $setOnInsert: { keyword, enabled: true, createdAt: new Date() }, $set: { updatedAt: new Date() } },
    { upsert: true }
  );

  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

app.patch("/api/escalation/keywords/:keyword", requireAdmin, async (req, res) => {
  const keyword = String(req.params.keyword || "").trim().toLowerCase();
  const enabled = Boolean(req.body?.enabled);

  await escalationKeywords.updateOne({ keyword }, { $set: { enabled, updatedAt: new Date() } });
  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

app.delete("/api/escalation/keywords/:keyword", requireAdmin, async (req, res) => {
  const keyword = String(req.params.keyword || "").trim().toLowerCase();
  await escalationKeywords.deleteOne({ keyword });
  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

app.get("/api/escalation/targets", requireAdmin, async (req, res) => {
  const items = await escalationTargets.find({}).sort({ createdAt: -1 }).toArray();
  res.json({ items });
});

app.post("/api/escalation/targets", requireAdmin, async (req, res) => {
  const name = String(req.body?.name || "").trim() || "Target";
  const number = String(req.body?.number || "").trim();
  if (!number) return res.status(400).json({ error: "number_required" });
  assertE164(number);

  await escalationTargets.updateOne(
    { number },
    { $setOnInsert: { number, enabled: true, createdAt: new Date() }, $set: { name, updatedAt: new Date() } },
    { upsert: true }
  );

  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

app.patch("/api/escalation/targets/:number", requireAdmin, async (req, res) => {
  const number = decodeURIComponent(req.params.number);
  const enabled = Boolean(req.body?.enabled);

  await escalationTargets.updateOne({ number }, { $set: { enabled, updatedAt: new Date() } });
  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

app.delete("/api/escalation/targets/:number", requireAdmin, async (req, res) => {
  const number = decodeURIComponent(req.params.number);
  await escalationTargets.deleteOne({ number });
  escalationCache.loadedAt = 0;
  res.json({ ok: true });
});

// FAQ endpoints (kept)
app.get("/api/faq", requireAdmin, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 200), 500);
  const items = await faqCollection.find({}).sort({ updatedAt: -1 }).limit(limit).toArray();
  res.json({ items });
});

app.post("/api/faq", requireAdmin, async (req, res) => {
  const question = String(req.body?.question || "").trim();
  const answer = String(req.body?.answer || "").trim();
  const keywords = Array.isArray(req.body?.keywords) ? req.body.keywords : [];

  if (!question || !answer) return res.status(400).json({ error: "question_and_answer_required" });

  await faqCollection.insertOne({
    question,
    answer,
    keywords: keywords.map((k) => String(k).trim()).filter(Boolean),
    enabled: true,
    createdAt: new Date(),
    updatedAt: new Date(),
  });

  res.json({ ok: true });
});

app.get("/api/dnc/keywords", requireAdmin, async (req, res) => {
  const items = await dncKeywords.find({}).sort({ keyword: 1 }).toArray();
  res.json({ items });
});

app.post("/api/dnc/keywords", requireAdmin, async (req, res) => {
  const keyword = String(req.body?.keyword || "").trim().toLowerCase();
  if (!keyword) return res.status(400).json({ error: "keyword_required" });

  await dncKeywords.updateOne(
    { keyword },
    { $setOnInsert: { keyword, enabled: true, createdAt: new Date() } },
    { upsert: true }
  );

  res.json({ ok: true });
});

app.get("/api/targets", async (req, res) => {
  try {
    const items = await escalationTargets
      .find({ enabled: true })
      .sort({ createdAt: -1 })
      .project({ _id: 0, name: 1, number: 1 })
      .toArray();

    res.json({ items });
  } catch (err) {
    console.error("targets fetch error:", err);
    res.status(500).json({ items: [] });
  }
});



app.patch("/api/dnc/keywords/:keyword", requireAdmin, async (req, res) => {
  const keyword = decodeURIComponent(req.params.keyword);
  const enabled = Boolean(req.body?.enabled);

  await dncKeywords.updateOne(
    { keyword },
    { $set: { enabled, updatedAt: new Date() } }
  );

  res.json({ ok: true });
});


app.delete("/api/dnc/keywords/:keyword", requireAdmin, async (req, res) => {
  const keyword = decodeURIComponent(req.params.keyword);
  await dncKeywords.deleteOne({ keyword });
  res.json({ ok: true });
});


app.get("/api/bot/goal", requireAdmin, async (req, res) => {
  const doc = await botSettings.findOne({ key: "overall_goal" });
  res.json({
    goal: doc?.goal || "",
    enabled: Boolean(doc?.enabled),
  });
});

app.post("/api/bot/goal", requireAdmin, async (req, res) => {
  const goal = String(req.body?.goal || "").trim();
  const enabled = typeof req.body?.enabled === "boolean" ? req.body.enabled : true;

  if (!goal) return res.status(400).json({ error: "goal_required" });

  await botSettings.updateOne(
    { key: "overall_goal" },
    {
      $set: { goal, enabled, updatedAt: new Date() },
      $setOnInsert: { key: "overall_goal", createdAt: new Date() },
    },
    { upsert: true }
  );

  // invalidate cache (we'll add it below)
  goalCache.loadedAt = 0;

  res.json({ ok: true });
});


app.patch("/api/faq/:id", requireAdmin, async (req, res) => {
  const id = req.params.id;
  if (!ObjectId.isValid(id)) return res.status(400).json({ error: "invalid_id" });

  const update = { updatedAt: new Date() };
  if (typeof req.body?.question === "string") update.question = req.body.question.trim();
  if (typeof req.body?.answer === "string") update.answer = req.body.answer.trim();
  if (typeof req.body?.enabled === "boolean") update.enabled = req.body.enabled;
  if (Array.isArray(req.body?.keywords)) update.keywords = req.body.keywords.map((k) => String(k).trim()).filter(Boolean);

  await faqCollection.updateOne({ _id: new ObjectId(id) }, { $set: update });
  res.json({ ok: true });
});

app.delete("/api/faq/:id", requireAdmin, async (req, res) => {
  const id = req.params.id;
  if (!ObjectId.isValid(id)) return res.status(400).json({ error: "invalid_id" });

  await faqCollection.deleteOne({ _id: new ObjectId(id) });
  res.json({ ok: true });
});

// Health
app.get("/health", (req, res) => res.json({ ok: true }));

// Boot
initDb()
  .then(() => {
    startReplyWorker();
    startOutboundWorker();
    app.listen(PORT, () => console.log(`✅ Server listening on :${PORT}`));
  })
  .catch((e) => {
    console.error("DB init failed:", e);
    process.exit(1);
  });
