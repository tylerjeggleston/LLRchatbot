/* eslint-disable no-console */
require("dotenv").config();

const express = require("express");
const cors = require("cors");
const axios = require("axios");
const { MongoClient, ObjectId } = require("mongodb");
const notificationapi = require("notificationapi-node-server-sdk").default;

const nodemailer = require("nodemailer");
const multer = require("multer");
const upload = multer();
const crypto = require("crypto");

const CONFIG_SECRET = process.env.CONFIG_SECRET || "";

function getConfigKey() {
  return crypto.createHash("sha256").update(String(CONFIG_SECRET)).digest();
}

function encryptSecret(plain) {
  const text = String(plain || "");
  if (!text) return "";
  if (!CONFIG_SECRET) throw new Error("Missing CONFIG_SECRET");

  const iv = crypto.randomBytes(12);
  const key = getConfigKey();
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);

  const enc = Buffer.concat([cipher.update(text, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();

  return [
    iv.toString("base64"),
    tag.toString("base64"),
    enc.toString("base64"),
  ].join(".");
}

function decryptSecret(payload) {
  const raw = String(payload || "");
  if (!raw) return "";
  if (!CONFIG_SECRET) throw new Error("Missing CONFIG_SECRET");

  const [ivB64, tagB64, encB64] = raw.split(".");
  if (!ivB64 || !tagB64 || !encB64) throw new Error("invalid_encrypted_secret");

  const iv = Buffer.from(ivB64, "base64");
  const tag = Buffer.from(tagB64, "base64");
  const enc = Buffer.from(encB64, "base64");

  const key = getConfigKey();
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, iv);
  decipher.setAuthTag(tag);

  const dec = Buffer.concat([decipher.update(enc), decipher.final()]);
  return dec.toString("utf8");
}

function maskSecret(v) {
  const s = String(v || "");
  if (!s) return "";
  if (s.length <= 8) return "********";
  return `${s.slice(0, 4)}********${s.slice(-4)}`;
}

const app = express();

app.use(
  cors({
    origin: true,
    allowedHeaders: ["Content-Type", "x-admin-key"],
  })
);
app.use(express.urlencoded({ extended: false, limit: "25mb" }));
app.use(express.json({ limit: "25mb" }));

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

const MAILGUN_API_KEY = process.env.MAILGUN_API_KEY;
const MAILGUN_DOMAIN = process.env.MAILGUN_DOMAIN;
const MAILGUN_FROM = process.env.MAILGUN_FROM;
const MAILGUN_BASE_URL = process.env.MAILGUN_BASE_URL || "https://api.mailgun.net";





// async function sendEmailViaMailgun({
//   from,
//   to,
//   subject,
//   text,
//   html,
//   headers,
//   variables,
// }) {
//   if (!MAILGUN_API_KEY || !MAILGUN_DOMAIN) {
//     throw new Error("mailgun_not_configured");
//   }

//   const url = `${MAILGUN_BASE_URL}/v3/${MAILGUN_DOMAIN}/messages`;
//   const params = new URLSearchParams();

//   const fromFinal = String(from || MAILGUN_FROM || "").trim();
//   if (!fromFinal) throw new Error("mailgun_from_missing");

//   params.append("from", fromFinal);
//   params.append("to", to);
//   params.append("subject", subject || "");
//   if (text) params.append("text", text);
//   if (html) params.append("html", html);

//   // turn tracking on per-message
//   params.append("o:tracking", "yes");
//   params.append("o:tracking-opens", "yes");

//   if (headers && typeof headers === "object") {
//     for (const [k, v] of Object.entries(headers)) {
//       if (v) params.append(`h:${k}`, String(v));
//     }
//   }

//   if (variables && typeof variables === "object") {
//     for (const [k, v] of Object.entries(variables)) {
//       if (v !== undefined && v !== null) {
//         params.append(`v:${k}`, String(v));
//       }
//     }
//   }

//   const resp = await axios.post(url, params, {
//     auth: { username: "api", password: MAILGUN_API_KEY },
//     timeout: 60000,
//     headers: { "Content-Type": "application/x-www-form-urlencoded" },
//   });

//   return resp.data;
// }

async function sendEmailViaMailgun({
  domainConfig,
  from,
  to,
  subject,
  text,
  html,
  headers,
  variables,
}) {
  if (!domainConfig) throw new Error("mailgun_domain_config_missing");

  const mailgunDomain = String(domainConfig.domain || "").trim();
  const mailgunBaseUrl = String(domainConfig.mailgunBaseUrl || "https://api.mailgun.net").trim();
  const mailgunApiKey = process.env.MAILGUN_API_KEY || "";

  if (!mailgunDomain || !mailgunApiKey) {
    throw new Error("mailgun_domain_not_configured");
  }

  const url = `${mailgunBaseUrl}/v3/${mailgunDomain}/messages`;
  const params = new URLSearchParams();

  const fromFinal = String(from || "").trim();
  if (!fromFinal) throw new Error("mailgun_from_missing");

  params.append("from", fromFinal);
  params.append("to", to);
  params.append("subject", subject || "");
  if (text) params.append("text", text);
  if (html) params.append("html", html);

  params.append("o:tracking", "yes");
  params.append("o:tracking-opens", "yes");

  if (headers && typeof headers === "object") {
    for (const [k, v] of Object.entries(headers)) {
      if (v) params.append(`h:${k}`, String(v));
    }
  }

  if (variables && typeof variables === "object") {
    for (const [k, v] of Object.entries(variables)) {
      if (v !== undefined && v !== null) {
        params.append(`v:${k}`, String(v));
      }
    }
  }

  const resp = await axios.post(url, params, {
    auth: { username: "api", password: mailgunApiKey },
    timeout: 60000,
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });

  return resp.data;
}

if (!NOTIF_CLIENT_ID || !NOTIF_CLIENT_SECRET) throw new Error("Missing NOTIF_CLIENT_ID / NOTIF_CLIENT_SECRET");
if (!NOTIF_INBOUND_WEBHOOK_SECRET) throw new Error("Missing NOTIF_INBOUND_WEBHOOK_SECRET");
if (!process.env.EMAIL_INBOUND_WEBHOOK_SECRET) throw new Error("Missing EMAIL_INBOUND_WEBHOOK_SECRET");

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

// Email blast:
let emailSettings;
let emailBatches;
let emailQueue;

let emailSessions;
let emailWebhookEvents;
let emailReplyJobs;

let emailSenders;
let emailValidations;
let emailEvents;
let emailUnsubscribes;
let emailDomains;

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

  emailSettings = db.collection("Email_Settings");
  emailBatches = db.collection("Email_Batches");
  emailQueue = db.collection("Email_Queue");

  emailSessions = db.collection("Email_Sessions");
  emailWebhookEvents = db.collection("Email_Webhook_Events");
  emailReplyJobs = db.collection("Email_ReplyJobs");

  emailSenders = db.collection("Email_Senders");
  emailValidations = db.collection("Email_Validations");
  emailEvents = db.collection("Email_Events");
  emailUnsubscribes = db.collection("Email_Unsubscribes");

  emailDomains = db.collection("Email_Domains");

  await emailDomains.createIndex({ domain: 1 }, { unique: true });
  await emailDomains.createIndex({ enabled: 1 });
  await emailDomains.createIndex({ isDefault: 1 });

  await emailSenders.createIndex({ email: 1 }, { unique: true });
  await emailSenders.createIndex({ enabled: 1 });

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
  await sessions.createIndex({ hidden: 1, updatedAt: -1, userId: 1 });

  await emailSettings.createIndex({ key: 1 }, { unique: true });
  await emailBatches.createIndex({ createdAt: -1 });
  await emailQueue.createIndex({ batchId: 1, runAt: 1 });
  await emailQueue.createIndex({ status: 1, runAt: 1 });
  await emailQueue.createIndex({ trackingId: 1 }, { unique: true });

  await emailSessions.createIndex({ userId: 1 }, { unique: true });
  await emailSessions.createIndex({ email: 1 });
  await emailSessions.createIndex({ updatedAt: -1 });

  await emailWebhookEvents.createIndex({ trackingId: 1 }, { unique: true });
  await emailWebhookEvents.createIndex({ createdAt: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });

  await emailReplyJobs.createIndex({ trackingId: 1 }, { unique: true });
  await emailReplyJobs.createIndex({ status: 1, runAt: 1 });

  await emailValidations.createIndex({ address: 1 }, { unique: true });
  await emailQueue.createIndex({ status: 1, sentAt: -1 });
  await emailQueue.createIndex({ status: 1, failedAt: -1 });


  await emailEvents.createIndex({ event: 1, createdAt: -1 });
  await emailEvents.createIndex({ recipient: 1, createdAt: -1 });
  await emailEvents.createIndex({ queueId: 1, createdAt: -1 });
  await emailEvents.createIndex({ messageId: 1 });

  await emailUnsubscribes.createIndex({ email: 1 }, { unique: true });
  await emailUnsubscribes.createIndex({ createdAt: -1 });

  // 2) TTL: auto-expire cache after 30 days
  await emailValidations.createIndex(
    { validatedAt: 1 },
    { expireAfterSeconds: 30 * 24 * 3600 }
  );


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

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function getDefaultEmailDomain() {
  let doc = await emailDomains.findOne({ enabled: true, isDefault: true });
  if (doc) return doc;

  doc = await emailDomains.findOne({ enabled: true }, { sort: { createdAt: 1 } });
  return doc || null;
}

async function getEmailDomainById(id) {
  if (!ObjectId.isValid(id)) return null;
  return emailDomains.findOne({ _id: new ObjectId(id), enabled: true });
}

function buildFromValue(fromName, fromEmail) {
  const name = String(fromName || "").trim();
  const email = String(fromEmail || "").trim();
  if (!email) return "";
  return name ? `${name} <${email}>` : email;
}

function escapeHtmlToParagraphs(text) {
  const lines = String(text || "").split(/\r?\n/);
  let html = "";
  let buf = [];

  for (const line of lines) {
    if (line.trim() === "") {
      if (buf.length) {
        html += `<p style="margin:0 0 12px 0;">${escapeHtml(buf.join(" "))}</p>`;
        buf = [];
      }
      continue;
    }
    buf.push(line.trim());
  }

  if (buf.length) html += `<p style="margin:0 0 12px 0;">${escapeHtml(buf.join(" "))}</p>`;
  return html || "";
}


async function verifySmtp() {
  if (!smtpTransport) {
    console.log("❌ SMTP not configured");
    return;
  }
  try {
    await smtpTransport.verify();
    console.log("✅ SMTP verified");
  } catch (e) {
    console.log("❌ SMTP verify failed:", e?.message || e);
  }
}


async function openaiEmailReply({ userText, history, faqContext = "" }) {
  const goalCfg = await getOverallGoal();
  const overallGoalText = goalCfg.enabled && goalCfg.goal ? goalCfg.goal : "(none)";

  const system = `
You are an AI email assistant for a business. Be clear, concise, and helpful.

OVERALL GOAL:
${overallGoalText}

FAQ KB (source of truth when relevant):
${faqContext || "(none)"}

Rules:
- Keep it under 120 words unless truly necessary.
- No markdown.
- Be direct and professional (not stiff).
- Ask at most 1 question back.
- If unsure, say what you need.
- Don't mention internal systems, webhooks, APIs.
- Always respond in the same language as the user.
`.trim();

  const messages = [
    { role: "system", content: system },
    ...(Array.isArray(history) ? history.slice(-10).map(m => ({
      role: m.role === "agent" ? "assistant" : m.role === "customer" ? "user" : (m.role || "user"),
      content: String(m.content || "")
    })) : []),
    { role: "user", content: userText },
  ];

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    { model: OPENAI_MODEL, messages, temperature: 0.3, max_tokens: 220 },
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` }, timeout: 60000 }
  );

  return resp.data?.choices?.[0]?.message?.content?.trim() || "";
}

async function processOneEmailReplyJob() {
  const now = new Date();

  const claimed = await emailReplyJobs.findOneAndUpdate(
    { status: "queued", runAt: { $lte: now } },
    { $set: { status: "processing", processingAt: now } },
    { sort: { runAt: 1 }, returnDocument: "after" }
  );

  const job = claimed?.value ?? claimed ?? null;
  if (!job?._id) return false;

  try {
    const sess = await emailSessions.findOne({ userId: job.userId });
    const history = sess?.history || [];

    const faqHits = await fetchRelevantFaqs(job.text, 5);
    const topFaq = faqHits[0];

    let replyText = "";
    if (topFaq && typeof topFaq.score === "number" && topFaq.score >= FAQ_DIRECT_THRESHOLD) {
      replyText = String(topFaq.answer || "").trim();
    } else {
      const faqContext = buildFaqContext(faqHits);
      replyText = await openaiEmailReply({ userText: job.text, history, faqContext });
    }

    replyText = String(replyText || "").trim();
    if (!replyText) {
      await emailReplyJobs.updateOne({ _id: job._id }, { $set: { status: "done", doneAt: new Date(), note: "empty_reply" } });
      return true;
    }

    // Add signature (keep stable)
    // ✅ 1) load template (fallback sigs)
const tpl = await getEmailBlastTemplate();

// ✅ 2) choose sender that received the inbound
let sender = null;

// Prefer inboundToEmail (most reliable)
if (job.inboundToEmail) {
  sender = await emailSenders.findOne({ email: normalizeEmail(job.inboundToEmail) });
}

// If not found, fallback: try to decode from message headers you injected
// (Sometimes providers include your custom header in inbound payload; not always)
if (!sender && job.senderId && ObjectId.isValid(job.senderId)) {
  sender = await emailSenders.findOne({ _id: new ObjectId(job.senderId) });
}

// If still not found, final fallback: first enabled sender (don’t block replies)
if (!sender) {
  sender = await emailSenders.findOne({ enabled: true }, { sort: { createdAt: 1 } });
}

const fromEmail = String(sender?.email || "").trim();
const fromName = String(sender?.name || "").trim();
const from = fromName ? `${fromName} <${fromEmail}>` : fromEmail;

const replyTo = String(sender?.replyTo || "").trim();

// ✅ 3) signature: sender override, else template fallback
const sigTextFinal = String(sender?.signatureText || sender?.signature || tpl.signatureText || "").trim();
const sigHtmlFinal = String(sender?.signatureHtml || tpl.signatureHtml || "").trim();


// ✅ 4) final bodies
const finalText =
  [replyText, sigTextFinal].filter(Boolean).join("\n\n").trim();

const sigHtmlFallback = sigHtmlFinal || textSigToHtml(sigTextFinal);

const finalHtml =
  `<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.45;color:#111;">` +
  `${escapeHtmlToParagraphs(replyText)}` +
  `${sigHtmlFallback ? `<div style="margin-top:16px;">${sigHtmlFallback}</div>` : ""}` +
  `</div>`;


    // ✅ 5) send via Mailgun using SAME sender
    await sendEmailViaMailgun({
    from,
    to: job.toEmail,
    subject: job.subject?.toLowerCase().startsWith("re:") ? job.subject : `Re: ${job.subject || "Quick question"}`,
    text: finalText,
    html: finalHtml,
    headers: {
        ...(replyTo ? { "Reply-To": replyTo } : {}),
        ...(job.messageId ? { "In-Reply-To": job.messageId, "References": job.messageId } : {}),
        "X-LLR-Sender-Id": String(sender?._id || ""),
        "X-LLR-Sender-Email": fromEmail,
    },
    });

    // ✅ Save outbound in session (store the actual body you sent)
    await emailSessions.updateOne(
  { userId: job.userId },
  {
    $set: { updatedAt: new Date() },
    $push: {
      history: {
        $each: [{ role: "assistant", content: finalText, createdAt: new Date() }],
        $slice: -50
      }
    }
  }
);


    await emailReplyJobs.updateOne({ _id: job._id }, { $set: { status: "done", doneAt: new Date() } });
    return true;

  } catch (e) {
    console.error("email reply job error:", e?.message || e);
    await emailReplyJobs.updateOne(
      { _id: job._id },
      { $set: { status: "failed", failedAt: new Date(), error: String(e?.message || e) } }
    );
    return true;
  }
}

async function validateEmailViaMailgun(address) {
  if (!MAILGUN_API_KEY) throw new Error("mailgun_not_configured");

  const url = `${MAILGUN_BASE_URL}/v4/address/validate`;

  const resp = await axios.get(url, {
    auth: { username: "api", password: MAILGUN_API_KEY }, // basicAuth :contentReference[oaicite:2]{index=2}
    params: { address, provider_lookup: true },            // query params :contentReference[oaicite:3]{index=3}
    timeout: 30_000,
  });

  return resp.data;
}

function parseCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }

  out.push(cur);
  return out.map((v) => String(v || "").trim());
}

function parseCsvText(text) {
  const raw = String(text || "").replace(/^\uFEFF/, "");
  const lines = raw.split(/\r?\n/).filter((x) => x.trim() !== "");
  if (!lines.length) return [];

  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const vals = parseCsvLine(lines[i]);
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = vals[idx] ?? "";
    });
    rows.push(row);
  }

  return rows;
}

function parseBooleanLoose(v, fallback = true) {
  if (typeof v === "boolean") return v;
  const s = String(v || "").trim().toLowerCase();
  if (!s) return fallback;
  if (["true", "1", "yes", "y", "enabled"].includes(s)) return true;
  if (["false", "0", "no", "n", "disabled"].includes(s)) return false;
  return fallback;
}

function pickValidationDecision(v) {
  const result = String(v?.result || "").toLowerCase();
  const risk = String(v?.risk || "").toLowerCase();

  // hard blocks for cold outbound
  if (v?.is_disposable_address) return { ok: false, action: "reject_disposable" };
  if (v?.is_role_address) return { ok: false, action: "reject_role" };

  // reputation killers / guaranteed pain
  if (result === "undeliverable" || result === "do_not_send") return { ok: false, action: "reject" };

  // don't send when risk is high even if "deliverable"
  if (risk === "high") return { ok: false, action: "skip_high_risk" };

  // safe-ish
  if (result === "deliverable" && (risk === "low" || risk === "medium")) return { ok: true, action: "send" };

  // catch_all / unknown -> skip by default
  return { ok: false, action: "skip_risky" };
}

function getZonedParts(date, tz) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const get = (t) => Number(parts.find((p) => p.type === t)?.value || 0);

  return {
    year: get("year"),
    month: get("month"),
    day: get("day"),
    hour: get("hour"),
    minute: get("minute"),
    second: get("second"),
  };
}

// Convert a "local time in tz" -> actual UTC Date (iterative correction)
function zonedTimeToUtc({ year, month, day, hour, minute, second }, tz) {
  // initial guess: treat the local time as if it were UTC
  let utcMs = Date.UTC(year, month - 1, day, hour, minute, second);

  // iterate a few times to correct offset / DST
  for (let i = 0; i < 4; i++) {
    const got = getZonedParts(new Date(utcMs), tz);

    const diffMinutes =
      (got.year - year) * 525600 +
      (got.month - month) * 43200 +
      (got.day - day) * 1440 +
      (got.hour - hour) * 60 +
      (got.minute - minute);

    // seconds don’t matter for midnight boundary
    if (diffMinutes === 0) break;

    utcMs -= diffMinutes * 60_000;
  }

  return new Date(utcMs);
}

function startOfTodayInTZ(tz = "America/Los_Angeles") {
  const now = new Date();
  const p = getZonedParts(now, tz);
  return zonedTimeToUtc(
    { year: p.year, month: p.month, day: p.day, hour: 0, minute: 0, second: 0 },
    tz
  );
}


async function getEmailValidationCached(email) {
  const address = normalizeEmail(email);
  if (!address) return { ok: false, reason: "invalid_email" };

  // cache hit?
  const cached = await emailValidations.findOne({ address });
  if (cached?.payload) {
    const decision = pickValidationDecision(cached.payload);
    return { ok: decision.ok, decision, payload: cached.payload, cached: true };
  }

  // cache miss -> call Mailgun
  const payload = await validateEmailViaMailgun(address);

  // store
  await emailValidations.updateOne(
    { address },
    {
      $set: {
        address,
        validatedAt: new Date(),
        payload,
        result: payload?.result || "",
        risk: payload?.risk || "",
        reason: payload?.reason || [],
      },
    },
    { upsert: true }
  );

  const decision = pickValidationDecision(payload);
  return { ok: decision.ok, decision, payload, cached: false };
}



function startEmailReplyWorker() {
  setInterval(async () => {
    try {
      await processOneEmailReplyJob();
    } catch (e) {
      console.error("Email reply worker tick error:", e?.message || e);
    }
  }, 1500);

  console.log("✅ Email reply worker started");
}


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

async function isOptedOutUserId(userId) {
  if (!userId) return false;
  const doc = await sessions.findOne(
    { userId: String(userId) },
    { projection: { optedOut: 1 } }
  );
  return !!doc?.optedOut;
}

function isHumanTakeover(session) {
  return !!session?.humanTakeover;
}

function isLowValueClosingMessage(text = "") {
  const t = normalizeTextForMatching(text);

  // only classify as closing if short
  const wordCount = t.split(" ").filter(Boolean).length;
  if (wordCount > 5) return false;

  const patterns = [
    "thanks", "thank you", "thx", "ty",
    "ok", "okay", "k", "cool", "awesome",
    "sounds good", "got it", "understood",
    "no problem", "all good",
    "bye", "goodbye", "see you", "take care",
    "have a great day", "have a good day"
  ];

  return patterns.some((p) => t === p);
}

function normalizeEmailUserId(email) {
  return String(email || "").trim().toLowerCase();
}



function safeJsonParse(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function extractFromEmail(payload) {
  // Common direct fields
  const direct =
    payload.from ||
    payload.sender ||
    payload.From ||
    payload["from_email"] ||
    payload["sender_email"] ||
    payload["from-address"];

  // SendGrid often provides "from" like: 'John Doe <john@example.com>'
  // Mailgun can provide "from" like that too.
  if (direct) {
    const m = String(direct).match(/<([^>]+)>/);
    const raw = m ? m[1] : direct;
    return normalizeEmail(raw);
  }

  // SendGrid inbound parse: envelope is JSON string containing from/to
  // envelope: {"to":["x@domain.com"],"from":"john@example.com",...}
  if (payload.envelope) {
    const env = typeof payload.envelope === "string" ? safeJsonParse(payload.envelope) : payload.envelope;
    if (env?.from) return normalizeEmail(env.from);
  }

  // Postmark sometimes uses FromFull: { Email, Name }
  if (payload.FromFull?.Email) return normalizeEmail(payload.FromFull.Email);

  // SES often includes mail.source or commonHeaders.from
  if (payload.mail?.source) return normalizeEmail(payload.mail.source);
  const fromArr = payload.mail?.commonHeaders?.from;
  if (Array.isArray(fromArr) && fromArr[0]) {
    const m = String(fromArr[0]).match(/<([^>]+)>/);
    return normalizeEmail(m ? m[1] : fromArr[0]);
  }

  return null;
}

function extractToEmail(payload) {
  const direct =
    payload.recipient ||
    payload.to ||
    payload.To ||
    payload["to"] ||
    payload["to_email"] ||
    payload["recipient-email"] ||
    payload["Delivered-To"];

  if (direct) {
    const m = String(direct).match(/<([^>]+)>/);
    const raw = m ? m[1] : direct;
    return normalizeEmail(raw);
  }

  // Mailgun commonly gives "recipient"
  if (payload.envelope) {
    const env = typeof payload.envelope === "string" ? safeJsonParse(payload.envelope) : payload.envelope;
    const to = Array.isArray(env?.to) ? env.to[0] : env?.to;
    if (to) return normalizeEmail(to);
  }

  return null;
}


function extractEmailText(payload) {
  // Try common plain-text fields first
  const candidates = [
    payload["stripped-text"],        // Mailgun best
    payload["body-plain"],           // Mailgun
    payload.text,                    // SendGrid inbound parse
    payload.TextBody,                // Postmark
    payload.textBody,
    payload.plain,
    payload["body"],                 // sometimes
  ];

  for (const c of candidates) {
    const t = String(c || "").trim();
    if (t) return t;
  }

  // HTML fallback (strip tags)
  const html = String(
    payload["stripped-html"] ||
    payload["body-html"] ||
    payload.html ||
    payload.HtmlBody ||
    ""
  ).trim();

  if (html) {
    return html
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  return "";
}


function extractMessageId(payload) {
  // Many providers include one of these:
  return String(
    payload["message-id"] ||
    payload["Message-Id"] ||
    payload.messageId ||
    payload.MessageID ||
    payload["MessageID"] ||
    ""
  ).trim();
}

function textSigToHtml(sigText) {
  const t = String(sigText || "").trim();
  if (!t) return "";
  // preserve line breaks nicely
  return `<div style="white-space:pre-line;">${escapeHtml(t)}</div>`;
}



function shouldIgnoreEmail(payload) {
  const subject = String(payload.subject || "").toLowerCase();
  // avoid loops: auto-replies, bounces, etc.
  const auto = String(payload["auto-submitted"] || "").toLowerCase();
  if (auto.includes("auto-replied") || auto.includes("auto-generated")) return true;
  if (subject.includes("undeliver") || subject.includes("delivery status")) return true;
  return false;
}



function lastAssistantWasClosing(history = []) {
  const lastOutbound = [...(history || [])].reverse().find(
    m => m?.role === "assistant" || m?.role === "agent"
  );
  if (!lastOutbound?.content) return false;

  const t = normalizeTextForMatching(lastOutbound.content);
  return (
    t.includes("have a great day") ||
    t.includes("have a good day") ||
    t.includes("take care") ||
    t.includes("bye")
  );
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
- Do NOT say "Thank you" unless the user is paying or did something meaningful.
- Never thank twice in one conversation.
- If the conversation is clearly ending, reply once with a short closing and stop.
- Avoid overly polite filler. Sound like a real person texting.
- Always respond in the same language the user used.
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

// async function sendEscalationAlerts({ userId, from, text, matchedKeywords }) {
//   const cfg = await getEscalationConfig();
//   if (!matchedKeywords.length || !cfg.targets.length) return;

//   const session = await sessions.findOne(
//     { userId },
//     { projection: { firstName: 1, lastName: 1, history: { $slice: -30 },  } }
//   );

//   const firstName = String(session?.firstName || "").trim();
//   const lastName = String(session?.lastName || "").trim();
//   const fullName = `${firstName} ${lastName}`.trim() || "Unknown";

//   const history = session?.history || [];
//   const lastOutbound = [...history].reverse().find((m) => m?.role === "assistant" || m?.role === "agent");
//   const previousMessage = lastOutbound?.content
//     ? String(lastOutbound.content).slice(0, 500)
//     : "(no previous outbound message found)";

//   const msg =
//     `🚨 Keyword alert: ${matchedKeywords.join(", ")}\n` +
//     `From: ${from}\n` +
//     `Name: ${fullName}\n` +
//     `Previous: ${previousMessage}\n` +
//     `Reply: ${String(text).slice(0, 500)}`;

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




function normalizeEmail(input) {
  const e = String(input || "").trim().toLowerCase();
  if (!e) return null;
  // simple sanity check (good enough for 99% lists)
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)) return null;
  return e;
}


function makeUnsubscribeToken(email) {
  return crypto
    .createHmac("sha256", process.env.EMAIL_INBOUND_WEBHOOK_SECRET)
    .update(String(email || "").trim().toLowerCase())
    .digest("hex");
}

function buildUnsubscribeUrl(baseUrl, email) {
  const safeEmail = encodeURIComponent(String(email || "").trim().toLowerCase());
  const token = encodeURIComponent(makeUnsubscribeToken(email));
  return `${baseUrl}/email/unsubscribe?email=${safeEmail}&token=${token}`;
}

function buildComplianceFooter(unsubscribeUrl) {
  const privacyUrl = "https://getleads.leadlockerroom.com/privacy-policy";

  const text =
    "You are receiving this email because your business may be interested in lead generation services.\n" +
    "We respect your privacy and do not sell your personal information. Your contact details may have been obtained through publicly available sources or trusted data providers.\n" +
    `Privacy Policy: ${privacyUrl}\n` +
    `Unsubscribe: ${unsubscribeUrl}\n` +
    "Pro Leads Marketing LLC\n" +
    "23811 Washington Ave # C110-103\n" +
    "Murrieta, CA 92562";

  const html =
    `<div style="margin-top:18px;font-size:12px;line-height:1.55;color:#666;border-top:1px solid #e5e7eb;padding-top:14px;">` +
      `<div>You are receiving this email because your business may be interested in lead generation services.</div>` +
      `<div style="margin-top:8px;">We respect your privacy and do not sell your personal information. Your contact details may have been obtained through publicly available sources or trusted data providers.</div>` +
      `<div style="margin-top:10px;">` +
        `<a href="${escapeHtml(privacyUrl)}" style="color:#666;text-decoration:underline;">Privacy Policy</a>` +
        ` &nbsp;|&nbsp; ` +
        `<a href="${escapeHtml(unsubscribeUrl)}" style="color:#666;text-decoration:underline;">Unsubscribe</a>` +
      `</div>` +
      `<div style="margin-top:10px;">` +
        `Pro Leads Marketing LLC<br/>` +
        `23811 Washington Ave # C110-103<br/>` +
        `Murrieta, CA 92562` +
      `</div>` +
    `</div>`;

  return { text, html };
}

async function isEmailOptedOut(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return true;

  const [sessionDoc, unsubDoc] = await Promise.all([
    emailSessions.findOne(
      { userId: normalizeEmailUserId(normalized) },
      { projection: { optedOut: 1 } }
    ),
    emailUnsubscribes.findOne(
      { email: normalized },
      { projection: { _id: 1 } }
    ),
  ]);

  return !!(sessionDoc?.optedOut || unsubDoc?._id);
}

async function addMailgunUnsubscribe(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return;

  const url = `${MAILGUN_BASE_URL}/v3/${MAILGUN_DOMAIN}/unsubscribes`;

  await axios.post(
    url,
    new URLSearchParams({ address: normalized }),
    {
      auth: { username: "api", password: MAILGUN_API_KEY },
      timeout: 30000,
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
    }
  );
}

function renderEmail(template, vars) {
  let out = String(template || "");
  const firstName = String(vars.firstName || "").trim();
  const lastName = String(vars.lastName || "").trim();
  const fullName = `${firstName} ${lastName}`.trim();

  const map = { firstName, lastName, fullName };

  for (const k of Object.keys(map)) {
    out = out.replaceAll(`{{${k}}}`, map[k]);
    out = out.replaceAll(`{${k}}`, map[k]);
  }

  // ✅ Only collapse repeated spaces/tabs (NOT newlines)
  out = out.replace(/[^\S\r\n]{2,}/g, " ");

  // Optional: normalize trailing spaces per line
  out = out.replace(/[^\S\r\n]+$/gm, "");

  return out.trim();
}


async function getEmailBlastTemplate() {
  const defaults = {
    key: "email_blast_template",
    subject: "Quick question, {{firstName}}",
    body: "Hey {{firstName}},\n\nAre you free for a quick chat?\n",
    signatureText: "",          // <-- default blank
    signatureHtml: "",          // <-- default blank
    flyerImageUrl: "",
    enabled: true,
  };

  const doc = await emailSettings.findOne({ key: "email_blast_template" });
  const merged = { ...defaults, ...(doc || {}) };

  // only backfill subject/body/flyer if blank
  for (const k of ["subject", "body", "flyerImageUrl"]) {
    if (typeof merged[k] === "string" && merged[k].trim() === "") {
      merged[k] = defaults[k];
    }
  }

  // DO NOT backfill signature fields — blank should mean “no template signature”
  if (typeof merged.enabled !== "boolean") merged.enabled = true;

  return merged;
}



const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;
const EMAIL_FROM = process.env.EMAIL_FROM || SMTP_USER;

const smtpTransport =
  SMTP_HOST && SMTP_USER && SMTP_PASS
    ? nodemailer.createTransport({
        host: SMTP_HOST,
        port: SMTP_PORT,
        secure: SMTP_PORT === 465,
        auth: { user: SMTP_USER, pass: SMTP_PASS },
      })
    : null;

async function sendEmailViaSMTP({ to, subject, text, html }) {
  if (!smtpTransport) throw new Error("smtp_not_configured");
  await smtpTransport.sendMail({
    from: EMAIL_FROM,
    to,
    subject,
    text,
    html,
  });
}


async function processOneEmailJob() {
  const now = new Date();

  const claimed = await emailQueue.findOneAndUpdate(
    { status: "queued", runAt: { $lte: now } },
    { $set: { status: "processing", processingAt: now } },
    { sort: { runAt: 1 }, returnDocument: "after" }
  );

  const job = claimed?.value ?? claimed ?? null;
  if (!job || !job._id) return false;

  try {
    // choose ONE sender (pick your path)
    // await sendEmailViaSendGrid({ to: job.email, subject: job.subject, text: job.body });
    // await sendEmailViaSMTP({ to: job.email, subject: job.subject, text: job.body });
    if (await isEmailOptedOut(job.email)) {
  await emailQueue.updateOne(
    { _id: job._id },
    { $set: { status: "skipped_unsubscribed", skippedAt: new Date() } }
  );

  await emailBatches.updateOne(
    { _id: job.batchId },
    { $inc: { skipped: 1 }, $set: { updatedAt: new Date() } }
  );

  return true;
}

const domainConfig = await getEmailDomainById(job.domainId);
if (!domainConfig) {
  throw new Error("job_domain_missing_or_disabled");
}

const mgResp = await sendEmailViaMailgun({
  domainConfig,
  from: job.from,
  to: job.email,
  subject: job.subject,
  text: job.textBody || job.body,
  html: job.htmlBody || undefined,
  headers: {
    ...(job.replyTo ? { "Reply-To": job.replyTo } : {}),
    ...(job.unsubscribeUrl
      ? {
          "List-Unsubscribe": `<${job.unsubscribeUrl}>`,
          "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        }
      : {}),
    "X-LLR-Sender-Id": job.senderId || "",
    "X-LLR-Sender-Email": (String(job.from || "").match(/<([^>]+)>/)?.[1] || job.from || ""),
  },
  variables: {
    llr_queue_id: String(job._id),
    llr_batch_id: String(job.batchId || ""),
    llr_email: String(job.email || ""),
    llr_domain_id: String(job.domainId || ""),
  },
});


  await emailQueue.updateOne(
  { _id: job._id },
  {
    $set: {
      status: "sent",
      sentAt: new Date(),
      provider: "mailgun",
      providerMessageId: String(mgResp?.id || ""),
    }
  }
);

    await emailBatches.updateOne(
      { _id: job.batchId },
      { $inc: { sent: 1 }, $set: { updatedAt: new Date() } }
    );

    return true;
  } catch (e) {
  const status = e?.response?.status;
  const data = e?.response?.data;
  const detail =
    data ? JSON.stringify(data) :
    (e?.message || String(e));

  console.error("❌ Mailgun send failed:", { status, detail });

  await emailQueue.updateOne(
    { _id: job._id },
    {
      $set: {
        status: "failed",
        failedAt: new Date(),
        error: detail,
        errorStatus: status || null,
      }
    }
  );

  await emailBatches.updateOne(
    { _id: job.batchId },
    { $inc: { failed: 1 }, $set: { updatedAt: new Date() } }
  );

  return true;
}

}

function startEmailWorker() {
  setInterval(async () => {
    try {
      await processOneEmailJob();
    } catch (e) {
      console.error("Email worker tick error:", e?.message || e);
    }
  }, 500);

  console.log("✅ Email worker started");
}



async function sendEscalationAlerts({ userId, from, text, matchedKeywords, inboundFirstName, inboundLastName }) {
  const cfg = await getEscalationConfig();
  if (!matchedKeywords.length) return;

  const session = await sessions.findOne(
    { userId },
    { projection: { firstName: 1, lastName: 1, history: { $slice: -30 }, assignedTarget: 1 } }
  );

  const firstName =
    String(inboundFirstName || session?.firstName || "").trim();
  const lastName =
    String(inboundLastName || session?.lastName || "").trim();

  const fullName = `${firstName} ${lastName}`.trim() || "Unknown";

  const history = session?.history || [];
  const lastOutbound = [...history].reverse().find(
    m => m?.role === "assistant" || m?.role === "agent"
  );

  const previousMessage = lastOutbound?.content
    ? String(lastOutbound.content).slice(0, 500)
    : "(no previous outbound message)";

  const msg =
    `🚨 Keyword alert: ${matchedKeywords.join(", ")}\n` +
    `From: ${from}\n` +
    `Name: ${fullName}\n` +
    `Previous: ${previousMessage}\n` +
    `Reply: ${String(text).slice(0, 500)}`;

  const targetsToNotify = [];

if (session?.assignedTarget?.number) {
  targetsToNotify.push(session.assignedTarget);
} else {
  // fallback only if you WANT global alerts
  cfg.targets.forEach(number => {
    targetsToNotify.push({ number, name: "Escalation Target" });
  });
}

await Promise.all(
  targetsToNotify.map(t =>
    notificationapi.send({
      type: "ai_sms_chat",
      to: {
        id: normalizeUserIdFromPhone(t.number),
        number: t.number
      },
      sms: { message: msg }
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

    const optedOut = await isOptedOutUserId(uid);
    if (optedOut) {
    return res.status(409).json({
        ok: false,
        error: "dnc_opted_out",
        message: "This number previously opted out. Outbound blocked."
    });
    }


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

    // if human takeover is ON, do NOT schedule AI replies
    if (existingSession?.humanTakeover) {
      return res.status(200).json({ ok: true, human_takeover: true });
    }

    // 3. Keyword-based DNC (admin-defined)
    const dnc = await dncKeywords.find({ enabled: true }).toArray();
    const matchedDnc = dnc.find(k => {
    const kw = String(k?.keyword || "").trim().toLowerCase();
    return kw && textLower.includes(kw);
    });


   if (matchedDnc) {
  const inboundFirst = String(firstName || "").trim();
  const inboundLast = String(lastName || "").trim();

  const finalFirstName =
    inboundFirst || String(existingSession?.firstName || "").trim();
  const finalLastName =
    inboundLast || String(existingSession?.lastName || "").trim();

  await sessions.updateOne(
    { userId },
    {
      $set: {
        userId,
        number: from,
        firstName: finalFirstName,
        lastName: finalLastName,
        optedOut: true,
        optedOutAt: new Date(),
        updatedAt: new Date(),
      },
      $push: {
        history: {
          $each: [{ role: "user", content: text, createdAt: new Date() }],
          $slice: -30,
        },
      },
    },
    { upsert: true }
  );

  await dncEvents.insertOne({
    userId,
    from,
    firstName: finalFirstName,
    lastName: finalLastName,
    keyword: String(matchedDnc.keyword || "").trim().toLowerCase(),
    text,
    createdAt: new Date(),
  });

  return res.status(200).json({ ok: true, opted_out: true });
}


    // 4. Carrier STOP words (persist them too)
if (["STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"].includes(upper)) {
  const inboundFirst = String(firstName || "").trim();
  const inboundLast = String(lastName || "").trim();

  const finalFirstName =
    inboundFirst || String(existingSession?.firstName || "").trim();
  const finalLastName =
    inboundLast || String(existingSession?.lastName || "").trim();

  await sessions.updateOne(
    { userId },
    {
      $set: {
        userId,
        number: from,
        firstName: finalFirstName,
        lastName: finalLastName,
        optedOut: true,
        optedOutAt: new Date(),
        updatedAt: new Date(),
      },
      $push: {
        history: {
          $each: [{ role: "user", content: text, createdAt: new Date() }],
          $slice: -30,
        },
      },
    },
    { upsert: true }
  );

  // ✅ ALSO log into DNC_Events so the table always reflects opt-outs
  await dncEvents.insertOne({
    userId,
    from,
    firstName: finalFirstName,
    lastName: finalLastName,
    keyword: upper.toLowerCase(), // "stop", "unsubscribe", etc
    text,
    createdAt: new Date(),
  });

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
    const existing = await sessions.findOne(
      { userId },
      { projection: { firstName: 1, lastName: 1 } }
    );

    const inboundFirstName = String(payload.firstName || "").trim();
    const inboundLastName = String(payload.lastName || "").trim();

    const finalFirstName = inboundFirstName || String(existing?.firstName || "").trim();
    const finalLastName = inboundLastName || String(existing?.lastName || "").trim();

    await sessions.updateOne(
      { userId },
      {
        $setOnInsert: { userId, createdAt: new Date() },
        $set: {
          number: from,
          firstName: finalFirstName,
          lastName: finalLastName,
          updatedAt: new Date(),
          lastInboundAt: new Date(),
          hidden: false,
          hiddenAt: null,
          unhiddenAt: new Date(),
          unhiddenReason: "inbound_reply",
        },
        $push: {
          history: {
            $each: [{ role: "user", content: text, createdAt: new Date() }],
            $slice: -30,
          },
        },
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
          { projection: { firstName: 1, lastName: 1, history: { $slice: -30 }, assignedTarget: 1 } }
        );

        const firstName = String(session?.firstName || inboundFirstName || "").trim();
        const lastName  = String(session?.lastName  || inboundLastName  || "").trim();


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

  // keep if you want audit trail (optional)
  alertedTargets: cfg.targets || [],

  // ✅ THIS is the rep assignment snapshot
  assignedTarget: session?.assignedTarget || null,
});


        await sendEscalationAlerts({
          userId,
          from,
          text,
          matchedKeywords: matched,
          inboundFirstName: firstName,
          inboundLastName: lastName
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

    // don't reply to low-value "closing" messages
    const sessForCloseCheck = await sessions.findOne(
      { userId },
      { projection: { history: { $slice: -30 } } }
    );

    const hist = sessForCloseCheck?.history || [];

    if (isLowValueClosingMessage(text) && lastAssistantWasClosing(hist)) {
      return res.status(200).json({ ok: true, skipped: "closing_no_reply" });
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

    // NEW: if agent took over, never send AI
    if (existing?.humanTakeover) {
      await replyJobs.updateOne(
        { _id: job._id },
        { $set: { status: "cancelled", cancelledAt: new Date(), note: "human_takeover_worker_guard" } }
      );
      return true;
    }

    // NEW: if agent already replied after this job was created → skip AI
    if (existing?.lastAgentAt && job?.createdAt) {
      if (new Date(existing.lastAgentAt) > new Date(job.createdAt)) {
        await replyJobs.updateOne(
          { _id: job._id },
          { $set: { status: "cancelled", cancelledAt: new Date(), note: "agent_replied_after_queue" } }
        );
        return true;
      }
    }


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


// Correct unread count (matches inbox logic)
app.get("/api/inbox/unread-count", async (req, res) => {
  try {
    const count = await sessions.countDocuments({
      lastInboundAt: { $exists: true },
      $expr: {
        $gt: [
          "$lastInboundAt",
          { $ifNull: ["$lastViewedAt", "$lastAgentAt"] }
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
          hasInbound: {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: "$history",
                    as: "m",
                    cond: { $eq: ["$$m.role", "user"] }
                  }
                }
              },
              0
            ]
          },
          outboundCount: {
            $size: {
              $filter: {
                input: "$history",
                as: "m",
                cond: { $eq: ["$$m.role", "assistant"] }
              }
            }
          }
        }
      },
      {
        $group: {
          _id: null,
          outbound: { $sum: "$outboundCount" },
          customersReplied: {
            $sum: {
              $cond: ["$hasInbound", 1, 0]
            }
          }
        }
      }
    ]).toArray();

    const row = agg[0] || { outbound: 0, customersReplied: 0 };

    res.json({
      outbound: row.outbound,
      customersReplied: row.customersReplied
    });
  } catch (e) {
    console.error("stats error:", e);
    res.json({ outbound: 0, customersReplied: 0 });
  }
});

app.post("/api/conversations/:userId/hide", async (req, res) => {
  try {
    await sessions.updateOne(
      { userId: req.params.userId },
      {
        $set: {
          hidden: true,
          updatedAt: new Date()
        }
      }
    );
    res.json({ ok: true });
  } catch (e) {
    console.error("hide error:", e);
    res.status(500).json({ ok: false });
  }
});


app.post("/api/conversations/:userId/restore", async (req, res) => {
  try {
    await sessions.updateOne(
      { userId: req.params.userId },
      {
        $set: {
          hidden: false,
          updatedAt: new Date()
        }
      }
    );
    res.json({ ok: true });
  } catch (e) {
    console.error("restore error:", e);
    res.status(500).json({ ok: false });
  }
});




// Create batch: frontend posts rows JSON parsed from CSV
// Body: { rows: [{ firstName, lastName, phone }], spacingMs?: 5000, defaultCountryCode?: "1" }
// app.post("/api/outbound/batch", requireAdmin, async (req, res) => {
//   try {
//     const { rows, spacingMs, assignedTarget } = req.body;

//     if (!rows.length) return res.status(400).json({ error: "rows_required" });
//     if (rows.length > 5000) return res.status(400).json({ error: "too_many_rows" });

//     const settings = await getFirstMessageTemplate();

//     const defaultCountryCode = String(req.body?.defaultCountryCode || settings.defaultCountryCode || "1");

//     const batchId = new ObjectId().toString();
//     const now = Date.now();

//     let accepted = 0;
//     let rejected = 0;
//     const rejects = [];

//     // Create batch record
//     await outboundBatches.insertOne({
//       _id: batchId,
//       status: "queued",
//       total: rows.length,
//       accepted: 0,
//       rejected: 0,
//       sent: 0,
//       failed: 0,
//       skipped: 0,
//       spacingMs,
//       assignedTarget: assignedTarget || null,
//       createdAt: new Date(),
//       updatedAt: new Date(),
//     });

//     // ✅ Prefetch opted-out userIds for this batch (so we can reject them fast)
//         const candidateUserIds = [];
//         for (const r of rows || []) {
//         const phoneRaw = String(r.phone || r.Phone || r.number || "").trim();
//         const e164 = normalizeToE164(phoneRaw, defaultCountryCode); // use your existing normalize
//         if (!e164) continue;
//         const uid = normalizeUserIdFromPhone(e164); // use your existing userId builder
//         candidateUserIds.push(uid);
//         }

//         const optedOutDocs = await sessions
//         .find(
//             { userId: { $in: candidateUserIds }, optedOut: true },
//             { projection: { userId: 1 } }
//         )
//         .toArray();

//         const optedOutSet = new Set(optedOutDocs.map(d => d.userId));


//     const queueDocs = [];
//     for (let i = 0; i < rows.length; i++) {
//       const r = rows[i] || {};
//       const firstName = String(r.firstName || r.firstname || "").trim();
//       const lastName = String(r.lastName || r.lastname || "").trim();
//       const phoneRaw = String(r.phone || r.Phone || r.number || "").trim();

//       const e164 = normalizeToE164(phoneRaw, defaultCountryCode);
//       if (!e164) {
//         rejected++;
//         rejects.push({ index: i, phone: phoneRaw, reason: "invalid_phone" });
//         continue;
//       }

//       const uid = normalizeUserIdFromPhone(e164);

//       if (optedOutSet.has(uid)) {
//         rejected++;
//         rejects.push({ index: i, phone: e164, reason: "dnc_opted_out" });
//         continue;
//         }

//       const message = renderTemplate(settings.template, { firstName, lastName });

//       const burstIndex = burstPauseMsFinal > 0 ? Math.floor(accepted / burstSizeFinal) : 0;
//       const runAt = new Date(now + accepted * spacingMs + burstIndex * burstPauseMsFinal);

//       const trackingId = `${batchId}:${uid}:${accepted}`;

//       queueDocs.push({
//         trackingId,
//         batchId,
//         userId: uid,
//         number: e164,
//         firstName,
//         lastName,
//         message,
//         status: "queued",
//         runAt,
//         createdAt: new Date(),
//       });

//       accepted++;
//     }

//     if (queueDocs.length) {
//       // insertMany with ordered:false so one duplicate won't kill everything
//       await outboundQueue.insertMany(queueDocs, { ordered: false });
//     }

//     await outboundBatches.updateOne(
//       { _id: batchId },
//       {
//         $set: { accepted, rejected, status: "queued", updatedAt: new Date(), nextSeq: accepted },
//       }
//     );

//     res.json({
//       ok: true,
//       batchId,
//       total: rows.length,
//       accepted,
//       rejected,
//       rejects: rejects.slice(0, 50),
//       spacingMs,
//     });
//   } catch (err) {
//     console.error("outbound/batch error:", err?.message || err);
//     res.status(500).json({ error: err?.message || "failed" });
//   }
// });

app.post("/api/outbound/batch", requireAdmin, async (req, res) => {
  try {
    const { rows, assignedTarget } = req.body;

    const spacingMs = Math.max(200, Number(req.body?.spacingMs || OUTBOUND_SPACING_MS || 5000));
    const burstSizeFinal = Math.max(1, Number(req.body?.burstSize || 100));
    const burstPauseMsFinal = Math.max(0, Number(req.body?.burstPauseMs || 60 * 1000));

    if (!rows.length) return res.status(400).json({ error: "rows_required" });
    if (rows.length > 5000) return res.status(400).json({ error: "too_many_rows" });

    const settings = await getFirstMessageTemplate();
    const defaultCountryCode = String(req.body?.defaultCountryCode || settings.defaultCountryCode || "1");

    const batchId = new ObjectId().toString();
    const now = Date.now();

    let accepted = 0;
    let rejected = 0;
    const rejects = [];

    await outboundBatches.insertOne({
      _id: batchId,
      status: "queued",
      total: rows.length,
      accepted: 0,
      rejected: 0,
      sent: 0,
      failed: 0,
      skipped: 0,
      spacingMs,
      burstSize: burstSizeFinal,
      burstPauseMs: burstPauseMsFinal,
      assignedTarget: assignedTarget || null,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    const candidateUserIds = [];
    for (const r of rows || []) {
      const phoneRaw = String(r.phone || r.Phone || r.number || "").trim();
      const e164 = normalizeToE164(phoneRaw, defaultCountryCode);
      if (!e164) continue;
      candidateUserIds.push(normalizeUserIdFromPhone(e164));
    }

    const optedOutDocs = await sessions
      .find(
        { userId: { $in: candidateUserIds }, optedOut: true },
        { projection: { userId: 1 } }
      )
      .toArray();

    const optedOutSet = new Set(optedOutDocs.map(d => d.userId));

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

      if (optedOutSet.has(uid)) {
        rejected++;
        rejects.push({ index: i, phone: e164, reason: "dnc_opted_out" });
        continue;
      }

      const message = renderTemplate(settings.template, { firstName, lastName });

      const burstIndex =
        burstPauseMsFinal > 0 ? Math.floor(accepted / burstSizeFinal) : 0;

      const runAt = new Date(
        now + accepted * spacingMs + burstIndex * burstPauseMsFinal
      );

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
      await outboundQueue.insertMany(queueDocs, { ordered: false });
    }

    await outboundBatches.updateOne(
      { _id: batchId },
      {
        $set: {
          accepted,
          rejected,
          status: "queued",
          updatedAt: new Date(),
          nextSeq: accepted,
        },
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
      burstSize: burstSizeFinal,
      burstPauseMs: burstPauseMsFinal,
    });
  } catch (err) {
    console.error("outbound/batch error:", err?.message || err);
    res.status(500).json({ error: err?.message || "failed" });
  }
});

app.get("/email/unsubscribe", async (req, res) => {
  try {
    const email = normalizeEmail(req.query.email);
    const token = String(req.query.token || "").trim();

    if (!email || !token) {
      return res.status(400).send("Invalid unsubscribe link.");
    }

    const expected = makeUnsubscribeToken(email);
    if (token !== expected) {
      return res.status(401).send("Invalid unsubscribe token.");
    }

    await emailSessions.updateOne(
      { userId: normalizeEmailUserId(email) },
      {
        $setOnInsert: { userId: normalizeEmailUserId(email), createdAt: new Date() },
        $set: {
          email,
          optedOut: true,
          optedOutAt: new Date(),
          updatedAt: new Date(),
        },
      },
      { upsert: true }
    );

    await emailUnsubscribes.updateOne(
      { email },
      {
        $set: {
          email,
          source: "local_link",
          updatedAt: new Date(),
        },
        $setOnInsert: {
          createdAt: new Date(),
        },
      },
      { upsert: true }
    );

    try {
      await addMailgunUnsubscribe(email);
    } catch (e) {
      console.error("mailgun unsubscribe sync error:", e?.response?.data || e?.message || e);
    }

    return res
      .status(200)
      .send(`<html><body style="font-family:Arial,sans-serif;padding:24px;">You have been unsubscribed.</body></html>`);
  } catch (e) {
    console.error("unsubscribe page error:", e?.message || e);
    return res.status(500).send("Something went wrong.");
  }
});

app.post("/webhook/mailgun/events", express.json({ limit: "2mb" }), async (req, res) => {
  try {
    if (req.query.token !== process.env.EMAIL_INBOUND_WEBHOOK_SECRET) {
      return res.status(401).send("unauthorized");
    }

    const payload = req.body?.["event-data"] || req.body || {};
    const event = String(payload?.event || "").trim();
    const recipient = normalizeEmail(payload?.recipient || "");
    const userVars = payload?.["user-variables"] || {};
    const queueId = String(userVars?.llr_queue_id || "").trim();
    const batchId = String(userVars?.llr_batch_id || "").trim();
    const messageId = String(payload?.message?.headers?.["message-id"] || "").trim();
    const eventAt = payload?.timestamp ? new Date(Number(payload.timestamp) * 1000) : new Date();

    await emailEvents.insertOne({
      provider: "mailgun",
      event,
      recipient,
      queueId,
      batchId,
      messageId,
      payload,
      createdAt: eventAt,
      receivedAt: new Date(),
    });

    if (queueId && ObjectId.isValid(queueId)) {
      const qid = new ObjectId(queueId);

      if (event === "opened") {
        await emailQueue.updateOne(
          { _id: qid },
          {
            $set: { openedAt: eventAt, openTracked: true },
            $inc: { openCount: 1 },
          }
        );
      }

      if (event === "unsubscribed") {
        await emailQueue.updateOne(
          { _id: qid },
          {
            $set: { unsubscribedAt: eventAt, unsubscribedTracked: true },
          }
        );
      }
    }

    if (event === "unsubscribed" && recipient) {
      await emailSessions.updateOne(
        { userId: normalizeEmailUserId(recipient) },
        {
          $setOnInsert: { userId: normalizeEmailUserId(recipient), createdAt: new Date() },
          $set: {
            email: recipient,
            optedOut: true,
            optedOutAt: eventAt,
            updatedAt: new Date(),
          },
        },
        { upsert: true }
      );

      await emailUnsubscribes.updateOne(
        { email: recipient },
        {
          $set: {
            email: recipient,
            source: "mailgun_webhook",
            updatedAt: new Date(),
          },
          $setOnInsert: {
            createdAt: eventAt,
          },
        },
        { upsert: true }
      );
    }

    return res.status(200).json({ ok: true });
  } catch (e) {
    console.error("mailgun events webhook error:", e?.message || e);
    return res.status(200).json({ ok: false });
  }
});

app.get("/api/escalation/events", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);

    const phone = String(req.query.phone || "").trim();
    const keyword = String(req.query.keyword || "").trim().toLowerCase();

    const q = {};
    if (phone) q.from = phone;

    // matchedKeywords is an array; match by element
    if (keyword) q.matchedKeywords = keyword;

    const items = await escalationEvents
      .find(q)
      .sort({ createdAt: -1 })
      .limit(limit)
      .toArray();

    // ✅ Collect assignedTarget numbers that need name lookup
    const needNumbers = new Set();
    for (const ev of items) {
      const n = ev?.assignedTarget?.number;
      if (n && !ev?.assignedTarget?.name) needNumbers.add(String(n));
    }

    // Lookup missing names from Escalation_Targets
    let targetMap = new Map();
    if (needNumbers.size) {
      const targetDocs = await escalationTargets
        .find(
          { number: { $in: Array.from(needNumbers) } },
          { projection: { _id: 0, name: 1, number: 1 } }
        )
        .toArray();

      targetMap = new Map(targetDocs.map(t => [String(t.number), t.name]));
    }

    // ✅ Return a single field the frontend can render: assignedTargetDetailed
    const enriched = items.map((ev) => {
      const num = ev?.assignedTarget?.number ? String(ev.assignedTarget.number) : "";
      const name =
        (ev?.assignedTarget?.name && String(ev.assignedTarget.name).trim()) ||
        (num ? targetMap.get(num) : "") ||
        "";

      return {
        ...ev,
        assignedTargetDetailed: num
          ? { name: name || "—", number: num }
          : null,
      };
    });

    res.json({ items: enriched });
  } catch (e) {
    console.error("GET /api/escalation/events error:", e);
    res.status(500).json({ error: "Failed to load events" });
  }
});

// Save/update a note on an escalation event
app.patch("/api/escalation/events/:id/note", requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ error: "id_required" });

    const noteRaw = req.body?.note;
    const note = noteRaw === null || noteRaw === undefined ? "" : String(noteRaw);

    // keep it sane
    if (note.length > 2000) return res.status(400).json({ error: "note_too_long" });

    const _id = new ObjectId(id);

    const r = await escalationEvents.updateOne(
      { _id },
      {
        $set: {
          note,
          noteUpdatedAt: new Date(),
        },
      }
    );

    if (!r.matchedCount) return res.status(404).json({ error: "not_found" });

    res.json({ ok: true, _id: id, note });
  } catch (e) {
    console.error("PATCH /api/escalation/events/:id/note error:", e?.message || e);
    res.status(500).json({ error: "failed_to_save_note" });
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

  // 🔒 DNC guard: never send outbound to opted-out users
const sess = await sessions.findOne(
  { userId },
  { projection: { optedOut: 1 } }
);

if (sess?.optedOut) {
  // mark job skipped
  await outboundQueue.updateOne(
    { _id: job._id },
    { $set: { status: "skipped_dnc", skippedAt: new Date() } }
  );

  // keep batch counters consistent
  if (job.batchId) {
    await outboundBatches.updateOne(
      { _id: job.batchId },
      { $inc: { skipped: 1 }, $set: { updatedAt: new Date() } }
    );

    // optional: close batch if finished
    const b = await outboundBatches.findOne({ _id: job.batchId });
    if (b && (b.sent + b.failed + (b.skipped || 0)) >= b.accepted) {
      await outboundBatches.updateOne(
        { _id: job.batchId },
        { $set: { status: "done", updatedAt: new Date() } }
      );
    }
  }

  return true; // job consumed
}


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
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const hidden = req.query.hidden === "true";
    const onlyUnread = req.query.onlyUnread === "true";

    const baseFilter = hidden ? { hidden: true } : { hidden: { $ne: true } };

    // ✅ If onlyUnread=true, ignore cursor and return unread-only page (sorted by inbound)
    // This is used by frontend to pin ALL unread at the top.
    if (onlyUnread) {
      const filter = {
        ...baseFilter,
        lastInboundAt: { $ne: null },
        $expr: {
          $gt: ["$lastInboundAt", { $ifNull: ["$lastViewedAt", "$lastAgentAt"] }],
        },
      };

      const docs = await sessions
        .find(filter, {
          projection: {
            userId: 1,
            number: 1,
            firstName: 1,
            lastName: 1,
            updatedAt: 1,
            assignedTarget: 1,
            lastInboundAt: 1,
            lastAgentAt: 1,
            lastViewedAt: 1,
            hidden: 1,
            humanTakeover: 1,
            history: { $slice: -1 },
          },
        })
        .sort({ lastInboundAt: -1, updatedAt: -1, userId: 1 })
        .limit(limit)
        .toArray();

      return res.json({
        items: docs.map((d) => {
          const lastSeen = d.lastViewedAt || d.lastAgentAt;
          const unread =
            d.lastInboundAt && (!lastSeen || new Date(d.lastInboundAt) > new Date(lastSeen));

          return {
            userId: d.userId,
            number: d.number,
            firstName: d.firstName || "",
            lastName: d.lastName || "",
            assignedTarget: d.assignedTarget || null,
            updatedAt: d.updatedAt,
            lastMessage: d.history?.[0]?.content || "",
            lastRole: d.history?.[0]?.role || "",
            unread,
            hidden: !!d.hidden,
            humanTakeover: !!d.humanTakeover,
          };
        }),
      });
    }

    // ✅ Existing cursor pagination path (UNCHANGED)
    let cursor = null;
    if (req.query.cursor) {
      try {
        cursor = JSON.parse(
          Buffer.from(req.query.cursor, "base64url").toString("utf8")
        );
      } catch {}
    }

    const sort = { updatedAt: -1, userId: 1 };

    let filter = baseFilter;

    if (cursor?.updatedAt && cursor?.userId) {
      const dt = new Date(cursor.updatedAt);
      filter = {
        $and: [
          baseFilter,
          {
            $or: [
              { updatedAt: { $lt: dt } },
              { updatedAt: dt, userId: { $gt: cursor.userId } },
            ],
          },
        ],
      };
    }

    const docs = await sessions
      .find(filter, {
        projection: {
          userId: 1,
          number: 1,
          firstName: 1,
          lastName: 1,
          updatedAt: 1,
          assignedTarget: 1,
          lastInboundAt: 1,
          lastAgentAt: 1,
          lastViewedAt: 1,
          hidden: 1,
          humanTakeover: 1,
          history: { $slice: -1 },
        },
      })
      .sort(sort)
      .limit(limit + 1)
      .toArray();

    const hasMore = docs.length > limit;
    const page = docs.slice(0, limit);

    const nextCursor =
      hasMore && page.length
        ? Buffer.from(
            JSON.stringify({
              updatedAt: page[page.length - 1].updatedAt,
              userId: page[page.length - 1].userId,
            }),
            "utf8"
          ).toString("base64url")
        : null;

    res.json({
      items: page.map((d) => {
        const lastSeen = d.lastViewedAt || d.lastAgentAt;
        const unread =
          d.lastInboundAt && (!lastSeen || new Date(d.lastInboundAt) > new Date(lastSeen));

        return {
          userId: d.userId,
          number: d.number,
          firstName: d.firstName || "",
          lastName: d.lastName || "",
          assignedTarget: d.assignedTarget || null,
          updatedAt: d.updatedAt,
          lastMessage: d.history?.[0]?.content || "",
          lastRole: d.history?.[0]?.role || "",
          unread,
          hidden: !!d.hidden,
          humanTakeover: !!d.humanTakeover,
        };
      }),
      nextCursor,
      hasMore,
    });
  } catch (e) {
    console.error("GET /api/conversations error:", e);
    res.status(500).json({ error: "Failed to load conversations" });
  }
});

// Mark ALL conversations as read (uses the same unread logic you already use everywhere)
app.post("/api/conversations/read-all", async (req, res) => {
  try {
    const now = new Date();

    // same unread definition used by /api/conversations?onlyUnread=true
    const filter = {
      hidden: { $ne: true },
      lastInboundAt: { $exists: true, $ne: null },
      $expr: {
        $gt: ["$lastInboundAt", { $ifNull: ["$lastViewedAt", "$lastAgentAt"] }],
      },
    };

    const r = await sessions.updateMany(filter, {
      $set: { lastViewedAt: now, updatedAt: now },
    });

    res.json({ ok: true, modified: r.modifiedCount });
  } catch (e) {
    console.error("read-all error:", e);
    res.status(500).json({ error: e?.message || "Failed to mark all read" });
  }
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
    humanTakeover: !!doc.humanTakeover,
    history: doc.history || [],
  });
});

app.post("/api/conversations/:userId/send", async (req, res) => {
  const now = new Date();
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
      lastAgentAt: new Date(),
      humanTakeover: true,
      humanTakeoverAt: now,
      humanTakeoverBy: "agent",
    },

      $push: { history: { $each: [{ role: "agent", content: String(message) , createdAt: new Date()}], $slice: -50 } },
    }
  );

  // cancel any pending AI jobs for this user
  await replyJobs.updateMany(
    { userId: doc.userId, status: "queued" },
    { $set: { status: "cancelled", cancelledAt: new Date(), note: "human_takeover" } }
  );


  res.json({ ok: true });
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

app.post("/api/conversations/:userId/ai/resume", requireAdmin, async (req, res) => {
  try {
    const userId = String(req.params.userId || "").trim();
    if (!userId) return res.status(400).json({ error: "userId_required" });

    await sessions.updateOne(
      { userId },
      {
        $set: {
          humanTakeover: false,
          humanTakeoverEndedAt: new Date(),
          updatedAt: new Date(),
        },
      }
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("resume ai error:", e);
    res.status(500).json({ ok: false });
  }
});

app.post("/api/conversations/:userId/ai/takeover", requireAdmin, async (req, res) => {
  try {
    const userId = String(req.params.userId || "").trim();
    if (!userId) return res.status(400).json({ error: "userId_required" });

    const now = new Date();

    await sessions.updateOne(
      { userId },
      {
        $set: {
          humanTakeover: true,
          humanTakeoverAt: now,
          updatedAt: now,
        },
      }
    );

    // cancel queued AI jobs
    await replyJobs.updateMany(
      { userId, status: "queued" },
      { $set: { status: "cancelled", cancelledAt: now, note: "manual_takeover" } }
    );

    res.json({ ok: true, humanTakeover: true });
  } catch (e) {
    console.error("takeover error:", e);
    res.status(500).json({ ok: false });
  }
});



// List DNC Events (ENRICH NAMES)
app.get("/api/dnc/events", requireAdmin, async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const phone = String(req.query.phone || "").trim();
    const keyword = String(req.query.keyword || "").trim().toLowerCase();

    const q = {};
    if (phone) q.from = phone;
    if (keyword) q.keyword = keyword;

    const items = await dncEvents
      .find(q)
      .sort({ createdAt: -1 })
      .limit(limit)
      .toArray();

    // ✅ Find which rows are missing names
    const needUserIds = new Set();
    for (const ev of items) {
      const hasName =
        (String(ev?.firstName || "").trim() || String(ev?.lastName || "").trim());
      if (hasName) continue;

      const uid = normalizeUserIdFromPhone(ev?.from || "");


      if (uid) needUserIds.add(uid);
    }

    // ✅ Lookup from sessions for missing names
    let sessionMap = new Map();
    if (needUserIds.size) {
      const sess = await sessions
        .find(
          { userId: { $in: Array.from(needUserIds) } },
          { projection: { _id: 0, userId: 1, firstName: 1, lastName: 1 } }
        )
        .toArray();

      sessionMap = new Map(
        sess.map((s) => [
          String(s.userId),
          {
            firstName: String(s.firstName || "").trim(),
            lastName: String(s.lastName || "").trim(),
          },
        ])
      );
    }

    const enriched = items.map((ev) => {
      const uid = normalizeUserIdFromPhone(ev?.from || "");


      const fromSess = uid ? sessionMap.get(uid) : null;

      const firstName =
        String(ev?.firstName || "").trim() || String(fromSess?.firstName || "").trim();
      const lastName =
        String(ev?.lastName || "").trim() || String(fromSess?.lastName || "").trim();

      return {
        ...ev,
        userId: uid || ev.userId,
        firstName,
        lastName,
      };
    });

    res.json({ items: enriched });
  } catch (e) {
    console.error("dnc events error:", e?.message || e);
    res.status(500).json({ error: "Failed to load DNC events" });
  }
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
    {
      $set: {
        keyword,
        enabled: true,
        updatedAt: new Date(),
      },
      $setOnInsert: {
        createdAt: new Date(),
      },
    },
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
  const keyword = decodeURIComponent(req.params.keyword).trim().toLowerCase();

  if (typeof req.body?.enabled !== "boolean") {
    return res.status(400).json({ error: "enabled_boolean_required" });
  }

  await dncKeywords.updateOne(
    { keyword },
    { $set: { enabled: req.body.enabled, updatedAt: new Date() } }
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


app.get("/api/email/template", async (req, res) => {
  const doc = await getEmailBlastTemplate();
  res.json({
    subject: doc.subject || "",
    body: doc.body || "",
    signatureText: doc.signatureText || "",
    signatureHtml: doc.signatureHtml || "",
    flyerImageUrl: doc.flyerImageUrl || "",
    enabled: !!doc.enabled,
  });
});



app.post("/api/email/template", async (req, res) => {
  const subject = String(req.body?.subject || "").trim();
  const body = String(req.body?.body || "").trim();
  const signatureText = String(req.body?.signatureText || "").trim();
  const signatureHtml = String(req.body?.signatureHtml || "").trim();
  const flyerImageUrl = String(req.body?.flyerImageUrl || "").trim();
  const enabled = req.body?.enabled;

  if (!subject) return res.status(400).json({ error: "subject_required" });
  if (!body) return res.status(400).json({ error: "body_required" });

  await emailSettings.updateOne(
    { key: "email_blast_template" },
    {
      $set: {
        subject,
        body,
        signatureText,
        signatureHtml,
        flyerImageUrl,
        enabled: typeof enabled === "boolean" ? enabled : true,
        updatedAt: new Date(),
      },
      $setOnInsert: { key: "email_blast_template", createdAt: new Date() },
    },
    { upsert: true }
  );

  res.json({ ok: true });
});

app.get("/api/email/stats", requireAdmin, async (req, res) => {
  try {
    // counts by status (queued/processing/sent/failed/etc.)
    const rows = await emailQueue.aggregate([
      { $group: { _id: "$status", n: { $sum: 1 } } },
    ]).toArray();

    const byStatus = {};
    for (const r of rows) byStatus[String(r._id || "unknown")] = r.n;

    // last 24h sent/failed
    const TZ = process.env.STATS_TZ || "America/Los_Angeles";
    const sinceToday = startOfTodayInTZ(TZ);

    const [sentToday, failedToday] = await Promise.all([
      emailQueue.countDocuments({ status: "sent", sentAt: { $gte: sinceToday } }),
      emailQueue.countDocuments({ status: "failed", failedAt: { $gte: sinceToday } }),
    ]);

    // ✅ lifetime totals from Email_Batches
    const batchAgg = await emailBatches.aggregate([
      {
        $group: {
          _id: null,
          lifetimeTotal: { $sum: { $ifNull: ["$total", 0] } },
          lifetimeAccepted: { $sum: { $ifNull: ["$accepted", 0] } },
          lifetimeRejected: { $sum: { $ifNull: ["$rejected", 0] } },
          lifetimeSent: { $sum: { $ifNull: ["$sent", 0] } },
          lifetimeFailed: { $sum: { $ifNull: ["$failed", 0] } },
          lifetimeSkipped: { $sum: { $ifNull: ["$skipped", 0] } },
        },
      },
    ]).toArray();

    const b = batchAgg?.[0] || {
      lifetimeTotal: 0,
      lifetimeAccepted: 0,
      lifetimeRejected: 0,
      lifetimeSent: 0,
      lifetimeFailed: 0,
      lifetimeSkipped: 0,
    };

    res.json({
      ok: true,
      byStatus,
      totals: {
        sent: byStatus.sent || 0,
        failed: byStatus.failed || 0,
        queued: byStatus.queued || 0,
        processing: byStatus.processing || 0,
        skipped: byStatus.skipped || 0,
      },
      today: { sent: sentToday, failed: failedToday, since: sinceToday.toISOString(), tz: TZ },

      // ✅ NEW: lifetime rollups (from batches)
      lifetime: {
        total: b.lifetimeTotal,
        accepted: b.lifetimeAccepted,
        rejected: b.lifetimeRejected,
        sent: b.lifetimeSent,
        failed: b.lifetimeFailed,
        skipped: b.lifetimeSkipped,
      },
    });
  } catch (e) {
    console.error("email stats error:", e?.message || e);
    res.status(500).json({ ok: false, error: "failed_to_load_stats" });
  }
});



app.post("/api/email/batch", async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    const spacingMs = Math.max(200, Number(req.body?.spacingMs || 800));
    const dailyCap = Math.max(1, Number(req.body?.dailyCap || 5000));
    const burstSizeFinal = Math.max(1, Number(req.body?.burstSize || 100));
    const burstPauseMsFinal = Math.max(0, Number(req.body?.burstPauseMs || 60 * 1000)); // 10 min default

    if (!rows.length) return res.status(400).json({ error: "rows_required" });
    if (rows.length > 20000) return res.status(400).json({ error: "too_many_rows" });

    const tpl = await getEmailBlastTemplate();
    if (!tpl.enabled) return res.status(409).json({ error: "email_template_disabled" });

    // ✅ fetch enabled senders ONCE
    const enabledSenders = await emailSenders
      .find({ enabled: true })
      .sort({ createdAt: 1 })
      .toArray();

    if (!enabledSenders.length) return res.status(409).json({ error: "no_enabled_senders" });

    const senderMode = String(req.body?.senderMode || "auto"); // "auto" | "single"
    const senderIdRaw = String(req.body?.senderId || "").trim();

    let singleSender = null;
    if (senderMode === "single") {
      if (!ObjectId.isValid(senderIdRaw)) return res.status(400).json({ error: "invalid_senderId" });
      singleSender = enabledSenders.find((s) => String(s._id) === senderIdRaw) || null;
      if (!singleSender) return res.status(409).json({ error: "sender_not_found_or_disabled" });
    }

    const batchId = new ObjectId().toString();
    const now = Date.now();
    const proto = (req.headers["x-forwarded-proto"] || req.protocol || "https").split(",")[0].trim();
    const host = (req.headers["x-forwarded-host"] || req.get("host") || "").split(",")[0].trim();
    const baseUrl = `${proto}://${host}`;

    // ✅ Create batch immediately with "validating" state
    await emailBatches.insertOne({
      _id: batchId,
      status: "validating", // ✅ NEW
      total: rows.length,
      accepted: 0,
      rejected: 0,
      sent: 0,
      failed: 0,
      skipped: 0,
      spacingMs,
      dailyCap,
      burstSize: burstSizeFinal,
      burstPauseMs: burstPauseMsFinal,
      senderMode,
      senderId: singleSender ? String(singleSender._id) : null,
      createdAt: new Date(),
      updatedAt: new Date(),
      startAtMs: now,
      nextSeq: 0,
      baseUrl,
    });

    // ✅ Respond immediately so frontend never "Failed to fetch"
    // Frontend already polls /api/email/batch/:batchId, so it will update.
    res.status(202).json({
      ok: true,
      batchId,
      total: rows.length,
      accepted: 0,
      rejected: 0,
      status: "validating",
      spacingMs,
      dailyCap,
      senderMode,
      senderId: singleSender ? String(singleSender._id) : null,
      burstSize: burstSizeFinal,
      burstPauseMs: burstPauseMsFinal,
    });

    // ------------------------------------------------------------
    // Background validation + enqueue (keeps your old perfect filtering)
    // ------------------------------------------------------------
    (async () => {
      let accepted = 0;
      let rejected = 0;
      const rejects = [];
      const queueDocs = [];

      // simple concurrency limiter (no new deps)
      const CONCURRENCY = 8; // tune: 4-12 is usually fine
      let idx = 0;

async function handleRow(i) {
        const r = rows[i] || {};
        const emailRaw = r.email || r.Email || "";
        const email = normalizeEmail(emailRaw);
        if (await isEmailOptedOut(email)) {
        rejected++;
        rejects.push({ index: i, email, reason: "opted_out" });
        return;
      }

        if (!email) {
          rejected++;
          rejects.push({ index: i, email: String(emailRaw), reason: "invalid_email" });
          return;
        }

        // stop if daily cap hit
        if (accepted >= dailyCap) return;

        // ✅ Mailgun deliverability validation BEFORE enqueue (same as your old behavior)
        let v;
        try {
          v = await getEmailValidationCached(email);
        } catch (e) {
          rejected++;
          rejects.push({
            index: i,
            email,
            reason: "validation_error",
            detail: e?.message || String(e),
          });
          return;
        }

        if (!v?.ok) {
          rejected++;
          rejects.push({
            index: i,
            email,
            reason: `not_sendable:${String(v?.payload?.result || "unknown")}`,
            risk: String(v?.payload?.risk || ""),
            why: Array.isArray(v?.payload?.reason) ? v.payload.reason : [],
            action: v?.decision?.action || "skip",
          });
          return;
        }

        const firstName = String(r.firstName || r.firstname || "").trim();
        const lastName = String(r.lastName || r.lastname || "").trim();

        const subject = renderEmail(tpl.subject, { firstName, lastName });
        const mainText = renderEmail(tpl.body, { firstName, lastName });
        const unsubscribeUrl = buildUnsubscribeUrl(baseUrl, email);
        const footer = buildComplianceFooter(unsubscribeUrl);
        const safeFlyerUrl = String(tpl.flyerImageUrl || "").trim();
        const flyerHtml = safeFlyerUrl
          ? `<div style="margin-top:12px;"><img src="${safeFlyerUrl}" alt="Leads Locker Room" style="max-width:600px;width:100%;height:auto;border:0;" /></div>`
          : "";

        // ✅ pick sender
        const sender = singleSender || enabledSenders[accepted % enabledSenders.length];

        const domainDoc = await getEmailDomainById(sender.domainId);
        if (!domainDoc) {
          rejected++;
          rejects.push({ index: i, email, reason: "sender_domain_missing_or_disabled" });
          return;
        }

        const fromEmail = String(sender.email || domainDoc.fromEmail || "").trim();
        const fromName = String(sender.name || domainDoc.fromName || "").trim();
        const from = buildFromValue(fromName, fromEmail);

        const replyTo = String(sender.replyTo || "").trim();

        // ✅ sender signature overrides template signature
        const sigTextFinal = renderEmail(
          (sender.signatureText || sender.signature || tpl.signatureText || ""),
          { firstName, lastName }
        );

        const sigHtmlFinal = renderEmail(
          (sender.signatureHtml || tpl.signatureHtml || ""),
          { firstName, lastName }
        );

        const sigHtmlFallback = sigHtmlFinal || textSigToHtml(sigTextFinal);

        // const textBody = [mainText, sigTextFinal].filter(Boolean).join("\n\n").trim();

        // const htmlBody =
        //   `<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.45;color:#111;">` +
        //   `${escapeHtmlToParagraphs(mainText)}` +
        //   `${sigHtmlFallback ? `<div style="margin-top:16px;">${sigHtmlFallback}</div>` : ""}` +
        //   `${flyerHtml}` +
        //   `</div>`;

        const textBody = [mainText, sigTextFinal, footer.text].filter(Boolean).join("\n\n").trim();

        const htmlBody =
          `<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.45;color:#111;">` +
          `${escapeHtmlToParagraphs(mainText)}` +
          `${sigHtmlFallback ? `<div style="margin-top:16px;">${sigHtmlFallback}</div>` : ""}` +
          `${flyerHtml}` +
          `${footer.html}` +
          `</div>`;

        const burstIndex = Math.floor(accepted / burstSizeFinal);
        const offsetInBurst = accepted % burstSizeFinal;

        const runAtMs =
          now +
          (burstIndex * burstPauseMsFinal) +
          ((burstIndex * burstSizeFinal + offsetInBurst) * spacingMs);

        const runAt = new Date(runAtMs);

        const trackingId = `${batchId}:${email}:${accepted}`;

        queueDocs.push({
          trackingId,
          batchId,
          email,
          firstName,
          lastName,
          subject,
          textBody,
          htmlBody,
          senderId: String(sender._id),
          domainId: String(domainDoc._id),
          from,
          replyTo,
          status: "queued",
          runAt,
          createdAt: new Date(),
          unsubscribeUrl,
        });

        accepted++;
      }

      // worker pool
      const workers = Array.from({ length: CONCURRENCY }).map(async () => {
        while (idx < rows.length && accepted < dailyCap) {
          const i = idx++;
          await handleRow(i);
        }
      });

      await Promise.all(workers);

      if (queueDocs.length) {
        await emailQueue.insertMany(queueDocs, { ordered: false });
      }

      await emailBatches.updateOne(
        { _id: batchId },
        {
          $set: {
            accepted,
            rejected,
            status: "queued", // ✅ done validating, now queued for worker send
            updatedAt: new Date(),
            nextSeq: accepted,
          },
          // optional: keep some reject samples on the batch doc (handy for debugging)
          $setOnInsert: {},
        }
      );

      // optional: if you want, you can store a small sample of rejects on batch:
      // await emailBatches.updateOne({ _id: batchId }, { $set: { rejectSample: rejects.slice(0, 50) } });

    })().catch(async (err) => {
      console.error("email/batch background error:", err?.message || err);
      try {
        await emailBatches.updateOne(
          { _id: batchId },
          { $set: { status: "failed", error: String(err?.message || err), updatedAt: new Date() } }
        );
      } catch {}
    });

  } catch (e) {
    console.error("email/batch error:", e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});


app.get("/api/email/batch/:batchId", requireAdmin, async (req, res) => {
  const doc = await emailBatches.findOne({ _id: req.params.batchId });
  if (!doc) return res.status(404).json({ error: "not_found" });
  res.json(doc);
});

app.get("/api/email/failed", requireAdmin, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);
  const items = await emailQueue.find({ status: "failed" }).sort({ failedAt: -1 }).limit(limit).toArray();
  res.json({ items });
});

app.post("/webhook/email/inbound", upload.any(), async (req, res) => {

  console.log("📩 inbound parsed", {
  contentType: req.headers["content-type"],
  bodyKeys: Object.keys(req.body || {}),
  fromEmail_guess: extractFromEmail(req.body || {}),
  subject: (req.body?.subject || req.body?.Subject || "").toString().slice(0, 120),
  textLen: (extractEmailText(req.body || {}) || "").length,
});

  try {
    if (req.query.token !== process.env.EMAIL_INBOUND_WEBHOOK_SECRET) {
      return res.status(401).send("unauthorized");
    }

    const payload = req.body || {};
    if (shouldIgnoreEmail(payload)) return res.status(200).json({ ok: true, ignored: "auto" });

    // Provider fields differ. You must map these:
    const fromEmail = extractFromEmail(payload);
    const toEmail = extractToEmail(payload); // ✅ the sender inbox that got the inbound

    const subject = String(payload.subject || payload.Subject || payload["subject"] || "").trim();

    const text = extractEmailText(payload);
    const messageId = extractMessageId(payload);

    const trackingId = messageId || `email:${fromEmail}:${Date.now()}`;

    if (!fromEmail || !text) return res.status(200).json({ ok: true, ignored: "missing_fields" });

    // dedupe
    try {
      await emailWebhookEvents.insertOne({ trackingId, createdAt: new Date() });
    } catch (e) {
      if (String(e?.code) === "11000") return res.status(200).json({ ok: true, deduped: true });
      throw e;
    }

    const userId = normalizeEmailUserId(fromEmail);

    const existing = await emailSessions.findOne({ userId });

    // opt-out guard (email)
    if (existing?.optedOut) return res.status(200).json({ ok: true, opted_out: true });

    // optional unsubscribe keyword detection
    const lower = text.toLowerCase();
    if (lower.includes("unsubscribe") || lower.includes("stop")) {
      await emailSessions.updateOne(
        { userId },
        {
          $setOnInsert: { userId, createdAt: new Date() },
          $set: { email: fromEmail, optedOut: true, optedOutAt: new Date(), updatedAt: new Date() },
          $push: { history: { $each: [{ role: "user", content: text, meta: { subject }, createdAt: new Date() }], $slice: -50 } },
        },
        { upsert: true }
      );
      return res.status(200).json({ ok: true, opted_out: true });
    }

    // Save inbound into Email_Sessions
    await emailSessions.updateOne(
      { userId },
      {
        $setOnInsert: { userId, createdAt: new Date() },
        $set: {
          email: fromEmail,
          updatedAt: new Date(),
          lastInboundAt: new Date(),
        },
        $push: {
          history: {
            $each: [{ role: "user", content: text, meta: { subject, messageId }, createdAt: new Date() }],
            $slice: -50,
          },
        },
      },
      { upsert: true }
    );

    // Enqueue reply job (same pattern as SMS)
await emailReplyJobs.updateOne(
  { trackingId },
  {
    $setOnInsert: {
      trackingId,
      userId,
      toEmail: fromEmail,          // ✅ the customer email (where to reply)
      inboundToEmail: toEmail || "", // ✅ which of YOUR inboxes received it
      subject,
      messageId,
      text,
      status: "queued",
      runAt: new Date(Date.now() + 10_000),
      createdAt: new Date(),
    },
  },
  { upsert: true }
);



    return res.status(200).json({ ok: true, scheduled: true });
  } catch (e) {
    console.error("email inbound fatal:", e?.message || e);
    return res.status(200).json({ ok: false });
  }
});

app.get("/api/email/reply-jobs", requireAdmin, async (req, res) => {
  const limit = Math.min(Number(req.query.limit || 50), 200);
  const items = await emailReplyJobs.find({})
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();
  res.json({ items });
});

app.get("/api/email/senders", requireAdmin, async (req, res) => {
  const items = await emailSenders
    .find({})
    .sort({ enabled: -1, createdAt: -1 })
    .toArray();
  res.json({ items });
});

app.post("/api/email/senders/bulk", requireAdmin, async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!rows.length) return res.status(400).json({ error: "rows_required" });
    if (rows.length > 5000) return res.status(400).json({ error: "too_many_rows" });

    let created = 0;
    let updated = 0;
    let rejected = 0;
    const rejects = [];

    for (let i = 0; i < rows.length; i++) {
      const r = rows[i] || {};

      const name = String(r.name || r.Name || "").trim();
      const email = normalizeEmail(r.email || r.Email || "");
      const replyTo = normalizeEmail(r.replyTo || r["reply_to"] || r["reply-to"] || r.ReplyTo || "") || "";
      const enabled = parseBooleanLoose(r.enabled ?? r.Enabled, true);
      const signatureText = String(
        r.signatureText ||
        r.signature ||
        r["signature_text"] ||
        r["signature text"] ||
        ""
      ).trim();
      const signatureHtml = String(
        r.signatureHtml ||
        r["signature_html"] ||
        r["signature html"] ||
        ""
      ).trim();

      if (!email) {
        rejected++;
        rejects.push({ index: i, email: String(r.email || r.Email || ""), reason: "invalid_email" });
        continue;
      }

      const existing = await emailSenders.findOne(
        { email },
        { projection: { _id: 1 } }
      );

      await emailSenders.updateOne(
        { email },
        {
          $set: {
            name,
            email,
            replyTo,
            enabled,
            signatureText,
            signatureHtml,
            updatedAt: new Date(),
          },
          $setOnInsert: { createdAt: new Date() },
        },
        { upsert: true }
      );

      if (existing?._id) updated++;
      else created++;
    }

    return res.json({
      ok: true,
      total: rows.length,
      created,
      updated,
      rejected,
      rejects: rejects.slice(0, 100),
    });
  } catch (e) {
    console.error("bulk email senders error:", e?.message || e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

app.post("/api/email/batch/append", async (req, res) => {
  try {
    const batchId = String(req.body?.batchId || "").trim();
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!batchId) return res.status(400).json({ error: "batchId_required" });
    if (!rows.length) return res.status(400).json({ error: "rows_required" });

    const batch = await emailBatches.findOne({ _id: batchId });
    if (!batch) return res.status(404).json({ error: "batch_not_found" });

    // lock-ish: prevent appending to already-done batches if you want
    if (["failed"].includes(batch.status)) {
      return res.status(409).json({ error: "batch_not_appendable" });
    }

    // bump total immediately so UI reflects progress while validating
    await emailBatches.updateOne(
      { _id: batchId },
      { $inc: { total: rows.length }, $set: { updatedAt: new Date() } }
    );

    // respond fast so frontend never times out
    res.status(202).json({ ok: true, batchId, appended: rows.length, status: "validating" });

    // background validate + enqueue (same vibe as your current async path)
    (async () => {
      const tpl = await getEmailBlastTemplate();
      if (!tpl.enabled) throw new Error("email_template_disabled");

      const enabledSenders = await emailSenders.find({ enabled: true }).sort({ createdAt: 1 }).toArray();
      if (!enabledSenders.length) throw new Error("no_enabled_senders");

      // use existing batch config if present; otherwise fall back
      const spacingMs = Math.max(200, Number(batch.spacingMs || 800));
      const dailyCap = Math.max(1, Number(batch.dailyCap || 5000));
      const burstSizeFinal = Math.max(1, Number(batch.burstSize || 100));
      const burstPauseMsFinal = Math.max(0, Number(batch.burstPauseMs || 60_000));

      let accepted = 0;
      let rejected = 0;
      const queueDocs = [];

      // IMPORTANT: to avoid runAt collisions, base scheduling on a monotonic sequence
      // We'll reserve a seq range up-front
      const reserve = await emailBatches.findOneAndUpdate(
        { _id: batchId },
        { $inc: { nextSeq: rows.length }, $set: { updatedAt: new Date() } },
        { returnDocument: "before" }
      );
      const startSeq = Number(reserve?.value?.nextSeq || 0);

      for (let i = 0; i < rows.length; i++) {
        const r = rows[i] || {};
        const emailRaw = r.email || r.Email || "";
        const email = normalizeEmail(emailRaw);
        if (await isEmailOptedOut(email)) {
          rejected++;
          continue;
        }
        if (!email) { rejected++; continue; }

        // enforce daily cap based on "sent plan size" if you want strict caps
        // (keep it simple here)
        if ((batch.accepted || 0) + accepted >= dailyCap) break;

        let v;
        try { v = await getEmailValidationCached(email); }
        catch { rejected++; continue; }

        if (!v?.ok) { rejected++; continue; }

        const firstName = String(r.firstName || r.firstname || "").trim();
        const lastName = String(r.lastName || r.lastname || "").trim();

        const subject = renderEmail(tpl.subject, { firstName, lastName });
        const mainText = renderEmail(tpl.body, { firstName, lastName });
        const unsubscribeUrl = buildUnsubscribeUrl(String(batch.baseUrl || ""), email);
        const footer = buildComplianceFooter(unsubscribeUrl);
        const sender = enabledSenders[(startSeq + i) % enabledSenders.length];
        const fromEmail = String(sender.email || "").trim();
        const fromName = String(sender.name || "").trim();
        const from = fromName ? `${fromName} <${fromEmail}>` : fromEmail;
        const replyTo = String(sender.replyTo || "").trim();

        const sigTextFinal = renderEmail((sender.signatureText || sender.signature || tpl.signatureText || ""), { firstName, lastName });
        const sigHtmlFinal = renderEmail((sender.signatureHtml || tpl.signatureHtml || ""), { firstName, lastName });
        const sigHtmlFallback = sigHtmlFinal || textSigToHtml(sigTextFinal);

        const textBody = [mainText, sigTextFinal, footer.text].filter(Boolean).join("\n\n").trim();

        const htmlBody =
          `<div style="font-family:Arial,sans-serif;font-size:14px;line-height:1.45;color:#111;">` +
          `${escapeHtmlToParagraphs(mainText)}` +
          `${sigHtmlFallback ? `<div style="margin-top:16px;">${sigHtmlFallback}</div>` : ""}` +
          `${footer.html}` +
          `</div>`;
        const seq = startSeq + i;
        const burstIndex = Math.floor(seq / burstSizeFinal);
        const offsetInBurst = seq % burstSizeFinal;

        const base = Number(batch.startAtMs || Date.now());

        const runAtMs =
          base +
          (burstIndex * burstPauseMsFinal) +
          ((burstIndex * burstSizeFinal + offsetInBurst) * spacingMs);

        queueDocs.push({
          trackingId: `${batchId}:${email}:${seq}`,
          batchId,
          email,
          firstName,
          lastName,
          subject,
          textBody,
          htmlBody,
          senderId: String(sender._id),
          from,
          replyTo,
          status: "queued",
          runAt: new Date(runAtMs),
          createdAt: new Date(),
          unsubscribeUrl,
        });

        accepted++;
      }

      if (queueDocs.length) await emailQueue.insertMany(queueDocs, { ordered: false });

      await emailBatches.updateOne(
        { _id: batchId },
        {
          $inc: { accepted, rejected },
          $set: { status: "queued", updatedAt: new Date() },
        }
      );
    })().catch(async (err) => {
      await emailBatches.updateOne(
        { _id: batchId },
        { $set: { status: "failed", error: String(err?.message || err), updatedAt: new Date() } }
      );
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

app.post("/api/email/senders", requireAdmin, async (req, res) => {
  const name = String(req.body?.name || "").trim();
  const email = normalizeEmail(req.body?.email);
  const replyTo = normalizeEmail(req.body?.replyTo) || "";
  const enabled = typeof req.body?.enabled === "boolean" ? req.body.enabled : true;
  const signatureText = String(req.body?.signatureText || req.body?.signature || "").trim();
  const signatureHtml = String(req.body?.signatureHtml || "").trim();
  const domainId = String(req.body?.domainId || "").trim();

  if (!email) return res.status(400).json({ error: "invalid_email" });
  if (!domainId || !ObjectId.isValid(domainId)) {
    return res.status(400).json({ error: "valid_domainId_required" });
  }

  await emailSenders.updateOne(
    { email },
    {
      $set: {
        name,
        email,
        replyTo,
        enabled,
        signatureText,
        signatureHtml,
        domainId,
        updatedAt: new Date(),
      },
      $setOnInsert: { createdAt: new Date() },
    },
    { upsert: true }
  );

  res.json({ ok: true });
});


app.patch("/api/email/senders/:id", requireAdmin, async (req, res) => {
  const id = String(req.params.id || "");
  if (!ObjectId.isValid(id)) return res.status(400).json({ error: "invalid_id" });

  const update = { updatedAt: new Date() };

  if (typeof req.body?.enabled === "boolean") update.enabled = req.body.enabled;
  if (typeof req.body?.name === "string") update.name = req.body.name.trim();
  if (typeof req.body?.replyTo === "string") update.replyTo = normalizeEmail(req.body.replyTo) || "";
  if (typeof req.body?.signatureText === "string") update.signatureText = req.body.signatureText.trim();
  if (typeof req.body?.signatureHtml === "string") update.signatureHtml = req.body.signatureHtml.trim();

  await emailSenders.updateOne({ _id: new ObjectId(id) }, { $set: update });
  res.json({ ok: true });
});


app.delete("/api/email/senders/:id", requireAdmin, async (req, res) => {
  const id = String(req.params.id || "");
  if (!ObjectId.isValid(id)) return res.status(400).json({ error: "invalid_id" });

  await emailSenders.deleteOne({ _id: new ObjectId(id) });
  res.json({ ok: true });
});

app.get("/api/email/domains", requireAdmin, async (req, res) => {
  try {
    const items = await emailDomains.find({})
      .sort({ isDefault: -1, enabled: -1, createdAt: -1 })
      .toArray();

    res.json({
      items: items.map((d) => ({
        ...d,
        apiKeyMasked: "",
        apiKeyEnc: undefined,
      })),
    });
  } catch (e) {
    console.error("GET /api/email/domains error:", e?.message || e);
    res.status(500).json({ error: "failed_to_load_domains" });
  }
});

app.post("/api/email/domains", requireAdmin, async (req, res) => {
  try {
    const label = String(req.body?.label || "").trim();
    const domain = String(req.body?.domain || "").trim().toLowerCase();
    const fromName = String(req.body?.fromName || "").trim();
    const fromEmail = normalizeEmail(req.body?.fromEmail);
    const emailFrom = normalizeEmail(req.body?.emailFrom) || fromEmail || "";
    const smtpUser = normalizeEmail(req.body?.smtpUser) || fromEmail || "";
    const mailgunBaseUrl = String(req.body?.mailgunBaseUrl || "https://api.mailgun.net").trim();
    const enabled = typeof req.body?.enabled === "boolean" ? req.body.enabled : true;
    const isDefault = !!req.body?.isDefault;

    if (!label) return res.status(400).json({ error: "label_required" });
    if (!domain) return res.status(400).json({ error: "domain_required" });
    if (!fromEmail) return res.status(400).json({ error: "fromEmail_required" });

    if (isDefault) {
      await emailDomains.updateMany({}, { $set: { isDefault: false, updatedAt: new Date() } });
    }

    const doc = {
      label,
      domain,
      fromName,
      fromEmail,
      emailFrom,
      smtpUser,
      mailgunBaseUrl,
      enabled,
      isDefault,
      updatedAt: new Date(),
    };

    await emailDomains.updateOne(
      { domain },
      {
        $set: doc,
        $setOnInsert: { createdAt: new Date() },
      },
      { upsert: true }
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error("POST /api/email/domains error:", e?.message || e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

app.patch("/api/email/domains/:id", requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || "");
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: "invalid_id" });
    }

    const update = { updatedAt: new Date() };

    if (typeof req.body?.label === "string") {
      update.label = req.body.label.trim();
    }

    if (typeof req.body?.domain === "string") {
      update.domain = req.body.domain.trim().toLowerCase();
    }

    if (typeof req.body?.fromName === "string") {
      update.fromName = req.body.fromName.trim();
    }

    if (typeof req.body?.fromEmail === "string") {
      const v = normalizeEmail(req.body.fromEmail);
      if (!v) return res.status(400).json({ error: "invalid_fromEmail" });
      update.fromEmail = v;
    }

    if (typeof req.body?.emailFrom === "string") {
      update.emailFrom = normalizeEmail(req.body.emailFrom) || "";
    }

    if (typeof req.body?.smtpUser === "string") {
      update.smtpUser = normalizeEmail(req.body.smtpUser) || "";
    }

    if (typeof req.body?.mailgunBaseUrl === "string") {
      update.mailgunBaseUrl = req.body.mailgunBaseUrl.trim() || "https://api.mailgun.net";
    }

    if (typeof req.body?.enabled === "boolean") {
      update.enabled = req.body.enabled;
    }

    if (typeof req.body?.isDefault === "boolean") {
      if (req.body.isDefault) {
        await emailDomains.updateMany({}, { $set: { isDefault: false, updatedAt: new Date() } });
      }
      update.isDefault = req.body.isDefault;
    }

    await emailDomains.updateOne(
      { _id: new ObjectId(id) },
      { $set: update }
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error("PATCH /api/email/domains/:id error:", e?.message || e);
    return res.status(500).json({ error: e?.message || "failed" });
  }
});

app.delete("/api/email/domains/:id", requireAdmin, async (req, res) => {
  try {
    const id = String(req.params.id || "");
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: "invalid_id" });

    await emailDomains.deleteOne({ _id: new ObjectId(id) });
    res.json({ ok: true });
  } catch (e) {
    console.error("DELETE /api/email/domains/:id error:", e?.message || e);
    res.status(500).json({ error: "failed_to_delete_domain" });
  }
});


// Health
app.get("/health", (req, res) => res.json({ ok: true }));

// Boot
initDb()
  .then(async () => {
    await verifySmtp();
    startReplyWorker();
    startOutboundWorker();
    startEmailWorker();
    startEmailReplyWorker();
    app.listen(PORT, () => console.log(`✅ Server listening on :${PORT}`));
  })
  .catch((e) => {
    console.error("DB init failed:", e);
    process.exit(1);
  });
