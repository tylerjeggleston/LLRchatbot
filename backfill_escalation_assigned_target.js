/* eslint-disable no-console */
require("dotenv").config();
const { MongoClient } = require("mongodb");

function normalizeUserIdFromPhone(phoneE164) {
  return String(phoneE164 || "").replace(/\D/g, "");
}

async function main() {
  const uri = process.env.MONGODB_URI;
  const dbName = process.env.MONGODB_DB || "llrchatbot";
  if (!uri) throw new Error("Missing MONGODB_URI");

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);

  const sessions = db.collection("SMS_AI_Sessions");
  const escalationEvents = db.collection("Escalation_Events");
  const escalationTargets = db.collection("Escalation_Targets");

  const cursor = escalationEvents.find({
    $or: [
      { assignedTarget: { $exists: false } },
      { assignedTarget: null },
      { "assignedTarget.number": { $exists: false } }
    ]
  });

  let scanned = 0;
  let updated = 0;
  let noSession = 0;
  let noAssigned = 0;

  while (await cursor.hasNext()) {
    const ev = await cursor.next();
    scanned++;

    const from = ev?.from;
    const userId = normalizeUserIdFromPhone(from);
    if (!userId) continue;

    const sess = await sessions.findOne(
      { userId },
      { projection: { assignedTarget: 1 } }
    );

    if (!sess) {
      noSession++;
      continue;
    }

    if (!sess.assignedTarget?.number) {
      noAssigned++;
      continue;
    }

    // ensure name is present; if missing, enrich from Escalation_Targets
    let assigned = sess.assignedTarget;
    if (!assigned.name) {
      const t = await escalationTargets.findOne(
        { number: assigned.number },
        { projection: { name: 1, number: 1 } }
      );
      if (t?.name) assigned = { ...assigned, name: t.name };
    }

    await escalationEvents.updateOne(
      { _id: ev._id },
      { $set: { assignedTarget: assigned } }
    );
    updated++;
  }

  console.log({
    scanned,
    updated,
    noSession,
    noAssigned,
  });

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
