import { MongoClient } from "mongodb";

const uri = process.env.MONGODB_URI;
const dbName = process.env.MONGODB_DB || "llrchatbot";

function normalizeUserIdFromPhone(phoneE164) {
  return String(phoneE164 || "").replace(/\D/g, "");
}

async function main() {
  const mongo = new MongoClient(uri);
  await mongo.connect();
  const db = mongo.db(dbName);

  const dncEvents = db.collection("DNC_Events");
  const sessions = db.collection("SMS_AI_Sessions");

  const cursor = dncEvents.find({
    $or: [{ firstName: { $in: [null, ""] } }, { lastName: { $in: [null, ""] } }],
  });

  let updated = 0;

  while (await cursor.hasNext()) {
    const ev = await cursor.next();
    const uid = normalizeUserIdFromPhone(ev.from);
    const sess = await sessions.findOne({ userId: uid }, { projection: { firstName: 1, lastName: 1 } });

    const firstName = String(sess?.firstName || "").trim();
    const lastName = String(sess?.lastName || "").trim();

    if (!firstName && !lastName) continue;

    await dncEvents.updateOne(
      { _id: ev._id },
      { $set: { userId: uid, firstName, lastName } }
    );

    updated++;
  }

  console.log("Backfill updated:", updated);
  await mongo.close();
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
