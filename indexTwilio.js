const express = require('express');
const bodyParser = require('body-parser');
const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
const axios = require('axios');
const Twilio = require('twilio');
const cors = require('cors');

dotenv.config();

const app = express();
app.use(cors());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

const uri = `mongodb+srv://${process.env.MONGODB_USER}:${process.env.MONGODB_PASSWORD}@bedrockdev.j1dmahl.mongodb.net/test`;
const clientDB = new MongoClient(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
});

let faqCollection;
let flaggedKeywordCollection;
let parametersCollection;
let ticketsCollection;
let archivesCollection;
let activePhoneNumbers = [];
let viewingConversations = [];
let rolesCollection;

async function connectToDB() {
    try {
        await clientDB.connect();
        const db = clientDB.db("HRMAI");
        faqCollection = db.collection("Knowledge_Base");
        flaggedKeywordCollection = db.collection("Flaged_Keywords");
        parametersCollection = db.collection("Parameters");
        ticketsCollection = db.collection("Front_End_Tickets");
        archivesCollection = db.collection("Front_End_Archives");
        rolesCollection = db.collection("User_Roles");
        await faqCollection.createIndex({ keywords: 'text', question: 'text', answer: 'text' });
        await parametersCollection.createIndex({ keywords: 'text', question: 'text', answer: 'text' });
    } catch (error) {
        console.error('Failed to connect to the database', error);
        throw error;
    }
}

connectToDB().catch(console.error);

const accountSid = process.env.TWILIO_ACCOUNT_SID;
const authToken = process.env.TWILIO_AUTH_TOKEN;
const twilioNumber = '+19512618044';
const twilioClient = new Twilio(accountSid, authToken);
const openaiApiKey = process.env.OPENAI_API_KEY;

function truncateAtSentence(text, maxLength) {
    if (text.length <= maxLength) return text;

    const sentenceEnd = /[\.\?\!](\s|$)/g;
    let lastValidIndex = -1;
    let match;

    while ((match = sentenceEnd.exec(text)) !== null) {
        if (match.index + 1 <= maxLength) {
            lastValidIndex = match.index + 1;
        } else {
            break;
        }
    }

    if (lastValidIndex === -1) {
        const lastSpaceIndex = text.lastIndexOf(' ', maxLength);
        if (lastSpaceIndex > 0) {
            lastValidIndex = lastSpaceIndex;
        } else {
            lastValidIndex = maxLength;
        }
    }

    return text.substring(0, lastValidIndex).trim();
}

function normalizePhoneNumber(phoneNumber) {
    return phoneNumber.replace(/\D/g, '');
}

app.get('/serviceStatus', (req, res) => {
    const { phoneNumber } = req.query;
    if (phoneNumber) {
        const normalizedPhoneNumber = normalizePhoneNumber(phoneNumber);
        const isActive = activePhoneNumbers.includes(normalizedPhoneNumber);
        console.log(`Checking status for ${normalizedPhoneNumber}: ${isActive}`);
        res.json({ isActive });
    } else {
        res.status(400).json({ error: 'Phone number is required' });
    }
});

app.post('/toggleService', (req, res) => {
    const { phoneNumber, isActive } = req.body;

    if (phoneNumber && typeof isActive === 'boolean') {
        const normalizedPhoneNumber = normalizePhoneNumber(phoneNumber);
        console.log(`Received request to set status for ${normalizedPhoneNumber} to ${isActive}`);

        if (isActive) {
            if (!activePhoneNumbers.includes(normalizedPhoneNumber)) {
                activePhoneNumbers.push(normalizedPhoneNumber);
                console.log(`Added ${normalizedPhoneNumber} to activePhoneNumbers`);
            } else {
                console.log(`${normalizedPhoneNumber} is already in the activePhoneNumbers list`);
            }
        } else {
            if (activePhoneNumbers.includes(normalizedPhoneNumber)) {
                activePhoneNumbers = activePhoneNumbers.filter(num => num !== normalizedPhoneNumber);
                console.log(`Removed ${normalizedPhoneNumber} from activePhoneNumbers`);
            } else {
                console.log(`${normalizedPhoneNumber} is not in the activePhoneNumbers list`);
            }
        }

        console.log('Updated active phone numbers:', JSON.stringify(activePhoneNumbers));
        res.status(200).json({ message: `Service for ${normalizedPhoneNumber} is now ${isActive}` });
    } else {
        res.status(400).json({ error: 'Invalid value. Please provide a phone number and a boolean value for isActive.' });
    }
});

app.post('/updateViewingStatus', (req, res) => {
    const { conversationSid, isViewing } = req.body;

    if (conversationSid) {
        if (isViewing) {
            if (!viewingConversations.includes(conversationSid)) {
                viewingConversations.push(conversationSid);
            }
        } else {
            viewingConversations = viewingConversations.filter(sid => sid !== conversationSid);
        }

        console.log('Updated viewing conversations list:', JSON.stringify(viewingConversations));
        res.status(200).json({ message: 'Viewing status updated successfully' });
    } else {
        res.status(400).json({ error: 'Invalid value. Please provide a conversationSid.' });
    }
});

app.get('/viewingStatus', (req, res) => {
    res.json({ viewingConversations });
});

app.post('/sms', async (req, res) => {
    const incomingMsg = req.body.Body;
    const fromNumber = req.body.From;

    const normalizedFromNumber = normalizePhoneNumber(fromNumber);

    console.log(`Received message from ${fromNumber}: ${incomingMsg}`);

    // You can handle SMS responses here

    console.log(`Message sent to ${fromNumber}`);
    res.type('text/xml').send('<Response></Response>');
});

async function participantExists(conversationSid, identity) {
    try {
        let participants = await twilioClient.conversations.v1.conversations(conversationSid).participants.list();
        return participants.some(participant => participant.identity === identity);
    } catch (error) {
        console.error(`Error checking participant existence: ${error.message}`);
        throw error;
    }
}

app.post('/updateConversationName', async (req, res) => {
    const { conversationSid, chatName } = req.body;

    if (!conversationSid || !chatName) {
      return res.status(400).json({ error: 'Invalid value. Please provide a conversationSid and a chatName.' });
    }

    try {
      const conversation = await twilioClient.conversations.v1.conversations(conversationSid).fetch();
      await conversation.update({ uniqueName: chatName });
      console.log("updated chatName successfully")
      res.status(200).json({ message: 'Conversation chat name updated successfully' });
    } catch (error) {
      console.error('Failed to update conversation chat name', error);
      res.status(500).json({ error: 'Failed to update conversation chat name' });
    }
});



app.post('/webhook', async (req, res) => {
    let eventType = req.body.EventType;
    let conversationSid = req.body.ConversationSid;
    let participantSid = req.body.ParticipantSid;
    try {

        const roleEmails = await rolesCollection.find({}, { projection: { email: 1, _id: 0 } }).toArray();
        const emails = roleEmails.map(doc => doc.email);
        const emailSet = new Set(emails);

        if (eventType === 'onParticipantAdded') {
            let phoneNumber = req.body["MessagingBinding.Address"];
            await twilioClient.conversations.v1.conversations(conversationSid)
                .update({ friendlyName: phoneNumber });


            if (!(await participantExists(conversationSid, phoneNumber))) {
                await twilioClient.conversations.v1.conversations(conversationSid)
                    .participants
                    .create({ identity: phoneNumber, attributes: JSON.stringify({ name: phoneNumber }) });
            }
            if (!(await participantExists(conversationSid, twilioNumber))) {
                await twilioClient.conversations.v1.conversations(conversationSid)
                    .participants
                    .create({ identity: twilioNumber, attributes: JSON.stringify({ name: twilioNumber }) });
            }

            for (const email of emails) {
                if (!(await participantExists(conversationSid, email))) {
                    await twilioClient.conversations.v1.conversations(conversationSid)
                        .participants
                        .create({ identity: email, attributes: JSON.stringify({ name: email }) });
                }
            }

        } else if (eventType === 'onMessageAdded') {
            let phoneNumber = req.body.Author;
            let incomingMsg = req.body.Body;
            const normalizedFromNumber = normalizePhoneNumber(phoneNumber);
            const conversation = await twilioClient.conversations.v1.conversations(conversationSid).fetch();

            const flaggedKeywords = await flaggedKeywordCollection.find().toArray();
            const keywordList = flaggedKeywords.map(keyword => keyword.question.toLowerCase());

            const prompt = `
            The following is a list of flagged keywords:
            ${keywordList.join(", ")}

            Determine stricktly if the following message contains a flagged keyword and requires human assistance:
            "${incomingMsg}"
            Respond with "yes" if it requires human help and "no" if it doesn't.
            `;

            const response = await axios.post("https://api.openai.com/v1/chat/completions", {
                model: "gpt-3.5-turbo",
                            messages: [
                                { role: "system", content: prompt },
                                { role: "system", content: incomingMsg },
                                { role: "user", content: incomingMsg }
                            ],
                            temperature: 0.5,
                            max_tokens: 10
            }, {
                headers: { "Authorization": `Bearer ${openaiApiKey}`, "Content-Type": "application/json" },
                timeout: 60000
            });

            const needsHumanHelp = response.data.choices[0].message.content.toLowerCase() === 'yes';

            if (needsHumanHelp) {
                const existingTicket = await ticketsCollection.findOne({ conversationSID: conversationSid });
                if (!existingTicket) {
                    await ticketsCollection.insertOne({ conversationSID: conversationSid });
                    console.log(`Flagged keyword found. Added conversationSID ${conversationSid} to tickets collection.`);
                } else {
                    console.log(`ConversationSID ${conversationSid} already exists in the tickets collection.`);
                }
            }

            if (emailSet.has(phoneNumber)) {
                console.log("Message from web, no reply will be sent.");
            } else if (!activePhoneNumbers.includes(normalizedFromNumber)) {
                try {
                    const [faqs, parameters] = await Promise.all([
                        faqCollection.find({ $text: { $search: incomingMsg } }).toArray(),
                        parametersCollection.find({ $text: { $search: incomingMsg } }).toArray()
                    ]);
                    let replyText = "";

                    if (faqs.length > 0) {
                        let relevantFaqs = faqs.filter(faq => faq.score >= 0.5);
                        if (relevantFaqs.length > 0) {
                            replyText = relevantFaqs[0].answer;
                        }
                    }

                    if (!replyText && parameters.length > 0) {
                        let relevantParams = parameters.filter(param => param.score >= 0.5);
                        if (relevantParams.length > 0) {
                            replyText = relevantParams[0].answer;
                        }
                    }

                    if (!replyText) {
                            const allFaqContext = faqs.map(faq => ` ${faq.question} : ${faq.answer}`).join('\n');
                            const allParamContext = parameters.map(param => `${param.question}`).join('\n');
                            const response = await axios.post("https://api.openai.com/v1/chat/completions", {
                                model: "gpt-3.5-turbo",
                                messages: [
                                    { role: "system", content: `Provide a clear, concise, and informative response within 150 tokens and ensure the response ends at a complete sentence and use ${allFaqContext} and ${allParamContext} as context, knowledge base when reply but don't respond if the incoming message is only emoji.` },
                                    { role: "system", content: `${allFaqContext}` },
                                    { role: "user", content: incomingMsg }
                                ],
                                temperature: 0.1,
                                max_tokens: 150
                            }, {
                                headers: { "Authorization": `Bearer ${openaiApiKey}`, "Content-Type": "application/json" },
                                timeout: 60000
                            });
                        replyText = response.data.choices[0].message.content;

                        await twilioClient.conversations.v1.conversations(conversationSid)
                            .messages
                            .create({ body: replyText, author: twilioNumber });
                        console.log(replyText);
                    }
                } catch (error) {
                    console.error('Failed to process request:', error);
                    return res.status(500).send('Failed to process your message.');
                }
            } else {
                console.log("Chatbot is inactive");
            }

            if (!conversation.friendlyName) {
                await twilioClient.conversations.v1.conversations(conversationSid)
                    .update({ friendlyName: phoneNumber });
            }


            if (!(await participantExists(conversationSid, phoneNumber))) {
                await twilioClient.conversations.v1.conversations(conversationSid)
                    .participants
                    .create({ identity: phoneNumber, attributes: JSON.stringify({ name: phoneNumber }) });
            }
            if (!(await participantExists(conversationSid, twilioNumber))) {
                await twilioClient.conversations.v1.conversations(conversationSid)
                    .participants
                    .create({ identity: twilioNumber, attributes: JSON.stringify({ name: twilioNumber }) });
            }

            for (const email of emails) {
                if (!(await participantExists(conversationSid, email))) {
                    await twilioClient.conversations.v1.conversations(conversationSid)
                        .participants
                        .create({ identity: email, attributes: JSON.stringify({ name: email }) });
                }
            }

            try {
                await archivesCollection.deleteOne({ conversationSID: conversationSid });
            } catch (err) {
                console.error(err);
                return res.status(500).json({ message: 'An error occurred while restoring conversation', error: err.message });
            }
        }
        return res.status(200).json({ message: `Service for webhook working` });
    } catch (error) {
        console.error(`Webhook error: ${error.message}`);
        return res.sendStatus(500);
    }
});

let PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});