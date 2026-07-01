import "dotenv/config";
import express from "express";
import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import mongoose from "mongoose";
import { S3Client, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";

const app = express();
app.use(express.json());

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(path.join(dataDir, "quiz_data_fallback.sqlite"));
db.exec("CREATE TABLE IF NOT EXISTS user_progress (session_id TEXT PRIMARY KEY, language TEXT, current_step INTEGER, consecutive_correct INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS current_quiz (session_id TEXT PRIMARY KEY, q_type TEXT, question TEXT, options TEXT, image_url TEXT, answer TEXT, explanation TEXT, success_msg TEXT, error_msg TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS user_info (session_id TEXT PRIMARY KEY, data TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS served_questions (session_id TEXT, quiz_id TEXT, PRIMARY KEY(session_id, quiz_id))");
db.exec("CREATE TABLE IF NOT EXISTS used_persons (session_id TEXT, person_name TEXT)");

const SERVER_URL = process.env.SERVER_URL;
const MONGO_URI = process.env.MONGO_URI;

const cfAccounts = [];
if (process.env.CF_ACCOUNT_ID && process.env.CF_TOKEN) {
  cfAccounts.push({ id: process.env.CF_ACCOUNT_ID, token: process.env.CF_TOKEN });
}
for (let i = 2; i <= 20; i++) {
  if (process.env[`CF_ACCOUNT_ID_${i}`] && process.env[`CF_TOKEN_${i}`]) {
    cfAccounts.push({ id: process.env[`CF_ACCOUNT_ID_${i}`], token: process.env[`CF_TOKEN_${i}`] });
  }
}

let globalRequestCounter = 0;

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

mongoose.connect(MONGO_URI, { dbName: "quiz" }).catch(e => e);

const baseQuizSchema = new mongoose.Schema({
  lang: String,
  level: { type: Number, default: 1 },
  qType: String,
  question: String,
  options: [String],
  imageUrl: String,
  answer: String,
  explanation: String,
  successMsg: String,
  errorMsg: String
});
const BaseQuiz = mongoose.model("BaseQuiz", baseQuizSchema, "quiz");

const progressSchema = new mongoose.Schema({ sessionId: String, language: String, currentStep: Number, consecutiveCorrect: Number });
const Progress = mongoose.model("Progress", progressSchema);

const userSchema = new mongoose.Schema({ sessionId: String, data: String });
const UserInfo = mongoose.model("UserInfo", userSchema);

const logger = (level, ctx, msg) => console.log(`[${level}] [${ctx}] ${msg}`);
const logInfo = (ctx, msg) => logger("INFO", ctx, msg);
const logWarn = (ctx, msg) => logger("WARN", ctx, msg);
const logError = (ctx, msg) => logger("ERROR", ctx, msg);

function getKeyFromUrl(url) {
  if (!url) return null;
  const index = url.indexOf("uploads/");
  if (index !== -1) {
    return url.substring(index);
  }
  return null;
}

async function deleteFromR2(key) {
  if (!key) return;
  try {
    await s3.send(new DeleteObjectCommand({
      Bucket: process.env.R2_BUCKET,
      Key: key
    }));
  } catch (e) {}
}

function calculateSimilarity(str1, str2) {
  if (!str1 || !str2) return 0;
  const set1 = new Set(str1.toLowerCase().split(/\s+/));
  const set2 = new Set(str2.toLowerCase().split(/\s+/));
  const intersection = new Set([...set1].filter(x => set2.has(x)));
  const union = new Set([...set1, ...set2]);
  if (union.size === 0) return 0;
  return intersection.size / union.size;
}

function isSimilarToExisting(newText, existingItems) {
  if (!newText || !existingItems || existingItems.length === 0) return { similar: false, pct: 0, matchedText: "" };
  for (const item of existingItems) {
    if (!item.question) continue;
    const sim = calculateSimilarity(newText, item.question);
    if (sim > 0.20) {
      return { similar: true, pct: Math.round(sim * 100), matchedText: item.question };
    }
  }
  return { similar: false, pct: 0, matchedText: "" };
}

async function getRandomFromJsonFile(lang, level) {
  try {
    const p = path.join(process.cwd(), "lang", `${lang}.json`);
    if (fs.existsSync(p)) {
      const content = fs.readFileSync(p, "utf-8");
      const data = JSON.parse(content);
      let filtered = data.filter(d => (d.level || d.niveau || 1) === level);
      if (filtered.length === 0) {
        filtered = data;
      }
      if (filtered.length > 0) {
        return filtered[Math.floor(Math.random() * filtered.length)];
      }
    }
  } catch(e) {}
  return null;
}

async function syncJsonToMongo() {
  logInfo("SYSTEM", "Starting JSON synchronisation, threshold 20%");
  const langs = ["en", "fr", "es", "ht"];
  for (const l of langs) {
    const p = path.join(process.cwd(), "lang", `${l}.json`);
    if (fs.existsSync(p)) {
      try {
        const content = fs.readFileSync(p, "utf-8");
        const data = JSON.parse(content);
        const existingItems = await BaseQuiz.find({ lang: l }).limit(500).lean().catch(() => []);
        for (const item of data) {
          const itemLevel = item.level || item.niveau || 1;
          const simCheck = isSimilarToExisting(item.question, existingItems);
          if (!simCheck.similar || simCheck.pct <= 70) {
            const exists = await BaseQuiz.findOne({ lang: l, explanation: item.explanation }).catch(() => true);
            if (!exists) {
              await BaseQuiz.create({ lang: l, level: itemLevel, ...item }).catch(() => {});
            }
          }
        }
      } catch (e) {}
    }
  }
}

function parseAIJsonResponse(rawResponse, expectedKeys) {
  const rawText = typeof rawResponse === "string" ? rawResponse : JSON.stringify(rawResponse || {});
  const firstBracket = rawText.indexOf('{');
  const lastBracket = rawText.lastIndexOf('}');
  const firstSquare = rawText.indexOf('[');
  const lastSquare = rawText.lastIndexOf(']');
  
  let extractedJson = "";
  let isArrayExpected = expectedKeys.includes("ARRAY_FORMAT_ONLY");

  if (isArrayExpected) {
    if (firstSquare !== -1 && lastSquare !== -1 && lastSquare > firstSquare) {
      extractedJson = rawText.substring(firstSquare, lastSquare + 1);
    } else {
      throw new Error(`Array structure '[...]' not found. Raw: ${rawText}`);
    }
  } else {
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      extractedJson = rawText.substring(firstBracket, lastBracket + 1);
    } else {
      throw new Error(`JSON structure '{...}' not found. Raw: ${rawText}`);
    }
  }

  let parsedData = null;
  try {
    parsedData = JSON.parse(extractedJson);
  } catch (err) {
    throw new Error(`SyntaxError during JSON parsing: ${err.message}`);
  }

  if (!isArrayExpected) {
    for (const key of expectedKeys) {
      if (parsedData[key] === undefined || parsedData[key] === null) {
        throw new Error(`Missing required JSON key '${key}'`);
      }
    }
  }

  return parsedData;
}

async function cfAIFetch(model, bodyPayload) {
  let lastError = "No CF credentials configured";
  for (const cred of cfAccounts) {
    const url = `https://api.cloudflare.com/client/v4/accounts/${cred.id}/ai/run/${model}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 25000);
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Authorization": `Bearer ${cred.token}`, "Content-Type": "application/json" },
        body: JSON.stringify(bodyPayload),
        signal: controller.signal
      });
      clearTimeout(timeout);
      
      const json = await response.json();
      
      const isRateLimited = response.status === 429;
      const isQuotaError = json.errors && json.errors.length > 0 && json.errors[0].message.toLowerCase().includes("allocation");
      
      if (isRateLimited || isQuotaError) {
        lastError = `Limit reached for account ${cred.id}`;
        logWarn("AI_FALLBACK", `Switching account. Reason: ${lastError}`);
        continue;
      }
      
      if (json.success && json.result) {
        return json.result;
      }
      lastError = "Invalid response format from CF";
    } catch (e) {
      clearTimeout(timeout);
      lastError = e.message;
    }
  }
  logError("AI_FALLBACK", "All CF accounts failed.");
  throw new Error(`All CF accounts failed. Last error: ${lastError}`);
}

async function runAI(messages, max_tokens) {
  return await cfAIFetch("@cf/meta/llama-3.1-8b-instruct", { messages, max_tokens });
}

async function executeBackgroundMassGeneration(isTrigger) {
  const processName = isTrigger ? "Trigger" : "Auto";
  logInfo("BG_GEN", `Initiation processus: ${processName}`);

  let targetLevels = [1, 2, 3];
  if (!isTrigger) {
    try {
      const rows = db.prepare("SELECT current_step FROM user_progress ORDER BY RANDOM() LIMIT 7").all();
      if (rows.length > 0) {
        targetLevels = rows.map(r => r.current_step);
      }
    } catch (e) {}
  }

  const langs = ["en", "fr", "es", "ht"];
  const qTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK"];

  for (const lang of langs) {
    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[lang] || "English";
    
    for (let i = 0; i < 7; i++) {
      try {
        const level = targetLevels[Math.floor(Math.random() * targetLevels.length)] || 1;
        const qType = qTypes[Math.floor(Math.random() * qTypes.length)];

        let seedText = "General Knowledge";
        try {
          const seedItem = await BaseQuiz.aggregate([{ $match: { lang: lang } }, { $sample: { size: 1 } }]);
          if (seedItem && seedItem.length > 0) {
            seedText = seedItem[0].question;
          }
        } catch(e) {}

        const systemInstruction = "You are a strict JSON data generator. Output ONLY raw valid JSON. No markdown tags, no formatting, no extra text.";
        
        const prompt = qType === "MCQ" 
          ? `Create a brand new unique MCQ quiz question in ${langName}. Difficulty Level: ${level}. Use this existing question as theme inspiration: "${seedText}". Return ONLY a raw valid JSON object matching this schema: {"level": ${level}, "lang": "${lang}", "qType": "MCQ", "question": "Write the MCQ question here", "options": ["Choice A", "Choice B", "Choice C", "Choice D"], "answer": "Exact correct choice", "explanation": "Detailed explanation", "successMsg": "Encouraging success feedback", "errorMsg": "Constructive correction feedback"}`
          : qType === "TRUE_FALSE"
          ? `Create a brand new unique True or False statement in ${langName}. Difficulty Level: ${level}. Use this existing question as theme inspiration: "${seedText}". Return ONLY a raw valid JSON object matching this schema: {"level": ${level}, "lang": "${lang}", "qType": "TRUE_FALSE", "question": "Write the statement statement", "options": ["True", "False"], "answer": "True", "explanation": "Detailed explanation", "successMsg": "Encouraging success feedback", "errorMsg": "Constructive correction feedback"}`
          : `Create a brand new unique Fill-in-the-blank question in ${langName}. Difficulty Level: ${level}. Use this existing question as theme inspiration: "${seedText}". Use ______ for the blank. Return ONLY a raw valid JSON object matching this schema: {"level": ${level}, "lang": "${lang}", "qType": "FILL_BLANK", "question": "Write the question with fill blank", "options": [], "answer": "Expected exact answer", "explanation": "Detailed explanation", "successMsg": "Encouraging success feedback", "errorMsg": "Constructive correction feedback"}`;

        const aiResponse = await runAI([
          { role: "system", content: systemInstruction },
          { role: "user", content: prompt }
        ], 1000);

        try {
          const parsed = parseAIJsonResponse(aiResponse.response, ["question", "answer", "explanation", "qType"]);
          const existingItems = await BaseQuiz.find({ lang: lang }).limit(250).lean().catch(() => []);
          const simCheck = isSimilarToExisting(parsed.question, existingItems);
          
          if (!simCheck.similar || simCheck.pct <= 70) {
            const exists = await BaseQuiz.findOne({ lang: lang, question: parsed.question }).catch(() => true);
            if (!exists) {
              await BaseQuiz.create({
                lang: lang,
                level: parsed.level || level,
                qType: parsed.qType || qType,
                question: parsed.question,
                options: Array.isArray(parsed.options) ? parsed.options : [],
                answer: parsed.answer,
                explanation: parsed.explanation,
                successMsg: parsed.successMsg || "Correct!",
                errorMsg: parsed.errorMsg || "Incorrect."
              }).catch(() => {});
              logInfo("BG_GEN", `Nouvelle question sauvegardee. Similarite: ${simCheck.pct}%`);
            }
          } else {
            logInfo("BG_GEN", `Annulee: Similarite de ${simCheck.pct}% superieure a 70%`);
          }
        } catch (parseError) {
          logError("BG_GEN", `Echec AI Parse: ${parseError.message}`);
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {
        logError("BG_GEN", `Erreur inattendue : ${e.message}`);
        break;
      }
    }
  }
}

mongoose.connection.once("open", () => {
  syncJsonToMongo();
  executeBackgroundMassGeneration(false);
  setInterval(() => executeBackgroundMassGeneration(false), 70 * 60 * 1000);
});

async function getProgress(sessionId) {
  try {
    const p = await Progress.findOne({ sessionId: sessionId });
    if (p) return { language: p.language, current_step: p.currentStep, consecutive_correct: p.consecutiveCorrect };
  } catch (e) {}
  const fallback = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(sessionId);
  if (fallback) return { language: fallback.language, current_step: fallback.current_step, consecutive_correct: fallback.consecutive_correct };
  return null;
}

async function saveProgress(sessionId, lang, step, consec) {
  try {
    await Progress.findOneAndUpdate({ sessionId: sessionId }, { language: lang, currentStep: step, consecutiveCorrect: consec }, { upsert: true });
  } catch (e) {}
  db.prepare("REPLACE INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, ?, ?, ?)").run(sessionId, lang, step, consec);
}

async function getCurrentQuiz(sessionId) {
  return db.prepare("SELECT * FROM current_quiz WHERE session_id = ?").get(sessionId);
}

async function saveCurrentQuiz(sessionId, qType, question, optionsStr, imageUrl, answer, explanation, success_msg, error_msg) {
  db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation, success_msg, error_msg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(sessionId, qType, question, optionsStr, imageUrl, answer, explanation, success_msg, error_msg);
}

async function clearCurrentQuiz(sessionId) {
  db.prepare("DELETE FROM current_quiz WHERE session_id = ?").run(sessionId);
}

async function saveUserInfo(sessionId, dataString) {
  try {
    await UserInfo.findOneAndUpdate({ sessionId: sessionId }, { data: dataString }, { upsert: true });
  } catch (e) {}
  db.prepare("REPLACE INTO user_info (session_id, data) VALUES (?, ?)").run(sessionId, dataString);
}

async function executeMode0PureDB(randomItem) {
  if (!randomItem) throw new Error("Item introuvable");
  return {
    parsed: { question: randomItem.question, options: randomItem.options, answer: randomItem.answer },
    randomType: randomItem.qType || "MCQ",
    imgUrl: randomItem.imageUrl || null,
    finalSuccess: randomItem.successMsg || null,
    finalError: randomItem.errorMsg || null,
    finalExplanation: randomItem.explanation || null
  };
}

async function executeMode1ImproveExisting(randomItem, langName) {
  if (!randomItem) throw new Error("Item introuvable");
  const prompt = `Improve this quiz question slightly making it more clear without changing the actual answer. Language: ${langName}. Do NOT generate explanations, success messages, or error messages. Original Question: "${randomItem.question}". Return ONLY a valid JSON object matching this schema: {"question":"string","options":["string","string","string","string"],"answer":"string"}`;
  const sysStrict = "You are a strict JSON API generator. Output ONLY raw valid JSON. No conversational text, no formatting, no markdown code blocks.";
  const aiResponse = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: prompt }], 1000);
  const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
  return {
    parsed: parsed,
    randomType: randomItem.qType || "MCQ",
    imgUrl: null,
    finalSuccess: "",
    finalError: "",
    finalExplanation: ""
  };
}

async function executeMode2CreateSimilar(randomItem, langName) {
  if (!randomItem) throw new Error("Item introuvable");
  const prompt = `Create a completely new quiz question in the EXACT same style and general topic as this one. Language: ${langName}. Do NOT generate explanations. Original Question: "${randomItem.question}". Return ONLY a valid JSON object matching this schema: {"question":"string","options":["string","string","string","string"],"answer":"string"}`;
  const sysStrict = "You are a strict JSON API generator. Output ONLY raw valid JSON. No conversational text, no formatting, no markdown code blocks.";
  const aiResponse = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: prompt }], 1000);
  const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
  return {
    parsed: parsed,
    randomType: randomItem.qType || "MCQ",
    imgUrl: null,
    finalSuccess: "",
    finalError: "",
    finalExplanation: ""
  };
}

async function executeMode3PureAIGeneration(session_id, language, langName) {
  const sysStrict = "You are a strict JSON API generator. Output ONLY raw valid JSON. No conversational text, no formatting, no markdown code blocks.";
  const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
  const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
  
  if (randomType === "IDENTITY_IMAGE") {
    const usedRes = db.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").all(session_id);
    const usedList = usedRes.map(r => r.person_name);
    let subjectName = "Eiffel Tower";
    
    const personPrompt = `Return ONLY a valid JSON array of 5 visually distinct subjects (famous historical figures, famous monuments, or iconic cities). Exclude these: ${usedList.join(",")}. Example exact output format: ["Subject1", "Subject2", "Subject3", "Subject4", "Subject5"]`;
    const nameResp = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: personPrompt }], 300);
    const candidates = parseAIJsonResponse(nameResp.response, ["ARRAY_FORMAT_ONLY"]);
    
    if (Array.isArray(candidates) && candidates.length > 0) {
       for (const name of candidates) {
          if (!usedList.includes(name)) {
             subjectName = name;
             break;
          }
       }
    }
    
    db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(session_id, subjectName);
    const imagePrompt = `A 100% authentic photograph or high quality image of ${subjectName}, ultra-realistic documentary style, lifelike, highly detailed, no digital art.`;
    
    const aiImgResponse = await cfAIFetch("@cf/black-forest-labs/flux-1-schnell", { prompt: imagePrompt });
    const base64Image = aiImgResponse.image;
    
    if (!base64Image) throw new Error("Image base64 manquante de Flux AI");
    
    const buffer = Buffer.from(base64Image, "base64");
    const filename = `img_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
    const r2Key = `uploads/${filename}`;
    
    await s3.send(new PutObjectCommand({
      Bucket: process.env.R2_BUCKET,
      Key: r2Key,
      Body: buffer,
      ContentType: "image/png"
    }));
    
    const imgUrl = `${process.env.R2_PUBLIC_URL}/${r2Key}`;
    const questionTexts = { en: "Who is this person or what is this?", fr: "Qui est cette personne, quelle est cette ville, ou quel est ce monument ?", es: "¿Quién es esta persona, qué es esta ciudad, o qué es este monument?", ht: "Kiyès moun sa, ki vil sa, oswa ki moniman sa?" };
    
    return {
      parsed: { question: questionTexts[language] || questionTexts.en, options: [], answer: subjectName },
      randomType: "IDENTITY_IMAGE",
      imgUrl: imgUrl,
      finalSuccess: "",
      finalError: "",
      finalExplanation: ""
    };
  }
  
  if (randomType === "MCQ") {
    const mcqPrompt = `Create a brand new unique Multiple Choice Question (MCQ). Topic: General Knowledge. Language: ${langName}. Return ONLY a raw JSON object matching this schema: {"question":"Your question?","options":["Choice A","Choice B","Choice C","Choice D"],"answer":"Choice B"}`;
    const aiResponse = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: mcqPrompt }], 1000);
    const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    return { parsed, randomType, imgUrl: null, finalSuccess: "", finalError: "", finalExplanation: "" };
  }
  
  if (randomType === "TRUE_FALSE") {
    const tfPrompt = `Create a brand new unique True or False statement. Topic: General Knowledge. Language: ${langName}. Return ONLY a raw JSON object matching this schema: {"question":"Your statement.","options":["True","False"],"answer":"False"}`;
    const aiResponse = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: tfPrompt }], 1000);
    const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    return { parsed, randomType, imgUrl: null, finalSuccess: "", finalError: "", finalExplanation: "" };
  }
  
  const fbPrompt = `Create a brand new unique Fill-in-the-blank question. Topic: General Knowledge. Language: ${langName}. Use ______ for the blank space. Return ONLY a raw JSON object matching this schema: {"question":"The capital of France is ______.","options":[],"answer":"Paris"}`;
  const aiResponse = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: fbPrompt }], 1000);
  const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
  return { parsed, randomType: "FILL_BLANK", imgUrl: null, finalSuccess: "", finalError: "", finalExplanation: "" };
}

app.use((req, res, next) => {
  globalRequestCounter++;
  if (globalRequestCounter >= 70) {
    globalRequestCounter = 0;
    executeBackgroundMassGeneration(true).catch(e => e);
  }

  const origin = req.headers.origin;
  const authHeader = req.headers.authorization;
  let isAllowed = false;
  let allowedOrigin = "*";
  if (origin && origin.endsWith(".adamdh7.org")) {
    isAllowed = true;
    allowedOrigin = origin;
  } else if (authHeader === "Bearer adamdh7") {
    isAllowed = true;
    if (origin) {
      allowedOrigin = origin;
    }
  }
  res.setHeader("Access-Control-Allow-Origin", allowedOrigin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }
  if (!isAllowed) {
    return res.status(403).json({ error: "Forbidden: Origin or Token not allowed" });
  }
  next();
});

app.get("/local-image/:filename", (req, res) => {
  const filePath = path.join(dataDir, req.params.filename);
  if (fs.existsSync(filePath)) {
    res.sendFile(filePath);
  } else {
    res.status(404).send("Image Not Found");
  }
});

app.post("/user-info", async (req, res) => {
  try {
    const body = req.body;
    const session_id = body.DH7?.trim();
    if (!session_id) {
      return res.status(400).json({ error: "DH7 required" });
    }
    const userData = {
      level: body.level || null,
      nivo: body.nivo || null,
      TFID: body.TFID || null
    };
    const dataString = JSON.stringify(userData);
    await saveUserInfo(session_id, dataString);
    let progress = await getProgress(session_id);
    let newStep = body.level !== undefined && body.level !== null ? parseInt(body.level) : (progress ? progress.current_step : 1);
    let newConsec = body.nivo !== undefined && body.nivo !== null ? parseInt(body.nivo) : (progress ? progress.consecutive_correct : 0);
    await saveProgress(session_id, progress ? progress.language : 'en', newStep, newConsec);
    return res.json({ success: true, message: "User info saved successfully" });
  } catch (e) {
    return res.json({ success: false, error: "Database error" });
  }
});

app.post("/quizz", async (req, res) => {
  try {
    const body = req.body;
    const session_id = body.session_id?.trim();
    if (!session_id) return res.status(400).json({ error: "session_id required" });

    logInfo("QUIZ", `Nouvelle requete utilisateur: ${session_id}`);

    const rawLang = body.lang?.trim();
    let lang = rawLang ? rawLang.toLowerCase() : null;
    const incomingLevel = body.level;

    let progress = await getProgress(session_id);
    if (!progress) {
      const default_lang = lang || "en";
      const start_step = incomingLevel || 1;
      await saveProgress(session_id, default_lang, start_step, 0);
      progress = { language: default_lang, current_step: start_step, consecutive_correct: 0 };
    } else {
      let updated = false;
      if (lang && lang !== progress.language) {
        progress.language = lang;
        updated = true;
      }
      if (incomingLevel !== undefined && incomingLevel !== progress.current_step) {
        progress.current_step = incomingLevel;
        updated = true;
      }
      if (updated) {
        await saveProgress(session_id, progress.language, progress.current_step, progress.consecutive_correct);
      }
    }

    const current_step_num = progress.current_step;
    const language = progress.language;
    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[language] || "English";
    
    const servedRows = db.prepare("SELECT quiz_id FROM served_questions WHERE session_id = ?").all(session_id);
    const servedIds = servedRows.map(r => r.quiz_id).filter(id => mongoose.Types.ObjectId.isValid(id)).map(id => new mongoose.Types.ObjectId(id));

    let matchCriteria = { lang: language, level: current_step_num };
    if (servedIds.length > 0) {
      matchCriteria._id = { $nin: servedIds };
    }

    let dbItems = await BaseQuiz.aggregate([{ $match: matchCriteria }, { $sample: { size: 1 } }]).catch(() => []);
    
    if (dbItems.length === 0) {
      let broadCriteria = { lang: language };
      if (servedIds.length > 0) {
        broadCriteria._id = { $nin: servedIds };
      }
      dbItems = await BaseQuiz.aggregate([{ $match: broadCriteria }, { $sample: { size: 1 } }]).catch(() => []);
    }

    if (dbItems.length === 0 && servedIds.length > 0) {
      db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
      dbItems = await BaseQuiz.aggregate([{ $match: { lang: language, level: current_step_num } }, { $sample: { size: 1 } }]).catch(() => []);
      if (dbItems.length === 0) {
        dbItems = await BaseQuiz.aggregate([{ $match: { lang: language } }, { $sample: { size: 1 } }]).catch(() => []);
      }
    }

    if (dbItems.length === 0) {
      const randomJsonRecord = await getRandomFromJsonFile(language, current_step_num);
      if (randomJsonRecord) {
        dbItems = [randomJsonRecord];
      }
    }

    let randomItem = dbItems.length > 0 ? dbItems[0] : null;
    if (randomItem && randomItem._id) {
      db.prepare("INSERT OR IGNORE INTO served_questions (session_id, quiz_id) VALUES (?, ?)").run(session_id, randomItem._id.toString());
    }

    let availableStrategies = [0, 1, 2, 3];
    for (let i = availableStrategies.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [availableStrategies[i], availableStrategies[j]] = [availableStrategies[j], availableStrategies[i]];
    }

    const modeNames = {
      0: "MongoDB Pure Mode",
      1: "AI Improved MongoDB Mode",
      2: "AI Replicated MongoDB Mode",
      3: "Pure AI Generation Mode"
    };

    let parsed = null;
    let randomType = "MCQ";
    let imgUrl = null;
    let finalSuccess = null;
    let finalError = null;
    let finalExplanation = null;
    let success = false;

    while (availableStrategies.length > 0 && !success) {
      const strategy = availableStrategies.shift();
      const currentStrategyName = modeNames[strategy];
      logInfo("QUIZ", `Tentative strategie: ${currentStrategyName}`);

      if (!randomItem && strategy !== 3) {
        continue;
      }

      try {
        let resultObj;
        if (strategy === 0) {
          resultObj = await executeMode0PureDB(randomItem);
        } else if (strategy === 1) {
          resultObj = await executeMode1ImproveExisting(randomItem, langName);
        } else if (strategy === 2) {
          resultObj = await executeMode2CreateSimilar(randomItem, langName);
        } else if (strategy === 3) {
          resultObj = await executeMode3PureAIGeneration(session_id, language, langName);
        }

        parsed = resultObj.parsed;
        randomType = resultObj.randomType;
        imgUrl = resultObj.imgUrl;
        finalSuccess = resultObj.finalSuccess;
        finalError = resultObj.finalError;
        finalExplanation = resultObj.finalExplanation;
        success = true;
        
        logInfo("QUIZ", `Succes avec la strategie ${currentStrategyName}`);
      } catch (e) {
        logWarn("QUIZ", `Echec strategie ${currentStrategyName} : ${e.message}`);
        success = false;
      }
    }

    if (!success) {
      logError("QUIZ", "Toutes les strategies ont echoue. Application du fallback de securite.");
      if (randomItem) {
        parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer };
        randomType = randomItem.qType || "MCQ";
        imgUrl = randomItem.imageUrl || null;
        finalSuccess = randomItem.successMsg || null;
        finalError = randomItem.errorMsg || null;
        finalExplanation = randomItem.explanation || null;
      } else {
        parsed = { question: "What is the capital of France?", options: ["Paris", "London", "Berlin"], answer: "Paris" };
        randomType = "MCQ";
        finalSuccess = "Correct!";
        finalError = "Incorrect.";
        finalExplanation = "Paris is the capital of France.";
      }
    }

    if (imgUrl && !imgUrl.startsWith("http")) {
      let resolvedPath = null;
      const possiblePaths = [
        path.join(process.cwd(), imgUrl),
        path.join(process.cwd(), "data", imgUrl),
        path.join(process.cwd(), "data", path.basename(imgUrl)),
        path.join(process.cwd(), "imaj", path.basename(imgUrl))
      ];
      for (const p of possiblePaths) {
        if (fs.existsSync(p)) {
          resolvedPath = p;
          break;
        }
      }

      if (resolvedPath) {
        try {
          const fileBuffer = fs.readFileSync(resolvedPath);
          const ext = path.extname(resolvedPath).toLowerCase();
          const mimeType = ext === ".png" ? "image/png" : (ext === ".jpg" || ext === ".jpeg") ? "image/jpeg" : "application/octet-stream";
          const uniqueFilename = `${Date.now()}_${crypto.randomUUID().split('-')[0]}_${path.basename(resolvedPath)}`;
          const r2Key = `uploads/${uniqueFilename}`;
          await s3.send(new PutObjectCommand({
            Bucket: process.env.R2_BUCKET,
            Key: r2Key,
            Body: fileBuffer,
            ContentType: mimeType
          }));
          imgUrl = `${process.env.R2_PUBLIC_URL}/${r2Key}`;
        } catch (e) {}
      }
    }

    const safeOptions = Array.isArray(parsed.options) ? parsed.options : [];
    const optionsStr = JSON.stringify(safeOptions);

    await saveCurrentQuiz(session_id, randomType, parsed.question, optionsStr, imgUrl, parsed.answer, finalExplanation || "", finalSuccess || "", finalError || "");

    if (imgUrl) {
      const scheduledKey = getKeyFromUrl(imgUrl);
      if (scheduledKey) {
        setTimeout(() => {
          deleteFromR2(scheduledKey).catch(() => {});
        }, 7 * 60 * 1000);
      }
    }

    const quizData = {
      current_step: current_step_num,
      consecutive_correct: progress.consecutive_correct,
      language: progress.language,
      needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct),
      type: randomType,
      question: parsed.question
    };
    if (imgUrl) quizData.image_url = imgUrl;
    if (safeOptions.length > 0) quizData.options = safeOptions;

    return res.json(quizData);

  } catch (e) {
    await saveCurrentQuiz(req.body.session_id || "default", "TRUE_FALSE", "Error loading question. True to continue.", JSON.stringify(["True","False"]), null, "True", "System recovery.", "", "");
    return res.json({ type: "TRUE_FALSE", question: "Error loading question. True to continue.", options: ["True", "False"], error_msg: e.message });
  }
});

app.post("/validate", async (req, res) => {
  try {
    const body = req.body;
    const session_id = body.session_id?.trim();
    const user_answer = body.user_answer?.trim() || "";
    if (!session_id || !user_answer) return res.status(400).json({ error: "session_id and user_answer required" });

    logInfo("VALIDATE", `Soumission par ${session_id} : ${user_answer}`);

    const current = await getCurrentQuiz(session_id);
    if (!current) return res.status(400).json({ error: "No active quiz" });

    if (current.image_url) {
      const activeKey = getKeyFromUrl(current.image_url);
      if (activeKey) {
        deleteFromR2(activeKey).catch(() => {});
      }
    }

    let progress = await getProgress(session_id);
    if (!progress) {
      progress = { language: "en", current_step: 1, consecutive_correct: 0 };
    }

    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";
    const cleanUser = user_answer.toLowerCase().trim();
    const cleanAnswer = current.answer ? current.answer.toLowerCase().trim() : "";

    let isCorrect = false;

    if (cleanAnswer !== "") {
      if (cleanAnswer.length <= 3) {
        if (cleanUser === cleanAnswer) {
          isCorrect = true;
        }
      } else {
        if (cleanUser === cleanAnswer || (cleanUser.length >= 3 && (cleanUser.includes(cleanAnswer) || cleanAnswer.includes(cleanUser)))) {
          isCorrect = true;
        }
      }
    }

    let finalFeedback = "";
    const isAiPur = (!current.success_msg && !current.error_msg && !current.explanation);

    if (isAiPur) {
      const sysStrict = "You are a strict JSON data generator. Output ONLY raw valid JSON without markdown formatting.";
      if (isCorrect) {
        const usr = `User answered CORRECTLY to the question: "${current.question}". Correct answer was: "${current.answer}". Write a short success message and brief explanation in ${langName}. Return ONLY a valid JSON object matching this schema: {"successMsg": "encouraging text", "explanation": "detailed reason"}`;
        try {
          const aiResp = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: usr }], 800);
          const parsedFeedback = parseAIJsonResponse(aiResp.response, ["successMsg", "explanation"]);
          finalFeedback = `${parsedFeedback.successMsg}\n\n${parsedFeedback.explanation}`;
        } catch(e) {
          finalFeedback = "Correct!\n\n" + current.answer;
        }
      } else {
        const usr = `User answered INCORRECTLY. Question: "${current.question}". Correct answer: "${current.answer}". User input: "${user_answer}". Write a brief error statement and educational explanation in ${langName}. Return ONLY a valid JSON object matching this schema: {"errorMsg": "mistake feedback", "explanation": "detailed context"}`;
        try {
          const aiResp = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: usr }], 800);
          const parsedFeedback = parseAIJsonResponse(aiResp.response, ["errorMsg", "explanation"]);
          finalFeedback = `${parsedFeedback.errorMsg}\n\n${parsedFeedback.explanation}`;
        } catch(e) {
          finalFeedback = "Incorrect.\n\nLa bonne reponse etait : " + current.answer;
        }
      }
    } else {
      const baseMessage = isCorrect ? current.success_msg : current.error_msg;
      finalFeedback = current.explanation ? `${baseMessage}\n\n${current.explanation}` : baseMessage;
    }

    let new_consec = progress.consecutive_correct;
    let new_step = progress.current_step;

    if (isCorrect) {
      new_consec += 1;
      if (new_consec >= 7) {
        new_step += 1;
        new_consec = 0;
      }
      await clearCurrentQuiz(session_id);
    } else {
      new_consec = 0;
    }

    await saveProgress(session_id, progress.language, new_step, new_consec);

    return res.json({
      correct: isCorrect,
      explanation: finalFeedback,
      consecutive_correct: new_consec,
      needed_for_next_level: Math.max(0, 7 - new_consec),
      current_step: new_step,
      language: progress.language
    });
  } catch (e) {
    return res.json({ correct: false, explanation: "Validation error. Please try again.", consecutive_correct: 0, needed_for_next_level: 7, current_step: 1, language: "en" });
  }
});

app.get("/step", async (req, res) => {
  try {
    const session_id = req.query.session_id;
    if (!session_id) return res.status(400).json({ error: "session_id required" });

    let progress = await getProgress(session_id);
    if (!progress) {
      await saveProgress(session_id, "en", 1, 0);
      progress = { language: "en", current_step: 1, consecutive_correct: 0 };
    }

    return res.json({
      language: progress.language,
      current_step: progress.current_step,
      consecutive_correct: progress.consecutive_correct,
      needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct)
    });
  } catch (e) {
    return res.json({ error: "Internal Server Error", message: e.message });
  }
});

app.post("/jerere", async (req, res) => {
  try {
    const prompt = req.body.prompt?.trim();
    if (!prompt) return res.status(400).json({ error: "No prompt provided" });

    const inputs = { prompt: prompt, num_steps: 4 };

    const aiResponse = await cfAIFetch("@cf/black-forest-labs/flux-1-schnell", inputs);
    
    await new Promise(resolve => setTimeout(resolve, 7));

    if (!aiResponse || !aiResponse.image) {
      throw new Error();
    }

    const binaryString = atob(aiResponse.image);
    const bytes = Uint8Array.from(binaryString, c => c.charCodeAt(0));
    const filename = `img_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
    const blob = new Blob([bytes.buffer], { type: "image/png" });
    const formData = new FormData();
    formData.append("file", blob, filename);

    const uploadRes = await fetch("https://v1bref.onrender.com/upload", { method: "POST", body: formData });
    await new Promise(resolve => setTimeout(resolve, 7));

    let uploadJson = null;
    let uploadText = null;
    try {
      uploadJson = await uploadRes.json();
    } catch (e) {
      try {
        uploadText = await uploadRes.text();
      } catch (e2) {
        uploadText = null;
      }
    }

    const returnedUrl = uploadJson?.url || uploadJson?.link || uploadText || null;
    if (!returnedUrl) {
      throw new Error();
    }

    return res.json({ url: returnedUrl });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logInfo("SYSTEM", `Server running on port ${PORT}`);
});
