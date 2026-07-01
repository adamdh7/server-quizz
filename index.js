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

const cfAccountId = process.env.CF_ACCOUNT_ID;
const cfToken = process.env.CF_TOKEN;
const SERVER_URL = process.env.SERVER_URL;
const MONGO_URI = process.env.MONGO_URI;

let aiLockoutUntil = 0;
let contactCounter = 0;

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
  const s1 = String(str1).toLowerCase().split(/\W+/).filter(w => w.length > 0);
  const s2 = String(str2).toLowerCase().split(/\W+/).filter(w => w.length > 0);
  if (s1.length === 0 && s2.length === 0) return 1;
  let matches = 0;
  const s2Copy = [...s2];
  for (const w of s1) {
    const idx = s2Copy.indexOf(w);
    if (idx !== -1) {
      matches++;
      s2Copy.splice(idx, 1);
    }
  }
  return matches / Math.max(s1.length, s2.length);
}

function getRandomLocalJsonQuestion(lang) {
  console.log("Lecture aleatoire dans les fichiers JSON lourds pour eviter le fallback");
  try {
    const langs = ["en", "fr", "es", "ht"];
    const targetLang = langs.includes(lang) ? lang : langs[Math.floor(Math.random() * langs.length)];
    const p = path.join(process.cwd(), "lang", `${targetLang}.json`);
    if (fs.existsSync(p)) {
      const content = fs.readFileSync(p, "utf-8");
      const data = JSON.parse(content);
      if (data && data.length > 0) {
        console.log("Une question a ete choisie aleatoirement depuis le fichier JSON lourd");
        const item = data[Math.floor(Math.random() * data.length)];
        return {
          question: item.question,
          options: item.options || [],
          answer: item.answer,
          explanation: item.explanation || "",
          qType: item.qType || "MCQ",
          imageUrl: item.imageUrl || null,
          successMsg: item.successMsg || "Correct!",
          errorMsg: item.errorMsg || "Incorrect."
        };
      }
    }
  } catch(e) {}
  return null;
}

async function syncJsonToMongo() {
  console.log("Demarrage de la synchronisation JSON avec verification de la difference (50%)");
  const langs = ["en", "fr", "es", "ht"];
  for (const l of langs) {
    const p = path.join(process.cwd(), "lang", `${l}.json`);
    if (fs.existsSync(p)) {
      try {
        const content = fs.readFileSync(p, "utf-8");
        const data = JSON.parse(content);
        const dbItems = await BaseQuiz.find({ lang: l }).limit(50).catch(e => []);
        for (let i = 0; i < 15; i++) {
          const randomIndex = Math.floor(Math.random() * data.length);
          const item = data[randomIndex];
          const itemLevel = item.level || item.niveau || 1;
          let isTooSimilar = false;
          for (const dbItem of dbItems) {
            if (calculateSimilarity(item.question, dbItem.question) >= 0.5) {
              isTooSimilar = true;
              break;
            }
          }
          if (!isTooSimilar) {
            console.log("Sauvegarde autorisee : element JSON suffisamment different des donnees existantes");
            await BaseQuiz.create({ lang: l, level: itemLevel, ...item }).catch(e => e);
          }
        }
      } catch (e) {}
    }
  }
}

async function generateAndSaveAiQuizzes() {
  if (Date.now() < aiLockoutUntil) return;
  let targetLevels = [1, 2, 3];
  try {
    const rows = db.prepare("SELECT current_step FROM user_progress ORDER BY RANDOM() LIMIT 7").all();
    if (rows.length > 0) {
      targetLevels = rows.map(r => r.current_step);
    }
  } catch (e) {}

  const langs = ["en", "fr", "es", "ht"];
  const qTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK"];

  for (const lang of langs) {
    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[lang] || "English";
    
    for (let i = 0; i < 7; i++) {
      try {
        if (Date.now() < aiLockoutUntil) return;
        const level = targetLevels[Math.floor(Math.random() * targetLevels.length)] || 1;
        const qType = qTypes[Math.floor(Math.random() * qTypes.length)];

        const prompt = `Create a quiz question in ${langName} language. Guidelines: - Difficulty Level: ${level} - Format/Type: ${qType} - The explanation field MUST be strictly between 300 and 400 characters long. - successMsg: short encouraging message. - errorMsg: short educational feedback. - For MCQ, provide exactly 4 options. For others, provide empty array []. Return ONLY a raw, valid JSON object matching this schema: {"level": ${level}, "lang": "${lang}", "qType": "${qType}", "question": "question text", "options": ["option1", "option2", "option3", "option4"], "answer": "exact correct answer", "explanation": "detailed explanation between 300 and 400 chars", "successMsg": "bravo text", "errorMsg": "mistake text"}`;

        const aiResponse = await runAI([
          { role: "system", content: "You are a JSON API. Return ONLY valid JSON." },
          { role: "user", content: prompt }
        ], 1000);

        const rawResponse = aiResponse.response || "";
        const firstBrace = rawResponse.indexOf('{');
        const lastBrace = rawResponse.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          const parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
          if (parsed.question && parsed.answer && parsed.explanation && parsed.explanation.length >= 300 && parsed.explanation.length <= 400) {
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
              }).catch(e => e);
            }
          }
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {}
    }
  }
}

async function triggerMassAiGeneration() {
  if (Date.now() < aiLockoutUntil) return;
  const langs = ["en", "fr", "es", "ht"];
  const qTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK"];
  for (const lang of langs) {
    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[lang] || "English";
    for (let i = 0; i < 7; i++) {
      try {
        if (Date.now() < aiLockoutUntil) return;
        const level = Math.floor(Math.random() * 3) + 1;
        const qType = qTypes[Math.floor(Math.random() * qTypes.length)];
        const prompt = `Create a quiz question in ${langName} language. Guidelines: - Difficulty Level: ${level} - Format/Type: ${qType} - The explanation field MUST be strictly between 300 and 400 characters long. - successMsg: short encouraging message. - errorMsg: short feedback. - For MCQ, provide exactly 4 options. For others, provide empty array []. Return ONLY a raw, valid JSON object matching this schema: {"level": ${level}, "lang": "${lang}", "qType": "${qType}", "question": "question text", "options": ["option1", "option2", "option3", "option4"], "answer": "exact correct answer", "explanation": "detailed explanation between 300 and 400 chars", "successMsg": "bravo", "errorMsg": "mistake"}`;
        
        const aiResponse = await runAI([
          { role: "system", content: "You are a JSON API. Return ONLY valid JSON." },
          { role: "user", content: prompt }
        ], 1000);

        const rawResponse = aiResponse.response || "";
        const firstBrace = rawResponse.indexOf('{');
        const lastBrace = rawResponse.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          const parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
          if (parsed.question && parsed.answer && parsed.explanation && parsed.explanation.length >= 300 && parsed.explanation.length <= 400) {
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
            }
          }
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {}
    }
  }
}

mongoose.connection.once("open", () => {
  syncJsonToMongo();
  generateAndSaveAiQuizzes();
  setInterval(generateAndSaveAiQuizzes, 70 * 60 * 1000);
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

app.get("/local-image/:filename", (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Cross-Origin-Resource-Policy", "cross-origin");
  const filePath = path.join(dataDir, req.params.filename);
  if (fs.existsSync(filePath)) {
    res.sendFile(filePath);
  } else {
    res.status(404).send("Image Not Found");
  }
});

app.use((req, res, next) => {
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

async function runAI(messages, max_tokens) {
  if (Date.now() < aiLockoutUntil) {
    throw new Error("AI Locked");
  }
  const aiModel = "@cf/meta/llama-3-8b-instruct";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  const aiUrl = `https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/${aiModel}`;
  try {
    const response = await fetch(aiUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${cfToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ messages, max_tokens }),
      signal: controller.signal
    });
    clearTimeout(timeout);
    if (response.status === 429 || response.status === 401 || response.status === 403) {
      aiLockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      throw new Error("AI Quota Exceeded");
    }
    const json = await response.json();
    if (json.errors && json.errors.length > 0) {
      aiLockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      throw new Error("AI Error");
    }
    if (json.success && json.result) {
      return json.result;
    }
    throw new Error();
  } catch (e) {
    clearTimeout(timeout);
    throw new Error();
  }
}

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

    console.log("=== NOUVELLE REQUETE QUIZ ===");
    console.log("Utilisateur demandeur: " + session_id);

    contactCounter++;
    if (contactCounter >= 7) {
      contactCounter = 0;
      triggerMassAiGeneration().catch(() => {});
    }

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
    
    console.log("Debut de l'aleatoire pour secouer les donnees fusion a commence (Recherche de materiel de base)");

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

    let randomItem = dbItems.length > 0 ? dbItems[0] : null;
    
    if (!randomItem) {
      console.log("Base de donnees vide, extraction d'une ressource aleatoire via les fichiers JSON complets");
      randomItem = getRandomLocalJsonQuestion(language);
    }

    if (randomItem && randomItem._id) {
      db.prepare("INSERT OR IGNORE INTO served_questions (session_id, quiz_id) VALUES (?, ?)").run(session_id, randomItem._id.toString());
    }

    let parsed = null;
    let randomType = "MCQ";
    let imgUrl = null;
    let finalSuccess = null;
    let finalError = null;
    let success = false;

    console.log("Debut de l'aleatoire pour detecte le mode a prendre a commencer");
    const availableStrategies = [0, 1, 2, 3].sort(() => Math.random() - 0.5);

    while (availableStrategies.length > 0 && !success) {
      const strategy = availableStrategies.shift();
      console.log("Tentative d'utilisation du mode/strategie numero: " + strategy);
      try {
        if (strategy === 0 && randomItem) {
          console.log("Mode selectionne: Donnees pures (AI + JSON donne existante / MongoDB)");
          parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer, explanation: randomItem.explanation };
          randomType = randomItem.qType || "MCQ";
          imgUrl = randomItem.imageUrl || null;
          finalSuccess = randomItem.successMsg || null;
          finalError = randomItem.errorMsg || null;
          success = true;
        } else if (strategy === 1 && randomItem && Date.now() >= aiLockoutUntil) {
          console.log("Mode selectionne: AI + Donnees (Amelioration d'une question JSON existante)");
          const systemPrompt = `Improve this quiz question slightly without changing the answer. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Original: ${randomItem.question}`;
          const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
          const rawResponse = aiResponse.response || "";
          const firstBrace = rawResponse.indexOf('{');
          const lastBrace = rawResponse.lastIndexOf('}');
          if (firstBrace !== -1 && lastBrace !== -1) {
            parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
            if (!parsed.question || !parsed.answer) throw new Error();
            randomType = randomItem.qType || "MCQ";
            finalSuccess = randomItem.successMsg || null;
            finalError = randomItem.errorMsg || null;
            success = true;
          } else throw new Error();
        } else if (strategy === 2 && randomItem && Date.now() >= aiLockoutUntil) {
          console.log("Mode selectionne: AI + Donnees (Creation d'une toute nouvelle question basee sur JSON existant)");
          const systemPrompt = `Create a new quiz question in the EXACT same style and topic as this one. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Original: ${randomItem.question}`;
          const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
          const rawResponse = aiResponse.response || "";
          const firstBrace = rawResponse.indexOf('{');
          const lastBrace = rawResponse.lastIndexOf('}');
          if (firstBrace !== -1 && lastBrace !== -1) {
            parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
            if (!parsed.question || !parsed.answer) throw new Error();
            randomType = randomItem.qType || "MCQ";
            success = true;
          } else throw new Error();
        } else if (strategy === 3 && Date.now() >= aiLockoutUntil) {
          console.log("Mode selectionne: AI pur (Generation 100% libre sans donnee initiale)");
          const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
          randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
          if (randomType === "IDENTITY_IMAGE") {
            const personPrompt = `Return ONLY a valid JSON array containing 5 random famous historical figures. Format: ["Name1", "Name2", "Name3", "Name4", "Name5"]`;
            const nameResp = await runAI([{ role: "system", content: "Output ONLY raw JSON." }, { role: "user", content: personPrompt }], 300);
            let personName = "Albert Einstein";
            const raw = nameResp.response || "";
            const firstBracket = raw.indexOf('[');
            const lastBracket = raw.lastIndexOf(']');
            if (firstBracket !== -1 && lastBracket !== -1) {
              const candidates = JSON.parse(raw.substring(firstBracket, lastBracket + 1));
              if (Array.isArray(candidates) && candidates.length > 0) {
                personName = candidates[Math.floor(Math.random() * candidates.length)];
              }
            }
            const imagePrompt = `A 100% authentic photograph of ${personName}, ultra-realistic documentary style, lifelike real human being, no digital art.`;
            const extImgResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/@cf/black-forest-labs/flux-1-schnell`, { method: "POST", headers: { "Authorization": `Bearer ${cfToken}`, "Content-Type": "application/json" }, body: JSON.stringify({ prompt: imagePrompt }) });
            if (extImgResponse.ok) {
              const aiJson = await extImgResponse.json();
              const base64Image = aiJson.result.image;
              if (!base64Image) throw new Error();
              const buffer = Buffer.from(base64Image, "base64");
              const filename = `img_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
              const r2Key = `uploads/${filename}`;
              await s3.send(new PutObjectCommand({
                Bucket: process.env.R2_BUCKET,
                Key: r2Key,
                Body: buffer,
                ContentType: "image/png"
              }));
              imgUrl = `${process.env.R2_PUBLIC_URL}/${r2Key}`;
            } else throw new Error();
            const questionTexts = { en: "Who is this person?", fr: "Qui est cette personne ?", es: "¿Quién es esta persona?", ht: "Kiyès moun sa?" };
            parsed = { question: questionTexts[language] || questionTexts.en, options: [], answer: personName, explanation: "" };
            success = true;
          } else {
            const systemPrompt = `Create a ${randomType} quiz question. Topic: General Knowledge. Language: ${langName}. Difficulty: Level ${current_step_num}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Do not write anything else.`;
            const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
            const rawResponse = aiResponse.response || "";
            const firstBrace = rawResponse.indexOf('{');
            const lastBrace = rawResponse.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
              parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
              if (!parsed.question || !parsed.answer) throw new Error();
              success = true;
            } else throw new Error();
          }
        }
      } catch (e) {
        console.log("Erreur durant l'execution de la strategie " + strategy + ", passage a la suivante.");
        success = false;
      }
    }

    if (!success) {
      console.log("Toutes les strategies ont echoue, activation du fallback d'urgence extremite");
      if (randomItem) {
        parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer, explanation: randomItem.explanation };
        randomType = randomItem.qType || "MCQ";
        imgUrl = randomItem.imageUrl || null;
        finalSuccess = randomItem.successMsg || null;
        finalError = randomItem.errorMsg || null;
      } else {
        parsed = { question: "What is the capital of France?", options: ["Paris", "London", "Berlin"], answer: "Paris", explanation: "Fallback question loaded due to network error." };
        randomType = "MCQ";
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

    await saveCurrentQuiz(session_id, randomType, parsed.question, optionsStr, imgUrl, parsed.answer, parsed.explanation || "", finalSuccess || "", finalError || "");

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

    console.log("Quiz envoye a l'utilisateur contenant la question: " + parsed.question);
    console.log("Reponse exacte interne (sauvegardee en BD): " + parsed.answer);
    console.log("Donnees structurees envoyees vers l'utilisateur: " + JSON.stringify(quizData));

    return res.json(quizData);

  } catch (e) {
    console.log("Erreur globale dans /quizz, generation du message d'erreur d'urgence");
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

    console.log("=== NOUVELLE REQUETE VALIDATION ===");
    console.log("L'utilisateur " + session_id + " a soumis la reponse: " + user_answer);

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

    const cleanUser = user_answer.toLowerCase().trim();
    const cleanAnswer = current.answer ? current.answer.toLowerCase().trim() : "";

    console.log("Comparaison de la reponse user: [" + cleanUser + "] avec la reponse exacte: [" + cleanAnswer + "]");

    let judgeResult = { correct: false };

    if (cleanAnswer !== "") {
      if (cleanAnswer.length <= 3) {
        if (cleanUser === cleanAnswer) {
          judgeResult = { correct: true };
        }
      } else {
        if (cleanUser === cleanAnswer) {
          judgeResult = { correct: true };
        } else if (cleanUser.length >= 3 && (cleanUser.includes(cleanAnswer) || cleanAnswer.includes(cleanUser))) {
          judgeResult = { correct: true };
        }
      }
    }

    if (!judgeResult.correct && Date.now() >= aiLockoutUntil) {
      console.log("Verification semantique via AI demarree car la comparaison basique a echoue");
      const judgePrompt = `Question: "${current.question}"\nExpected Answer: "${current.answer}"\nUser Answer: "${user_answer}"\nTask: Determine if the User Answer is correct or means the same thing as the Expected Answer.\nReturn ONLY valid JSON: {"correct": true or false}`;
      try {
        const judgeResp = await runAI([{ role: "system", content: "You evaluate answers. Output ONLY strict JSON." }, { role: "user", content: judgePrompt }], 800);
        const text = judgeResp.response || "";
        const firstBrace = text.indexOf('{');
        const lastBrace = text.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          judgeResult = JSON.parse(text.substring(firstBrace, lastBrace + 1));
        }
      } catch (e) {
        judgeResult = { correct: false };
      }
    }

    const correctValue = judgeResult.correct ?? judgeResult.Correct ?? false;
    const isCorrect = correctValue === true || String(correctValue).toLowerCase() === "true";
    
    console.log("Resultat final de la validation : " + (isCorrect ? "CORRECT" : "INCORRECT"));

    const baseMessage = isCorrect ? (current.success_msg || "Correct!") : (current.error_msg || `Incorrect. Answer: ${current.answer}`);
    const explanation = current.explanation ? `${baseMessage}\n\n${current.explanation}` : baseMessage;

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
      explanation: explanation,
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

    console.log("=== NOUVELLE REQUETE STEP ===");
    console.log("Demande de progres pour l'utilisateur: " + session_id);

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
    if (Date.now() < aiLockoutUntil) {
      return res.status(503).json({ error: "AI service currently unavailable due to limit quota restriction" });
    }
    const prompt = req.body.prompt?.trim();
    if (!prompt) return res.status(400).json({ error: "No prompt provided" });

    const inputs = { prompt: prompt, num_steps: 4 };

    const cfImgUrl = `https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/@cf/black-forest-labs/flux-1-schnell`;
    const aiReq = await fetch(cfImgUrl, {
      method: "POST",
      headers: { "Authorization": `Bearer ${cfToken}`, "Content-Type": "application/json" },
      body: JSON.stringify(inputs)
    });
    
    if (aiReq.status === 429 || aiReq.status === 401 || aiReq.status === 403) {
      aiLockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      return res.status(503).json({ error: "AI service limit reached" });
    }

    const aiJson = await aiReq.json();
    const aiResponse = aiJson.result;

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
  console.log(`Server running on port ${PORT}`);
});
