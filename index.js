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
  const set1 = new Set(str1.toLowerCase().split(/\s+/));
  const set2 = new Set(str2.toLowerCase().split(/\s+/));
  const intersection = new Set([...set1].filter(x => set2.has(x)));
  const union = new Set([...set1, ...set2]);
  if (union.size === 0) return 0;
  return intersection.size / union.size;
}

function isSimilarToExisting(newText, existingItems) {
  if (!newText || !existingItems || existingItems.length === 0) return { similar: false, pct: 0 };
  for (const item of existingItems) {
    if (!item.question) continue;
    const sim = calculateSimilarity(newText, item.question);
    if (sim > 0.20) {
      return { similar: true, pct: Math.round(sim * 100) };
    }
  }
  return { similar: false, pct: 0 };
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
  console.log("Demarrage de la synchronisation JSON avec verification de la difference a 20%");
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
          if (!simCheck.similar) {
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

        let seedText = "General Knowledge";
        try {
          const seedItem = await BaseQuiz.aggregate([{ $match: { lang: lang } }, { $sample: { size: 1 } }]);
          if (seedItem && seedItem.length > 0) {
            seedText = seedItem[0].question;
          }
        } catch(e) {}

        const prompt = `Create a brand new, unique quiz question in ${langName}. Difficulty Level: ${level}. Format: ${qType}. Use this existing question as an inspiration for the theme but make it completely different: "${seedText}". You MUST generate the full JSON format with explanations exactly like this example: {"level": ${level}, "lang": "${lang}", "qType": "${qType}", "question": "Quel est 2 + 2 ?", "options": ["3", "4", "5", "6"], "answer": "4", "explanation": "On additionne 2 et 2 pour obtenir 4.", "successMsg": "Bravo !", "errorMsg": "Ce n'est pas la bonne réponse."}. Return ONLY raw, valid JSON.`;

        console.log("Requete IA Generation Masse avec prompt : " + prompt);

        const aiResponse = await runAI([
          { role: "system", content: "You are a JSON API. Return ONLY valid JSON." },
          { role: "user", content: prompt }
        ], 1000);

        const rawResponse = typeof aiResponse.response === "string" ? aiResponse.response : JSON.stringify(aiResponse.response || aiResponse || {});
        console.log("Reponse brute IA Masse : " + rawResponse);
        const firstBrace = rawResponse.indexOf('{');
        const lastBrace = rawResponse.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          const parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
          if (parsed.question && parsed.answer && parsed.explanation) {
            
            const existingItems = await BaseQuiz.find({ lang: lang }).limit(250).lean().catch(() => []);
            const simCheck = isSimilarToExisting(parsed.question, existingItems);
            if (!simCheck.similar) {
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
                console.log("Nouvelle question sauvegardee en BDD avec succes.");
              }
            } else {
              console.log("Sauvegarde IA Masse annulee: Similarite de " + simCheck.pct + "% detectee avec une question existante.");
            }
          }
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {
        console.log("Erreur durant generation IA de masse : " + e.message);
      }
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
        
        let seedText = "General Knowledge";
        try {
          const seedItem = await BaseQuiz.aggregate([{ $match: { lang: lang } }, { $sample: { size: 1 } }]);
          if (seedItem && seedItem.length > 0) {
            seedText = seedItem[0].question;
          }
        } catch(e) {}

        const prompt = `Create a brand new, unique quiz question in ${langName}. Difficulty Level: ${level}. Format: ${qType}. Use this existing question as an inspiration for the theme but make it completely different: "${seedText}". You MUST generate the full JSON format with explanations exactly like this example: {"level": ${level}, "lang": "${lang}", "qType": "${qType}", "question": "Quel est 2 + 2 ?", "options": ["3", "4", "5", "6"], "answer": "4", "explanation": "On additionne 2 et 2 pour obtenir 4.", "successMsg": "Bravo !", "errorMsg": "Ce n'est pas la bonne réponse."}. Return ONLY raw, valid JSON.`;

        console.log("Requete Trigger IA Masse avec prompt : " + prompt);
        
        const aiResponse = await runAI([
          { role: "system", content: "You are a JSON API. Return ONLY valid JSON." },
          { role: "user", content: prompt }
        ], 1000);

        const rawResponse = typeof aiResponse.response === "string" ? aiResponse.response : JSON.stringify(aiResponse.response || aiResponse || {});
        console.log("Reponse brute Trigger IA Masse : " + rawResponse);
        const firstBrace = rawResponse.indexOf('{');
        const lastBrace = rawResponse.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          const parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
          if (parsed.question && parsed.answer && parsed.explanation) {
            
            const existingItems = await BaseQuiz.find({ lang: lang }).limit(250).lean().catch(() => []);
            const simCheck = isSimilarToExisting(parsed.question, existingItems);
            if (!simCheck.similar) {
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
                console.log("Nouvelle question via trigger sauvegardee en BDD avec succes.");
              }
            } else {
              console.log("Sauvegarde Trigger IA annulee: Similarite de " + simCheck.pct + "% detectee avec une question existante.");
            }
          }
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {
        console.log("Erreur durant trigger IA de masse : " + e.message);
      }
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
  console.log("Requete interceptée sur: " + req.path);
  console.log("Corps de la requête (Body) : " + JSON.stringify(req.body));
  generateAndSaveAiQuizzes().catch(e => e);

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
    return { response: "{}" };
  }
  const aiModel = "@cf/meta/llama-3.1-8b-instruct";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
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
    if (response.status === 429) {
      aiLockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      return { response: "{}" };
    }
    const json = await response.json();
    if (json.success && json.result) {
      return json.result;
    }
    return { response: "{}" };
  } catch (e) {
    clearTimeout(timeout);
    return { response: "{}" };
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
    
    console.log("Recherche de donnee source MongoDB commencee...");

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
      console.log("Base de donnees vide, extraction d'une ressource aleatoire via fichiers JSON.");
      const randomJsonRecord = await getRandomFromJsonFile(language, current_step_num);
      if (randomJsonRecord) {
        dbItems = [randomJsonRecord];
      }
    }

    let randomItem = dbItems.length > 0 ? dbItems[0] : null;
    if (randomItem && randomItem._id) {
      db.prepare("INSERT OR IGNORE INTO served_questions (session_id, quiz_id) VALUES (?, ?)").run(session_id, randomItem._id.toString());
      console.log("Item source selectionne : " + JSON.stringify(randomItem));
    }

    console.log("Mise en place du tableau aleatoire parfait pour les strategies...");

    let availableStrategies = [0, 1, 2, 3];
    for (let i = availableStrategies.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [availableStrategies[i], availableStrategies[j]] = [availableStrategies[j], availableStrategies[i]];
    }
    console.log("Ordre aleatoire des strategies choisi pour cette requete : " + JSON.stringify(availableStrategies));

    let parsed = null;
    let randomType = "MCQ";
    let imgUrl = null;
    let finalSuccess = null;
    let finalError = null;
    let finalExplanation = null;
    let success = false;

    while (availableStrategies.length > 0 && !success) {
      const strategy = availableStrategies.shift();
      console.log("Tentative d'utilisation de la strategie numero: " + strategy);

      if (!randomItem && strategy !== 3) {
        console.log("Strategie " + strategy + " impossible : Aucune donnee source MongoDB. Retrait de la strategie.");
        continue;
      }

      try {
        if (strategy === 0) {
          if (!randomItem) throw new Error("Item introuvable");
          parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer };
          randomType = randomItem.qType || "MCQ";
          imgUrl = randomItem.imageUrl || null;
          finalSuccess = randomItem.successMsg || null;
          finalError = randomItem.errorMsg || null;
          finalExplanation = randomItem.explanation || null;
          success = true;
          console.log("Succes avec Strategie 0 (Donnee MongoDB pure)");
        } else if (strategy === 1) {
          if (!randomItem) throw new Error("Item introuvable");
          const systemPrompt = `Improve this quiz question slightly without changing the answer. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string","string","string"],"answer":"string"}. Do NOT generate explanations or success/error messages. Original: ${randomItem.question}`;
          console.log("Envoi du prompt IA Strategie 1: " + systemPrompt);
          const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
          const rawResponse = typeof aiResponse.response === "string" ? aiResponse.response : JSON.stringify(aiResponse.response || aiResponse || {});
          console.log("Reponse brute IA Strategie 1: " + rawResponse);
          const firstBrace = rawResponse.indexOf('{');
          const lastBrace = rawResponse.lastIndexOf('}');
          if (firstBrace !== -1 && lastBrace !== -1) {
            parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
            if (!parsed.question || !parsed.answer) throw new Error("Champs JSON manquants");
            randomType = randomItem.qType || "MCQ";
            finalSuccess = "";
            finalError = "";
            finalExplanation = "";
            success = true;
            console.log("Succes avec Strategie 1 (Amelioration de l'existant)");
          } else throw new Error("Format JSON introuvable");
        } else if (strategy === 2) {
          if (!randomItem) throw new Error("Item introuvable");
          const systemPrompt = `Create a new quiz question in the EXACT same style and topic as this one. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string","string","string"],"answer":"string"}. Do NOT generate explanations or success/error messages. Original: ${randomItem.question}`;
          console.log("Envoi du prompt IA Strategie 2: " + systemPrompt);
          const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
          const rawResponse = typeof aiResponse.response === "string" ? aiResponse.response : JSON.stringify(aiResponse.response || aiResponse || {});
          console.log("Reponse brute IA Strategie 2: " + rawResponse);
          const firstBrace = rawResponse.indexOf('{');
          const lastBrace = rawResponse.lastIndexOf('}');
          if (firstBrace !== -1 && lastBrace !== -1) {
            parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
            if (!parsed.question || !parsed.answer) throw new Error("Champs JSON manquants");
            randomType = randomItem.qType || "MCQ";
            finalSuccess = "";
            finalError = "";
            finalExplanation = "";
            success = true;
            console.log("Succes avec Strategie 2 (Creation d'un nouveau dans le meme style)");
          } else throw new Error("Format JSON introuvable");
        } else if (strategy === 3) {
          const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
          randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
          if (randomType === "IDENTITY_IMAGE") {
            const usedRes = db.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").all(session_id);
            const usedList = usedRes.map(r => r.person_name);
            let subjectName = "Eiffel Tower";
            const personPrompt = `Return ONLY a valid JSON array containing 5 random visually distinct subjects: it can be famous historical figures, famous monuments, countries, flags, or iconic cities. Exclude: ${usedList.join(",")}. Format: ["Name1", "Name2", "Name3", "Name4", "Name5"]`;
            console.log("Envoi du prompt IA Strategie 3 (Image Sujets): " + personPrompt);
            const nameResp = await runAI([{ role: "system", content: "Output ONLY raw JSON." }, { role: "user", content: personPrompt }], 300);
            const raw = typeof nameResp.response === "string" ? nameResp.response : JSON.stringify(nameResp.response || nameResp || {});
            console.log("Reponse brute IA Strategie 3 (Sujets): " + raw);
            const firstBracket = raw.indexOf('[');
            const lastBracket = raw.lastIndexOf(']');
            if (firstBracket !== -1 && lastBracket !== -1) {
              const candidates = JSON.parse(raw.substring(firstBracket, lastBracket + 1));
              if (Array.isArray(candidates) && candidates.length > 0) {
                 for (const name of candidates) {
                    if (!usedList.includes(name)) {
                       subjectName = name;
                       break;
                    }
                 }
              }
            }
            db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(session_id, subjectName);
            const imagePrompt = `A 100% authentic photograph or high quality image of ${subjectName}, ultra-realistic documentary style, lifelike, highly detailed, no digital art.`;
            console.log("Requete d'image avec le prompt: " + imagePrompt);
            const extImgResponse = await fetch(`https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/@cf/black-forest-labs/flux-1-schnell`, { method: "POST", headers: { "Authorization": `Bearer ${cfToken}`, "Content-Type": "application/json" }, body: JSON.stringify({ prompt: imagePrompt }) });
            if (extImgResponse.ok) {
              const aiJson = await extImgResponse.json();
              const base64Image = aiJson.result.image;
              if (!base64Image) throw new Error("Image base64 manquante");
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
            } else throw new Error("Echec requete image API Flux");
            const questionTexts = { en: "Who is this person or what is this?", fr: "Qui est cette personne, quelle est cette ville, ou quel est ce monument ?", es: "¿Quién es esta persona, qué es esta ciudad, o qué es este monumento?", ht: "Kiyès moun sa, ki vil sa, oswa ki moniman sa?" };
            parsed = { question: questionTexts[language] || questionTexts.en, options: [], answer: subjectName };
            finalSuccess = "";
            finalError = "";
            finalExplanation = "";
            success = true;
            console.log("Succes avec Strategie 3 (Creation Image et Sujet Visuel)");
          } else {
            const systemPrompt = `Create a ${randomType} quiz question. Topic: General Knowledge. Language: ${langName}. Difficulty: Level ${current_step_num}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string","string","string"],"answer":"string"}. Do NOT write explanations or success/error messages. Return exactly 4 options if MCQ, otherwise empty array [].`;
            console.log("Envoi du prompt IA Strategie 3 (Texte): " + systemPrompt);
            const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
            const rawResponse = typeof aiResponse.response === "string" ? aiResponse.response : JSON.stringify(aiResponse.response || aiResponse || {});
            console.log("Reponse brute IA Strategie 3 (Texte): " + rawResponse);
            const firstBrace = rawResponse.indexOf('{');
            const lastBrace = rawResponse.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
              parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
              if (!parsed.question || !parsed.answer) throw new Error("Champs JSON manquants");
              finalSuccess = "";
              finalError = "";
              finalExplanation = "";
              success = true;
              console.log("Succes avec Strategie 3 (Creation Texte Pure)");
            } else throw new Error("Format JSON introuvable");
          }
        }
      } catch (e) {
        console.log("Echec de la strategie " + strategy + " : " + e.message);
        success = false;
      }
    }

    if (!success) {
      console.log("Toutes les strategies ont echoue. Application du fallback de securite.");
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

    console.log("Question finalisee et envoyee a l'utilisateur: " + parsed.question);
    console.log("Reponse exacte sauvegardee en BD: " + parsed.answer);
    console.log("Donnees json envoyees vers l'utilisateur: " + JSON.stringify(quizData));

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

    console.log("=== NOUVELRE QUETE VALIDATION ===");
    console.log("L'utilisateur " + session_id + " a soumis la reponse : " + user_answer);

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

    console.log("Comparaison de la reponse utilisateur : [" + cleanUser + "] avec la reponse exacte sauvegardee : [" + cleanAnswer + "]");

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

    console.log("Resultat algorithmique de validation : " + (isCorrect ? "CORRECT" : "INCORRECT"));

    let finalFeedback = "";
    const isAiPur = (!current.success_msg && !current.error_msg && !current.explanation);

    if (isAiPur) {
      console.log("Aucun message de succes/erreur en base, l'IA de Validation est appelee pour generer le feedback.");
      if (isCorrect) {
        const sys = "You are a JSON API. Output ONLY valid JSON.";
        const usr = `User answered CORRECTLY to the question: "${current.question}". The exact answer is "${current.answer}". Write a concise success message and a brief explanation of the answer. Language: ${langName}. Return ONLY valid JSON: {"explanation": "string"}`;
        console.log("Prompt IA Validation (Succes) : " + usr);
        try {
          const aiResp = await runAI([{ role: "system", content: sys }, { role: "user", content: usr }], 800);
          const txt = typeof aiResp.response === "string" ? aiResp.response : JSON.stringify(aiResp.response || {});
          console.log("Reponse brute IA Validation (Succes) : " + txt);
          const fb = txt.indexOf('{');
          const lb = txt.lastIndexOf('}');
          if (fb !== -1 && lb !== -1) {
            finalFeedback = JSON.parse(txt.substring(fb, lb + 1)).explanation;
          } else {
            finalFeedback = "Correct!\n\n" + current.answer;
          }
        } catch(e) {
          finalFeedback = "Correct!\n\n" + current.answer;
        }
      } else {
        const sys = "You are a JSON API. Output ONLY valid JSON.";
        const usr = `User answered INCORRECTLY. Question: "${current.question}". Correct answer: "${current.answer}". User input: "${user_answer}". Write a concise error message correcting the user and a brief explanation. Language: ${langName}. Return ONLY valid JSON: {"explanation": "string"}`;
        console.log("Prompt IA Validation (Erreur) : " + usr);
        try {
          const aiResp = await runAI([{ role: "system", content: sys }, { role: "user", content: usr }], 800);
          const txt = typeof aiResp.response === "string" ? aiResp.response : JSON.stringify(aiResp.response || {});
          console.log("Reponse brute IA Validation (Erreur) : " + txt);
          const fb = txt.indexOf('{');
          const lb = txt.lastIndexOf('}');
          if (fb !== -1 && lb !== -1) {
            finalFeedback = JSON.parse(txt.substring(fb, lb + 1)).explanation;
          } else {
            finalFeedback = "Incorrect.\n\nLa bonne reponse etait : " + current.answer;
          }
        } catch(e) {
          finalFeedback = "Incorrect.\n\nLa bonne reponse etait : " + current.answer;
        }
      }
    } else {
      console.log("Messages predefinis trouves. Affichage direct sans IA de Validation.");
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
