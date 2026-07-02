import "dotenv/config";
import express from "express";
import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import mongoose from "mongoose";
import { S3Client, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";

const logEvent = (level, context, message) => {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] [${context}] ${message}`);
};

const app = express();
app.use(express.json());

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
  logEvent("INFO", "SYSTEM", `Created data directory at ${dataDir}`);
}

const db = new Database(path.join(dataDir, "quiz_data_fallback.sqlite"));
db.exec("CREATE TABLE IF NOT EXISTS user_progress (session_id TEXT PRIMARY KEY, language TEXT, current_step INTEGER, consecutive_correct INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS current_quiz (session_id TEXT PRIMARY KEY, q_type TEXT, question TEXT, options TEXT, image_url TEXT, answer TEXT, explanation TEXT, success_msg TEXT, error_msg TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS user_info (session_id TEXT PRIMARY KEY, data TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS served_questions (session_id TEXT, quiz_id TEXT, PRIMARY KEY(session_id, quiz_id))");
db.exec("CREATE TABLE IF NOT EXISTS used_persons (session_id TEXT, person_name TEXT)");

const MONGO_URI = process.env.MONGO_URI;

const cfCredentialsStr = process.env.CF_CREDENTIALS || "";
let cfCredentials = [];
if (cfCredentialsStr) {
    cfCredentialsStr.split(",").forEach(pair => {
        const parts = pair.split(":");
        if (parts.length === 2) {
            cfCredentials.push({ accountId: parts[0], token: parts[1], lockoutUntil: 0 });
        }
    });
}
if (cfCredentials.length === 0 && process.env.CF_ACCOUNT_ID && process.env.CF_TOKEN) {
    cfCredentials.push({ accountId: process.env.CF_ACCOUNT_ID, token: process.env.CF_TOKEN, lockoutUntil: 0 });
}

function getAvailableCFCredential() {
    const now = Date.now();
    for (let i = 0; i < cfCredentials.length; i++) {
        if (now > cfCredentials[i].lockoutUntil) {
            return { cred: cfCredentials[i], index: i };
        }
    }
    return null;
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

mongoose.connect(MONGO_URI, { dbName: "quiz" }).then(() => {
  logEvent("SUCCESS", "DATABASE", "Connected to MongoDB successfully");
}).catch(e => {
  logEvent("ERROR", "DATABASE", `MongoDB connection failed: ${e.message}`);
});

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
    logEvent("SUCCESS", "STORAGE", `Deleted object from R2: ${key}`);
  } catch (e) {
    logEvent("ERROR", "STORAGE", `Failed to delete object ${key} from R2: ${e.message}`);
  }
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

function getLevenshteinDistance(a, b) {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(matrix[i - 1][j - 1] + 1, Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1));
      }
    }
  }
  return matrix[b.length][a.length];
}

function checkAnswerTolerance(userAns, realAns) {
  if (!userAns || !realAns) return false;
  const u = userAns.toLowerCase().trim();
  const r = realAns.toLowerCase().trim();
  
  if (u === r) return true;
  
  if (!isNaN(u) && !isNaN(r)) {
    return Number(u) === Number(r);
  }
  
  if (r.length > 3 && (u.includes(r) || r.includes(u))) return true;
  
  const tokenSim = calculateSimilarity(u, r);
  if (tokenSim >= 0.5) return true;
  
  const distance = getLevenshteinDistance(u, r);
  const maxLength = Math.max(u.length, r.length);
  const charSim = (maxLength - distance) / maxLength;
  if (charSim >= 0.5) return true;
  
  return false;
}

const localizedTrueFalse = {
  en: ["True", "False"],
  fr: ["Vrai", "Faux"],
  es: ["Verdadero", "Falso"],
  ht: ["Vrè", "Fo"]
};

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
        const selected = filtered[Math.floor(Math.random() * filtered.length)];
        logEvent("INFO", "FALLBACK_DATA", `Retrieved random JSON item for lang: ${lang}, level: ${level}`);
        return selected;
      }
    }
  } catch(e) {
    logEvent("ERROR", "FALLBACK_DATA", `Error reading JSON file for lang: ${lang} - ${e.message}`);
  }
  return null;
}

async function syncJsonToMongo() {
  logEvent("INFO", "SYSTEM", "Starting JSON synchronisation, threshold 20%");
  const langs = ["en", "fr", "es", "ht"];
  for (const l of langs) {
    const p = path.join(process.cwd(), "lang", `${l}.json`);
    if (fs.existsSync(p)) {
      try {
        const content = fs.readFileSync(p, "utf-8");
        const data = JSON.parse(content);
        const existingItems = await BaseQuiz.find({ lang: l }).limit(500).lean().catch(() => []);
        let addedCount = 0;
        for (const item of data) {
          const itemLevel = item.level || item.niveau || 1;
          const simCheck = isSimilarToExisting(item.question, existingItems);
          if (!simCheck.similar || simCheck.pct <= 70) {
            const exists = await BaseQuiz.findOne({ lang: l, explanation: item.explanation }).catch(() => true);
            if (!exists) {
              await BaseQuiz.create({ lang: l, level: itemLevel, ...item }).catch(() => {});
              addedCount++;
            }
          }
        }
        logEvent("SUCCESS", "SYSTEM", `Sync completed for language ${l}. Added ${addedCount} items.`);
      } catch (e) {
        logEvent("ERROR", "SYSTEM", `Failed to sync JSON to Mongo for lang ${l}: ${e.message}`);
      }
    }
  }
}

function analyzeJsonParseError(rawStr, err) {
  logEvent("ERROR", "AI_MANAGER", `JSON parsing failed. Error: ${err.message}`);
  if (!rawStr || rawStr.trim() === "") {
    logEvent("WARN", "AI_MANAGER", "Diagnosis -> Response completely empty.");
    return;
  }
  const trimmed = rawStr.trim();
  if (trimmed[0] !== "{" && trimmed[0] !== "[") {
    logEvent("WARN", "AI_MANAGER", "Diagnosis -> Response does not start with JSON brackets.");
    return;
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
      throw new Error(`[Parse Error] Array structure not found.`);
    }
  } else {
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      extractedJson = rawText.substring(firstBracket, lastBracket + 1);
    } else {
      throw new Error(`[Parse Error] JSON structure not found.`);
    }
  }

  let parsedData = null;
  try {
    parsedData = JSON.parse(extractedJson);
  } catch (err) {
    analyzeJsonParseError(rawText, err);
    throw new Error(`[Parse Error] SyntaxError: ${err.message}.`);
  }

  if (!isArrayExpected) {
    for (const key of expectedKeys) {
      if (parsedData[key] === undefined || parsedData[key] === null) {
        throw new Error(`[Parse Error] Missing key '${key}'.`);
      }
    }
  }

  return parsedData;
}

async function runAI(messages, max_tokens, retries = 0) {
  const available = getAvailableCFCredential();
  if (!available) {
      logEvent("ERROR", "AI_MANAGER", "All configured Cloudflare AI credentials are locked out or exhausted.");
      return { response: "{}" };
  }
  
  const { cred, index } = available;
  const aiModel = "@cf/meta/llama-3.1-8b-instruct";
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25000);
  const aiUrl = `https://api.cloudflare.com/client/v4/accounts/${cred.accountId}/ai/run/${aiModel}`;

  try {
    const response = await fetch(aiUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${cred.token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ messages, max_tokens }),
      signal: controller.signal
    });
    clearTimeout(timeout);

    const rawText = await response.text();
    let json = {};
    try {
        json = JSON.parse(rawText);
    } catch(e) {
        logEvent("ERROR", "AI_MANAGER", "Error parsing Cloudflare API response JSON.");
    }

    const isRateLimited = response.status === 429 || response.status === 401 || response.status === 403 || (json.errors && json.errors.length > 0 && json.errors.some(err => err.message && (err.message.includes("allocation") || err.message.includes("limit"))));

    if (isRateLimited) {
        logEvent("WARN", "AI_MANAGER", `Fallback Triggered: Credential index ${index} exhausted or limited. Locking out for 24 hours.`);
        cfCredentials[index].lockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
        if (retries < cfCredentials.length) {
            return await runAI(messages, max_tokens, retries + 1);
        }
        return { response: "{}" };
    }

    if (json.success && json.result) {
      return json.result;
    }
    return { response: "{}" };
  } catch (e) {
    clearTimeout(timeout);
    logEvent("ERROR", "AI_MANAGER", `Network or Abort error on credential index ${index}. Message: ${e.message}`);
    return { response: "{}" };
  }
}

async function runAIImage(prompt, retries = 0) {
    const available = getAvailableCFCredential();
    if (!available) {
        logEvent("ERROR", "AI_MANAGER", "All AI credentials exhausted for Image Generation.");
        return null;
    }
    const { cred, index } = available;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 25000);
    const url = `https://api.cloudflare.com/client/v4/accounts/${cred.accountId}/ai/run/@cf/black-forest-labs/flux-1-schnell`;

    try {
        const response = await fetch(url, {
            method: "POST",
            headers: { "Authorization": `Bearer ${cred.token}`, "Content-Type": "application/json" },
            body: JSON.stringify({ prompt, num_steps: 4 }),
            signal: controller.signal
        });
        clearTimeout(timeout);

        const rawText = await response.text();
        let json = {};
        try { json = JSON.parse(rawText); } catch(e) {
            logEvent("ERROR", "AI_MANAGER", `Failed to parse AI Image API response: ${e.message}`);
        }

        const isRateLimited = response.status === 429 || response.status === 401 || response.status === 403 || (json.errors && json.errors.length > 0 && json.errors.some(err => err.message && (err.message.includes("allocation") || err.message.includes("limit"))));

        if (isRateLimited) {
            logEvent("WARN", "AI_MANAGER", `Fallback Triggered: Credential index ${index} exhausted on Image generation. Lockout 24h.`);
            cfCredentials[index].lockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
            if (retries < cfCredentials.length) {
                return await runAIImage(prompt, retries + 1);
            }
            return null;
        }
        return json.result;
    } catch(e) {
        clearTimeout(timeout);
        logEvent("ERROR", "AI_MANAGER", `Network or Abort error during image generation: ${e.message}`);
        return null;
    }
}

async function executeBackgroundMassGeneration(isTrigger) {
  const available = getAvailableCFCredential();
  if (!available) return;

  const processName = isTrigger ? "Trigger" : "Auto";
  logEvent("INFO", "SYSTEM", `Initiation mass generation process: ${processName}`);

  let targetLevels = [1, 2, 3];
  if (!isTrigger) {
    try {
      const rows = db.prepare("SELECT current_step FROM user_progress ORDER BY RANDOM() LIMIT 7").all();
      if (rows.length > 0) {
        targetLevels = rows.map(r => r.current_step);
      }
    } catch (e) {
      logEvent("WARN", "SYSTEM", `Could not fetch target levels for mass gen: ${e.message}`);
    }
  }

  const langs = ["en", "fr", "es", "ht"];
  const qTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK"];

  for (const lang of langs) {
    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[lang] || "English";
    const tfOpts = localizedTrueFalse[lang] || ["True", "False"];
    
    for (let i = 0; i < 7; i++) {
      try {
        const currentCheck = getAvailableCFCredential();
        if (!currentCheck) return;

        const level = targetLevels[Math.floor(Math.random() * targetLevels.length)] || 1;
        const qType = qTypes[Math.floor(Math.random() * qTypes.length)];

        let seedText = "General Knowledge";
        try {
          const seedItem = await BaseQuiz.aggregate([{ $match: { lang: lang } }, { $sample: { size: 1 } }]);
          if (seedItem && seedItem.length > 0) {
            seedText = seedItem[0].question;
          }
        } catch(e) {
            logEvent("WARN", "SYSTEM", `Seed generation fallback triggered: ${e.message}`);
        }

        let systemInstruction = "You are an API that ONLY generates valid JSON. You MUST NOT output any text, markdown, or explanation outside the JSON object. Follow the exact schema provided strictly.";
        let prompt = "";

        if (qType === "MCQ") {
          prompt = `Generate an MCQ in ${langName} (${level}) in strict JSON format.

Rules:
- Question: Direct, ends with '?', no answer inside.
- Options: Array of max 4  logical choices.
- Answer: Must match one option.
- Explanation: 1-2 sentences (300-400 chars) teaching a fact.

Format:
{"level": "${level}", "lang": "${langName}", "qType": "MCQ", "question": "...", "options": ["A", "B", "C"], "answer": "...", "explanation": "...", "successMsg": "Excellent!", "errorMsg": "Incorrect."}`;
        } else if (qType === "TRUE_FALSE") {
          prompt = `Generate a True/False statement in ${langName} (${level}) in strict JSON format.

Rules:
- Question: Declarative statement of fact (not a question).
- Options: Must be exactly ["${tfOpts[0]}", "${tfOpts[1]}"].
- Answer: Must be exactly "${tfOpts[0]}" or "${tfOpts[1]}".
- Explanation: Explain why the statement is true or false.

Format:
{"level": "${level}", "lang": "${lang}", "qType": "TRUE_FALSE", "question": "...", "options": ["${tfOpts[0]}", "${tfOpts[1]}"], "answer": "...", "explanation": "...", "successMsg": "Well done!", "errorMsg": "Not quite."}`;
        } else {
          prompt = `Generate a Fill-in-the-blank question in ${langName} (${level}) in strict JSON format.

Rules:
- Question: Full sentence with exactly one '______' in the MIDDLE (context before and after).
- Options: Must be empty [].
- Answer: only 1 words that fit perfectly in the blank.

Format:
{"level": ${level}, "lang": "${lang}", "qType": "FILL_BLANK", "question": "...", "options": [], "answer": "...", "explanation": "...", "successMsg": "Perfect!", "errorMsg": "Wrong."}`;
        }

        const aiResponse = await runAI([
          { role: "system", content: systemInstruction },
          { role: "user", content: prompt }
        ], 1000);

        try {
          const parsed = parseAIJsonResponse(aiResponse.response, ["question", "answer", "explanation", "qType"]);
          
          if (parsed.qType === "TRUE_FALSE") {
            parsed.options = tfOpts;
            if (parsed.answer !== tfOpts[0] && parsed.answer !== tfOpts[1]) {
                parsed.answer = tfOpts[0];
            }
          }

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
              }).catch((e) => { logEvent("ERROR", "SYSTEM", `Failed saving generated content: ${e.message}`); });
              logEvent("SUCCESS", "SYSTEM", `New mass question saved for ${lang}. Similarity: ${simCheck.pct}%`);
            }
          }
        } catch (parseError) {
            logEvent("ERROR", "SYSTEM", `Failed to parse generated mass question: ${parseError.message}`);
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      } catch (e) {
          logEvent("ERROR", "SYSTEM", `Exception in mass generation loop: ${e.message}`);
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
  } catch (e) {
      logEvent("ERROR", "DATABASE", `MongoDB getProgress failed: ${e.message}`);
  }
  const fallback = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(sessionId);
  if (fallback) return { language: fallback.language, current_step: fallback.current_step, consecutive_correct: fallback.consecutive_correct };
  return null;
}

async function saveProgress(sessionId, lang, step, consec) {
  try {
    await Progress.findOneAndUpdate({ sessionId: sessionId }, { language: lang, currentStep: step, consecutiveCorrect: consec }, { upsert: true });
  } catch (e) {
      logEvent("ERROR", "DATABASE", `MongoDB saveProgress failed: ${e.message}`);
  }
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
  } catch (e) {
      logEvent("ERROR", "DATABASE", `MongoDB saveUserInfo failed: ${e.message}`);
  }
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
    logEvent("WARN", "ROUTER", `Local image not found: ${req.params.filename}`);
    res.status(404).send("Image Not Found");
  }
});

app.use((req, res, next) => {
  logEvent("INFO", "ROUTER", `Intercepted HTTP Request on: ${req.method} ${req.path}`);
  
  globalRequestCounter++;
  if (globalRequestCounter >= 70) {
    logEvent("INFO", "SYSTEM", "70 requests reached. Triggering mass generation.");
    globalRequestCounter = 0;
    executeBackgroundMassGeneration(true).catch(e => {
        logEvent("ERROR", "SYSTEM", `Trigger mass generation failed: ${e.message}`);
    });
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
    logEvent("WARN", "ROUTER", `Forbidden access attempt from Origin: ${origin}`);
    return res.status(403).json({ error: "Forbidden: Origin or Token not allowed" });
  }
  next();
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
    logEvent("SUCCESS", "ROUTER", `User info saved successfully for session: ${session_id}`);
    return res.json({ success: true, message: "User info saved successfully" });
  } catch (e) {
    logEvent("ERROR", "ROUTER", `User info save failed: ${e.message}`);
    return res.json({ success: false, error: "Database error" });
  }
});

async function executeMode0PureDB(randomItem) {
    logEvent("INFO", "MODE_0_PURE_DB", "Execution started");
    if (!randomItem) throw new Error("Source item missing");
    const parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer };
    const randomType = randomItem.qType || "MCQ";
    const imgUrl = randomItem.imageUrl || null;
    const finalSuccess = randomItem.successMsg || null;
    const finalError = randomItem.errorMsg || null;
    const finalExplanation = randomItem.explanation || null;
    logEvent("SUCCESS", "MODE_0_PURE_DB", "Execution completed successfully");
    return { parsed, randomType, imgUrl, finalSuccess, finalError, finalExplanation };
}

async function executeMode1ImproveExisting(randomItem, langName, langCode) {
    logEvent("INFO", "MODE_1_IMPROVE_EXISTING", "Execution started");
    if (!randomItem) throw new Error("Source item missing");
    const prompt = `Improve this quiz question in ${langName} for clarity and logic. Original: "${randomItem.question}".

Rules:
- Question: only If MCQ, must end with '?',  If a true/false the "question" must be an affirmation. Do NOT include the answer inside.
- Options: Array of max 4 logical choices.
- Output: Return ONLY a valid JSON object matching the format below.

Format:
{"question":"string","options":["string","string","string"],"answer":"string"}`;
    const aiResponse = await runAI([{ role: "system", content: "You are an API that ONLY generates valid JSON. You MUST NOT output any text, markdown, or explanation outside the JSON object." }, { role: "user", content: prompt }], 1000);
    const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    
    if (randomItem.qType === "TRUE_FALSE") {
        parsed.options = localizedTrueFalse[langCode] || ["True", "False"];
        if (parsed.answer !== parsed.options[0] && parsed.answer !== parsed.options[1]) {
            parsed.answer = parsed.options[0];
        }
    }
    
    logEvent("SUCCESS", "MODE_1_IMPROVE_EXISTING", "Execution completed successfully");
    return { parsed, randomType: randomItem.qType || "MCQ", imgUrl: null, finalSuccess: "", finalError: "", finalExplanation: "" };
}

async function executeMode2CreateSimilar(randomItem, langName, langCode) {
    logEvent("INFO", "MODE_2_CREATE_SIMILAR", "Execution started");
    if (!randomItem) throw new Error("Source item missing");
    const prompt = `Create a new factual quiz question in ${langName} mimicking the style and topic of: "${randomItem.question}".

Rules:
- Question: only If MCQ, must end with '?',  If a true/false the "question" must be an affirmation. Do NOT include the answer inside.
- Options: Array of max 4 logical choices.
- Output: Return ONLY a valid JSON object matching the format below.

Format:
{"question":"string","options":["string","string","string"],"answer":"string"}`;
    const aiResponse = await runAI([{ role: "system", content: "You are an API that ONLY generates valid JSON. You MUST NOT output any text, markdown, or explanation outside the JSON object." }, { role: "user", content: prompt }], 1000);
    const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    
    if (randomItem.qType === "TRUE_FALSE") {
        parsed.options = localizedTrueFalse[langCode] || ["True", "False"];
        if (parsed.answer !== parsed.options[0] && parsed.answer !== parsed.options[1]) {
            parsed.answer = parsed.options[0];
        }
    }
    
    logEvent("SUCCESS", "MODE_2_CREATE_SIMILAR", "Execution completed successfully");
    return { parsed, randomType: randomItem.qType || "MCQ", imgUrl: null, finalSuccess: "", finalError: "", finalExplanation: "" };
}

async function executeMode3PureAIGeneration(session_id, language, langName) {
    logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Execution started");
    const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
    const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
    const systemInstructionStrict = "You are an API that ONLY generates valid JSON. You MUST NOT output any text, markdown, or explanation outside the JSON object. Follow the exact schema provided strictly.";
    let parsed = null;
    let imgUrl = null;
    const tfOpts = localizedTrueFalse[language] || ["True", "False"];

    if (randomType === "IDENTITY_IMAGE") {
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Sub-mode: IDENTITY_IMAGE selected");
        const categories = ["Pays/Ville/region", "Personne célèbre", "Anime/film/série", "Animaux", "Plante", "Planète"];
        const selectedCategory = categories[Math.floor(Math.random() * categories.length)];
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", `Random category chosen: ${selectedCategory}`);

        const usedRes = db.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").all(session_id);
        const usedList = usedRes.map(r => r.person_name);
        
        const combinedPrompt = `Create an image quiz item for category "${selectedCategory}" in ${langName}. Exclude subjects in: [${usedList.join(",")}].

Rules:
- ImagePrompt: Detailed English prompt for an authentic photo (no text).
- Question: Direct inquiry ending with '?', without the answer inside.
- Answer: Exactly a single word.
- Output: Return ONLY a valid JSON object matching the format below.

Format:
{
  "imagePrompt": "...",
  "question": "...",
  "answer": "..."
}`;
        
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Requesting AI to generate combined image prompt, question, and answer");
        const comboResp = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: combinedPrompt }], 800);
        const parsedCombo = parseAIJsonResponse(comboResp.response, ["imagePrompt", "question", "answer"]);
        
        const subjectName = parsedCombo.answer;
        db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(session_id, subjectName);
        
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", `Starting Image generation for generated prompt: ${parsedCombo.imagePrompt}`);
        const aiJsonResult = await runAIImage(parsedCombo.imagePrompt);
        
        if (aiJsonResult && aiJsonResult.image) {
          const base64Image = aiJsonResult.image;
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
          logEvent("SUCCESS", "MODE_3_PURE_AI_GENERATION", `Image uploaded successfully to R2 at ${imgUrl}`);
        } else {
            logEvent("ERROR", "MODE_3_PURE_AI_GENERATION", "Flux AI Image API failed to return image data");
            throw new Error("Flux AI Image API failed");
        }
        
        parsed = { question: parsedCombo.question, options: [], answer: subjectName };
        
    } else if (randomType === "MCQ") {
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Sub-mode: MCQ selected");
        const mcqPrompt = `Generate an MCQ in ${langName} in strict JSON format.

Rules:
- Question: Direct, ends with '?', don't put The answer inside.
- Options: Array of max 4 logical choices.
- Answer: Must match one option.

Format:
{"question": "... ?", "options": ["A", "B", "more if necessary"], "answer": "..."}`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: mcqPrompt }], 1000);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    } else if (randomType === "TRUE_FALSE") {
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Sub-mode: TRUE_FALSE selected");
        const tfPrompt = `Generate a True/False statement in ${langName} in strict JSON format.

Rules:
- Question: Declarative statement of fact (no question mark).
- Options: Must be exactly ["${tfOpts[0]}", "${tfOpts[1]}"].
- Answer: Must match one option.

Format:
{"question": "...", "options": ["${tfOpts[0]}", "${tfOpts[1]}"], "answer": "..."}`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: tfPrompt }], 1000);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
        parsed.options = tfOpts;
        if (parsed.answer !== tfOpts[0] && parsed.answer !== tfOpts[1]) {
            parsed.answer = tfOpts[0];
        }
    } else if (randomType === "FILL_BLANK") {
        logEvent("INFO", "MODE_3_PURE_AI_GENERATION", "Sub-mode: FILL_BLANK selected");
        const fbPrompt = `Generate a Fill-in-the-blank question in ${langName} in strict JSON format.

Rules:
- Question: Full sentence with exactly one '______' in the MIDDLE.
- Answer: One single word to fill the empty space, to complete the sentence.

Format:
{"question": "...", "answer": "..."}`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: fbPrompt }], 1000);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer"]);
    }

    logEvent("SUCCESS", "MODE_3_PURE_AI_GENERATION", "Execution completed successfully");
    return { parsed, randomType, imgUrl, finalSuccess: "", finalError: "", finalExplanation: "" };
}

app.post("/quizz", async (req, res) => {
  try {
    const body = req.body;
    const session_id = body.session_id?.trim();
    if (!session_id) return res.status(400).json({ error: "session_id required" });

    logEvent("INFO", "ROUTER", `Quiz request initiated for user session: ${session_id}`);

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
    
    const countQuery = db.prepare("SELECT COUNT(*) as count FROM served_questions WHERE session_id = ?").get(session_id);
    if (countQuery && countQuery.count >= 100) {
      logEvent("INFO", "DATABASE", `User ${session_id} reached 100 served questions limit. Clearing history.`);
      db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
    }

    const servedRows = db.prepare("SELECT quiz_id FROM served_questions WHERE session_id = ?").all(session_id);
    const servedIds = servedRows.map(r => r.quiz_id).filter(id => mongoose.Types.ObjectId.isValid(id)).map(id => new mongoose.Types.ObjectId(id));

    let matchCriteria = { lang: language, level: current_step_num };
    if (servedIds.length > 0) {
      matchCriteria._id = { $nin: servedIds };
    }

    let dbItems = await BaseQuiz.aggregate([{ $match: matchCriteria }, { $sample: { size: 100 } }]).catch((e) => {
        logEvent("ERROR", "DATABASE", `Aggregate query failed: ${e.message}`);
        return [];
    });
    
    if (dbItems.length === 0) {
      let broadCriteria = { lang: language };
      if (servedIds.length > 0) {
        broadCriteria._id = { $nin: servedIds };
      }
      dbItems = await BaseQuiz.aggregate([{ $match: broadCriteria }, { $sample: { size: 100 } }]).catch(() => []);
    }

    if (dbItems.length === 0 && servedIds.length > 0) {
      logEvent("INFO", "DATABASE", `Exhausted DB items for user ${session_id}. Resetting served questions.`);
      db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
      dbItems = await BaseQuiz.aggregate([{ $match: { lang: language, level: current_step_num } }, { $sample: { size: 100 } }]).catch(() => []);
      if (dbItems.length === 0) {
        dbItems = await BaseQuiz.aggregate([{ $match: { lang: language } }, { $sample: { size: 100 } }]).catch(() => []);
      }
    }

    if (dbItems.length === 0) {
      const randomJsonRecord = await getRandomFromJsonFile(language, current_step_num);
      if (randomJsonRecord) {
        dbItems = [randomJsonRecord];
      }
    }

    let randomItem = null;
    if (dbItems.length > 0) {
      randomItem = dbItems[Math.floor(Math.random() * dbItems.length)];
    }

    if (randomItem && randomItem._id) {
      db.prepare("INSERT OR IGNORE INTO served_questions (session_id, quiz_id) VALUES (?, ?)").run(session_id, randomItem._id.toString());
    }

    if (!randomItem) {
        logEvent("WARN", "STRATEGY_SELECTOR", "No randomItem found from DB or JSON. Forcing Mode 3 to ensure generation.");
    }

    let availableStrategies = [0, 1, 2, 3];
    for (let i = availableStrategies.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      const temp = availableStrategies[i];
      availableStrategies[i] = availableStrategies[j];
      availableStrategies[j] = temp;
    }
    
    logEvent("INFO", "STRATEGY_SELECTOR", `Randomised strategy sequence for this request: [${availableStrategies.join(", ")}]`);

    let parsed = null;
    let randomType = "MCQ";
    let imgUrl = null;
    let finalSuccess = null;
    let finalError = null;
    let finalExplanation = null;
    let success = false;

    while (availableStrategies.length > 0 && !success) {
      const strategy = availableStrategies.shift();
      
      if (!randomItem && strategy !== 3) {
        logEvent("INFO", "STRATEGY_SELECTOR", `Skipping strategy ${strategy} because there is no base DB item available.`);
        continue;
      }

      try {
        logEvent("INFO", "STRATEGY_SELECTOR", `Attempting to execute Strategy ${strategy}`);
        let strategyResult;
        if (strategy === 0) strategyResult = await executeMode0PureDB(randomItem);
        else if (strategy === 1) strategyResult = await executeMode1ImproveExisting(randomItem, langName, language);
        else if (strategy === 2) strategyResult = await executeMode2CreateSimilar(randomItem, langName, language);
        else if (strategy === 3) strategyResult = await executeMode3PureAIGeneration(session_id, language, langName);

        parsed = strategyResult.parsed;
        randomType = strategyResult.randomType;
        imgUrl = strategyResult.imgUrl;
        finalSuccess = strategyResult.finalSuccess;
        finalError = strategyResult.finalError;
        finalExplanation = strategyResult.finalExplanation;
        success = true;
        logEvent("SUCCESS", "STRATEGY_SELECTOR", `Strategy ${strategy} succeeded and data is ready`);
      } catch (e) {
        logEvent("ERROR", "STRATEGY_SELECTOR", `Strategy ${strategy} execution failed with error: ${e.message}`);
        success = false;
      }
    }

    if (!success) {
      logEvent("ERROR", "STRATEGY_SELECTOR", "All strategies failed or exhausted. Fallback system applied.");
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
          logEvent("SUCCESS", "STORAGE", `Local image migrated to R2 storage at ${imgUrl}`);
        } catch (e) {
            logEvent("ERROR", "STORAGE", `Failed to migrate local image to R2: ${e.message}`);
        }
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

    logEvent("INFO", "ROUTER", `Successfully responding with quiz data for session ${session_id}`);
    return res.json(quizData);

  } catch (e) {
    logEvent("ERROR", "ROUTER", `Critical failure in /quizz endpoint: ${e.message}`);
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

    logEvent("INFO", "ROUTER", `Validation requested for session ${session_id}`);

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
    const cleanAnswer = current.answer ? current.answer.toLowerCase().trim() : "";
    
    let isCorrect = false;
    if (cleanAnswer !== "") {
        isCorrect = checkAnswerTolerance(user_answer, cleanAnswer);
    }

    logEvent("INFO", "VALIDATION", `Answer checking completed. User Answer: ${user_answer}. Correct Answer: ${cleanAnswer}. Status: ${isCorrect}`);

    let finalFeedback = "";
    const isAiPur = (!current.success_msg && !current.error_msg && !current.explanation);

    if (isAiPur) {
      const sysStrict = "You are a strict JSON data generator. Output ONLY raw valid JSON without markdown formatting.";
      if (isCorrect) {
        const usr = `User answered CORRECTLY to the question: "${current.question}". Correct answer was: "${current.answer}". Write an encouraging success message and educational explanation in ${langName}. The explanation MUST be strictly between 300 and 400 characters long, teaching a fact. Return ONLY a valid JSON object matching this schema: {"successMsg": "encouraging text", "explanation": "detailed reason"}`;
        try {
          const aiResp = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: usr }], 800);
          const parsedFeedback = parseAIJsonResponse(aiResp.response, ["successMsg", "explanation"]);
          finalFeedback = `${parsedFeedback.successMsg}\n\n${parsedFeedback.explanation}`;
          logEvent("SUCCESS", "VALIDATION_AI", "Generated success feedback");
        } catch(e) {
          logEvent("WARN", "VALIDATION_AI", `AI success feedback failed: ${e.message}`);
          finalFeedback = "Correct!\n\n" + current.answer;
        }
      } else {
        const usr = `User answered INCORRECTLY. Question: "${current.question}". Correct answer: "${current.answer}". User input: "${user_answer}". The user CANNOT retry. Write a direct correction and an educational explanation in ${langName} teaching them the fact. The explanation MUST be strictly between 300 and 400 characters long. Return ONLY a valid JSON object matching this schema: {"errorMsg": "direct correction feedback", "explanation": "factual educational context"}`;
        try {
          const aiResp = await runAI([{ role: "system", content: sysStrict }, { role: "user", content: usr }], 800);
          const parsedFeedback = parseAIJsonResponse(aiResp.response, ["errorMsg", "explanation"]);
          finalFeedback = `${parsedFeedback.errorMsg}\n\n${parsedFeedback.explanation}`;
          logEvent("SUCCESS", "VALIDATION_AI", "Generated error feedback");
        } catch(e) {
          logEvent("WARN", "VALIDATION_AI", `AI error feedback failed: ${e.message}`);
          finalFeedback = "Incorrect.\n\nThe correct answer was: " + current.answer;
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
        logEvent("INFO", "VALIDATION", `User ${session_id} progressed to step ${new_step}`);
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
    logEvent("ERROR", "VALIDATION", `Exception during validation: ${e.message}`);
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
    logEvent("ERROR", "ROUTER", `Failed fetching step info: ${e.message}`);
    return res.json({ error: "Internal Server Error", message: e.message });
  }
});

app.post("/jerere", async (req, res) => {
  try {
    const prompt = req.body.prompt?.trim();
    if (!prompt) return res.status(400).json({ error: "No prompt provided" });
    
    logEvent("INFO", "UPLOAD", `External image generation requested with prompt: ${prompt}`);

    const aiJsonResult = await runAIImage(prompt);
    
    if (!aiJsonResult || !aiJsonResult.image) {
      throw new Error("Flux Image generation returned no image data");
    }

    const binaryString = atob(aiJsonResult.image);
    const bytes = Uint8Array.from(binaryString, c => c.charCodeAt(0));
    const filename = `img_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
    const blob = new Blob([bytes.buffer], { type: "image/png" });
    const formData = new FormData();
    formData.append("file", blob, filename);

    logEvent("INFO", "UPLOAD", "Uploading generated image to external server");
    const uploadRes = await fetch("https://bref.adamdh7.org/upload", { method: "POST", body: formData });
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
      throw new Error("Upload server did not return a valid URL");
    }

    logEvent("SUCCESS", "UPLOAD", `Image uploaded successfully: ${returnedUrl}`);
    return res.json({ url: returnedUrl });
  } catch (e) {
    logEvent("ERROR", "UPLOAD", `Failure during image routing/upload: ${e.message}`);
    return res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  logEvent("SUCCESS", "SYSTEM", `Server running on port ${PORT}`);
});
