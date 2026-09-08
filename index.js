import "dotenv/config";
import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
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
db.exec("CREATE TABLE IF NOT EXISTS game_sessions (game_id TEXT PRIMARY KEY, game_slug TEXT, owner_tfid TEXT, players TEXT, state TEXT, expires_at INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS game_words (game_id TEXT PRIMARY KEY, words TEXT, expires_at INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS game_invitations (invitation_id TEXT PRIMARY KEY, game_id TEXT, from_tfid TEXT, to_tfid TEXT, game_slug TEXT, status TEXT, created_at INTEGER, expires_at INTEGER)");

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
const GAME_TTL_MS = 7 * 60 * 1000;
const SILENT_VALIDATE_COOLDOWN_MS = 17 * 1000;
const MAIN_AI_MODEL = "@cf/zai-org/glm-4.7-flash";
const VALIDATOR_AI_MODEL = "@cf/meta/llama-3.1-8b-instruct-fast";
const wsClients = new Map();
const silentValidationAt = new Map();

const invitationVariants = {
  en: [
    "You have received a game invitation from TF-{TFID}. Would you like to accept it?",
    "TF-{TFID} has invited you to join a game. Would you like to accept the invitation?",
    "A game invitation from TF-{TFID} is waiting for your response. Would you like to accept it?",
    "TF-{TFID} has sent you an invitation to participate in a game. Would you like to accept?",
    "You are invited by TF-{TFID} to participate in a game. Please choose whether to accept the invitation.",
    "An invitation from TF-{TFID} has been received. Would you like to join the proposed game?",
    "TF-{TFID} has requested your participation in a game. Would you like to accept the invitation?"
  ],
  fr: [
    "Vous avez reçu une invitation de TF-{TFID}. Souhaitez-vous l’accepter ?",
    "TF-{TFID} vous a invité à rejoindre une partie. Souhaitez-vous accepter cette invitation ?",
    "Une invitation de TF-{TFID} attend votre réponse. Souhaitez-vous l’accepter ?",
    "TF-{TFID} vous propose de participer à une partie. Souhaitez-vous accepter cette invitation ?",
    "Vous êtes invité par TF-{TFID} à participer à une partie. Veuillez choisir si vous souhaitez accepter.",
    "Vous avez reçu une invitation de participation envoyée par TF-{TFID}. Souhaitez-vous rejoindre la partie ?",
    "TF-{TFID} sollicite votre participation à une partie. Souhaitez-vous accepter l’invitation ?"
  ],
  es: [
    "Ha recibido una invitación de TF-{TFID}. ¿Desea aceptarla?",
    "TF-{TFID} le ha invitado a participar en una partida. ¿Desea aceptar la invitación?",
    "Tiene una invitación de TF-{TFID} pendiente de respuesta. ¿Desea aceptarla?",
    "TF-{TFID} le propone participar en una partida. ¿Desea aceptar esta invitación?",
    "TF-{TFID} le ha invitado a participar en una partida. Seleccione si desea aceptar la invitación.",
    "Ha recibido una invitación para participar en una partida enviada por TF-{TFID}. ¿Desea unirse?",
    "TF-{TFID} solicita su participación en una partida. ¿Desea aceptar la invitación?"
  ],
  ht: [
    "Ou resevwa yon envitasyon nan men TF-{TFID}. Èske ou vle aksepte li?",
    "TF-{TFID} envite w pou patisipe nan yon jwèt. Èske ou vle aksepte envitasyon an?",
    "Gen yon envitasyon nan men TF-{TFID} k ap tann repons ou. Èske ou vle aksepte li?",
    "TF-{TFID} pwopoze pou ou patisipe nan yon jwèt. Èske ou vle aksepte envitasyon sa a?",
    "TF-{TFID} envite w pou patisipe nan yon jwèt. Tanpri chwazi si ou vle aksepte envitasyon an.",
    "Ou resevwa yon envitasyon pou patisipe nan yon jwèt nan men TF-{TFID}. Èske ou vle antre nan jwèt la?",
    "TF-{TFID} mande patisipasyon ou nan yon jwèt. Èske ou vle aksepte envitasyon an?"
  ]
};

const localizedConnectionMessages = {
  en: {
    connected: "The player connection was established successfully.",
    disconnected: "The player connection was closed.",
    disconnectError: "The player connection could not be completed correctly.",
    accepted: "The game invitation was accepted successfully.",
    declined: "The game invitation was declined."
  },
  fr: {
    connected: "La connexion du joueur a été établie avec succès.",
    disconnected: "La connexion du joueur a été interrompue.",
    disconnectError: "La connexion du joueur n’a pas pu être établie correctement.",
    accepted: "L’invitation à la partie a été acceptée avec succès.",
    declined: "L’invitation à la partie a été refusée."
  },
  es: {
    connected: "La conexión del jugador se estableció correctamente.",
    disconnected: "La conexión del jugador se ha cerrado.",
    disconnectError: "La conexión del jugador no pudo completarse correctamente.",
    accepted: "La invitación a la partida fue aceptada correctamente.",
    declined: "La invitación a la partida fue rechazada."
  },
  ht: {
    connected: "Koneksyon jwè a etabli avèk siksè.",
    disconnected: "Koneksyon jwè a fèmen.",
    disconnectError: "Koneksyon jwè a pa t kapab etabli kòrèkteman.",
    accepted: "Envitasyon jwèt la aksepte avèk siksè.",
    declined: "Envitasyon jwèt la refize."
  }
};

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

const gameDefinitionSchema = new mongoose.Schema({
  slug: { type: String, unique: true },
  name: String,
  description: String,
  systemDirectives: String,
  soloPoints: { type: Number, default: 1 },
  modes: [String],
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const GameDefinition = mongoose.model("GameDefinition", gameDefinitionSchema, "game_definitions");

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
  const rawText = cleanAIResponse(typeof rawResponse === "string" ? rawResponse : JSON.stringify(rawResponse || {}));
  const firstBracket = rawText.indexOf('{');
  const lastBracket = rawText.lastIndexOf('}');
  const firstSquare = rawText.indexOf('[');
  const lastSquare = rawText.lastIndexOf(']');
  const isArrayExpected = expectedKeys.includes("ARRAY_FORMAT_ONLY");
  const candidates = [];

  if (isArrayExpected) {
    if (firstSquare !== -1 && lastSquare !== -1 && lastSquare > firstSquare) {
      candidates.push(rawText.substring(firstSquare, lastSquare + 1));
    }
  } else {
    if (firstBracket !== -1 && lastBracket !== -1 && lastBracket > firstBracket) {
      candidates.push(rawText.substring(firstBracket, lastBracket + 1));
    }
  }

  if (candidates.length === 0) throw new Error(`[Parse Error] JSON structure not found.`);

  let parsedData = null;
  try {
    parsedData = JSON.parse(candidates[0]);
  } catch (err) {
    analyzeJsonParseError(rawText, err);
    throw new Error(`[Parse Error] SyntaxError: ${err.message}.`);
  }

  if (isArrayExpected) return parsedData;

  const containsKeys = value => value && typeof value === "object" && expectedKeys.every(key => key === "ARRAY_FORMAT_ONLY" || value[key] !== undefined && value[key] !== null);
  if (containsKeys(parsedData)) return parsedData;

  const queue = [parsedData];
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== "object") continue;
    if (containsKeys(current)) return current;
    for (const value of Object.values(current)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }

  const missing = expectedKeys.filter(key => key !== "ARRAY_FORMAT_ONLY" && (parsedData[key] === undefined || parsedData[key] === null));
  throw new Error(`[Parse Error] Missing key '${missing[0] || "response"}'.`);
}

function cleanAIResponse(raw) {
  if (typeof raw !== "string") return "";
  let text = raw.trim();
  text = text.replace(/<think>[\s\S]*?<\/think>/gi, "").trim();
  text = text.replace(/<reasoning>[\s\S]*?<\/reasoning>/gi, "").trim();
  text = text.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim();
  return text;
}

async function runAI(messages, max_tokens, retries = 0, model = MAIN_AI_MODEL) {
  const available = getAvailableCFCredential();
  if (!available) {
      logEvent("ERROR", "AI_MANAGER", "All configured Cloudflare AI credentials are locked out or exhausted.");
      return { response: "{}" };
  }
  
  const { cred, index } = available;
  const aiModel = model;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 45000);
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
            return await runAI(messages, max_tokens, retries + 1, model);
        }
        return { response: "{}" };
    }

    if (json.success && json.result) {
      const result = json.result;
      if (typeof result === "string") return { response: result };
      if (typeof result.response === "string") return { response: result.response };
      const choiceContent = result.choices?.[0]?.message?.content;
      if (typeof choiceContent === "string") return { response: choiceContent };
      const textContent = result.output_text || result.text || result.content;
      if (typeof textContent === "string") return { response: textContent };
      return { response: JSON.stringify(result) };
    }
    return { response: "{}" };
  } catch (e) {
    clearTimeout(timeout);
    logEvent("ERROR", "AI_MANAGER", `Network or Abort error on credential index ${index}. Message: ${e.message}`);
    return { response: "{}" };
  }
}

async function runAIValidator(question, correctAnswer, userAnswer, language, gameName, retries = 0) {
  const system = `<system_directives name="answer_validator">
You are Asistan, the answer validation engine for Mizik games.
Validate the user's answer against the supplied correct answer and question.
Use semantic meaning, spelling tolerance, equivalent wording, and the language supplied.
Return exactly one lowercase word: correct or incorrect.
</system_directives>`;
  const user = `Question: ${question}
Correct answer: ${correctAnswer}
User answer: ${userAnswer}
Language: ${language}
Game: ${gameName}`;
  const result = await runAI([
    { role: "system", content: system },
    { role: "user", content: user }
  ], 40, retries, VALIDATOR_AI_MODEL);
  const normalized = cleanAIResponse(result.response).toLowerCase().replace(/[^a-z]/g, "");
  if (normalized === "correct") return true;
  if (normalized === "incorrect") return false;
  throw new Error("Validator returned an invalid status");
}

async function runAIImage(prompt, retries = 0) {
    const available = getAvailableCFCredential();
    if (!available) {
        logEvent("ERROR", "AI_MANAGER", "All AI credentials exhausted for Image Generation.");
        return null;
    }
    const { cred, index } = available;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 45000);
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

        let systemInstruction = `<system_directives name="mass_question_${qType.toLowerCase()}">
You are Asistan, the ${qType} question engine for Mizik.
Generate one factually verified ${qType} question in ${langName} at level ${level}.
Set qType to ${qType}.
Provide the exact answer, a concise explanation, a professional successMsg, a professional errorMsg, and an integer timeLimit in seconds.
Return one JSON object with exactly these fields: level, lang, qType, question, options, answer, explanation, successMsg, errorMsg, timeLimit.
</system_directives>`;
        let prompt = `Language: ${langName}
Level: ${level}
Question type: ${qType}
Verified source topic: ${seedText}`;

        const aiResponse = await runAI([
          { role: "system", content: systemInstruction },
          { role: "user", content: prompt }
        ], 1000);

        try {
          const parsed = parseAIJsonResponse(aiResponse.response, ["question", "answer", "options", "explanation", "qType", "successMsg", "errorMsg", "timeLimit"]);
          
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
                successMsg: parsed.successMsg || "",
                errorMsg: parsed.errorMsg || "",
                timeLimit: Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20))
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
    const prompt = `<system_directives name="improved_question">
You are Asistan, a factual question editor.
Rewrite the supplied question while preserving its verified subject and answer.
Match the original question type and language.
Return a raw JSON object containing question, options, and answer.
Question formatting follows the supplied type.
</system_directives>

Language: ${langName}
Question type: ${randomItem.qType || "MCQ"}
Source question: ${randomItem.question}
Source answer: ${randomItem.answer}`;
    const aiResponse = await runAI([{ role: "system", content: "<system_directives name=\"question_editor\">\nYou are Asistan, a factual question editor for Mizik.\nReturn a raw JSON object containing the requested fields.\nPreserve verified meaning, answer, language, and question type.\n</system_directives>" }, { role: "user", content: prompt }], 1000);
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
    const prompt = `<system_directives name="similar_question">
You are Asistan, a factual question generator.
Create a new question about the same verified subject area as the supplied source.
Keep the requested language and question type.
Return a raw JSON object containing question, options, and answer.
Use knowledge grounded in the supplied source subject.
</system_directives>

Language: ${langName}
Question type: ${randomItem.qType || "MCQ"}
Source question: ${randomItem.question}`;
    const aiResponse = await runAI([{ role: "system", content: "<system_directives name=\"question_editor\">\nYou are Asistan, a factual question editor for Mizik.\nReturn a raw JSON object containing the requested fields.\nPreserve verified meaning, answer, language, and question type.\n</system_directives>" }, { role: "user", content: prompt }], 1000);
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
    const tfOpts = localizedTrueFalse[language] || ["True", "False"];
    const systemInstructionStrict = `<system_directives name="${randomType.toLowerCase()}">
You are Asistan, the ${randomType} game engine for Mizik.
Generate one factually verified game question in the requested language.
Set qType to ${randomType}.
Provide the requested answer, a concise explanation, a professional successMsg and a professional errorMsg in the requested language.
Return one JSON object with the fields required by the current game.
</system_directives>`;
    let parsed = null;
    let imgUrl = null;

    if (randomType === "IDENTITY_IMAGE") {
        const categories = ["Country, city or region", "Public person", "Anime, film or series", "Animal", "Plant", "Planet"];
        const selectedCategory = categories[Math.floor(Math.random() * categories.length)];
        const combinedPrompt = `<system_directives name="identity_image">
You are Asistan, the visual-question generator for the Identity Image game.
Create a factual image-generation target and one valid question about the represented subject.
The image target has no text.
The question asks for the identity represented by the image.
The answer is the exact identity used for the generated image.
Return a raw JSON object containing imagePrompt, options, question, and answer.
The response language is ${langName}.
</system_directives>

Category: ${selectedCategory}`;
        const comboResp = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: combinedPrompt }], 900);
        const parsedCombo = parseAIJsonResponse(comboResp.response, ["imagePrompt", "options", "question", "answer", "explanation", "successMsg", "errorMsg", "timeLimit"]);
        const aiJsonResult = await runAIImage(parsedCombo.imagePrompt);
        if (!aiJsonResult || !aiJsonResult.image) throw new Error("Flux AI Image API failed");
        const buffer = Buffer.from(aiJsonResult.image, "base64");
        const filename = `img_${Date.now()}_${crypto.randomUUID().split("-")[0]}.png`;
        const r2Key = `uploads/${filename}`;
        await s3.send(new PutObjectCommand({
          Bucket: process.env.R2_BUCKET,
          Key: r2Key,
          Body: buffer,
          ContentType: "image/png"
        }));
        imgUrl = `${process.env.R2_PUBLIC_URL}/${r2Key}`;
        parsed = { question: parsedCombo.question, options: [], answer: parsedCombo.answer, explanation: parsedCombo.explanation, successMsg: parsedCombo.successMsg, errorMsg: parsedCombo.errorMsg, timeLimit: parsedCombo.timeLimit, qType: "IDENTITY_IMAGE" };
    } else if (randomType === "MCQ") {
        const mcqPrompt = `Language: ${langName}
Question type: MCQ
Required fields: question, options, answer
Question shape: direct question ending with a question mark
Options: at most 4
Answer: one option`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: mcqPrompt }], 700);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit"]);
    } else if (randomType === "TRUE_FALSE") {
        const tfPrompt = `Language: ${langName}
Question type: TRUE_FALSE
Required fields: question, options, answer
Options: ${JSON.stringify(tfOpts)}
Question shape: factual statement
Answer: one localized option`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: tfPrompt }], 600);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit"]);
        parsed.options = tfOpts;
        if (parsed.answer !== tfOpts[0] && parsed.answer !== tfOpts[1]) throw new Error("Invalid True/False answer");
    } else {
        const fbPrompt = `Language: ${langName}
Question type: FILL_BLANK
Required fields: question, options, answer
Question shape: one sentence containing one blank marker
Options: []
Answer: one word`;
        const aiResponse = await runAI([{ role: "system", content: systemInstructionStrict }, { role: "user", content: fbPrompt }], 600);
        parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit"]);
        parsed.options = [];
    }

    logEvent("SUCCESS", "MODE_3_PURE_AI_GENERATION", "Execution completed successfully");
    parsed.timeLimit = Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20));
    return { parsed, randomType, imgUrl, finalSuccess: parsed.successMsg || "", finalError: parsed.errorMsg || "", finalExplanation: parsed.explanation || "" };
}

const builtInGames = [
  {
    slug: "mcq",
    name: "MCQ",
    description: "Factual multiple choice questions.",
    systemDirectives: `<system_directives name="mcq">
You are Asistan, the MCQ game engine for Mizik.
Generate one factually verified question in the requested language.
Set qType to MCQ.
Provide 2 to 4 distinct options and set answer to one option.
Provide a concise explanation that identifies the verified fact behind the answer.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the question difficulty.
Return one JSON object with exactly these fields: question, options, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "true_false",
    name: "True or False",
    description: "Factual true or false statements.",
    systemDirectives: `<system_directives name="true_false">
You are Asistan, the True or False game engine for Mizik.
Generate one factually verified statement in the requested language.
Set qType to TRUE_FALSE.
Use the two localized labels supplied by the server as options and select one as answer.
Provide a concise explanation that identifies the verified fact.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the statement difficulty.
Return one JSON object with exactly these fields: question, options, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "fill_blank",
    name: "Fill Blank",
    description: "Complete a factual sentence.",
    systemDirectives: `<system_directives name="fill_blank">
You are Asistan, the Fill Blank game engine for Mizik.
Generate one factually verified sentence in the requested language with one blank represented by ____.
Set qType to FILL_BLANK.
Set answer to the exact missing word or short phrase.
Keep options empty.
Provide a concise explanation tied to the verified fact.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the difficulty.
Return one JSON object with exactly these fields: question, options, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "identity_image",
    name: "Identity Image",
    description: "Identify a subject represented by an image.",
    systemDirectives: `<system_directives name="identity_image">
You are Asistan, the Identity Image game engine for Mizik.
An image is supplied for visual identification.
Determine the intended identity from the supplied image context and generate one valid identification question in the requested language.
Set qType to IDENTITY_IMAGE.
Set answer to the exact intended identity.
Provide a concise explanation describing why the identity is correct from the available task information.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the visual difficulty.
Return one JSON object with exactly these fields: question, options, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 3,
    modes: ["solo", "multi"]
  },
  {
    slug: "word_twist",
    name: "Word Twist",
    description: "Unscramble letters into the target word.",
    systemDirectives: `<system_directives name="word_twist">
You are Asistan, the Word Twist game engine for Mizik.
Use only a word supplied by the server for the current round.
Set qType to WORD_TWIST.
Confirm the supplied word is the answer and create a concise explanation for the word.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the word length and difficulty.
Return one JSON object with exactly these fields: scrambled, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 3,
    modes: ["solo", "multi"]
  },
  {
    slug: "text_twist",
    name: "Text Twist",
    description: "Build words from a supplied letter set.",
    systemDirectives: `<system_directives name="text_twist">
You are Asistan, the Text Twist game engine for Mizik.
Use only the supplied letter set and requested language.
Set qType to TEXT_TWIST.
Select one valid target word that can be formed from the supplied letters.
Provide a concise explanation for the target word.
Provide a professional successMsg and a professional errorMsg in the requested language.
Set timeLimit to an integer in seconds suited to the word difficulty.
Return one JSON object with exactly these fields: letters, answer, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 4,
    modes: ["solo", "multi"]
  },
  {
    slug: "2048",
    name: "2048",
    description: "Tile-merging puzzle game.",
    systemDirectives: `<system_directives name="2048">
You are Asistan, the 2048 game engine for Mizik.
Define the current board setup and target for the requested level.
Set qType to 2048.
Provide a concise explanation of the current objective and professional successMsg and errorMsg in the requested language.
Return one JSON object with exactly these fields: boardSize, startTileValues, targetValue, explanation, successMsg, errorMsg, timeLimit, qType.
</system_directives>`,
    soloPoints: 4,
    modes: ["solo"]
  }
];
async function seedBuiltInGames() {
  for (const game of builtInGames) {
    await GameDefinition.findOneAndUpdate(
      { slug: game.slug },
      { ...game, updatedAt: new Date() },
      { upsert: true, new: true, setDefaultsOnInsert: true }
    ).catch(() => {});
  }
}

async function getGameDefinition(slugOrName) {
  if (!slugOrName) return null;
  const normalized = String(slugOrName).trim().toLowerCase().replace(/\s+/g, "_");
  const built = builtInGames.find(game => game.slug === normalized || game.name.toLowerCase() === String(slugOrName).trim().toLowerCase());
  if (built) return built;
  const stored = await GameDefinition.findOne({
    $or: [{ slug: normalized }, { name: new RegExp(`^${String(slugOrName).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") }]
  }).lean().catch(() => null);
  return stored || null;
}

async function listGameDefinitions() {
  const stored = await GameDefinition.find({}).sort({ name: 1 }).lean().catch(() => []);
  const map = new Map();
  for (const game of builtInGames) map.set(game.slug, game);
  for (const game of stored) map.set(game.slug, game);
  return [...map.values()].map(game => ({
    slug: game.slug,
    name: game.name,
    description: game.description,
    soloPoints: game.soloPoints,
    modes: game.modes
  }));
}

function normalizeGameSlug(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "_");
}

function hashText(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex");
}

function scrambleWord(word) {
  const chars = String(word || "").split("");
  for (let i = chars.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [chars[i], chars[j]] = [chars[j], chars[i]];
  }
  const scrambled = chars.join("");
  return scrambled === word && word.length > 1 ? `${chars.slice(1).join("")}${chars[0]}` : scrambled;
}

async function generateGameQuestion(game, language, level, imageUrl = null, gameContext = "", words = []) {
  if (game.slug === "word_twist") {
    const pool = Array.isArray(words) && words.length > 0 ? words.filter(word => typeof word === "string" && word.trim().length >= 2) : [];
    if (pool.length === 0) throw new Error("Word Twist requires a word pool");
    const answer = pool[Math.floor(Math.random() * pool.length)].trim();
    const prompt = `${game.systemDirectives}
Language: ${language}
Word: ${answer}
Level: ${level}
Scrambled: ${scrambleWord(answer)}`;
    const response = await runAI([{ role: "system", content: game.systemDirectives }, { role: "user", content: prompt }], 550);
    const parsed = parseAIJsonResponse(response.response, ["scrambled", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
    parsed.answer = answer;
    parsed.scrambled = scrambleWord(answer);
    parsed.qType = "WORD_TWIST";
    parsed.timeLimit = Math.max(8, Math.min(40, Number(parsed.timeLimit) || answer.length * 3));
    return parsed;
  }

  if (game.slug === "text_twist") {
    const letters = String(gameContext || "").replace(/[^A-Za-zÀ-ÿ]/g, "").toUpperCase();
    if (letters.length < 4) throw new Error("Text Twist requires a letter set");
    const prompt = `${game.systemDirectives}

Language: ${language}
Letter set: ${letters}
Level: ${level}`;
    const response = await runAI([{ role: "system", content: game.systemDirectives }, { role: "user", content: prompt }], 500);
    const parsed = parseAIJsonResponse(response.response, ["letters", "answer", "explanation", "successMsg", "errorMsg", "timeLimit"]);
    parsed.qType = "TEXT_TWIST";
    return parsed;
  }

  if (game.slug === "2048") {
    const prompt = `${game.systemDirectives}
Language: ${language}
Level: ${level}`;
    const response = await runAI([{ role: "system", content: game.systemDirectives }, { role: "user", content: prompt }], 500);
    const parsed = parseAIJsonResponse(response.response, ["boardSize", "startTileValues", "targetValue", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
    parsed.boardSize = Math.max(4, Number(parsed.boardSize) || 4);
    parsed.startTileValues = Array.isArray(parsed.startTileValues) ? parsed.startTileValues : [2, 4];
    parsed.targetValue = Number(parsed.targetValue) || 2048;
    parsed.qType = "2048";
    parsed.timeLimit = Math.max(0, Number(parsed.timeLimit) || 0);
    return parsed;
  }

  const system = game.systemDirectives;
  let userPrompt = `Language: ${language}
Level: ${level}
Game: ${game.name}
Context: ${gameContext || "general factual knowledge"}`;
  if (imageUrl) userPrompt += `\nImage URL: ${imageUrl}`;
  const response = await runAI([{ role: "system", content: system }, { role: "user", content: userPrompt }], 900);
  const required = ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
  const parsed = parseAIJsonResponse(response.response, required);
  parsed.timeLimit = Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20));
  return parsed;
}

async function generateAndSaveGame(gameInput) {
  const name = String(gameInput.name || "").trim();
  const description = String(gameInput.description || "").trim();
  const language = String(gameInput.language || "en").trim().toLowerCase();
  const baseRules = String(gameInput.rules || "").trim();
  if (!name || !description) throw new Error("name and description required");
  const slug = normalizeGameSlug(gameInput.slug || name);
  const generatorSystem = `<system_directives name="game_generator">
You are Asistan, the game-definition generator for Mizik.
Create a reusable game definition from the supplied game title, description, and rules.
The definition contains one concise English systemDirectives block dedicated to the supplied game.
The directive defines the game identity, objective, exact output fields, answer semantics, feedback fields, and timing field for that game.
The directive remains specific to the game and uses only the current turn data supplied by the server.
Return one JSON object with slug, name, description, systemDirectives, soloPoints, and modes.
The systemDirectives value starts with <system_directives name="..."> and ends with </system_directives>.
</system_directives>`;
  const prompt = `Game title: ${name}
Description: ${description}
Rules: ${baseRules}
Language context: ${language}`;
  const response = await runAI([{ role: "system", content: generatorSystem }, { role: "user", content: prompt }], 1200);
  const generated = parseAIJsonResponse(response.response, ["slug", "name", "description", "systemDirectives", "soloPoints", "modes"]);
  generated.slug = normalizeGameSlug(generated.slug || slug) || slug;
  generated.name = String(generated.name || name).trim();
  generated.description = String(generated.description || description).trim();
  generated.systemDirectives = String(generated.systemDirectives || "").trim();
  generated.soloPoints = Math.max(1, Math.min(10, Number(generated.soloPoints) || 1));
  generated.modes = Array.isArray(generated.modes) && generated.modes.length > 0 ? generated.modes : ["solo", "multi"];
  if (!generated.systemDirectives.startsWith("<system_directives") || !generated.systemDirectives.includes("</system_directives>")) {
    throw new Error("Generated game prompt is invalid");
  }
  const saved = await GameDefinition.findOneAndUpdate(
    { slug: generated.slug },
    { ...generated, updatedAt: new Date() },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
  return saved.toObject();
}

function normalizeLanguage(language) {
  const lang = String(language || "en").trim().toLowerCase();
  if (lang === "fr" || lang === "es" || lang === "ht" || lang === "en") return lang;
  return "en";
}

async function getUserLanguageByTfid(tfid) {
  const normalized = String(tfid || "").trim();
  if (!normalized) return "en";
  try {
    const users = mongoose.connection.db.collection("users");
    const user = await users.findOne({
      $or: [{ tfid: normalized }, { TFID: normalized }, { dh7: normalized }]
    }, { projection: { language: 1, lang: 1, langue: 1 } });
    return normalizeLanguage(user?.language || user?.lang || user?.langue || "en");
  } catch {
    return "en";
  }
}

function buildInvitationText(language, fromTfid) {
  const lang = normalizeLanguage(language);
  const variants = invitationVariants[lang] || invitationVariants.en;
  const template = variants[Math.floor(Math.random() * variants.length)];
  return template.replaceAll("{TFID}", String(fromTfid || "").trim());
}

function invitationChoices(language) {
  const lang = normalizeLanguage(language);
  if (lang === "fr") return ["Accepté", "Refuser"];
  if (lang === "es") return ["Aceptar", "Rechazar"];
  if (lang === "ht") return ["Aksepte", "Refize"];
  return ["Accept", "Decline"];
}

function createGameInvitation({ gameId, fromTfid, toTfid, gameSlug }) {
  const invitationId = crypto.randomUUID();
  const now = Date.now();
  db.prepare("INSERT INTO game_invitations (invitation_id, game_id, from_tfid, to_tfid, game_slug, status, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)").run(
    invitationId,
    gameId,
    fromTfid,
    toTfid,
    gameSlug,
    "pending",
    now,
    now + GAME_TTL_MS
  );
  return invitationId;
}

function loadPendingInvitationsForTfid(tfid) {
  const rows = db.prepare("SELECT * FROM game_invitations WHERE to_tfid = ? AND status = ? AND expires_at > ? ORDER BY created_at DESC").all(String(tfid || "").trim(), "pending", Date.now());
  return rows.map(row => row);
}

function updateInvitation(invitationId, status) {
  db.prepare("UPDATE game_invitations SET status = ? WHERE invitation_id = ? AND status = ? AND expires_at > ?").run(status, invitationId, "pending", Date.now());
  return db.prepare("SELECT * FROM game_invitations WHERE invitation_id = ?").get(invitationId);
}

function deleteExpiredInvitations() {
  db.prepare("DELETE FROM game_invitations WHERE expires_at <= ?").run(Date.now());
}

function connectionMessage(language, status) {
  const lang = normalizeLanguage(language);
  const messages = localizedConnectionMessages[lang] || localizedConnectionMessages.en;
  return messages[status] || messages.disconnectError;
}

async function getGamePlayerLanguage(gameSession, tfid) {
  if (gameSession?.state?.playerLanguages?.[tfid]) return normalizeLanguage(gameSession.state.playerLanguages[tfid]);
  return getUserLanguageByTfid(tfid);
}

async function sendInvitationToTfid(gameSession, toTfid) {
  const target = String(toTfid || "").trim();
  if (!target) return null;
  const language = await getUserLanguageByTfid(target);
  const invitationId = createGameInvitation({
    gameId: gameSession.gameId,
    fromTfid: gameSession.ownerTfid,
    toTfid: target,
    gameSlug: gameSession.gameSlug
  });
  const invitation = {
    invitationId,
    gameId: gameSession.gameId,
    fromTfid: gameSession.ownerTfid,
    toTfid: target,
    gameSlug: gameSession.gameSlug,
    status: "pending",
    text: buildInvitationText(language, gameSession.ownerTfid),
    choices: invitationChoices(language),
    language,
    expiresAt: Date.now() + GAME_TTL_MS
  };
  const sockets = wsClients.get(`user:${target}`);
  if (sockets) {
    for (const socket of sockets) wsSend(socket, { type: "game:invitation", invitation });
  }
  return invitation;
}

function gameExpiresAt() {
  return Date.now() + GAME_TTL_MS;
}

function gamePlayersFromRow(row) {
  try {
    return JSON.parse(row.players || "[]");
  } catch {
    return [];
  }
}

function gameStateFromRow(row) {
  try {
    return JSON.parse(row.state || "{}");
  } catch {
    return {};
  }
}

function saveGameSession(gameId, gameSlug, ownerTfid, players, state, expiresAt = gameExpiresAt()) {
  db.prepare("REPLACE INTO game_sessions (game_id, game_slug, owner_tfid, players, state, expires_at) VALUES (?, ?, ?, ?, ?, ?)").run(
    gameId,
    gameSlug,
    ownerTfid,
    JSON.stringify(players),
    JSON.stringify(state),
    expiresAt
  );
}

function loadGameSession(gameId) {
  const row = db.prepare("SELECT * FROM game_sessions WHERE game_id = ? AND expires_at > ?").get(gameId, Date.now());
  if (!row) return null;
  return {
    gameId: row.game_id,
    gameSlug: row.game_slug,
    ownerTfid: row.owner_tfid,
    players: gamePlayersFromRow(row),
    state: gameStateFromRow(row),
    expiresAt: row.expires_at
  };
}

function cleanupExpiredGames() {
  const now = Date.now();
  db.prepare("DELETE FROM game_sessions WHERE expires_at <= ?").run(now);
  db.prepare("DELETE FROM game_words WHERE expires_at <= ?").run(now);
  deleteExpiredInvitations();
  for (const [gameId, sockets] of wsClients.entries()) {
    if (!loadGameSession(gameId) && sockets.size === 0) wsClients.delete(gameId);
  }
}

function wsSend(ws, payload) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}

function wsBroadcast(gameId, payload) {
  const sockets = wsClients.get(gameId);
  if (!sockets) return;
  for (const ws of sockets) wsSend(ws, payload);
}

async function getStoredWords(gameId) {
  const row = db.prepare("SELECT words FROM game_words WHERE game_id = ? AND expires_at > ?").get(gameId, Date.now());
  if (!row) return [];
  try {
    return JSON.parse(row.words || "[]");
  } catch {
    return [];
  }
}

function saveStoredWords(gameId, words) {
  db.prepare("REPLACE INTO game_words (game_id, words, expires_at) VALUES (?, ?, ?)").run(gameId, JSON.stringify(words), gameExpiresAt());
}

async function createGameSession({ gameSlug, ownerTfid, tfids, language = "en", level = 1, words = [], gameContext = "" }) {
  const game = await getGameDefinition(gameSlug);
  if (!game) throw new Error("Game not found");
  const players = [...new Set([ownerTfid, ...(Array.isArray(tfids) ? tfids : [])].map(value => String(value || "").trim()).filter(Boolean))];
  if (players.length === 0) throw new Error("At least one TFID is required");
  if (players.length > 2 && !String(game.modes || []).includes("multi")) throw new Error("Game does not support multiple players");
  const mode = players.length === 1 ? "solo" : "multi";
  if (!Array.isArray(game.modes) || !game.modes.includes(mode)) throw new Error(`Game does not support ${mode}`);
  const gameId = crypto.randomUUID();
  const playerLanguages = Object.fromEntries(await Promise.all(players.map(async tfid => [tfid, tfid === ownerTfid ? normalizeLanguage(language) : await getUserLanguageByTfid(tfid)])));
  const pending = players.length > 1;
  const state = {
    status: pending ? "pending" : "active",
    language: normalizeLanguage(language),
    playerLanguages,
    level: Number(level) || 1,
    mode,
    players,
    scores: Object.fromEntries(players.map(tfid => [tfid, 0])),
    turnIndex: 0,
    question: null,
    questionNumber: 1,
    askedTo: players[0],
    answeredBy: null,
    currentAnswer: null,
    timeLimit: null,
    questionStartedAt: null,
    gameContext
  };
  saveGameSession(gameId, game.slug, ownerTfid, players, state);
  if (Array.isArray(words) && words.length > 0) saveStoredWords(gameId, words);
  if (!pending) {
    const question = await generateGameQuestion(game, language, Number(level) || 1, null, gameContext, words);
    state.question = question.question || null;
    state.questionPayload = question;
    state.currentAnswer = question.answer || null;
    state.timeLimit = Number(question.timeLimit) || 0;
    state.questionStartedAt = Date.now();
    saveGameSession(gameId, game.slug, ownerTfid, players, state);
  }
  return loadGameSession(gameId);
}

async function advanceGameTurn(session) {
  const game = await getGameDefinition(session.gameSlug);
  if (!game) throw new Error("Game not found");
  const words = await getStoredWords(session.gameId);
  const question = await generateGameQuestion(game, session.state.language, session.state.level, null, session.state.gameContext || "", words);
  session.state.question = question.question || null;
  session.state.questionPayload = question;
  session.state.currentAnswer = question.answer || null;
  session.state.timeLimit = Number(question.timeLimit) || 0;
  session.state.questionStartedAt = Date.now();
  session.state.askedTo = session.players[session.state.turnIndex % session.players.length];
  session.state.answeredBy = null;
  session.state.questionNumber = Number(session.state.questionNumber || 0) + 1;
  saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
  return loadGameSession(session.gameId);
}

function passSameQuestionToOtherPlayer(session) {
  if (session.players.length > 1) {
    session.state.turnIndex = (session.state.turnIndex + 1) % session.players.length;
    session.state.askedTo = session.players[session.state.turnIndex];
  } else {
    session.state.askedTo = session.players[0];
  }
  session.state.answeredBy = null;
  session.state.questionStartedAt = Date.now();
  saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
  return loadGameSession(session.gameId);
}

async function validateGameAnswer(session, tfid, answer) {
  if (!session || !session.state || session.state.status !== "active") throw new Error("Game is not active");
  if (!session.players.includes(tfid)) throw new Error("TFID is not part of this game");
  if (session.state.askedTo !== tfid) throw new Error("It is another player's turn");

  const elapsedMs = Date.now() - Number(session.state.questionStartedAt || Date.now());
  const timeLimit = Number(session.state.timeLimit || 0);

  if (timeLimit > 0 && elapsedMs > timeLimit * 1000) {
    const nextSession = passSameQuestionToOtherPlayer(session);
    return { correct: false, timedOut: true, completed: false, session: nextSession };
  }

  const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[session.state.language] || "English";
  const visibleQuestion = session.state.questionPayload?.question || session.state.questionPayload?.scrambled || session.state.questionPayload?.letters || session.state.question || "";
  let correct;
  try {
    correct = await runAIValidator(visibleQuestion, session.state.currentAnswer, answer, langName, session.gameSlug);
  } catch {
    correct = checkAnswerTolerance(answer, session.state.currentAnswer);
  }

  session.state.answeredBy = tfid;

  if (correct) {
    const game = await getGameDefinition(session.gameSlug);
    const points = session.players.length === 1 ? Number(game?.soloPoints || 3) : 1;
    session.state.scores[tfid] = Number(session.state.scores[tfid] || 0) + points;
    session.state.lastResult = {
      correct: true,
      player: tfid,
      points,
      answer: session.state.currentAnswer
    };

    if (session.players.length === 1 && session.state.scores[tfid] >= 10) {
      session.state.status = "completed";
      session.state.winner = tfid;
      saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
      return { correct: true, completed: true, session: loadGameSession(session.gameId) };
    }

    if (session.players.length > 1) {
      session.state.turnIndex = (session.state.turnIndex + 1) % session.players.length;
      saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
      const nextSession = await advanceGameTurn(loadGameSession(session.gameId));
      return { correct: true, completed: false, session: nextSession };
    }

    saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
    return { correct: true, completed: false, session: loadGameSession(session.gameId) };
  }

  const nextSession = passSameQuestionToOtherPlayer(session);
  nextSession.state.lastResult = {
    correct: false,
    player: tfid,
    points: 0
  };
  saveGameSession(nextSession.gameId, nextSession.gameSlug, nextSession.ownerTfid, nextSession.players, nextSession.state);
  return { correct: false, completed: false, session: loadGameSession(nextSession.gameId) };
}


function buildPublicGameState(session) {
  return {
    gameId: session.gameId,
    gameSlug: session.gameSlug,
    ownerTfid: session.ownerTfid,
    players: session.players,
    state: {
      ...session.state,
      currentAnswer: undefined,
      questionPayload: session.state.questionPayload ? {
        ...session.state.questionPayload,
        answer: undefined
      } : null
    },
    expiresAt: session.expiresAt || gameExpiresAt()
  };
}

async function generateGameHelp(session) {
  const game = await getGameDefinition(session.gameSlug);
  if (!game) throw new Error("Game not found");
  const questionData = session.state.questionPayload || {};
  const system = `<system_directives name="${normalizeGameSlug(game.name)}_help">
You are Asistan, the hint engine for the current Mizik game.
Help the player reach the stored correct answer through a progressively useful clue.
The final answer remains hidden.
Return a raw JSON object containing hint, hintType, nextHintAvailableInMs.
The hint can reveal a letter, a letter position, a structural clue, a semantic clue, or a focused explanation suited to the current game.
</system_directives>`;
  const prompt = `Game: ${game.name}
Question data: ${JSON.stringify({ ...questionData, answer: session.state.currentAnswer })}
Language: ${session.state.language}
Current player: ${session.state.askedTo}`;
  const response = await runAI([{ role: "system", content: system }, { role: "user", content: prompt }], 450);
  const parsed = parseAIJsonResponse(response.response, ["hint", "hintType", "nextHintAvailableInMs"]);
  parsed.nextHintAvailableInMs = Math.max(0, Number(parsed.nextHintAvailableInMs) || 0);
  return parsed;
}

app.get("/games", async (req, res) => {
  try {
    return res.json({ games: await listGameDefinitions() });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

app.post("/games/generate", async (req, res) => {
  try {
    const game = await generateAndSaveGame(req.body || {});
    return res.json({
      success: true,
      game: {
        slug: game.slug,
        name: game.name,
        description: game.description,
        soloPoints: game.soloPoints,
        modes: game.modes
      }
    });
  } catch (e) {
    return res.status(500).json({ success: false, error: e.message });
  }
});

async function buildQuizForSession(body) {
  const session_id = body.session_id?.trim();
  if (!session_id) throw new Error("session_id required");

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
    db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
  }

  const servedRows = db.prepare("SELECT quiz_id FROM served_questions WHERE session_id = ?").all(session_id);
  const servedIds = servedRows.map(r => r.quiz_id).filter(id => mongoose.Types.ObjectId.isValid(id)).map(id => new mongoose.Types.ObjectId(id));
  let matchCriteria = { lang: language, level: current_step_num };
  if (servedIds.length > 0) matchCriteria._id = { $nin: servedIds };

  let dbItems = await BaseQuiz.aggregate([{ $match: matchCriteria }, { $sample: { size: 100 } }]).catch(() => []);
  if (dbItems.length === 0) {
    let broadCriteria = { lang: language };
    if (servedIds.length > 0) broadCriteria._id = { $nin: servedIds };
    dbItems = await BaseQuiz.aggregate([{ $match: broadCriteria }, { $sample: { size: 100 } }]).catch(() => []);
  }
  if (dbItems.length === 0 && servedIds.length > 0) {
    db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
    dbItems = await BaseQuiz.aggregate([{ $match: { lang: language, level: current_step_num } }, { $sample: { size: 100 } }]).catch(() => []);
    if (dbItems.length === 0) dbItems = await BaseQuiz.aggregate([{ $match: { lang: language } }, { $sample: { size: 100 } }]).catch(() => []);
  }
  if (dbItems.length === 0) {
    const randomJsonRecord = await getRandomFromJsonFile(language, current_step_num);
    if (randomJsonRecord) dbItems = [randomJsonRecord];
  }

  let randomItem = dbItems.length > 0 ? dbItems[Math.floor(Math.random() * dbItems.length)] : null;
  if (randomItem && randomItem._id) {
    db.prepare("INSERT OR IGNORE INTO served_questions (session_id, quiz_id) VALUES (?, ?)").run(session_id, randomItem._id.toString());
  }

  const requestedGame = body.game?.trim();
  let generatedGame = null;
  if (requestedGame) generatedGame = await getGameDefinition(requestedGame);

  if (generatedGame) {
    const storedWords = body.words || await getStoredWords(body.game_id || "");
    const gameQuestion = await generateGameQuestion(generatedGame, language, current_step_num, body.image_url || null, body.game_context || "", storedWords);
    randomItem = {
      _id: null,
      question: gameQuestion.question,
      options: gameQuestion.options || [],
      answer: gameQuestion.answer,
      qType: gameQuestion.qType || "MCQ",
      imageUrl: gameQuestion.imageUrl || null,
      explanation: gameQuestion.explanation || "",
      successMsg: gameQuestion.successMsg || "",
      errorMsg: gameQuestion.errorMsg || ""
    };
  }

  let parsed = null;
  let randomType = "MCQ";
  let imgUrl = null;
  let finalSuccess = null;
  let finalError = null;
  let finalExplanation = null;
  let success = false;

  if (randomItem) {
    const strategy = Math.floor(Math.random() * 3);
    try {
      const result = strategy === 0
        ? await executeMode0PureDB(randomItem)
        : strategy === 1
          ? await executeMode1ImproveExisting(randomItem, langName, language)
          : await executeMode2CreateSimilar(randomItem, langName, language);
      parsed = result.parsed;
      randomType = result.randomType;
      imgUrl = result.imgUrl || randomItem.imageUrl || null;
      finalSuccess = result.finalSuccess || randomItem.successMsg || "";
      finalError = result.finalError || randomItem.errorMsg || "";
      finalExplanation = result.finalExplanation || randomItem.explanation || "";
      success = true;
    } catch (e) {
      logEvent("WARN", "STRATEGY_SELECTOR", `Primary strategy failed: ${e.message}`);
    }
  }

  if (!success) {
    try {
      const result = await executeMode3PureAIGeneration(session_id, language, langName);
      parsed = result.parsed;
      randomType = result.randomType;
      imgUrl = result.imgUrl;
      finalSuccess = result.finalSuccess;
      finalError = result.finalError;
      finalExplanation = result.finalExplanation;
      success = true;
    } catch (e) {
      logEvent("ERROR", "STRATEGY_SELECTOR", `AI generation fallback failed: ${e.message}`);
    }
  }

  if (!success) {
    if (randomItem) {
      parsed = { question: randomItem.question, options: Array.isArray(randomItem.options) ? randomItem.options : [], answer: randomItem.answer };
      randomType = randomItem.qType || "MCQ";
      imgUrl = randomItem.imageUrl || null;
      finalSuccess = randomItem.successMsg || "";
      finalError = randomItem.errorMsg || "";
      finalExplanation = randomItem.explanation || "";
    } else {
      parsed = { question: "System recovery question.", options: ["True", "False"], answer: "True" };
      randomType = "TRUE_FALSE";
      finalSuccess = "";
      finalError = "";
      finalExplanation = "";
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
        const uniqueFilename = `${Date.now()}_${crypto.randomUUID().split("-")[0]}_${path.basename(resolvedPath)}`;
        const r2Key = `uploads/${uniqueFilename}`;
        await s3.send(new PutObjectCommand({
          Bucket: process.env.R2_BUCKET,
          Key: r2Key,
          Body: fileBuffer,
          ContentType: mimeType
        }));
        imgUrl = `${process.env.R2_PUBLIC_URL}/${r2Key}`;
      } catch (e) {
        logEvent("ERROR", "STORAGE", `Failed to migrate local image: ${e.message}`);
      }
    }
  }

  const safeOptions = Array.isArray(parsed.options) ? parsed.options : [];
  await saveCurrentQuiz(session_id, randomType, parsed.question, JSON.stringify(safeOptions), imgUrl, parsed.answer, finalExplanation || "", finalSuccess || "", finalError || "");

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
  if (requestedGame) quizData.game = requestedGame;

  return quizData;
}

async function validateQuizForSession(body) {
  const session_id = body.session_id?.trim();
  const user_answer = body.user_answer?.trim() || "";
  const silent = Boolean(body.silent);
  if (!session_id) throw new Error("session_id required");

  const current = await getCurrentQuiz(session_id);
  if (!current) throw new Error("No active quiz");

  if (silent && !user_answer) {
    const now = Date.now();
    const last = silentValidationAt.get(session_id) || 0;
    if (now - last < SILENT_VALIDATE_COOLDOWN_MS) {
      return {
        correct: false,
        validation_skipped: true,
        next_validation_in_ms: SILENT_VALIDATE_COOLDOWN_MS - (now - last),
        explanation: "",
        language: (await getProgress(session_id))?.language || "en"
      };
    }
    silentValidationAt.set(session_id, now);
    return {
      correct: false,
      validation_skipped: true,
      next_validation_in_ms: SILENT_VALIDATE_COOLDOWN_MS,
      explanation: "",
      language: (await getProgress(session_id))?.language || "en"
    };
  }

  if (!user_answer) throw new Error("user_answer required");

  if (current.image_url) {
    const activeKey = getKeyFromUrl(current.image_url);
    if (activeKey) deleteFromR2(activeKey).catch(() => {});
  }

  let progress = await getProgress(session_id);
  if (!progress) progress = { language: "en", current_step: 1, consecutive_correct: 0 };

  const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";
  const gameName = current.q_type || "Quiz";
  let isCorrect = false;
  try {
    isCorrect = await runAIValidator(current.question, current.answer, user_answer, langName, gameName);
  } catch (e) {
    isCorrect = checkAnswerTolerance(user_answer, current.answer);
    logEvent("WARN", "VALIDATION_AI", `Validator fallback used: ${e.message}`);
  }

  let finalFeedback = "";
  const baseMessage = isCorrect ? current.success_msg : current.error_msg;
  if (current.explanation) finalFeedback = baseMessage ? `${baseMessage}\n\n${current.explanation}` : current.explanation;
  else finalFeedback = baseMessage || "";

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

  return {
    correct: isCorrect,
    explanation: finalFeedback,
    successMsg: current.success_msg || "",
    errorMsg: current.error_msg || "",
    consecutive_correct: new_consec,
    needed_for_next_level: Math.max(0, 7 - new_consec),
    current_step: new_step,
    language: progress.language
  };
}

app.post("/quizz", async (req, res) => {
  try {
    const quizData = await buildQuizForSession(req.body || {});
    logEvent("INFO", "ROUTER", `Quiz data generated for ${req.body?.session_id || "unknown"}`);
    return res.json(quizData);
  } catch (e) {
    logEvent("ERROR", "ROUTER", `Critical failure in /quizz endpoint: ${e.message}`);
    const sessionId = req.body?.session_id || "default";
    await saveCurrentQuiz(sessionId, "TRUE_FALSE", "System recovery question.", JSON.stringify(["True", "False"]), null, "True", "", "", "");
    return res.json({ type: "TRUE_FALSE", question: "System recovery question.", options: ["True", "False"], error_msg: e.message });
  }
});

app.post("/validate", async (req, res) => {
  try {
    return res.json(await validateQuizForSession(req.body || {}));
  } catch (e) {
    logEvent("ERROR", "VALIDATION", `Exception during validation: ${e.message}`);
    return res.json({ correct: false, explanation: "", successMsg: "", errorMsg: "", consecutive_correct: 0, needed_for_next_level: 7, current_step: 1, language: "en" });
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

app.post("/game/create", async (req, res) => {
  try {
    const body = req.body || {};
    const session = await createGameSession({
      gameSlug: body.game,
      ownerTfid: body.owner_tfid || body.TFID,
      tfids: body.tfids,
      language: body.lang || "en",
      level: body.level || 1,
      words: body.words || [],
      gameContext: body.game_context || ""
    });
    const invitations = [];
    for (const tfid of session.players) {
      if (tfid !== session.ownerTfid) {
        const invitation = await sendInvitationToTfid(session, tfid);
        if (invitation) invitations.push(invitation);
      }
    }
    return res.json({ success: true, game: buildPublicGameState(session), invitations });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

app.post("/game/invitation/accept", async (req, res) => {
  try {
    const invitationId = String(req.body?.invitation_id || req.body?.invitationId || "").trim();
    const tfid = String(req.body?.tfid || "").trim();
    const invitation = updateInvitation(invitationId, "accepted");
    if (!invitation || invitation.to_tfid !== tfid) return res.status(404).json({ success: false, error: "Invitation not found or expired" });
    const session = loadGameSession(invitation.game_id);
    if (!session) return res.status(404).json({ success: false, error: "Game not found or expired" });
    if (session.state.status === "pending") {
      session.state.status = "active";
      const game = await getGameDefinition(session.gameSlug);
      const words = await getStoredWords(session.gameId);
      const question = await generateGameQuestion(game, session.state.language, session.state.level, null, session.state.gameContext || "", words);
      session.state.question = question.question || null;
      session.state.questionPayload = question;
      session.state.currentAnswer = question.answer || null;
      session.state.timeLimit = Number(question.timeLimit) || 0;
      session.state.questionStartedAt = Date.now();
      saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
    }
    wsBroadcast(session.gameId, { type: "game:update", game: buildPublicGameState(session) });
    wsBroadcast(session.gameId, { type: "game:question", game: buildPublicGameState(session) });
    return res.json({ success: true, message: connectionMessage(await getUserLanguageByTfid(tfid), "accepted"), game: buildPublicGameState(session) });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

app.post("/game/invitation/refuse", async (req, res) => {
  try {
    const invitationId = String(req.body?.invitation_id || req.body?.invitationId || "").trim();
    const tfid = String(req.body?.tfid || "").trim();
    const invitation = updateInvitation(invitationId, "declined");
    if (!invitation || invitation.to_tfid !== tfid) return res.status(404).json({ success: false, error: "Invitation not found or expired" });
    return res.json({ success: true, message: connectionMessage(await getUserLanguageByTfid(tfid), "declined"), game_id: invitation.game_id });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

app.get("/game/state/:gameId", async (req, res) => {
  const session = loadGameSession(req.params.gameId);
  if (!session) return res.status(404).json({ error: "Game not found or expired" });
  return res.json({ success: true, game: buildPublicGameState(session) });
});

app.post("/game/answer", async (req, res) => {
  try {
    const body = req.body || {};
    const session = loadGameSession(body.game_id);
    if (!session) return res.status(404).json({ error: "Game not found or expired" });
    const result = await validateGameAnswer(session, String(body.tfid || "").trim(), String(body.answer || "").trim());
    if (result.session) wsBroadcast(session.gameId, { type: "game:update", game: buildPublicGameState(result.session) });
    return res.json({
      success: true,
      correct: result.correct,
      timedOut: Boolean(result.timedOut),
      completed: Boolean(result.completed),
      game: buildPublicGameState(result.session || session)
    });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

app.post("/game/help", async (req, res) => {
  try {
    const session = loadGameSession(req.body?.game_id);
    if (!session) return res.status(404).json({ error: "Game not found or expired" });
    const help = await generateGameHelp(session);
    return res.json({ success: true, help });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

const httpServer = createServer(app);
const webSocketServer = new WebSocketServer({ noServer: true });

function registerSocketForGame(gameId, ws) {
  if (!wsClients.has(gameId)) wsClients.set(gameId, new Set());
  wsClients.get(gameId).add(ws);
}

function registerSocketForUser(tfid, ws) {
  const key = `user:${String(tfid || "").trim()}`;
  if (!key.slice(5)) return;
  if (!wsClients.has(key)) wsClients.set(key, new Set());
  wsClients.get(key).add(ws);
}

async function notifyGamePlayerConnection(gameId, changedTfid, status, error = false) {
  const session = loadGameSession(gameId);
  if (!session) return;
  for (const tfid of session.players) {
    if (tfid === changedTfid) continue;
    const sockets = wsClients.get(`user:${tfid}`);
    if (!sockets) continue;
    const language = await getGamePlayerLanguage(session, tfid);
    for (const socket of sockets) {
      wsSend(socket, {
        type: "connection:status",
        success: !error,
        status,
        tfid: changedTfid,
        message: connectionMessage(language, error ? "disconnectError" : status)
      });
    }
  }
}

async function unregisterSocket(ws, connectionError = false) {
  const tfid = ws.tfid;
  const activeGameIds = [];
  if (tfid) {
    const rows = db.prepare("SELECT game_id FROM game_sessions WHERE expires_at > ?").all(Date.now());
    for (const row of rows) {
      const session = loadGameSession(row.game_id);
      if (session && session.players.includes(tfid) && session.state.status === "active") activeGameIds.push(session.gameId);
    }
  }
  let userStillConnected = false;
  for (const [key, sockets] of wsClients.entries()) {
    sockets.delete(ws);
    if (key === `user:${String(tfid || "").trim()}` && sockets.size > 0) userStillConnected = true;
    if (sockets.size === 0) wsClients.delete(key);
  }
  if (!userStillConnected) {
    for (const gameId of activeGameIds) {
      await notifyGamePlayerConnection(gameId, tfid, "disconnected", connectionError);
    }
  }
}

async function handleWebSocketMessage(ws, message) {
  const data = typeof message === "string" ? JSON.parse(message) : JSON.parse(message.toString());
  const type = data.type;

  if (type === "auth") {
    ws.sessionId = String(data.session_id || "").trim();
    ws.tfid = String(data.tfid || data.TFID || "").trim();
    if (!ws.sessionId && !ws.tfid) {
      wsSend(ws, { type: "error", error: "session_id or tfid required" });
      return;
    }
    wsSend(ws, { type: "ready", tfid: ws.tfid || null, session_id: ws.sessionId || null });
    if (ws.tfid) {
      registerSocketForUser(ws.tfid, ws);
      const pendingInvitations = loadPendingInvitationsForTfid(ws.tfid);
      for (const item of pendingInvitations) {
        const language = await getUserLanguageByTfid(ws.tfid);
        wsSend(ws, { type: "game:invitation", invitation: { invitationId: item.invitation_id, gameId: item.game_id, fromTfid: item.from_tfid, toTfid: item.to_tfid, gameSlug: item.game_slug, status: item.status, text: buildInvitationText(language, item.from_tfid), choices: invitationChoices(language), language, expiresAt: item.expires_at } });
      }
    }
    wsSend(ws, { type: "games:list", games: await listGameDefinitions() });
    const activeGames = [];
    const rows = db.prepare("SELECT * FROM game_sessions WHERE expires_at > ?").all(Date.now());
    for (const row of rows) {
      const players = gamePlayersFromRow(row);
      if ((ws.tfid && players.includes(ws.tfid)) || (ws.sessionId && row.owner_tfid === ws.sessionId)) {
        activeGames.push(buildPublicGameState({
          gameId: row.game_id,
          gameSlug: row.game_slug,
          ownerTfid: row.owner_tfid,
          players,
          state: gameStateFromRow(row),
          expiresAt: row.expires_at
        }));
        registerSocketForGame(row.game_id, ws);
      }
    }
    if (activeGames.length > 0) wsSend(ws, { type: "games:active", games: activeGames });
    return;
  }

  if (type === "games:list") {
    wsSend(ws, { type: "games:list", games: await listGameDefinitions() });
    return;
  }

  if (type === "game:create") {
    const ownerTfid = String(data.owner_tfid || ws.tfid || "").trim();
    const session = await createGameSession({
      gameSlug: data.game,
      ownerTfid,
      tfids: data.tfids,
      language: data.lang || "en",
      level: data.level || 1,
      words: data.words || [],
      gameContext: data.game_context || ""
    });
    registerSocketForGame(session.gameId, ws);
    if (ws.tfid) registerSocketForUser(ws.tfid, ws);
    const invitations = [];
    for (const tfid of session.players) {
      if (tfid !== session.ownerTfid) {
        const invitation = await sendInvitationToTfid(session, tfid);
        if (invitation) invitations.push(invitation);
      }
    }
    wsSend(ws, { type: "game:created", game: buildPublicGameState(session), invitations });
    if (session.state.status === "active") wsBroadcast(session.gameId, { type: "game:question", game: buildPublicGameState(session) });
    return;
  }

  if (type === "game:join") {
    const session = loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const tfid = String(data.tfid || ws.tfid || "").trim();
    if (!tfid || !session.players.includes(tfid)) throw new Error("TFID is not part of this game");
    ws.tfid = tfid;
    registerSocketForGame(session.gameId, ws);
    registerSocketForUser(tfid, ws);
    const language = await getGamePlayerLanguage(session, tfid);
    wsSend(ws, { type: "game:state", game: buildPublicGameState(session), connection: { success: true, message: connectionMessage(language, "connected"), status: "connected" } });
    if (session.state.status === "active") await notifyGamePlayerConnection(session.gameId, tfid, "connected", false);
    return;
  }

  if (type === "game:invitation:accept") {
    const tfid = String(data.tfid || ws.tfid || "").trim();
    const invitation = updateInvitation(String(data.invitation_id || data.invitationId || ""), "accepted");
    if (!invitation || invitation.to_tfid !== tfid) throw new Error("Invitation not found or expired");
    const session = loadGameSession(invitation.game_id);
    if (!session) throw new Error("Game not found or expired");
    if (session.state.status === "pending") {
      session.state.status = "active";
      session.state.turnIndex = 0;
      session.state.askedTo = session.players[0];
      const game = await getGameDefinition(session.gameSlug);
      const words = await getStoredWords(session.gameId);
      const question = await generateGameQuestion(game, session.state.language, session.state.level, null, session.state.gameContext || "", words);
      session.state.question = question.question || null;
      session.state.questionPayload = question;
      session.state.currentAnswer = question.answer || null;
      session.state.timeLimit = Number(question.timeLimit) || 0;
      session.state.questionStartedAt = Date.now();
      saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
    }
    ws.tfid = tfid;
    registerSocketForUser(tfid, ws);
    registerSocketForGame(session.gameId, ws);
    const language = await getGamePlayerLanguage(session, tfid);
    wsSend(ws, { type: "game:invitation", invitation: { invitationId: invitation.invitation_id, gameId: invitation.game_id, fromTfid: invitation.from_tfid, toTfid: invitation.to_tfid, gameSlug: invitation.game_slug, status: "accepted", text: connectionMessage(language, "accepted"), choices: invitationChoices(language), language } });
    await notifyGamePlayerConnection(session.gameId, tfid, "connected", false);
    wsBroadcast(session.gameId, { type: "game:update", game: buildPublicGameState(session) });
    if (session.state.status === "active") wsBroadcast(session.gameId, { type: "game:question", game: buildPublicGameState(session) });
    return;
  }

  if (type === "game:invitation:refuse") {
    const tfid = String(data.tfid || ws.tfid || "").trim();
    const invitation = updateInvitation(String(data.invitation_id || data.invitationId || ""), "declined");
    if (!invitation || invitation.to_tfid !== tfid) throw new Error("Invitation not found or expired");
    const language = await getUserLanguageByTfid(tfid);
    wsSend(ws, { type: "game:invitation", invitation: { invitationId: invitation.invitation_id, gameId: invitation.game_id, fromTfid: invitation.from_tfid, toTfid: invitation.to_tfid, gameSlug: invitation.game_slug, status: "declined", text: connectionMessage(language, "declined"), choices: invitationChoices(language), language } });
    const ownerSockets = wsClients.get(`user:${invitation.from_tfid}`);
    if (ownerSockets) {
      const ownerLanguage = await getUserLanguageByTfid(invitation.from_tfid);
      for (const socket of ownerSockets) wsSend(socket, { type: "connection:status", success: true, status: "declined", tfid, message: connectionMessage(ownerLanguage, "declined") });
    }
    return;
  }

  if (type === "game:words") {
    const session = loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const words = Array.isArray(data.words) ? data.words : [];
    saveStoredWords(session.gameId, words);
    wsBroadcast(session.gameId, { type: "game:words", game_id: session.gameId, words });
    return;
  }

  if (type === "game:answer") {
    const session = loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const tfid = String(data.tfid || ws.tfid || "").trim();
    const result = await validateGameAnswer(session, tfid, String(data.answer || "").trim());
    if (result.session) {
      registerSocketForGame(result.session.gameId, ws);
      wsBroadcast(result.session.gameId, {
        type: result.completed ? "game:complete" : "game:update",
        correct: result.correct,
        timedOut: Boolean(result.timedOut),
        game: buildPublicGameState(result.session)
      });
      if (!result.completed && result.session.state.askedTo) {
        wsBroadcast(result.session.gameId, { type: "game:question", game: buildPublicGameState(result.session) });
      }
    }
    return;
  }

  if (type === "game:help") {
    const session = loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const help = await generateGameHelp(session);
    wsSend(ws, { type: "game:help", game_id: session.gameId, help });
    return;
  }

  if (type === "quiz:next") {
    const quiz = await buildQuizForSession({
      session_id: data.session_id || ws.sessionId,
      lang: data.lang,
      level: data.level,
      game: data.game,
      image_url: data.image_url,
      game_context: data.game_context
    });
    wsSend(ws, { type: "quiz:question", quiz });
    return;
  }

  if (type === "quiz:answer") {
    const result = await validateQuizForSession({
      session_id: data.session_id || ws.sessionId,
      user_answer: data.user_answer,
      silent: Boolean(data.silent)
    });
    wsSend(ws, { type: "quiz:result", result });
    return;
  }

  if (type === "quiz:validate_silent") {
    const result = await validateQuizForSession({
      session_id: data.session_id || ws.sessionId,
      user_answer: "",
      silent: true
    });
    wsSend(ws, { type: "quiz:validate_silent", result });
    return;
  }

  throw new Error("Unknown WebSocket message type");
}

webSocketServer.on("connection", ws => {
  ws.on("message", async message => {
    try {
      await handleWebSocketMessage(ws, message);
    } catch (e) {
      wsSend(ws, { type: "error", error: e.message });
    }
  });
  ws.on("close", () => { unregisterSocket(ws, false).catch(() => {}); });
  ws.on("error", () => { unregisterSocket(ws, true).catch(() => {}); });
});

httpServer.on("upgrade", (request, socket, head) => {
  const origin = request.headers.origin || "";
  const authHeader = request.headers.authorization || "";
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  const token = url.searchParams.get("token") || "";
  const authorized = (origin && origin.endsWith(".adamdh7.org")) || authHeader === "Bearer adamdh7" || token === "adamdh7";
  if (!authorized) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }
  webSocketServer.handleUpgrade(request, socket, head, ws => {
    webSocketServer.emit("connection", ws, request);
  });
});

setInterval(cleanupExpiredGames, 1000);
setInterval(() => silentValidationAt.clear(), 60 * 60 * 1000);

mongoose.connection.once("open", async () => {
  await seedBuiltInGames();
  syncJsonToMongo();
  executeBackgroundMassGeneration(false);
  setInterval(() => executeBackgroundMassGeneration(false), 70 * 60 * 1000);
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  logEvent("SUCCESS", "SYSTEM", `Server running on port ${PORT} with WebSocket support`);
});
