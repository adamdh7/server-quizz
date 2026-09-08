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

const localizedQuizErrors = {
  en: "The requested game question could not be prepared.",
  fr: "La question demandée n’a pas pu être préparée.",
  es: "No se pudo preparar la pregunta solicitada.",
  ht: "Kesyon jwèt yo mande a pa t kapab prepare."
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

mongoose.connect(MONGO_URI, { dbName: "quiz" }).then(async () => {
  await migrateLegacyGameDefinitions();
  logEvent("SUCCESS", "DATABASE", "Connected to MongoDB successfully");
}).catch(e => {
  logEvent("ERROR", "DATABASE", `MongoDB connection failed: ${e.message}`);
});

const baseQuizSchema = new mongoose.Schema({
  recordType: { type: String, default: "question" },
  recordKey: { type: String, index: true },
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
baseQuizSchema.index({ recordKey: 1 }, { unique: true, sparse: true });
const BaseQuiz = mongoose.model("BaseQuiz", baseQuizSchema, "quiz");

const gameDefinitionSchema = new mongoose.Schema({
  recordType: { type: String, default: "game" },
  recordKey: { type: String, index: true },
  slug: String,
  name: String,
  description: String,
  systemDirectives: String,
  soloPoints: { type: Number, default: 1 },
  modes: [String]
}, { collection: "quiz" });
gameDefinitionSchema.index({ recordKey: 1 }, { unique: true, sparse: true });
const GameDefinition = mongoose.model("GameDefinition", gameDefinitionSchema, "quiz");

const progressSchema = new mongoose.Schema({ sessionId: { type: String, unique: true }, language: String, currentStep: Number, consecutiveCorrect: Number });
const Progress = mongoose.model("Progress", progressSchema);

const userSchema = new mongoose.Schema({ sessionId: { type: String, unique: true }, data: String });
const UserInfo = mongoose.model("UserInfo", userSchema);

async function migrateLegacyGameDefinitions() {
  try {
    const database = mongoose.connection.db;
    const collections = await database.listCollections({ name: "game_definitions" }).toArray();
    if (!collections.length) return;
    const legacy = database.collection("game_definitions");
    const legacyGames = await legacy.find({}).toArray();
    for (const game of legacyGames) {
      const slug = normalizeGameSlug(game.slug || game.name);
      if (!slug || !game.systemDirectives) continue;
      await GameDefinition.findOneAndUpdate(
        { recordType: "game", recordKey: `game:${slug}` },
        {
          recordType: "game",
          recordKey: `game:${slug}`,
          slug,
          name: String(game.name || slug),
          description: String(game.description || ""),
          systemDirectives: String(game.systemDirectives),
          soloPoints: Number(game.soloPoints) || 1,
          modes: Array.isArray(game.modes) && game.modes.length ? game.modes : ["solo", "multi"]
        },
        { upsert: true, setDefaultsOnInsert: true }
      );
    }
    await legacy.drop();
  } catch {}
}

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
    
  } catch (e) {
    
  }
}

const localizedTrueFalse = {
  en: ["True", "False"],
  fr: ["Vrai", "Faux"],
  es: ["Verdadero", "Falso"],
  ht: ["Vrè", "Fo"]
};

function parseAIJsonResponse(rawResponse, expectedKeys) {
  const rawText = cleanAIResponse(typeof rawResponse === "string" ? rawResponse : JSON.stringify(rawResponse || {}));
  const isArrayExpected = expectedKeys.includes("ARRAY_FORMAT_ONLY");
  const candidates = [];
  if (isArrayExpected) {
    const first = rawText.indexOf("[");
    const last = rawText.lastIndexOf("]");
    if (first !== -1 && last > first) candidates.push(rawText.slice(first, last + 1));
  } else {
    const first = rawText.indexOf("{");
    const last = rawText.lastIndexOf("}");
    if (first !== -1 && last > first) candidates.push(rawText.slice(first, last + 1));
  }
  if (candidates.length === 0) throw new Error("JSON response unavailable");
  let parsedData;
  try {
    parsedData = JSON.parse(candidates[0]);
  } catch {
    throw new Error("JSON response invalid");
  }
  if (isArrayExpected) return parsedData;
  const hasKeys = value => value && typeof value === "object" && expectedKeys.every(key => key === "ARRAY_FORMAT_ONLY" || Object.prototype.hasOwnProperty.call(value, key));
  if (hasKeys(parsedData)) return parsedData;
  const queue = [parsedData];
  while (queue.length) {
    const current = queue.shift();
    if (hasKeys(current)) return current;
    if (current && typeof current === "object") {
      for (const value of Object.values(current)) {
        if (value && typeof value === "object") queue.push(value);
      }
    }
  }
  const missing = expectedKeys.filter(key => key !== "ARRAY_FORMAT_ONLY" && !Object.prototype.hasOwnProperty.call(parsedData, key));
  throw new Error(`JSON response missing ${missing[0] || "required field"}`);
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
  if (!available) throw new Error("Cloudflare AI unavailable");
  const { cred, index } = available;
  const timeoutMs = model === VALIDATOR_AI_MODEL ? 15000 : 60000;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const aiUrl = `https://api.cloudflare.com/client/v4/accounts/${cred.accountId}/ai/run/${model}`;
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
    const rawText = await response.text();
    let json = {};
    try {
      json = JSON.parse(rawText);
    } catch {}
    const isRateLimited = response.status === 429 || response.status === 401 || response.status === 403 || (json.errors && json.errors.some(err => err?.message && /allocation|limit/i.test(err.message)));
    if (isRateLimited) {
      cfCredentials[index].lockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      clearTimeout(timeout);
      if (retries + 1 < cfCredentials.length) return runAI(messages, max_tokens, retries + 1, model);
      throw new Error("Cloudflare AI rate limit");
    }
    clearTimeout(timeout);
    if (!response.ok) throw new Error(`Cloudflare AI HTTP ${response.status}`);
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
    throw new Error("Cloudflare AI returned no result");
  } catch (e) {
    clearTimeout(timeout);
    if (retries + 1 < cfCredentials.length && /fetch failed|aborted|network|HTTP 5/i.test(String(e.message))) {
      return runAI(messages, max_tokens, retries + 1, model);
    }
    throw e;
  }
}

async function runAIValidator(question, correctAnswer, userAnswer, language, gameName, retries = 0) {
  const system = `<system_directives name="answer_validator">
You are Asistan, the answer validation engine for Mizik.
Assess the supplied user answer against the supplied question and verified answer.
Interpret equivalent wording, ordinary spelling variation, number formatting, and language variation.
Return exactly one lowercase status word: correct or incorrect.
</system_directives>`;
  const user = `Question: ${question}
Verified answer: ${correctAnswer}
User answer: ${userAnswer}
Language: ${language}
Game: ${gameName}`;
  const result = await runAI([
    { role: "system", content: system },
    { role: "user", content: user }
  ], 20, retries, VALIDATOR_AI_MODEL);
  const normalized = cleanAIResponse(result.response).toLowerCase().trim();
  if (normalized === "correct") return true;
  if (normalized === "incorrect") return false;
  const match = normalized.match(/^(correct|incorrect)$/);
  if (match) return match[1] === "correct";
  throw new Error("Validator status unavailable");
}

async function runAIImage(prompt, retries = 0) {
    const available = getAvailableCFCredential();
    if (!available) {
        
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
            
        }

        const isRateLimited = response.status === 429 || response.status === 401 || response.status === 403 || (json.errors && json.errors.length > 0 && json.errors.some(err => err.message && (err.message.includes("allocation") || err.message.includes("limit"))));

        if (isRateLimited) {
            
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
    logEvent("ERROR", "ROUTER", `User info save failed: ${e.message}`);
    return res.json({ success: false, error: "Database error" });
  }
});

async function executeMode0PureDB(randomItem) {
    if (!randomItem) throw new Error("Source item missing");
    const parsed = { question: randomItem.question, options: randomItem.options, answer: randomItem.answer };
    const randomType = randomItem.qType || "MCQ";
    const imgUrl = randomItem.imageUrl || null;
    const finalSuccess = randomItem.successMsg || null;
    const finalError = randomItem.errorMsg || null;
    const finalExplanation = randomItem.explanation || null;
    return { parsed, randomType, imgUrl, finalSuccess, finalError, finalExplanation };
}

async function executeMode1ImproveExisting(randomItem, langName, langCode) {
  if (!randomItem) throw new Error("Source item missing");
  const game = getBuiltInGameByQType(randomItem.qType) || {
    name: randomItem.qType || "Quiz",
    systemDirectives: `<system_directives name="stored_question">
You are Asistan, the current Mizik game engine.
Generate one question matching the supplied game type and language.
Return question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType as a JSON object.
</system_directives>`
  };
  const prompt = `${game.systemDirectives}

Language: ${langName}
Level: ${randomItem.level || 1}
Current verified question: ${randomItem.question}
Current verified answer: ${randomItem.answer}

Create a fresh question for the same game using the supplied verified content as the factual basis.
Return question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType as one JSON object.`;
  const aiResponse = await runAI([
    { role: "system", content: game.systemDirectives },
    { role: "user", content: prompt }
  ], 900);
  const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
  if (randomItem.qType === "TRUE_FALSE") {
    parsed.options = localizedTrueFalse[langCode] || ["True", "False"];
    if (!parsed.options.includes(parsed.answer)) throw new Error("Invalid True or False answer");
  }
  parsed.timeLimit = Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20));
  return {
    parsed,
    randomType: randomItem.qType || parsed.qType || "MCQ",
    imgUrl: randomItem.imageUrl || null,
    finalSuccess: parsed.successMsg || randomItem.successMsg || "",
    finalError: parsed.errorMsg || randomItem.errorMsg || "",
    finalExplanation: parsed.explanation || randomItem.explanation || ""
  };
}

async function executeMode2CreateSimilar(randomItem, langName, langCode) {
  if (!randomItem) throw new Error("Source item missing");
  const game = getBuiltInGameByQType(randomItem.qType) || {
    name: randomItem.qType || "Quiz",
    systemDirectives: `<system_directives name="stored_question">
You are Asistan, the current Mizik game engine.
Generate one question matching the supplied game type and language.
Return question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType as a JSON object.
</system_directives>`
  };
  const prompt = `${game.systemDirectives}

Language: ${langName}
Level: ${randomItem.level || 1}
Verified subject: ${randomItem.question}
Verified answer: ${randomItem.answer}

Create a new question on the same verified subject area while keeping the same game type and language.
Return question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType as one JSON object.`;
  const aiResponse = await runAI([
    { role: "system", content: game.systemDirectives },
    { role: "user", content: prompt }
  ], 900);
  const parsed = parseAIJsonResponse(aiResponse.response, ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
  if (randomItem.qType === "TRUE_FALSE") {
    parsed.options = localizedTrueFalse[langCode] || ["True", "False"];
    if (!parsed.options.includes(parsed.answer)) throw new Error("Invalid True or False answer");
  }
  parsed.timeLimit = Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20));
  return {
    parsed,
    randomType: randomItem.qType || parsed.qType || "MCQ",
    imgUrl: randomItem.imageUrl || null,
    finalSuccess: parsed.successMsg || randomItem.successMsg || "",
    finalError: parsed.errorMsg || randomItem.errorMsg || "",
    finalExplanation: parsed.explanation || randomItem.explanation || ""
  };
}

async function executeMode3PureAIGeneration(language, langName, requestedGame = null, imageUrl = null, gameContext = "", level = 1) {
  const selectedGame = requestedGame ? await getGameDefinition(requestedGame) : null;
  const game = selectedGame || builtInGames[Math.floor(Math.random() * builtInGames.length)];
  if (!game) throw new Error("No game available");
  const result = await generateGameQuestion(game, language, level, imageUrl, gameContext, []);
  return {
    parsed: result,
    randomType: result.qType || game.slug.toUpperCase(),
    imgUrl: result.imageUrl || null,
    finalSuccess: result.successMsg || "",
    finalError: result.errorMsg || "",
    finalExplanation: result.explanation || ""
  };
}

const builtInGames = [
  {
    slug: "mcq",
    name: "MCQ",
    description: "Multiple choice knowledge game.",
    systemDirectives: `<system_directives name="mcq">
You are Asistan, the MCQ game engine for Mizik.
Create one accurate question in the requested language.
Build 2 to 4 distinct answer options.
Set answer to one exact option.
Explain the verified fact briefly.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to difficulty and return it as timeLimit in seconds.
Return one JSON object with question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType.
Set qType to MCQ.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "true_false",
    name: "True or False",
    description: "Factual statement judgment game.",
    systemDirectives: `<system_directives name="true_false">
You are Asistan, the True or False game engine for Mizik.
Create one accurate factual statement in the requested language.
Use the two answer labels supplied by the server.
Set answer to the correct localized label.
Explain the verified fact briefly.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to difficulty and return it as timeLimit in seconds.
Return one JSON object with question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType.
Set qType to TRUE_FALSE.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "fill_blank",
    name: "Fill Blank",
    description: "Factual missing-word game.",
    systemDirectives: `<system_directives name="fill_blank">
You are Asistan, the Fill Blank game engine for Mizik.
Create one accurate factual sentence in the requested language.
Place one blank marker ____ inside the sentence.
Set answer to the missing word or short phrase.
Keep options as an empty array.
Explain the verified fact briefly.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to difficulty and return it as timeLimit in seconds.
Return one JSON object with question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType.
Set qType to FILL_BLANK.
</system_directives>`,
    soloPoints: 1,
    modes: ["solo", "multi"]
  },
  {
    slug: "identity_image",
    name: "Identity Image",
    description: "Visual identification game.",
    systemDirectives: `<system_directives name="identity_image">
You are Asistan, the Identity Image game engine for Mizik.
An image is supplied for visual identification.
Identify one clear subject represented by the supplied image context.
Write one direct identification question in the requested language.
Set answer to the exact intended identity.
Explain the identifying fact briefly.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to visual difficulty and return it as timeLimit in seconds.
Return one JSON object with question, options, answer, explanation, successMsg, errorMsg, timeLimit and qType.
Set qType to IDENTITY_IMAGE.
</system_directives>`,
    soloPoints: 3,
    modes: ["solo", "multi"]
  },
  {
    slug: "word_twist",
    name: "Word Twist",
    description: "Unscramble a supplied word.",
    systemDirectives: `<system_directives name="word_twist">
You are Asistan, the Word Twist game engine for Mizik.
Use the supplied target word as the answer.
Produce a scrambled form using the same letters.
Set qType to WORD_TWIST.
Explain the word briefly in the requested language.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to word difficulty and return it as timeLimit in seconds.
Return one JSON object with scrambled, answer, explanation, successMsg, errorMsg, timeLimit and qType.
</system_directives>`,
    soloPoints: 3,
    modes: ["solo", "multi"]
  },
  {
    slug: "text_twist",
    name: "Text Twist",
    description: "Build a word from supplied letters.",
    systemDirectives: `<system_directives name="text_twist">
You are Asistan, the Text Twist game engine for Mizik.
Use only the supplied letter set for the current round.
Select one valid target word in the requested language.
Set qType to TEXT_TWIST.
Explain the target word briefly.
Write successMsg and errorMsg naturally in the requested language.
Choose a response time suited to word difficulty and return it as timeLimit in seconds.
Return one JSON object with letters, answer, explanation, successMsg, errorMsg, timeLimit and qType.
</system_directives>`,
    soloPoints: 4,
    modes: ["solo", "multi"]
  },
  {
    slug: "2048",
    name: "2048",
    description: "Tile merging puzzle game.",
    systemDirectives: `<system_directives name="2048">
You are Asistan, the 2048 game engine for Mizik.
Define a playable board configuration for the requested level.
Set qType to 2048.
Set boardSize, startTileValues and targetValue.
Explain the current objective briefly in the requested language.
Write successMsg and errorMsg naturally in the requested language.
Set timeLimit to a suitable number of seconds or zero for an untimed round.
Return one JSON object with boardSize, startTileValues, targetValue, explanation, successMsg, errorMsg, timeLimit and qType.
</system_directives>`,
    soloPoints: 4,
    modes: ["solo"]
  }
];

function getBuiltInGameByQType(qType) {
  const map = {
    MCQ: "mcq",
    TRUE_FALSE: "true_false",
    FILL_BLANK: "fill_blank",
    IDENTITY_IMAGE: "identity_image",
    WORD_TWIST: "word_twist",
    TEXT_TWIST: "text_twist",
    "2048": "2048"
  };
  const slug = map[String(qType || "").toUpperCase()];
  return builtInGames.find(game => game.slug === slug) || null;
}

async function getGameDefinition(slugOrName) {
  if (!slugOrName) return null;
  const text = String(slugOrName).trim();
  const normalized = normalizeGameSlug(text);
  const built = builtInGames.find(game => game.slug === normalized || game.name.toLowerCase() === text.toLowerCase());
  if (built) return built;
  const stored = await GameDefinition.findOne({
    recordType: "game",
    $or: [{ slug: normalized }, { name: new RegExp(`^${text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") }]
  }).lean().catch(() => null);
  return stored || null;
}

async function listGameDefinitions() {
  const stored = await GameDefinition.find({ recordType: "game" }).sort({ name: 1 }).lean().catch(() => []);
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
  if (!game) throw new Error("Game not found");

  if (game.slug === "word_twist") {
    const pool = Array.isArray(words) ? words.map(word => String(word || "").trim()).filter(word => word.length >= 2) : [];
    if (!pool.length) throw new Error("Word Twist requires supplied words");
    const answer = pool[Math.floor(Math.random() * pool.length)];
    const scrambled = scrambleWord(answer);
    const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}
Target word: ${answer}
Scrambled letters: ${scrambled}`;
    const response = await runAI([
      { role: "system", content: game.systemDirectives },
      { role: "user", content: prompt }
    ], 450);
    const parsed = parseAIJsonResponse(response.response, ["scrambled", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
    parsed.scrambled = scrambled;
    parsed.answer = answer;
    parsed.qType = "WORD_TWIST";
    parsed.timeLimit = Math.max(8, Math.min(60, Number(parsed.timeLimit) || answer.length * 3));
    return parsed;
  }

  if (game.slug === "text_twist") {
    const letters = String(gameContext || "").replace(/[^A-Za-zÀ-ÿ]/g, "").toUpperCase();
    if (letters.length < 4) throw new Error("Text Twist requires a letter set");
    const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}
Letter set: ${letters}`;
    const response = await runAI([
      { role: "system", content: game.systemDirectives },
      { role: "user", content: prompt }
    ], 450);
    const parsed = parseAIJsonResponse(response.response, ["letters", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
    parsed.letters = letters;
    parsed.qType = "TEXT_TWIST";
    parsed.timeLimit = Math.max(8, Math.min(90, Number(parsed.timeLimit) || 25));
    return parsed;
  }

  if (game.slug === "2048") {
    const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}`;
    const response = await runAI([
      { role: "system", content: game.systemDirectives },
      { role: "user", content: prompt }
    ], 450);
    const parsed = parseAIJsonResponse(response.response, ["boardSize", "startTileValues", "targetValue", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
    parsed.boardSize = Math.max(4, Math.min(8, Number(parsed.boardSize) || 4));
    parsed.startTileValues = Array.isArray(parsed.startTileValues) ? parsed.startTileValues.map(Number).filter(Number.isFinite) : [2, 4];
    parsed.targetValue = Number(parsed.targetValue) || 2048;
    parsed.qType = "2048";
    parsed.timeLimit = Math.max(0, Math.min(600, Number(parsed.timeLimit) || 0));
    return parsed;
  }

  const system = game.systemDirectives;
  let userPrompt = `${system}

Language: ${language}
Level: ${level}
Game: ${game.name}
Current context: ${gameContext || "general factual knowledge"}`;
  if (imageUrl) userPrompt += `\nImage URL: ${imageUrl}`;
  if (game.slug === "true_false") userPrompt += `\nLocalized choices: ${JSON.stringify(localizedTrueFalse[language] || localizedTrueFalse.en)}`;
  const response = await runAI([
    { role: "system", content: system },
    { role: "user", content: userPrompt }
  ], 800);
  const required = ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
  const parsed = parseAIJsonResponse(response.response, required);
  if (game.slug === "true_false") {
    const tfOpts = localizedTrueFalse[language] || localizedTrueFalse.en;
    parsed.options = tfOpts;
    if (!tfOpts.includes(parsed.answer)) throw new Error("Invalid True or False answer");
  }
  parsed.timeLimit = Math.max(5, Math.min(180, Number(parsed.timeLimit) || 20));
  return parsed;
}

async function generateAndSaveGame(gameInput) {
  const name = String(gameInput.name || "").trim();
  const description = String(gameInput.description || "").trim();
  const language = normalizeLanguage(gameInput.language || "en");
  const baseRules = String(gameInput.rules || "").trim();
  if (!name || !description) throw new Error("name and description required");
  const slug = normalizeGameSlug(gameInput.slug || name);
  const generatorSystem = `<system_directives name="game_generator">
You are Asistan, the reusable game-definition engine for Mizik.
Create one durable game definition from the supplied title, description and rules.
Produce one English systemDirectives block dedicated to that game.
The directive identifies the game, its objective, its exact JSON fields, answer semantics, explanation field, feedback fields and timing field.
The directive is concise, specific and reusable.
Return one JSON object with slug, name, description, systemDirectives, soloPoints and modes.
</system_directives>`;
  const prompt = `Game title: ${name}
Description: ${description}
Rules: ${baseRules}
Language context: ${language}`;
  const response = await runAI([
    { role: "system", content: generatorSystem },
    { role: "user", content: prompt }
  ], 1000);
  const generated = parseAIJsonResponse(response.response, ["slug", "name", "description", "systemDirectives", "soloPoints", "modes"]);
  generated.slug = normalizeGameSlug(generated.slug || slug) || slug;
  generated.name = String(generated.name || name).trim();
  generated.description = String(generated.description || description).trim();
  generated.systemDirectives = String(generated.systemDirectives || "").trim();
  generated.soloPoints = Math.max(1, Math.min(10, Number(generated.soloPoints) || 1));
  generated.modes = Array.isArray(generated.modes) && generated.modes.length ? generated.modes : ["solo", "multi"];
  if (!/^<system_directives\b[\s\S]*<\/system_directives>$/.test(generated.systemDirectives)) throw new Error("Generated game prompt invalid");
  const saved = await GameDefinition.findOneAndUpdate(
    { recordType: "game", recordKey: `game:${generated.slug}` },
    {
      recordType: "game",
      recordKey: `game:${generated.slug}`,
      slug: generated.slug,
      name: generated.name,
      description: generated.description,
      systemDirectives: generated.systemDirectives,
      soloPoints: generated.soloPoints,
      modes: generated.modes
    },
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
  const correct = await runAIValidator(visibleQuestion, session.state.currentAnswer, answer, langName, session.gameSlug);

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
  let matchCriteria = { recordType: { $in: [null, "question"] }, lang: language, level: current_step_num };
  if (servedIds.length > 0) matchCriteria._id = { $nin: servedIds };

  let dbItems = await BaseQuiz.aggregate([{ $match: matchCriteria }, { $sample: { size: 100 } }]).catch(() => []);
  if (dbItems.length === 0) {
    let broadCriteria = { recordType: { $in: [null, "question"] }, lang: language };
    if (servedIds.length > 0) broadCriteria._id = { $nin: servedIds };
    dbItems = await BaseQuiz.aggregate([{ $match: broadCriteria }, { $sample: { size: 100 } }]).catch(() => []);
  }
  if (dbItems.length === 0 && servedIds.length > 0) {
    db.prepare("DELETE FROM served_questions WHERE session_id = ?").run(session_id);
    dbItems = await BaseQuiz.aggregate([{ $match: { recordType: { $in: [null, "question"] }, lang: language, level: current_step_num } }, { $sample: { size: 100 } }]).catch(() => []);
    if (dbItems.length === 0) dbItems = await BaseQuiz.aggregate([{ $match: { recordType: { $in: [null, "question"] }, lang: language } }, { $sample: { size: 100 } }]).catch(() => []);
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
      const result = await executeMode3PureAIGeneration(language, langName, requestedGame || null, body.image_url || null, body.game_context || "", current_step_num);
      parsed = result.parsed;
      randomType = result.randomType;
      imgUrl = result.imgUrl;
      finalSuccess = result.finalSuccess;
      finalError = result.finalError;
      finalExplanation = result.finalExplanation;
      success = true;
    } catch (e) {
      throw new Error(`Question generation failed: ${e.message}`);
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
  isCorrect = await runAIValidator(current.question, current.answer, user_answer, langName, gameName);

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
    return res.json(quizData);
  } catch (e) {
    const sessionId = req.body?.session_id || "";
    const progress = sessionId ? await getProgress(sessionId).catch(() => null) : null;
    const language = normalizeLanguage(req.body?.lang || progress?.language || "en");
    return res.status(503).json({
      success: false,
      type: null,
      question: null,
      options: [],
      image_url: null,
      explanation: "",
      successMsg: "",
      errorMsg: localizedQuizErrors[language] || localizedQuizErrors.en,
      language,
      retryable: true
    });
  }
});

app.post("/validate", async (req, res) => {
  try {
    return res.json(await validateQuizForSession(req.body || {}));
  } catch (e) {
    const sessionId = req.body?.session_id || "";
    const progress = sessionId ? await getProgress(sessionId).catch(() => null) : null;
    const language = normalizeLanguage(req.body?.lang || progress?.language || "en");
    return res.status(422).json({
      correct: false,
      explanation: "",
      successMsg: "",
      errorMsg: localizedQuizErrors[language] || localizedQuizErrors.en,
      consecutive_correct: progress?.consecutive_correct || 0,
      needed_for_next_level: Math.max(0, 7 - (progress?.consecutive_correct || 0)),
      current_step: progress?.current_step || 1,
      language
    });
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


const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  logEvent("SUCCESS", "SYSTEM", `Server running on port ${PORT} with WebSocket support`);
});
