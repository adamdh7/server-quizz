import "dotenv/config";
import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";
import mongoose from "mongoose";
import { S3Client, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";

const logEvent = (level, context, message) => {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level}] [${context}] ${message}`);
};

const app = express();
app.use(express.json());

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
  logEvent("SUCCESS", "DATABASE", "Connected to MongoDB successfully");
  await RuntimeData.createCollection().catch(() => {});
  startBackgroundPreGeneration();
}).catch(e => {
  logEvent("ERROR", "DATABASE", `MongoDB connection failed: ${e.message}`);
});

const baseQuizSchema = new mongoose.Schema({
  recordType: { type: String, default: "question" },
  recordKey: { type: String, unique: true, sparse: true },
  lang: String,
  level: { type: Number, default: 1 },
  gameSlug: String,
  qType: String,
  question: String,
  options: [String],
  imageUrl: String,
  answer: String,
  explanation: String,
  successMsg: String,
  errorMsg: String,
  timeLimit: Number,
  gameData: mongoose.Schema.Types.Mixed,
  lastUsedAt: Date,
  usageCount: { type: Number, default: 0 }
});
const BaseQuiz = mongoose.model("BaseQuiz", baseQuizSchema, "quiz");

const runtimeSchema = new mongoose.Schema({
  recordKey: { type: String, unique: true, required: true },
  kind: { type: String, required: true },
  sessionId: String,
  data: mongoose.Schema.Types.Mixed,
  expiresAt: Date
}, { timestamps: true });
runtimeSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0, sparse: true });
const RuntimeData = mongoose.model("RuntimeData", runtimeSchema, "runtime_data");

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

async function runAI(messages, max_tokens, retries = 0, model = MAIN_AI_MODEL, temperature = null) {
  const available = getAvailableCFCredential();
  if (!available) throw new Error("Cloudflare AI unavailable");
  const { cred, index } = available;
  const timeoutMs = model === VALIDATOR_AI_MODEL ? 15000 : 0;
  const controller = timeoutMs > 0 ? new AbortController() : null;
  const timeout = timeoutMs > 0 ? setTimeout(() => controller.abort(), timeoutMs) : null;
  const aiUrl = `https://api.cloudflare.com/client/v4/accounts/${cred.accountId}/ai/run/${model}`;
  try {
    const response = await fetch(aiUrl, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${cred.token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ messages, max_tokens, ...(Number.isFinite(Number(temperature)) ? { temperature: Number(temperature) } : {}) }),
      signal: controller ? controller.signal : undefined
    });
    const rawText = await response.text();
    let json = {};
    try {
      json = JSON.parse(rawText);
    } catch {}
    const isRateLimited = response.status === 429 || response.status === 401 || response.status === 403 || (json.errors && json.errors.some(err => err?.message && /allocation|limit/i.test(err.message)));
    if (isRateLimited) {
      cfCredentials[index].lockoutUntil = Date.now() + 24 * 60 * 60 * 1000;
      if (timeout) clearTimeout(timeout);
      if (retries + 1 < cfCredentials.length) return runAI(messages, max_tokens, retries + 1, model, temperature);
      throw new Error("Cloudflare AI rate limit");
    }
    if (timeout) clearTimeout(timeout);
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
    if (timeout) clearTimeout(timeout);
    if (retries + 1 < cfCredentials.length && /fetch failed|aborted|network|HTTP 5/i.test(String(e.message))) {
      return runAI(messages, max_tokens, retries + 1, model, temperature);
    }
    throw e;
  }
}

function normalizeValidationText(value) {
  return String(value ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
}

async function runAIValidator(question, correctAnswer, userAnswer, language, gameName, retries = 0) {
  const normalizedVerified = normalizeValidationText(correctAnswer);
  const normalizedUser = normalizeValidationText(userAnswer);
  const exactMatch = normalizedVerified && normalizedVerified === normalizedUser ? "true" : "false";
  const system = `<system_directives name="answer_validator">
You are Asistan, the answer validation engine for Mizik.
Determine whether the user answer matches the verified answer for the supplied question.
Equivalent wording, ordinary spelling variation, accents, punctuation, number formatting, and natural language variation count as valid matches.
An exact normalized answer match is a correct answer.
Return exactly one lowercase status word: correct or incorrect.
</system_directives>`;
  const user = `Question: ${question}
Verified answer: ${correctAnswer}
User answer: ${userAnswer}
Normalized verified answer: ${normalizedVerified}
Normalized user answer: ${normalizedUser}
Exact normalized match: ${exactMatch}
Language: ${language}
Game: ${gameName}`;
  const result = await runAI([
    { role: "system", content: system },
    { role: "user", content: user }
  ], 8, retries, VALIDATOR_AI_MODEL, 0);
  const normalized = cleanAIResponse(result.response).toLowerCase().trim();
  try {
    const parsed = JSON.parse(normalized);
    const candidate = String(parsed.status || parsed.validation || parsed.result || "").toLowerCase().trim();
    if (candidate === "correct") return true;
    if (candidate === "incorrect") return false;
  } catch {}
  const match = normalized.match(/\b(correct|incorrect)\b/);
  if (match) return match[1] === "correct";
  if (exactMatch === "true") {
    const retryResult = await runAI([
      { role: "system", content: system },
      { role: "user", content: `${user}\nValidation status: correct` }
    ], 8, retries, VALIDATOR_AI_MODEL, 0);
    const retryNormalized = cleanAIResponse(retryResult.response).toLowerCase().trim();
    const retryMatch = retryNormalized.match(/\b(correct|incorrect)\b/);
    if (retryMatch) return retryMatch[1] === "correct";
  }
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

function resolveQuizUserKey(body = {}) {
  return String(body.DH7 || body.dh7 || body.TFID || body.tfid || body.user_id || body.session_id || "").trim();
}

async function getProgress(sessionId) {
  const key = `progress:${String(sessionId || "").trim()}`;
  if (!key.slice(9)) return null;
  try {
    const record = await RuntimeData.findOne({ recordKey: key, kind: "progress" }).lean();
    if (record?.data) {
      return {
        language: normalizeLanguage(record.data.language || "en"),
        current_step: Math.max(1, Number(record.data.currentStep || record.data.current_step || 1)),
        consecutive_correct: Math.max(0, Number(record.data.consecutiveCorrect || record.data.consecutive_correct || 0)),
        recent_quiz_ids: Array.isArray(record.data.recentQuizIds) ? record.data.recentQuizIds.map(String).slice(-12) : []
      };
    }
  } catch (e) {
    logEvent("ERROR", "DATABASE", `MongoDB getProgress failed: ${e.message}`);
  }
  return null;
}

async function saveProgress(sessionId, lang, step, consec, recentQuizIds = null) {
  const key = `progress:${String(sessionId || "").trim()}`;
  if (!key.slice(9)) return;
  const data = {
    language: normalizeLanguage(lang),
    currentStep: Math.max(1, Number(step) || 1),
    consecutiveCorrect: Math.max(0, Number(consec) || 0)
  };
  if (Array.isArray(recentQuizIds)) data.recentQuizIds = recentQuizIds.map(String).filter(Boolean).slice(-12);
  await RuntimeData.findOneAndUpdate(
    { recordKey: key },
    { $set: { kind: "progress", sessionId: String(sessionId).trim(), data, expiresAt: null } },
    { upsert: true, setDefaultsOnInsert: true }
  );
}

async function markQuizUsedForUser(sessionId, quizId) {
  const userKey = String(sessionId || "").trim();
  const questionId = String(quizId || "").trim();
  if (!userKey || !questionId || !mongoose.Types.ObjectId.isValid(questionId)) return;
  const key = `progress:${userKey}`;
  const now = new Date();
  await RuntimeData.findOneAndUpdate(
    { recordKey: key, kind: "progress" },
    {
      $set: { "data.lastQuizAt": now, sessionId: userKey, expiresAt: null },
      $push: { "data.recentQuizIds": { $each: [questionId], $slice: -12 } }
    },
    { upsert: true }
  );
}

async function getCurrentQuiz(sessionId) {
  const key = `current_quiz:${String(sessionId || "").trim()}`;
  if (!key.slice(13)) return null;
  const record = await RuntimeData.findOne({ recordKey: key, kind: "current_quiz" }).lean();
  if (!record?.data) return null;
  return record.data;
}

async function hydrateCurrentQuizFeedback(sessionId, current, language) {
  if (!current) return null;
  const currentSuccess = String(current.success_msg || "").trim();
  const currentError = String(current.error_msg || "").trim();
  const currentExplanation = String(current.explanation || "").trim();
  if (currentSuccess && currentError && currentExplanation) return current;

  const qTypeMap = {
    MCQ: "MCQ",
    TRUE_FALSE: "TRUE_FALSE",
    FILL_BLANK: "FILL_BLANK",
    IDENTITY_IMAGE: "IDENTITY_IMAGE",
    WORD_TWIST: "WORD_TWIST",
    TEXT_TWIST: "TEXT_TWIST",
    "2048": "2048"
  };
  const filters = [];
  const base = { lang: normalizeLanguage(language || "en") };
  if (current.question) filters.push({ ...base, question: current.question, answer: current.answer || undefined });
  if (current.question) filters.push({ ...base, question: current.question });
  if (current.answer) filters.push({ ...base, answer: current.answer, qType: qTypeMap[String(current.q_type || "").toUpperCase()] || current.q_type || undefined });

  let source = null;
  for (const filter of filters) {
    const cleanFilter = Object.fromEntries(Object.entries(filter).filter(([, value]) => value !== undefined && value !== null && value !== ""));
    source = await BaseQuiz.findOne({
      ...cleanFilter,
      successMsg: { $type: "string", $ne: "" },
      errorMsg: { $type: "string", $ne: "" },
      explanation: { $type: "string", $ne: "" }
    }).lean().catch(() => null);
    if (source) break;
  }

  if (!source) return current;

  current.success_msg = String(source.successMsg || current.success_msg || "").trim();
  current.error_msg = String(source.errorMsg || current.error_msg || "").trim();
  current.explanation = String(source.explanation || current.explanation || "").trim();
  if (!current.answer && source.answer !== undefined && source.answer !== null) current.answer = String(source.answer);
  if (!current.q_type && source.qType) current.q_type = source.qType;
  await saveCurrentQuiz(sessionId, current.q_type || "MCQ", current.question || source.question || "", current.options || "[]", current.image_url || source.imageUrl || null, current.answer || source.answer || "", current.explanation, current.success_msg, current.error_msg);
  return current;
}

async function saveCurrentQuiz(sessionId, qType, question, optionsStr, imageUrl, answer, explanation, success_msg, error_msg, gameSlug = null, quizId = null) {
  const key = `current_quiz:${String(sessionId || "").trim()}`;
  if (!key.slice(13)) return;
  let options = [];
  try {
    options = JSON.parse(optionsStr || "[]");
  } catch {
    options = [];
  }
  await RuntimeData.findOneAndUpdate(
    { recordKey: key },
    { $set: { kind: "current_quiz", sessionId: String(sessionId).trim(), data: { q_type: qType, question, options, image_url: imageUrl || null, answer, explanation, success_msg, error_msg, game_slug: gameSlug, quiz_id: quizId || null }, expiresAt: null } },
    { upsert: true, setDefaultsOnInsert: true }
  );
}

async function clearCurrentQuiz(sessionId) {
  await RuntimeData.deleteOne({ recordKey: `current_quiz:${String(sessionId || "").trim()}`, kind: "current_quiz" });
}

async function saveUserInfo(sessionId, dataString) {
  const key = `user_info:${String(sessionId || "").trim()}`;
  if (!key.slice(10)) return;
  let data = {};
  try {
    data = JSON.parse(dataString || "{}");
  } catch {
    data = {};
  }
  await RuntimeData.findOneAndUpdate(
    { recordKey: key },
    { $set: { kind: "user_info", sessionId: String(sessionId).trim(), data, expiresAt: null } },
    { upsert: true, setDefaultsOnInsert: true }
  );
}

function triggerAdminGeneration(languages, total) {
  const normalizedLanguages = [...new Set(languages.map(normalizeLanguage))].sort();
  const lockKey = `admin:${normalizedLanguages.join(",")}:${total}`;
  if (adminGenerationLocks.has(lockKey)) return false;
  adminGenerationLocks.add(lockKey);
  runAdminGeneration(normalizedLanguages, total, 1).catch(error => {
    logEvent("WARN", "ADMIN_PREGEN", `Administrative generation stopped: ${error.message}`);
  }).finally(() => {
    adminGenerationLocks.delete(lockKey);
  });
  return true;
}

app.get("/adamdh7=modpas/adamdh7", (req, res) => {
  const started = triggerAdminGeneration(PREGEN_LANGUAGES, 17);
  return res.json({ success: true, admin: true, total_requested: 17, generation_started: started, languages: PREGEN_LANGUAGES });
});

app.get("/adamdh7=modpas/adamdh7/:lang", (req, res) => {
  const rawLanguage = String(req.params.lang || "").trim().toLowerCase();
  if (!PREGEN_LANGUAGES.includes(rawLanguage)) return res.status(400).json({ success: false, admin: true, error: "Unsupported language", language: rawLanguage });
  const started = triggerAdminGeneration([rawLanguage], 17);
  return res.json({ success: true, admin: true, total_requested: 17, generation_started: started, language: rawLanguage });
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
    await saveProgress(session_id, progress ? progress.language : 'en', newStep, newConsec, progress?.recent_quiz_ids || []);
    triggerPreGeneration("level_up", newStep, progress ? progress.language : "en");
    return res.json({ success: true, message: "User info saved successfully" });
  } catch (e) {
    logEvent("ERROR", "ROUTER", `User info save failed: ${e.message}`);
    return res.json({ success: false, error: "Database error" });
  }
});

function hasUsableQuizFeedback(item) {
  return Boolean(String(item?.successMsg || "").trim() && String(item?.errorMsg || "").trim() && String(item?.explanation || "").trim());
}

async function executeMode0PureDB(randomItem) {
  if (!randomItem) throw new Error("Source item missing");
  if (!hasUsableQuizFeedback(randomItem)) throw new Error("Stored quiz feedback incomplete");
  const parsed = {
    ...(randomItem.gameData && typeof randomItem.gameData === "object" ? randomItem.gameData : {}),
    question: randomItem.question || "",
    options: randomItem.options || [],
    answer: randomItem.answer,
    explanation: randomItem.explanation || "",
    successMsg: randomItem.successMsg || "",
    errorMsg: randomItem.errorMsg || "",
    qType: randomItem.qType || "MCQ"
  };
  parsed.question = parsed.question || randomItem.question || "";
  parsed.options = Array.isArray(parsed.options) ? parsed.options : (randomItem.options || []);
  parsed.answer = parsed.answer ?? randomItem.answer;
  parsed.explanation = parsed.explanation || randomItem.explanation || "";
  parsed.successMsg = parsed.successMsg || randomItem.successMsg || "";
  parsed.errorMsg = parsed.errorMsg || randomItem.errorMsg || "";
  parsed.qType = parsed.qType || randomItem.qType || "MCQ";
  if (randomItem.timeLimit !== undefined && randomItem.timeLimit !== null) parsed.timeLimit = Number(randomItem.timeLimit) || 0;
  const randomType = randomItem.qType || parsed.qType || "MCQ";
  const imgUrl = randomItem.imageUrl || parsed.imageUrl || null;
  const finalSuccess = randomItem.successMsg || parsed.successMsg || null;
  const finalError = randomItem.errorMsg || parsed.errorMsg || null;
  const finalExplanation = randomItem.explanation || parsed.explanation || null;
  return { parsed, randomType, imgUrl, finalSuccess, finalError, finalExplanation };
}

function getQuestionRequiredKeys(game) {
  if (game?.slug === "word_twist") return ["scrambled", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
  if (game?.slug === "text_twist") return ["letters", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
  if (game?.slug === "2048") return ["boardSize", "startTileValues", "targetValue", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
  return ["question", "options", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"];
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
  const requiredKeys = getQuestionRequiredKeys(game);
  const prompt = `${game.systemDirectives}

Language: ${langName}
Level: ${randomItem.level || 1}
Current verified question: ${randomItem.question || ""}
Current verified answer: ${randomItem.answer || ""}

Create a fresh playable question for the same game using the supplied verified content as the factual basis.
Return one JSON object containing: ${requiredKeys.join(", ")}.`;
  const aiResponse = await runAI([
    { role: "system", content: game.systemDirectives },
    { role: "user", content: prompt }
  ], 900);
  const parsed = parseAIJsonResponse(aiResponse.response, requiredKeys);
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
  const requiredKeys = getQuestionRequiredKeys(game);
  const prompt = `${game.systemDirectives}

Language: ${langName}
Level: ${randomItem.level || 1}
Verified subject: ${randomItem.question || ""}
Verified answer: ${randomItem.answer || ""}

Create a new playable question on the same verified subject area while keeping the same game type and language.
Return one JSON object containing: ${requiredKeys.join(", ")}.`;
  const aiResponse = await runAI([
    { role: "system", content: game.systemDirectives },
    { role: "user", content: prompt }
  ], 900);
  const parsed = parseAIJsonResponse(aiResponse.response, requiredKeys);
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
  const game = selectedGame;
  if (!game) throw new Error("Game required");
  const pureContext = `${game.name} game generation`;
  const result = await generateGameQuestion(game, language, level, imageUrl, pureContext, []);
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
Create one accurate question in the requested language. Language determines output language only; subject selection remains global and independent of language.
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
Create one accurate factual statement in the requested language. Language determines output language only; subject selection remains global and independent of language.
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
Create one accurate factual sentence in the requested language. Language determines output language only; subject selection remains global and independent of language.
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
Identify one clear subject represented by the supplied image context. Language determines wording only; the visual subject comes from a global knowledge domain independent of language.
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
Use the supplied target word as the answer. Language determines explanatory text only; word selection remains independent of country and language association.
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
Use only the supplied letter set for the current round. Language determines the accepted word set only; theme selection remains independent of country and language association.
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
Define a playable board configuration for the requested level. Language determines explanatory text only; the puzzle configuration remains universal.
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
  return builtInGames.find(game => game.slug === normalized || game.name.toLowerCase() === text.toLowerCase()) || null;
}

async function listGameDefinitions() {
  return builtInGames.map(game => ({
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
    let answer = pool.length ? pool[Math.floor(Math.random() * pool.length)] : null;
    let parsed = null;
    if (answer) {
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
      parsed = parseAIJsonResponse(response.response, ["scrambled", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
      parsed.scrambled = scrambled;
      parsed.answer = answer;
    } else {
      const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}`;
      const response = await runAI([
        { role: "system", content: game.systemDirectives },
        { role: "user", content: prompt }
      ], 450);
      parsed = parseAIJsonResponse(response.response, ["scrambled", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
      answer = String(parsed.answer || "").trim();
      if (answer.length < 2) throw new Error("Generated Word Twist answer invalid");
      parsed.scrambled = scrambleWord(answer);
      parsed.answer = answer;
    }
    parsed.qType = "WORD_TWIST";
    parsed.timeLimit = Math.max(8, Math.min(60, Number(parsed.timeLimit) || answer.length * 3));
    return parsed;
  }

  if (game.slug === "text_twist") {
    let letters = String(gameContext || "").replace(/[^A-Za-zÀ-ÿ]/g, "").toUpperCase();
    let parsed = null;
    if (letters.length >= 4) {
      const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}
Letter set: ${letters}`;
      const response = await runAI([
        { role: "system", content: game.systemDirectives },
        { role: "user", content: prompt }
      ], 450);
      parsed = parseAIJsonResponse(response.response, ["letters", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
      parsed.letters = letters;
    } else {
      const prompt = `${game.systemDirectives}

Language: ${language}
Level: ${level}`;
      const response = await runAI([
        { role: "system", content: game.systemDirectives },
        { role: "user", content: prompt }
      ], 450);
      parsed = parseAIJsonResponse(response.response, ["letters", "answer", "explanation", "successMsg", "errorMsg", "timeLimit", "qType"]);
      letters = String(parsed.letters || "").replace(/[^A-Za-zÀ-ÿ]/g, "").toUpperCase();
      if (letters.length < 4) throw new Error("Generated Text Twist letters invalid");
      parsed.letters = letters;
    }
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
Current context: ${gameContext || "global general knowledge"}
Language is an output language, not a topic restriction. Select the subject independently from language, country, region or language-speaking community.`;
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

function quizRecordKey({ language, level, gameSlug, question, answer }) {
  const raw = `${language}|${level}|${gameSlug}|${String(question || "").trim().toLowerCase()}|${String(answer || "").trim().toLowerCase()}`;
  return `question:${crypto.createHash("sha256").update(raw).digest("hex")}`;
}

async function saveGeneratedQuizQuestion(result, game, language, level, generationMode) {
  const parsed = result?.parsed || result;
  if (!parsed || !parsed.qType) throw new Error("Generated question qType missing");
  if (!String(parsed.successMsg || "").trim() || !String(parsed.errorMsg || "").trim() || !String(parsed.explanation || "").trim()) throw new Error("Generated question feedback incomplete");
  const normalizedGameSlug = normalizeGameSlug(game?.slug || game?.name || parsed.qType);
  const questionText = String(parsed.question || parsed.scrambled || parsed.letters || normalizedGameSlug).trim();
  const answerText = String(parsed.answer ?? "").trim();
  const recordKey = quizRecordKey({ language, level, gameSlug: normalizedGameSlug, question: questionText, answer: answerText });
  const gameData = {};
  if (parsed.scrambled) gameData.scrambled = parsed.scrambled;
  if (parsed.letters) gameData.letters = parsed.letters;
  if (parsed.boardSize) gameData.boardSize = parsed.boardSize;
  if (Array.isArray(parsed.startTileValues)) gameData.startTileValues = parsed.startTileValues;
  if (parsed.targetValue) gameData.targetValue = parsed.targetValue;
  const document = {
    recordType: "question",
    recordKey,
    lang: language,
    level: Number(level) || 1,
    gameSlug: normalizedGameSlug,
    qType: parsed.qType || "MCQ",
    question: parsed.question || "",
    options: Array.isArray(parsed.options) ? parsed.options : [],
    imageUrl: parsed.imageUrl || result?.imgUrl || null,
    answer: parsed.answer ?? "",
    explanation: parsed.explanation || "",
    successMsg: parsed.successMsg || result?.finalSuccess || "",
    errorMsg: parsed.errorMsg || result?.finalError || "",
    timeLimit: Number.isFinite(Number(parsed.timeLimit)) ? Math.max(0, Number(parsed.timeLimit)) : 0
  };
  if (Object.keys(gameData).length > 0) document.gameData = gameData;
  await BaseQuiz.updateOne({ recordKey }, { $setOnInsert: document }, { upsert: true });
  return document;
}

const PREGEN_BATCH_GAMES = 3;
const PREGEN_LANGUAGES = ["ht", "fr", "en", "es"];
const PREGEN_INTERVAL_MS = 4 * 60 * 1000;
const PREGEN_CONCURRENCY = 2;
const MIN_QUIZ_POOL_SIZE = 8;
const RECENT_GLOBAL_USE_MS = 10 * 60 * 1000;
let preGenerationRunning = false;
let quizRequestCount = 0;
let preGenerationLanguageCursor = 0;
const adminGenerationLocks = new Set();

function choosePreGenerationGames() {
  const pool = [...builtInGames];
  for (let i = pool.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }
  return pool.slice(0, Math.min(PREGEN_BATCH_GAMES, pool.length));
}

async function getPreGenerationLevel(preferredLevel = null) {
  if (preferredLevel !== null && preferredLevel !== undefined) return Math.max(1, Number(preferredLevel) || 1);
  const levels = await RuntimeData.distinct("data.currentStep", { kind: "progress" }).catch(() => []);
  const validLevels = levels.map(value => Math.max(1, Number(value) || 1)).filter(Number.isFinite);
  return validLevels.length ? Math.max(...validLevels) : 1;
}

async function generateBackgroundQuestion(game, language, level, mode, source) {
  const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[language] || "English";
  let result;
  if (mode === 1 && source) {
    result = await executeMode1ImproveExisting(source, langName, language);
  } else if (mode === 2 && source) {
    result = await executeMode2CreateSimilar(source, langName, language);
  } else {
    result = await executeMode3PureAIGeneration(language, langName, game.slug, null, "", level);
    mode = 3;
  }
  await saveGeneratedQuizQuestion(result, game, language, level, `MODE_${mode}`);
}

function chooseBackgroundMode(source, reason) {
  if (!source) return 3;
  if (reason === "level_up") return 1;
  return 1 + Math.floor(Math.random() * 3);
}

function quizStorageCriteria(language, level, gameSlug) {
  const normalizedLanguage = normalizeLanguage(language);
  const normalizedLevel = Math.max(1, Number(level) || 1);
  const normalizedSlug = normalizeGameSlug(gameSlug);
  const qTypeMap = { mcq: "MCQ", true_false: "TRUE_FALSE", fill_blank: "FILL_BLANK", identity_image: "IDENTITY_IMAGE", word_twist: "WORD_TWIST", text_twist: "TEXT_TWIST", "2048": "2048" };
  const qType = qTypeMap[normalizedSlug];
  return {
    lang: normalizedLanguage,
    level: normalizedLevel,
    recordType: { $in: [null, "question"] },
    successMsg: { $type: "string", $ne: "" },
    errorMsg: { $type: "string", $ne: "" },
    explanation: { $type: "string", $ne: "" },
    $or: [{ gameSlug: normalizedSlug }, { gameSlug: { $exists: false }, qType }, { gameSlug: null, qType }]
  };
}

async function countUsableQuizQuestions(language, level, gameSlug) {
  return BaseQuiz.countDocuments(quizStorageCriteria(language, level, gameSlug)).catch(() => 0);
}

async function findSourceQuestion(language, level, gameSlug) {
  const game = builtInGames.find(item => item.slug === normalizeGameSlug(gameSlug));
  if (!game) return null;
  const criteria = quizStorageCriteria(language, level, game.slug);
  const recentCutoff = new Date(Date.now() - RECENT_GLOBAL_USE_MS);
  const freshCriteria = { ...criteria, $and: [{ $or: criteria.$or }, { $or: [{ lastUsedAt: { $exists: false } }, { lastUsedAt: null }, { lastUsedAt: { $lt: recentCutoff } }] }] };
  delete freshCriteria.$or;
  const sampled = await BaseQuiz.aggregate([{ $match: freshCriteria }, { $sample: { size: 8 } }]).catch(() => []);
  if (sampled.length) return sampled[Math.floor(Math.random() * sampled.length)];
  const fallback = await BaseQuiz.aggregate([{ $match: criteria }, { $sample: { size: 8 } }]).catch(() => []);
  return fallback.length ? fallback[Math.floor(Math.random() * fallback.length)] : null;
}

async function generateOnePreGenerationTask(game, language, level, reason, force = false) {
  if (!force) {
    const existingCount = await countUsableQuizQuestions(language, level, game.slug);
    if (existingCount >= MIN_QUIZ_POOL_SIZE) return false;
  }
  const source = await findSourceQuestion(language, level, game.slug);
  const mode = chooseBackgroundMode(source, reason);
  try {
    await generateBackgroundQuestion(game, language, level, mode, source);
    return true;
  } catch {
    if (mode !== 3) {
      try {
        await generateBackgroundQuestion(game, language, level, 3, null);
        return true;
      } catch {}
    }
    return false;
  }
}

async function runPreGenerationRound(reason = "interval", preferredLevel = null, preferredLanguage = null) {
  if (preGenerationRunning) return;
  preGenerationRunning = true;
  try {
    const level = await getPreGenerationLevel(preferredLevel);
    const games = choosePreGenerationGames();
    const languages = preferredLanguage ? [normalizeLanguage(preferredLanguage)] : (reason === "startup" ? PREGEN_LANGUAGES : [PREGEN_LANGUAGES[preGenerationLanguageCursor++ % PREGEN_LANGUAGES.length]]);
    const tasks = [];
    for (const language of languages) {
      for (const game of games) tasks.push({ game, language, level });
    }
    let cursor = 0;
    const worker = async () => {
      while (true) {
        const index = cursor++;
        if (index >= tasks.length) return;
        const task = tasks[index];
        await generateOnePreGenerationTask(task.game, task.language, task.level, reason).catch(() => false);
      }
    };
    await Promise.all(Array.from({ length: Math.min(PREGEN_CONCURRENCY, tasks.length) }, worker));
  } finally {
    preGenerationRunning = false;
  }
}

async function runAdminGeneration(languages, total, level = 1) {
  const normalizedLanguages = [...new Set(languages.map(normalizeLanguage))].filter(Boolean);
  const targets = [];
  const combos = [];
  for (const language of normalizedLanguages) {
    for (const game of builtInGames) combos.push({ language, game, level });
  }
  for (let i = combos.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [combos[i], combos[j]] = [combos[j], combos[i]];
  }
  for (const combo of combos) targets.push(combo);
  let cursor = 0;
  let created = 0;
  const worker = async () => {
    while (true) {
      const index = cursor++;
      if (index >= total) return;
      let task = targets[index % targets.length];
      let attempts = 0;
      while (attempts < 3) {
        attempts += 1;
        const generated = await generateOnePreGenerationTask(task.game, task.language, task.level, "admin", true).catch(() => false);
        if (generated) {
          created += 1;
          break;
        }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(PREGEN_CONCURRENCY, total) }, worker));
  return created;
}

function triggerPreGeneration(reason, preferredLevel = null, preferredLanguage = null) {
  runPreGenerationRound(reason, preferredLevel, preferredLanguage).catch(() => {});
}

function startBackgroundPreGeneration() {
  setTimeout(() => triggerPreGeneration("startup", 1), 1000);
  setInterval(() => triggerPreGeneration("interval"), PREGEN_INTERVAL_MS);
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

async function createGameInvitation({ gameId, fromTfid, toTfid, gameSlug }) {
  const invitationId = crypto.randomUUID();
  const now = Date.now();
  await RuntimeData.create({
    recordKey: `game_invitation:${invitationId}`,
    kind: "game_invitation",
    data: { invitation_id: invitationId, game_id: gameId, from_tfid: fromTfid, to_tfid: toTfid, game_slug: gameSlug, status: "pending", created_at: now, expires_at: now + GAME_TTL_MS },
    expiresAt: new Date(now + GAME_TTL_MS)
  });
  return invitationId;
}

async function loadPendingInvitationsForTfid(tfid) {
  const now = Date.now();
  const records = await RuntimeData.find({ kind: "game_invitation", "data.to_tfid": String(tfid || "").trim(), "data.status": "pending", expiresAt: { $gt: new Date(now) } }).sort({ "data.created_at": -1 }).lean();
  return records.map(record => record.data);
}

async function updateInvitation(invitationId, status) {
  const now = new Date();
  const record = await RuntimeData.findOneAndUpdate(
    { recordKey: `game_invitation:${String(invitationId || "").trim()}`, kind: "game_invitation", "data.status": "pending", expiresAt: { $gt: now } },
    { $set: { "data.status": status } },
    { new: true }
  ).lean();
  return record?.data || null;
}

async function deleteExpiredInvitations() {
  await RuntimeData.deleteMany({ kind: "game_invitation", expiresAt: { $lte: new Date() } });
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
  const invitationId = await createGameInvitation({
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

async function saveGameSession(gameId, gameSlug, ownerTfid, players, state, expiresAt = gameExpiresAt()) {
  await RuntimeData.findOneAndUpdate(
    { recordKey: `game_session:${String(gameId || "").trim()}` },
    { $set: { kind: "game_session", data: { gameId, gameSlug, ownerTfid, players, state }, expiresAt: new Date(expiresAt) } },
    { upsert: true, setDefaultsOnInsert: true }
  );
}

async function loadGameSession(gameId) {
  const record = await RuntimeData.findOne({ recordKey: `game_session:${String(gameId || "").trim()}`, kind: "game_session", expiresAt: { $gt: new Date() } }).lean();
  if (!record?.data) return null;
  return { ...record.data, expiresAt: record.expiresAt?.getTime ? record.expiresAt.getTime() : record.expiresAt };
}

async function cleanupExpiredGames() {
  const now = new Date();
  await RuntimeData.deleteMany({ kind: { $in: ["game_session", "game_word", "game_invitation"] }, expiresAt: { $lte: now } });
  for (const [gameId, sockets] of wsClients.entries()) {
    if (!(await loadGameSession(gameId)) && sockets.size === 0) wsClients.delete(gameId);
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
  const record = await RuntimeData.findOne({ recordKey: `game_words:${String(gameId || "").trim()}`, kind: "game_word", expiresAt: { $gt: new Date() } }).lean();
  return Array.isArray(record?.data?.words) ? record.data.words : [];
}

async function saveStoredWords(gameId, words) {
  await RuntimeData.findOneAndUpdate(
    { recordKey: `game_words:${String(gameId || "").trim()}` },
    { $set: { kind: "game_word", data: { words: Array.isArray(words) ? words : [] } }, expiresAt: new Date(gameExpiresAt()) },
    { upsert: true, setDefaultsOnInsert: true }
  );
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
    gameContext,
    servedQuizIds: []
  };
  await saveGameSession(gameId, game.slug, ownerTfid, players, state);
  if (Array.isArray(words) && words.length > 0) await saveStoredWords(gameId, words);
  if (!pending) {
    const storedQuestion = await loadStoredGameQuestion(game, language, Number(level) || 1);
    const question = executeMode0PureDB(storedQuestion);
    state.question = question.parsed.question || question.parsed.scrambled || question.parsed.letters || null;
    state.questionPayload = question.parsed;
    state.currentAnswer = question.parsed.answer || null;
    state.timeLimit = Number(question.parsed.timeLimit || storedQuestion.gameData?.timeLimit || 0);
    state.questionStartedAt = Date.now();
    state.servedQuizIds = [String(storedQuestion._id)];
    await saveGameSession(gameId, game.slug, ownerTfid, players, state);
  }
  return await loadGameSession(gameId);
}

async function advanceGameTurn(session) {
  const game = await getGameDefinition(session.gameSlug);
  if (!game) throw new Error("Game not found");
  const storedQuestion = await loadStoredGameQuestion(game, session.state.language, session.state.level, session.state.servedQuizIds || []);
  const question = executeMode0PureDB(storedQuestion);
  session.state.question = question.parsed.question || question.parsed.scrambled || question.parsed.letters || null;
  session.state.questionPayload = question.parsed;
  session.state.currentAnswer = question.parsed.answer || null;
  session.state.timeLimit = Number(question.parsed.timeLimit || storedQuestion.gameData?.timeLimit || 0);
  session.state.servedQuizIds = [...new Set([...(session.state.servedQuizIds || []), String(storedQuestion._id)])].slice(-50);
  session.state.questionStartedAt = Date.now();
  session.state.askedTo = session.players[session.state.turnIndex % session.players.length];
  session.state.answeredBy = null;
  session.state.questionNumber = Number(session.state.questionNumber || 0) + 1;
  await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
  return await loadGameSession(session.gameId);
}

async function passSameQuestionToOtherPlayer(session) {
  if (session.players.length > 1) {
    session.state.turnIndex = (session.state.turnIndex + 1) % session.players.length;
    session.state.askedTo = session.players[session.state.turnIndex];
  } else {
    session.state.askedTo = session.players[0];
  }
  session.state.answeredBy = null;
  session.state.questionStartedAt = Date.now();
  await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
  return await loadGameSession(session.gameId);
}

async function validateGameAnswer(session, tfid, answer) {
  if (!session || !session.state || session.state.status !== "active") throw new Error("Game is not active");
  if (!session.players.includes(tfid)) throw new Error("TFID is not part of this game");
  if (session.state.askedTo !== tfid) throw new Error("It is another player's turn");

  const elapsedMs = Date.now() - Number(session.state.questionStartedAt || Date.now());
  const timeLimit = Number(session.state.timeLimit || 0);

  if (timeLimit > 0 && elapsedMs > timeLimit * 1000) {
    const nextSession = await passSameQuestionToOtherPlayer(session);
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
      answer: session.state.currentAnswer,
      explanation: session.state.questionPayload?.explanation || "",
      successMsg: session.state.questionPayload?.successMsg || "",
      errorMsg: session.state.questionPayload?.errorMsg || ""
    };

    if (session.players.length === 1 && session.state.scores[tfid] >= 10) {
      session.state.status = "completed";
      session.state.winner = tfid;
      await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
      return { correct: true, completed: true, session: await loadGameSession(session.gameId) };
    }

    if (session.players.length > 1) {
      session.state.turnIndex = (session.state.turnIndex + 1) % session.players.length;
      await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
      const nextSession = await advanceGameTurn(await loadGameSession(session.gameId));
      return { correct: true, completed: false, session: nextSession };
    }

    await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
    return { correct: true, completed: false, session: await loadGameSession(session.gameId) };
  }

  const nextSession = await passSameQuestionToOtherPlayer(session);
  nextSession.state.lastResult = {
    correct: false,
    player: tfid,
    points: 0,
    explanation: nextSession.state.questionPayload?.explanation || "",
    successMsg: nextSession.state.questionPayload?.successMsg || "",
    errorMsg: nextSession.state.questionPayload?.errorMsg || ""
  };
  await saveGameSession(nextSession.gameId, nextSession.gameSlug, nextSession.ownerTfid, nextSession.players, nextSession.state);
  return { correct: false, completed: false, session: await loadGameSession(nextSession.gameId) };
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
      questionPayload: session.state.questionPayload ? (() => {
        const { answer, explanation, successMsg, errorMsg, ...publicPayload } = session.state.questionPayload;
        return publicPayload;
      })() : null
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

function quizStorageCriteria(language, level, gameSlug) {
  const normalizedLanguage = normalizeLanguage(language);
  const normalizedLevel = Math.max(1, Number(level) || 1);
  const normalizedSlug = normalizeGameSlug(gameSlug);
  const qTypeMap = { mcq: "MCQ", true_false: "TRUE_FALSE", fill_blank: "FILL_BLANK", identity_image: "IDENTITY_IMAGE", word_twist: "WORD_TWIST", text_twist: "TEXT_TWIST", "2048": "2048" };
  const qType = qTypeMap[normalizedSlug];
  return {
    lang: normalizedLanguage,
    level: normalizedLevel,
    recordType: { $in: [null, "question"] },
    successMsg: { $type: "string", $ne: "" },
    errorMsg: { $type: "string", $ne: "" },
    explanation: { $type: "string", $ne: "" },
    $or: [{ gameSlug: normalizedSlug }, { gameSlug: { $exists: false }, qType }, { gameSlug: null, qType }]
  };
}

async function getAvailableQuizGames(language, level) {
  const normalizedLanguage = normalizeLanguage(language);
  const normalizedLevel = Math.max(1, Number(level) || 1);
  const qTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE", "WORD_TWIST", "TEXT_TWIST", "2048"];
  const rows = await BaseQuiz.aggregate([
    { $match: {
      lang: normalizedLanguage,
      level: normalizedLevel,
      recordType: { $in: [null, "question"] },
      successMsg: { $type: "string", $ne: "" },
      errorMsg: { $type: "string", $ne: "" },
      explanation: { $type: "string", $ne: "" },
      $or: [{ gameSlug: { $in: builtInGames.map(game => game.slug) } }, { gameSlug: { $exists: false }, qType: { $in: qTypes } }, { gameSlug: null, qType: { $in: qTypes } }]
    } },
    { $group: { _id: { $ifNull: ["$gameSlug", "$qType"] } } }
  ]).catch(() => []);
  const games = rows.map(row => {
    const key = normalizeGameSlug(row?._id);
    if (builtInGames.some(game => game.slug === key)) return key;
    return getBuiltInGameByQType(String(row?._id || "").toUpperCase())?.slug || null;
  }).filter(Boolean);
  return [...new Set(games)];
}

async function loadStoredGameQuestion(game, language, level, excludedIds = []) {
  const criteria = quizStorageCriteria(language, level, game.slug);
  const cleanExcluded = Array.isArray(excludedIds) ? excludedIds.filter(value => mongoose.Types.ObjectId.isValid(value)).map(value => new mongoose.Types.ObjectId(value)) : [];
  const recentCutoff = new Date(Date.now() - RECENT_GLOBAL_USE_MS);
  const freshCriteria = {
    ...criteria,
    $and: [
      { $or: criteria.$or },
      ...(cleanExcluded.length ? [{ _id: { $nin: cleanExcluded } }] : []),
      { $or: [{ lastUsedAt: { $exists: false } }, { lastUsedAt: null }, { lastUsedAt: { $lt: recentCutoff } }] }
    ]
  };
  delete freshCriteria.$or;
  let rows = await BaseQuiz.aggregate([{ $match: freshCriteria }, { $sample: { size: 10 } }]).catch(() => []);
  if (!rows.length) {
    const userPoolCriteria = { ...criteria };
    if (cleanExcluded.length) userPoolCriteria._id = { $nin: cleanExcluded };
    rows = await BaseQuiz.aggregate([{ $match: userPoolCriteria }, { $sample: { size: 10 } }]).catch(() => []);
  }
  if (!rows.length) rows = await BaseQuiz.aggregate([{ $match: criteria }, { $sample: { size: 10 } }]).catch(() => []);
  if (!rows.length) return null;
  const selected = rows[Math.floor(Math.random() * rows.length)];
  const now = new Date();
  await BaseQuiz.updateOne({ _id: selected._id }, { $set: { lastUsedAt: now }, $inc: { usageCount: 1 } }).catch(() => {});
  return selected;
}

async function buildQuizForSession(body) {
  const session_id = resolveQuizUserKey(body);
  if (!session_id) throw new Error("session_id required");

  const rawLang = body.lang?.trim();
  const incomingLevel = body.level;
  let progress = await getProgress(session_id);
  if (!progress) {
    const default_lang = rawLang ? normalizeLanguage(rawLang) : "en";
    const start_step = Math.max(1, Number(incomingLevel) || 1);
    await saveProgress(session_id, default_lang, start_step, 0, []);
    progress = { language: default_lang, current_step: start_step, consecutive_correct: 0, recent_quiz_ids: [] };
  } else {
    let updated = false;
    if (rawLang && normalizeLanguage(rawLang) !== progress.language) {
      progress.language = normalizeLanguage(rawLang);
      updated = true;
    }
    if (incomingLevel !== undefined && Math.max(1, Number(incomingLevel) || 1) !== progress.current_step) {
      progress.current_step = Math.max(1, Number(incomingLevel) || 1);
      updated = true;
    }
    if (updated) await saveProgress(session_id, progress.language, progress.current_step, progress.consecutive_correct, progress.recent_quiz_ids);
  }

  const current_step_num = Math.max(1, Number(progress.current_step) || 1);
  const language = normalizeLanguage(progress.language);
  const stableUserId = String(body.DH7 || body.dh7 || body.TFID || body.tfid || "").trim();
  if (stableUserId) {
    await saveUserInfo(stableUserId, JSON.stringify({ level: current_step_num, nivo: progress.consecutive_correct, TFID: body.TFID || body.tfid || null })).catch(() => {});
  }

  const availableGameSlugs = await getAvailableQuizGames(language, current_step_num);
  if (!availableGameSlugs.length) throw new Error("Quiz pool is being prepared");
  const shuffledGames = [...availableGameSlugs];
  for (let i = shuffledGames.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffledGames[i], shuffledGames[j]] = [shuffledGames[j], shuffledGames[i]];
  }

  let selectedGame = null;
  let randomItem = null;
  for (const gameSlug of shuffledGames) {
    const candidateGame = builtInGames.find(game => game.slug === gameSlug);
    if (!candidateGame) continue;
    const candidateItem = await loadStoredGameQuestion(candidateGame, language, current_step_num, progress.recent_quiz_ids);
    if (candidateItem) {
      selectedGame = candidateGame;
      randomItem = candidateItem;
      break;
    }
  }
  if (!selectedGame || !randomItem) throw new Error("Quiz pool is being prepared");

  const result = executeMode0PureDB(randomItem);
  let imgUrl = result.imgUrl || null;
  const parsed = result.parsed;
  if (imgUrl && !/^https?:\/\//i.test(imgUrl)) imgUrl = null;
  const safeOptions = Array.isArray(parsed.options) ? parsed.options : [];

  await saveCurrentQuiz(session_id, result.randomType, parsed.question || "", JSON.stringify(safeOptions), imgUrl, parsed.answer ?? "", result.finalExplanation || parsed.explanation || "", result.finalSuccess || parsed.successMsg || "", result.finalError || parsed.errorMsg || "", selectedGame.slug, randomItem._id?.toString() || null);
  await markQuizUsedForUser(session_id, randomItem._id?.toString() || null);

  const quizData = {
    success: true,
    current_step: current_step_num,
    consecutive_correct: progress.consecutive_correct,
    language,
    needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct),
    type: result.randomType,
    question: parsed.question || ""
  };
  if (imgUrl) quizData.image_url = imgUrl;
  if (safeOptions.length > 0) quizData.options = safeOptions;
  if (parsed.scrambled) quizData.scrambled = parsed.scrambled;
  if (parsed.letters) quizData.letters = parsed.letters;
  if (parsed.boardSize) quizData.boardSize = parsed.boardSize;
  if (parsed.startTileValues) quizData.startTileValues = parsed.startTileValues;
  if (parsed.targetValue) quizData.targetValue = parsed.targetValue;
  quizData.game = selectedGame.slug;
  quizData.q_type = result.randomType;
  quizData.quiz_type = result.randomType;
  quizData.qType = result.randomType;
  quizData.game_slug = selectedGame.slug;
  quizData.gameSlug = selectedGame.slug;
  quizData.imageUrl = imgUrl;
  quizData.choices = safeOptions;
  quizData.data = {
    type: quizData.type,
    q_type: quizData.q_type,
    quiz_type: quizData.quiz_type,
    game: quizData.game_slug,
    question: quizData.question,
    options: safeOptions,
    image_url: imgUrl,
    scrambled: parsed.scrambled || null,
    letters: parsed.letters || null,
    boardSize: parsed.boardSize || null,
    startTileValues: Array.isArray(parsed.startTileValues) ? parsed.startTileValues : null,
    targetValue: parsed.targetValue || null,
    current_step: current_step_num,
    consecutive_correct: progress.consecutive_correct,
    needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct),
    language
  };
  return quizData;
}

async function validateQuizForSession(body) {
  const session_id = resolveQuizUserKey(body);
  const user_answer = body.user_answer?.trim() || "";
  const silent = Boolean(body.silent);
  if (!session_id) throw new Error("session_id required");

  let current = await getCurrentQuiz(session_id);
  if (!current) throw new Error("No active quiz");

  let progress = await getProgress(session_id);
  if (!progress) progress = { language: "en", current_step: 1, consecutive_correct: 0 };

  if (silent && !user_answer) {
    const now = Date.now();
    const last = silentValidationAt.get(session_id) || 0;
    if (now - last < SILENT_VALIDATE_COOLDOWN_MS) {
      return {
        success: true,
        correct: false,
        isCorrect: false,
        status: "skipped",
        validation: "skipped",
        validation_skipped: true,
        next_validation_in_ms: SILENT_VALIDATE_COOLDOWN_MS - (now - last),
        explanation: "",
        message: "",
        feedback: "",
        successMsg: "",
        errorMsg: "",
        consecutive_correct: progress.consecutive_correct,
        needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct),
        current_step: progress.current_step,
        language: progress.language
      };
    }
    silentValidationAt.set(session_id, now);
    return {
      success: true,
      correct: false,
      isCorrect: false,
      status: "skipped",
      validation: "skipped",
      validation_skipped: true,
      next_validation_in_ms: SILENT_VALIDATE_COOLDOWN_MS,
      explanation: "",
      message: "",
      feedback: "",
      successMsg: "",
      errorMsg: "",
      consecutive_correct: progress.consecutive_correct,
      needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct),
      current_step: progress.current_step,
      language: progress.language
    };
  }

  if (!user_answer) throw new Error("user_answer required");

  current = await hydrateCurrentQuizFeedback(session_id, current, progress.language);

  const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";
  const gameName = current.q_type || "Quiz";
  const isCorrect = await runAIValidator(current.question || "", current.answer || "", user_answer, langName, gameName);

  const successMsg = String(current.success_msg || "").trim();
  const errorMsg = String(current.error_msg || "").trim();
  const explanation = String(current.explanation || "").trim();
  const selectedMessage = isCorrect ? successMsg : errorMsg;
  const finalFeedback = selectedMessage && explanation ? `${selectedMessage}\n\n${explanation}` : (selectedMessage || explanation);
  if (!finalFeedback) throw new Error("Stored quiz feedback unavailable");
  let new_consec = Number(progress.consecutive_correct || 0);
  let new_step = Number(progress.current_step || 1);
  let levelUp = false;

  if (isCorrect) {
    new_consec += 1;
    if (new_consec >= 7) {
      new_step += 1;
      new_consec = 0;
      levelUp = true;
    }
    await clearCurrentQuiz(session_id);
  } else {
    new_consec = 0;
  }

  await saveProgress(session_id, progress.language, new_step, new_consec, progress.recent_quiz_ids);

  if (levelUp) {
    triggerPreGeneration("level_up", new_step, progress.language);
  }

  return {
    success: true,
    correct: isCorrect,
    isCorrect,
    status: isCorrect ? "correct" : "incorrect",
    validation: isCorrect ? "correct" : "incorrect",
    message: selectedMessage,
    feedback: selectedMessage,
    explanation: finalFeedback,
    successMsg: isCorrect ? successMsg : "",
    errorMsg: isCorrect ? "" : errorMsg,
    consecutive_correct: new_consec,
    needed_for_next_level: Math.max(0, 7 - new_consec),
    current_step: new_step,
    language: progress.language
  };
}
app.post("/quizz", async (req, res) => {
  try {
    const quizData = await buildQuizForSession(req.body || {});
    quizRequestCount += 1;
    if (quizRequestCount % 7 === 0) {
      triggerPreGeneration("quiz_batch", quizData.current_step);
    }
    return res.json(quizData);
  } catch (e) {
    const sessionId = resolveQuizUserKey(req.body || {});
    const progress = sessionId ? await getProgress(sessionId).catch(() => null) : null;
    const language = normalizeLanguage(req.body?.lang || progress?.language || "en");
    const message = localizedQuizErrors[language] || localizedQuizErrors.en;
    return res.status(503).json({
      success: false,
      type: null,
      q_type: null,
      quiz_type: null,
      question: null,
      options: [],
      image_url: null,
      explanation: "",
      message,
      feedback: message,
      successMsg: "",
      errorMsg: message,
      language,
      preparing: true,
      retryable: true,
      retry_after_ms: 3000
    });
  }
});

app.post("/validate", async (req, res) => {
  try {
    return res.json(await validateQuizForSession(req.body || {}));
  } catch (e) {
    const sessionId = resolveQuizUserKey(req.body || {});
    const progress = sessionId ? await getProgress(sessionId).catch(() => null) : null;
    const language = normalizeLanguage(req.body?.lang || progress?.language || "en");
    const message = localizedQuizErrors[language] || localizedQuizErrors.en;
    return res.status(422).json({
      success: false,
      correct: false,
      status: "error",
      message,
      feedback: message,
      explanation: "",
      successMsg: "",
      errorMsg: message,
      consecutive_correct: progress?.consecutive_correct || 0,
      needed_for_next_level: Math.max(0, 7 - (progress?.consecutive_correct || 0)),
      current_step: progress?.current_step || 1,
      language
    });
  }
});

app.get("/step", async (req, res) => {
  try {
    const session_id = resolveQuizUserKey({ session_id: req.query.session_id, DH7: req.query.DH7, TFID: req.query.TFID });
    if (!session_id) return res.status(400).json({ error: "session_id required" });

    let progress = await getProgress(session_id);
    if (!progress) {
      await saveProgress(session_id, "en", 1, 0, []);
      progress = { language: "en", current_step: 1, consecutive_correct: 0, recent_quiz_ids: [] };
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
    const invitation = await updateInvitation(invitationId, "accepted");
    if (!invitation || invitation.to_tfid !== tfid) return res.status(404).json({ success: false, error: "Invitation not found or expired" });
    const session = await loadGameSession(invitation.game_id);
    if (!session) return res.status(404).json({ success: false, error: "Game not found or expired" });
    if (session.state.status === "pending") {
      session.state.status = "active";
      const game = await getGameDefinition(session.gameSlug);
      const storedQuestion = await loadStoredGameQuestion(game, session.state.language, session.state.level, session.state.servedQuizIds || []);
      const question = executeMode0PureDB(storedQuestion);
      session.state.question = question.parsed.question || question.parsed.scrambled || question.parsed.letters || null;
      session.state.questionPayload = question.parsed;
      session.state.currentAnswer = question.parsed.answer || null;
      session.state.timeLimit = Number(question.parsed.timeLimit || storedQuestion.gameData?.timeLimit || 0);
      session.state.servedQuizIds = [...new Set([...(session.state.servedQuizIds || []), String(storedQuestion._id)])].slice(-50);
      session.state.questionStartedAt = Date.now();
      await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
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
    const invitation = await updateInvitation(invitationId, "declined");
    if (!invitation || invitation.to_tfid !== tfid) return res.status(404).json({ success: false, error: "Invitation not found or expired" });
    return res.json({ success: true, message: connectionMessage(await getUserLanguageByTfid(tfid), "declined"), game_id: invitation.game_id });
  } catch (e) {
    return res.status(400).json({ success: false, error: e.message });
  }
});

app.get("/game/state/:gameId", async (req, res) => {
  const session = await loadGameSession(req.params.gameId);
  if (!session) return res.status(404).json({ error: "Game not found or expired" });
  return res.json({ success: true, game: buildPublicGameState(session) });
});

app.post("/game/answer", async (req, res) => {
  try {
    const body = req.body || {};
    const session = await loadGameSession(body.game_id);
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
    const session = await loadGameSession(req.body?.game_id);
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
  const session = await loadGameSession(gameId);
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
    const rows = await RuntimeData.find({ kind: "game_session", expiresAt: { $gt: new Date() } }).lean();
    for (const row of rows) {
      const session = await loadGameSession(row.data?.gameId);
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
      const pendingInvitations = await loadPendingInvitationsForTfid(ws.tfid);
      for (const item of pendingInvitations) {
        const language = await getUserLanguageByTfid(ws.tfid);
        wsSend(ws, { type: "game:invitation", invitation: { invitationId: item.invitation_id, gameId: item.game_id, fromTfid: item.from_tfid, toTfid: item.to_tfid, gameSlug: item.game_slug, status: item.status, text: buildInvitationText(language, item.from_tfid), choices: invitationChoices(language), language, expiresAt: item.expires_at } });
      }
    }
    wsSend(ws, { type: "games:list", games: await listGameDefinitions() });
    const activeGames = [];
    const activeGameRecords = await RuntimeData.find({ kind: "game_session", expiresAt: { $gt: new Date() } }).lean();
    for (const row of activeGameRecords) {
      const stored = row.data || {};
      const players = Array.isArray(stored.players) ? stored.players : [];
      if ((ws.tfid && players.includes(ws.tfid)) || (ws.sessionId && stored.ownerTfid === ws.sessionId)) {
        const activeSession = await loadGameSession(stored.gameId);
        if (activeSession) {
          activeGames.push(buildPublicGameState(activeSession));
          registerSocketForGame(stored.gameId, ws);
        }
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
    const session = await loadGameSession(data.game_id);
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
    const invitation = await updateInvitation(String(data.invitation_id || data.invitationId || ""), "accepted");
    if (!invitation || invitation.to_tfid !== tfid) throw new Error("Invitation not found or expired");
    const session = await loadGameSession(invitation.game_id);
    if (!session) throw new Error("Game not found or expired");
    if (session.state.status === "pending") {
      session.state.status = "active";
      session.state.turnIndex = 0;
      session.state.askedTo = session.players[0];
      const game = await getGameDefinition(session.gameSlug);
      const storedQuestion = await loadStoredGameQuestion(game, session.state.language, session.state.level, session.state.servedQuizIds || []);
      const question = executeMode0PureDB(storedQuestion);
      session.state.question = question.parsed.question || question.parsed.scrambled || question.parsed.letters || null;
      session.state.questionPayload = question.parsed;
      session.state.currentAnswer = question.parsed.answer || null;
      session.state.timeLimit = Number(question.parsed.timeLimit || storedQuestion.gameData?.timeLimit || 0);
      session.state.servedQuizIds = [...new Set([...(session.state.servedQuizIds || []), String(storedQuestion._id)])].slice(-50);
      session.state.questionStartedAt = Date.now();
      await saveGameSession(session.gameId, session.gameSlug, session.ownerTfid, session.players, session.state);
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
    const invitation = await updateInvitation(String(data.invitation_id || data.invitationId || ""), "declined");
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
    const session = await loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const words = Array.isArray(data.words) ? data.words : [];
    await saveStoredWords(session.gameId, words);
    wsBroadcast(session.gameId, { type: "game:words", game_id: session.gameId, words });
    return;
  }

  if (type === "game:answer") {
    const session = await loadGameSession(data.game_id);
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
    const session = await loadGameSession(data.game_id);
    if (!session) throw new Error("Game not found or expired");
    const help = await generateGameHelp(session);
    wsSend(ws, { type: "game:help", game_id: session.gameId, help });
    return;
  }

  if (type === "quiz:next") {
    const quiz = await buildQuizForSession({
      session_id: data.session_id || ws.sessionId,
      DH7: data.DH7 || data.dh7,
      TFID: data.TFID || data.tfid || ws.tfid,
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
      DH7: data.DH7 || data.dh7,
      TFID: data.TFID || data.tfid || ws.tfid,
      user_answer: data.user_answer,
      silent: Boolean(data.silent)
    });
    wsSend(ws, { type: "quiz:result", result });
    return;
  }

  if (type === "quiz:validate_silent") {
    const result = await validateQuizForSession({
      session_id: data.session_id || ws.sessionId,
      DH7: data.DH7 || data.dh7,
      TFID: data.TFID || data.tfid || ws.tfid,
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

setInterval(() => cleanupExpiredGames().catch(() => {}), 60 * 1000);
setInterval(() => silentValidationAt.clear(), 60 * 60 * 1000);


const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  logEvent("SUCCESS", "SYSTEM", `Server running on port ${PORT} with WebSocket support`);
});
