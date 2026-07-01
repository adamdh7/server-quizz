import "dotenv/config";
import express from "express";
import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import mongoose from "mongoose";

const app = express();
app.use(express.json());

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(path.join(dataDir, "quiz_data_fallback.sqlite"));
db.exec("CREATE TABLE IF NOT EXISTS user_progress (session_id TEXT PRIMARY KEY, language TEXT, current_step INTEGER, consecutive_correct INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS used_persons (session_id TEXT, person_name TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS current_quiz (session_id TEXT PRIMARY KEY, q_type TEXT, question TEXT, options TEXT, image_url TEXT, answer TEXT, explanation TEXT, success_msg TEXT, error_msg TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS user_info (session_id TEXT PRIMARY KEY, data TEXT)");

const cfAccountId = process.env.CF_ACCOUNT_ID;
const cfToken = process.env.CF_TOKEN;
const SERVER_URL = process.env.SERVER_URL;
const MONGO_URI = process.env.MONGO_URI;

mongoose.connect(MONGO_URI).catch(e => e);

const baseQuizSchema = new mongoose.Schema({
  lang: String,
  qType: String,
  question: String,
  options: [String],
  imageUrl: String,
  answer: String,
  explanation: String,
  successMsg: String,
  errorMsg: String
});
const BaseQuiz = mongoose.model("BaseQuiz", baseQuizSchema);

const progressSchema = new mongoose.Schema({ sessionId: String, language: String, currentStep: Number, consecutiveCorrect: Number });
const Progress = mongoose.model("Progress", progressSchema);

const quizSchema = new mongoose.Schema({ sessionId: String, qType: String, question: String, options: String, imageUrl: String, answer: String, explanation: String, successMsg: String, errorMsg: String });
const CurrentQuiz = mongoose.model("CurrentQuiz", quizSchema);

const userSchema = new mongoose.Schema({ sessionId: String, data: String });
const UserInfo = mongoose.model("UserInfo", userSchema);

const usedPersonSchema = new mongoose.Schema({ sessionId: String, personName: String });
const UsedPersons = mongoose.model("UsedPersons", usedPersonSchema);

async function syncJsonToMongo() {
  const langs = ["en", "fr", "es", "ht"];
  for (const l of langs) {
    const p = path.join(process.cwd(), `${l}.json`);
    if (fs.existsSync(p)) {
      try {
        const content = fs.readFileSync(p, "utf-8");
        const data = JSON.parse(content);
        for (const item of data) {
          const exists = await BaseQuiz.findOne({ lang: l, question: item.question }).catch(e => true);
          if (!exists) {
            await BaseQuiz.create({ lang: l, ...item }).catch(e => e);
          }
        }
      } catch (e) {}
    }
  }
}

mongoose.connection.once("open", () => {
  syncJsonToMongo();
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
  try {
    const q = await CurrentQuiz.findOne({ sessionId: sessionId });
    if (q) return q;
  } catch (e) {}
  return db.prepare("SELECT * FROM current_quiz WHERE session_id = ?").get(sessionId);
}

async function saveCurrentQuiz(sessionId, qType, question, optionsStr, imageUrl, answer, explanation, successMsg, errorMsg) {
  try {
    await CurrentQuiz.findOneAndUpdate({ sessionId: sessionId }, { qType: qType, question: question, options: optionsStr, imageUrl: imageUrl, answer: answer, explanation: explanation, successMsg: successMsg, errorMsg: errorMsg }, { upsert: true });
  } catch (e) {}
  db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation, success_msg, error_msg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").run(sessionId, qType, question, optionsStr, imageUrl, answer, explanation, successMsg, errorMsg);
}

async function clearCurrentQuiz(sessionId) {
  try {
    await CurrentQuiz.deleteOne({ sessionId: sessionId });
  } catch (e) {}
  db.prepare("DELETE FROM current_quiz WHERE session_id = ?").run(sessionId);
}

async function saveUserInfo(sessionId, dataString) {
  try {
    await UserInfo.findOneAndUpdate({ sessionId: sessionId }, { data: dataString }, { upsert: true });
  } catch (e) {}
  db.prepare("REPLACE INTO user_info (session_id, data) VALUES (?, ?)").run(sessionId, dataString);
}

async function getUsedPersons(sessionId) {
  try {
    const docs = await UsedPersons.find({ sessionId: sessionId });
    return docs.map(d => d.personName);
  } catch (e) {}
  const res = db.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").all(sessionId);
  return res.map(r => r.person_name);
}

async function addUsedPerson(sessionId, name) {
  try {
    await UsedPersons.create({ sessionId: sessionId, personName: name });
  } catch (e) {}
  db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(sessionId, name);
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
    const json = await response.json();
    if (json.success && json.result) {
      return json.result;
    }
    throw new Error("AI Request Failed");
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
    
    const dbItems = await BaseQuiz.aggregate([{ $match: { lang: language } }, { $sample: { size: 1 } }]).catch(e => []);
    const randomItem = dbItems.length > 0 ? dbItems[0] : null;

    const strategy = Math.floor(Math.random() * 4);
    let parsed = null;
    let randomType = "MCQ";
    let imgUrl = null;
    let finalSuccess = null;
    let finalError = null;

    if (randomItem && strategy === 0) {
        parsed = {
            question: randomItem.question,
            options: randomItem.options,
            answer: randomItem.answer,
            explanation: randomItem.explanation
        };
        randomType = randomItem.qType || "MCQ";
        imgUrl = randomItem.imageUrl || null;
        finalSuccess = randomItem.successMsg || null;
        finalError = randomItem.errorMsg || null;
    } else if (randomItem && strategy === 1) {
        const systemPrompt = `Improve this quiz question slightly without changing the answer. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Original: ${randomItem.question}`;
        try {
            const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
            const rawResponse = aiResponse.response || "";
            const firstBrace = rawResponse.indexOf('{');
            const lastBrace = rawResponse.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
                parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
                if (!parsed.question || !parsed.answer) throw new Error("Invalid");
            } else throw new Error("No JSON structure");
        } catch (e) {
            parsed = randomItem;
        }
        randomType = randomItem.qType || "MCQ";
        finalSuccess = randomItem.successMsg || null;
        finalError = randomItem.errorMsg || null;
    } else if (randomItem && strategy === 2) {
         const systemPrompt = `Create a new quiz question in the EXACT same style and topic as this one. Language: ${langName}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Original: ${randomItem.question}`;
         try {
            const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
            const rawResponse = aiResponse.response || "";
            const firstBrace = rawResponse.indexOf('{');
            const lastBrace = rawResponse.lastIndexOf('}');
            if (firstBrace !== -1 && lastBrace !== -1) {
                parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
                if (!parsed.question || !parsed.answer) throw new Error("Invalid");
            } else throw new Error("No JSON structure");
        } catch (e) {
            parsed = randomItem;
        }
        randomType = randomItem.qType || "MCQ";
    } else {
         const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
         randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
         if (randomType === "IDENTITY_IMAGE") {
            const usedList = await getUsedPersons(session_id);
            let personName = "Albert Einstein";
            const personPrompt = `Return ONLY a valid JSON array containing 5 random famous historical figures. Exclude: ${usedList.join(",")}. Format: ["Name1", "Name2", "Name3", "Name4", "Name5"]`;
            try {
                const nameResp = await runAI([{ role: "system", content: "Output ONLY raw JSON." }, { role: "user", content: personPrompt }], 300);
                let candidates = [];
                const raw = nameResp.response || "";
                const firstBracket = raw.indexOf('[');
                const lastBracket = raw.lastIndexOf(']');
                if (firstBracket !== -1 && lastBracket !== -1) {
                    candidates = JSON.parse(raw.substring(firstBracket, lastBracket + 1));
                    if (Array.isArray(candidates)) {
                        for (const name of candidates) {
                            if (!usedList.includes(name)) {
                                personName = name;
                                break;
                            }
                        }
                    }
                }
            } catch(e) {}
            const imagePrompt = `A 100% authentic photograph of ${personName}, ultra-realistic documentary style, lifelike real human being, no digital art.`;
            let imageUrl = "";
            try {
                const cfImgUrl = `https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/@cf/black-forest-labs/flux-1-schnell`;
                const extImgResponse = await fetch(cfImgUrl, { method: "POST", headers: { "Authorization": `Bearer ${cfToken}`, "Content-Type": "application/json" }, body: JSON.stringify({ prompt: imagePrompt }) });
                if (extImgResponse.ok) {
                    const aiJson = await extImgResponse.json();
                    const base64Image = aiJson.result.image;
                    if (!base64Image) throw new Error("No image");
                    const buffer = Buffer.from(base64Image, "base64");
                    const filename = `img_${Date.now()}.png`;
                    const filePath = path.join(dataDir, filename);
                    fs.writeFileSync(filePath, buffer);
                    imageUrl = `${SERVER_URL}/local-image/${filename}`;
                } else {
                    throw new Error("Cloudflare image API failed");
                }
            } catch(e) {
                imageUrl = "https://placehold.co/600x400?text=Image+Error";
            }
            await addUsedPerson(session_id, personName);
            const questionTexts = { en: "Who is this person?", fr: "Qui est cette personne ?", es: "¿Quién es esta persona?", ht: "Kiyès moun sa?" };
            let questionText = questionTexts[language] || questionTexts.en;
            parsed = { question: questionText, options: [], answer: personName, explanation: "" };
            imgUrl = imageUrl;
         } else {
             const systemPrompt = `Create a ${randomType} quiz question. Topic: General Knowledge. Language: ${langName}. Difficulty: Level ${current_step_num}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Do not write anything else.`;
             try {
                const aiResponse = await runAI([{ role: "system", content: "You are a JSON API. Return ONLY valid JSON." }, { role: "user", content: systemPrompt }], 1000);
                const rawResponse = aiResponse.response || "";
                const firstBrace = rawResponse.indexOf('{');
                const lastBrace = rawResponse.lastIndexOf('}');
                if (firstBrace !== -1 && lastBrace !== -1) {
                    parsed = JSON.parse(rawResponse.substring(firstBrace, lastBrace + 1));
                    if (!parsed.question || !parsed.answer) throw new Error("Invalid format");
                } else {
                    throw new Error("No JSON structure");
                }
            } catch (e) {
                parsed = { question: language === "fr" ? "Quelle est la capitale de la France ?" : "What is the capital of France?", options: language === "fr" ? ["Paris", "Lyon", "Marseille"] : ["Paris", "London", "Berlin"], answer: "Paris", explanation: language === "fr" ? "Question de secours chargée suite à une erreur." : "Fallback question loaded due to network error." };
            }
         }
    }

    const safeOptions = Array.isArray(parsed.options) ? parsed.options : [];
    const optionsStr = JSON.stringify(safeOptions);

    await saveCurrentQuiz(session_id, randomType, parsed.question, optionsStr, imgUrl, parsed.answer, parsed.explanation || "", finalSuccess || "", finalError || "");

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

    const current = await getCurrentQuiz(session_id);
    if (!current) return res.status(400).json({ error: "No active quiz" });

    let progress = await getProgress(session_id);
    if (!progress) {
      progress = { language: "en", current_step: 1, consecutive_correct: 0 };
    }

    const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";
    const cleanUser = user_answer.toLowerCase().trim();
    const cleanAnswer = current.answer ? current.answer.toLowerCase().trim() : "";

    let judgeResult = { correct: false, explanation: "" };

    if (cleanAnswer !== "" && cleanUser === cleanAnswer) {
      judgeResult = { correct: true, explanation: current.successMsg || current.success_msg || current.explanation || "Correct!" };
    } else if (cleanAnswer !== "" && (cleanUser.includes(cleanAnswer) || cleanAnswer.includes(cleanUser))) {
      judgeResult = { correct: true, explanation: current.successMsg || current.success_msg || current.explanation || "Correct!" };
    } else {
      const judgePrompt = `Question: "${current.question}"\nExpected Answer: "${current.answer}"\nUser Answer: "${user_answer}"\nTask: Determine if the User Answer is correct or means the same thing as the Expected Answer.\nReturn ONLY valid JSON: {"correct": true or false, "explanation": "Brief explanation in ${langName}"}`;
      try {
        const judgeResp = await runAI([{ role: "system", content: "You evaluate answers. Output ONLY strict JSON." }, { role: "user", content: judgePrompt }], 800);
        const text = judgeResp.response || "";
        const firstBrace = text.indexOf('{');
        const lastBrace = text.lastIndexOf('}');
        if (firstBrace !== -1 && lastBrace !== -1) {
          judgeResult = JSON.parse(text.substring(firstBrace, lastBrace + 1));
        } else {
          throw new Error("No JSON found");
        }
      } catch (e) {
         judgeResult = { correct: false, explanation: current.errorMsg || current.error_msg || current.answer || "Incorrect." };
      }
    }

    const correctValue = judgeResult.correct ?? judgeResult.Correct ?? false;
    const isCorrect = correctValue === true || String(correctValue).toLowerCase() === "true";
    const explanation = isCorrect ? (current.successMsg || current.success_msg || judgeResult.explanation || "Correct!") : (current.errorMsg || current.error_msg || judgeResult.explanation || `Incorrect. Answer: ${current.answer}`);

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
    
    const aiJson = await aiReq.json();
    const aiResponse = aiJson.result;

    await new Promise(resolve => setTimeout(resolve, 7));

    if (!aiResponse || !aiResponse.image) {
      throw new Error("The AI did not return a valid image.");
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
      throw new Error("Upload failed or no URL returned by the upload service.");
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
