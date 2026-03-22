import express from "express";
import Database from "better-sqlite3";
import fs from "fs";
import path from "path";
import crypto from "crypto";

const app = express();
app.use(express.json());

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const db = new Database(path.join(dataDir, "quiz_data.sqlite"));

db.exec("CREATE TABLE IF NOT EXISTS user_progress (session_id TEXT PRIMARY KEY, language TEXT, current_step INTEGER, consecutive_correct INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS used_persons (session_id TEXT, person_name TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS current_quiz (session_id TEXT PRIMARY KEY, q_type TEXT, question TEXT, options TEXT, image_url TEXT, answer TEXT, explanation TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS user_info (session_id TEXT PRIMARY KEY, data TEXT)");

const cfAccountId = process.env.CF_ACCOUNT_ID || "REPLACE_WITH_YOUR_ACCOUNT_ID";
const cfToken = process.env.CF_TOKEN || "cfut_PkxDXlTK6zC6iAaDG2jtZj73oOB5f2HBKDrQ0Pxb073c4bf5";
const SERVER_URL = "https://server.quiz.adamdh7.org";

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

app.post("/user-info", (req, res) => {
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
    db.prepare("REPLACE INTO user_info (session_id, data) VALUES (?, ?)").run(session_id, dataString);

    let progress = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(session_id);
    
    let newStep = body.level !== undefined && body.level !== null ? parseInt(body.level) : (progress ? progress.current_step : 1);
    let newConsec = body.nivo !== undefined && body.nivo !== null ? parseInt(body.nivo) : (progress ? progress.consecutive_correct : 0);

    if (!progress) {
      db.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, 'en', ?, ?)").run(session_id, newStep, newConsec);
    } else {
      db.prepare("UPDATE user_progress SET current_step = ?, consecutive_correct = ? WHERE session_id = ?").run(newStep, newConsec, session_id);
    }

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

    let progress = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(session_id);

    if (!progress) {
      const default_lang = lang || "en";
      const start_step = incomingLevel || 1;
      db.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, ?, ?, 0)").run(session_id, default_lang, start_step);
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
        db.prepare("UPDATE user_progress SET language = ?, current_step = ? WHERE session_id = ?").run(progress.language, progress.current_step, session_id);
      }
    }

    const current_step_num = progress.current_step;
    const language = progress.language;

    const langName = {
      en: "English",
      fr: "French",
      es: "Spanish",
      ht: "Haitian Creole"
    }[language] || "English";

    const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
    const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];

    const quizData = {
      current_step: current_step_num,
      consecutive_correct: progress.consecutive_correct,
      language: progress.language,
      needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct)
    };

    if (randomType === "IDENTITY_IMAGE") {
      const usedRes = db.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").all(session_id);
      const usedList = usedRes.map(r => r.person_name);

      let personName = "Albert Einstein";
      
      const personPrompt = `Return ONLY a valid JSON array containing 5 random famous historical figures. Exclude: ${usedList.join(",")}. Format: ["Name1", "Name2", "Name3", "Name4", "Name5"]`;

      try {
        const nameResp = await runAI([
          { role: "system", content: "Output ONLY raw JSON. No markdown." },
          { role: "user", content: personPrompt }
        ], 300);

        let candidates = [];
        const raw = nameResp.response || "";
        const match = raw.match(/\[[\s\S]*\]/);
        
        if (match) {
          candidates = JSON.parse(match[0]);
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
        
        const extImgResponse = await fetch(cfImgUrl, {
          method: "POST",
          headers: { 
            "Authorization": `Bearer ${cfToken}`,
            "Content-Type": "application/json" 
          },
          body: JSON.stringify({ prompt: imagePrompt })
        });

        if (extImgResponse.ok) {
          const aiJson = await extImgResponse.json();
          const base64Image = aiJson.result.image;
          
          if (!base64Image) throw new Error("No image");

          const buffer = Buffer.from(base64Image, 'base64');
          const filename = `img_${Date.now()}.png`;
          const blob = new Blob([buffer], { type: "image/png" });
          
          const formData = new FormData();
          formData.append("file", blob, filename);
          
          const uploadRes = await fetch("https://v1bref.onrender.com/upload", {
            method: "POST",
            body: formData
          });
          
          let uploadData = null;
          try {
            uploadData = await uploadRes.json();
          } catch (e) {
            const text = await uploadRes.text();
            if (text.startsWith("http")) uploadData = { url: text };
          }
          
          imageUrl = uploadData?.url || uploadData?.link || uploadData?.file?.url || null;
          if (!imageUrl) throw new Error("Upload failed");
        }
      } catch(e) {
        imageUrl = "https://placehold.co/600x400?text=Image+Error";
      }

      db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(session_id, personName);

      const questionTexts = {
        en: "Who is this person?",
        fr: "Qui est cette personne ?",
        es: "¿Quién es esta persona?",
        ht: "Kiyès moun sa?"
      };

      let questionText = questionTexts[language] || questionTexts.en;

      db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)").run(session_id, randomType, questionText, imageUrl, personName);

      quizData.type = randomType;
      quizData.question = questionText;
      quizData.image_url = imageUrl;

    } else {
      const systemPrompt = `Create a ${randomType} quiz question. Topic: General Knowledge. Language: ${langName}. Difficulty: Level ${current_step_num}. Return ONLY a valid JSON object. Schema: {"question":"string","options":["string","string"],"answer":"string","explanation":"string"}. Do not write anything else.`;

      let parsed = null;
      try {
        const aiResponse = await runAI([
          { role: "system", content: "You are a JSON API. Return ONLY valid JSON." },
          { role: "user", content: systemPrompt }
        ], 1000);

        const rawResponse = aiResponse.response || "";
        const match = rawResponse.match(/\{[\s\S]*\}/);
        
        if (match) {
          parsed = JSON.parse(match[0]);
          if (!parsed.question || !parsed.answer) throw new Error("Invalid format");
        } else {
          throw new Error("No JSON structure");
        }
      } catch (e) {
        parsed = {
          question: language === "fr" ? "Quelle est la capitale de la France ?" : "What is the capital of France?",
          options: language === "fr" ? ["Paris", "Lyon", "Marseille"] : ["Paris", "London", "Berlin"],
          answer: "Paris",
          explanation: language === "fr" ? "Question de secours chargée suite à une erreur réseau." : "Fallback question loaded due to network error."
        };
      }

      const safeOptions = Array.isArray(parsed.options) ? parsed.options : [];
      const optionsStr = JSON.stringify(safeOptions);

      db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)").run(session_id, randomType, parsed.question, optionsStr, parsed.answer, parsed.explanation || null);

      quizData.type = randomType;
      quizData.question = parsed.question;
      if (safeOptions.length > 0) quizData.options = safeOptions;
    }

    return res.json(quizData);
  } catch (e) {
    db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, 'TRUE_FALSE', 'Error loading question. True to continue.', '[\"True\",\"False\"]', NULL, 'True', 'System recovery.')").run(req.body.session_id || "default");
    
    return res.json({
      type: "TRUE_FALSE",
      question: "Error loading question. True to continue.",
      options: ["True", "False"],
      error_msg: e.message
    });
  }
});

app.post("/validate", async (req, res) => {
  try {
    const body = req.body;
    const session_id = body.session_id?.trim();
    const user_answer = body.user_answer?.trim() || "";
    if (!session_id || !user_answer) return res.status(400).json({ error: "session_id and user_answer required" });

    const current = db.prepare("SELECT * FROM current_quiz WHERE session_id = ?").get(session_id);
    if (!current) return res.status(400).json({ error: "No active quiz" });

    let progress = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(session_id);
    if (!progress) {
      progress = { language: "en", current_step: 1, consecutive_correct: 0 };
    }

    const langName = {
      en: "English",
      fr: "French",
      es: "Spanish",
      ht: "Haitian Creole"
    }[progress.language] || "English";

    const judgePrompt = `Question: "${current.question}"
Expected Answer: "${current.answer}"
User Answer: "${user_answer}"
Task: Determine if the User Answer is correct or means the same thing as the Expected Answer.
Return ONLY valid JSON: {"correct": true or false, "explanation": "Brief explanation in ${langName}"}`;

    let judgeResult = { correct: false, explanation: "" };
    try {
      const judgeResp = await runAI([
        { role: "system", content: "You evaluate answers. Output ONLY strict JSON." },
        { role: "user", content: judgePrompt }
      ], 800);

      const text = judgeResp.response || "";
      const match = text.match(/\{[\s\S]*\}/);
      
      if (match) {
        judgeResult = JSON.parse(match[0]);
      } else {
        throw new Error("No JSON found");
      }
    } catch (e) {
       const safeAnswer = current.answer ? current.answer.toLowerCase().trim() : "";
       const safeUser = user_answer.toLowerCase().trim();
       judgeResult = { 
         correct: safeAnswer !== "" && (safeUser.includes(safeAnswer) || safeAnswer.includes(safeUser)), 
         explanation: current.answer
       };
    }

    const correctValue = judgeResult.correct ?? judgeResult.Correct ?? false;
    const isCorrect = correctValue === true || String(correctValue).toLowerCase() === "true";
    const explanation = judgeResult.explanation || (isCorrect ? "Correct!" : `Incorrect. Answer: ${current.answer}`);

    let new_consec = progress.consecutive_correct;
    let new_step = progress.current_step;

    if (isCorrect) {
      new_consec += 1;
      if (new_consec >= 7) {
        new_step += 1;
        new_consec = 0;
      }
      db.prepare("DELETE FROM current_quiz WHERE session_id = ?").run(session_id);
    } else {
      new_consec = 0;
    }

    db.prepare("UPDATE user_progress SET consecutive_correct = ?, current_step = ? WHERE session_id = ?").run(new_consec, new_step, session_id);

    return res.json({
      correct: isCorrect,
      explanation: explanation,
      consecutive_correct: new_consec,
      needed_for_next_level: Math.max(0, 7 - new_consec),
      current_step: new_step,
      language: progress.language
    });
  } catch (e) {
    return res.json({
      correct: false,
      explanation: "Validation error. Please try again.",
      consecutive_correct: 0,
      needed_for_next_level: 7,
      current_step: 1,
      language: "en"
    });
  }
});

app.get("/step", async (req, res) => {
  try {
    const session_id = req.query.session_id;
    if (!session_id) return res.status(400).json({ error: "session_id required" });

    let progress = db.prepare("SELECT * FROM user_progress WHERE session_id = ?").get(session_id);
    if (!progress) {
      db.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, 'en', 1, 0)").run(session_id);
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
