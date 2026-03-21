import express from "express";
import Database from "better-sqlite3";

const app = express();
app.use(express.json());

const db = new Database("quiz_data.sqlite");

db.exec("CREATE TABLE IF NOT EXISTS user_progress (session_id TEXT PRIMARY KEY, language TEXT, current_step INTEGER, consecutive_correct INTEGER)");
db.exec("CREATE TABLE IF NOT EXISTS used_persons (session_id TEXT, person_name TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS current_quiz (session_id TEXT PRIMARY KEY, q_type TEXT, question TEXT, options TEXT, image_url TEXT, answer TEXT, explanation TEXT)");
db.exec("CREATE TABLE IF NOT EXISTS used_questions (session_id TEXT, question TEXT)");

const cfAccountId = process.env.CF_ACCOUNT_ID || "REPLACE_WITH_YOUR_ACCOUNT_ID";
const cfToken = "cfut_PkxDXlTK6zC6iAaDG2jtZj73oOB5f2HBKDrQ0Pxb073c4bf5";
const r2BaseUrl = process.env.R2_BASE_URL || "https://r2.adamdh7.org";

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

app.get("/r2/:key", async (req, res) => {
  try {
    const key = req.params.key;
    const response = await fetch(`${r2BaseUrl}/${key}`);
    if (!response.ok) {
      return res.status(404).send("Image Not Found");
    }
    const arrayBuffer = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    res.setHeader("Content-Type", response.headers.get("Content-Type") || "image/jpeg");
    res.setHeader("Cache-Control", "public, max-age=31536000, immutable");
    res.send(buffer);
  } catch (e) {
    res.status(500).send("Error fetching from R2");
  }
});

async function runAI(messages, max_tokens) {
  const aiUrl = `https://api.cloudflare.com/client/v4/accounts/${cfAccountId}/ai/run/@cf/meta/llama-3.2-3b-instruct`;
  const aiResponse = await fetch(aiUrl, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${cfToken}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ messages, max_tokens })
  });
  const aiJson = await aiResponse.json();
  if (aiJson.success && aiJson.result) {
    return aiJson.result;
  }
  throw new Error("AI Request Failed");
}

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
      
      const personPrompt = `Generate a JSON array of 5 globally famous historical figures. Exclude: ${usedList.join(", ")}. Format: ["Name1", "Name2", "Name3", "Name4", "Name5"]. NO EXTRA TEXT.`;

      try {
        const nameResp = await runAI([
          { role: "system", content: "You output strict JSON arrays." },
          { role: "user", content: personPrompt }
        ], 300);

        let candidates = [];
        const raw = (nameResp.response || "").replace(/```json|```/g, "").trim();
        candidates = JSON.parse(raw);
        
        if (Array.isArray(candidates)) {
           for (const name of candidates) {
              if (!usedList.includes(name)) {
                 personName = name;
                 break;
              }
           }
        }
      } catch(e) {}

      const imagePrompt = `Professional portrait of ${personName}, realistic, 8k, studio lighting`;
      
      const extImgResponse = await fetch("https://server4.adamdh7.org/jerere", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: imagePrompt })
      });

      if (!extImgResponse.ok) {
         return res.status(500).json({ error: "External image generation failed" });
      }

      const imgJson = await extImgResponse.json();
      const imageUrl = imgJson.url;

      db.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").run(session_id, personName);

      const questionTexts = {
        en: "Who is this person?",
        fr: "Qui est cette personne ?",
        es: "¿Quién es esta persona?",
        ht: "Kiyès moun sa a ye?"
      };

      let questionText = questionTexts[language] || questionTexts.en;

      db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)").run(session_id, randomType, questionText, imageUrl, personName);

      quizData.type = randomType;
      quizData.question = questionText;
      quizData.image_url = imageUrl;

    } else {
      
      const usedQRes = db.prepare("SELECT question FROM used_questions WHERE session_id = ? ORDER BY rowid DESC LIMIT 10").all(session_id);
      const usedQList = usedQRes.map(r => r.question).join(" | ");

      const systemPrompt = `Task: Create ONE unique quiz question. Topic: Random. Lang: ${langName}. Level: ${current_step_num}. Type: ${randomType}. Do NOT repeat these questions: ${usedQList}. Output STRICT JSON format: { "question": "text", "options": ["opt1","opt2"], "answer": "text", "explanation": "text" }. No markdown, no extra text.`;

      let parsed = null;
      try {
        const aiResponse = await runAI([
          { role: "system", content: "You are a strict JSON quiz generator." },
          { role: "user", content: systemPrompt }
        ], 1000);

        let rawResponse = (aiResponse.response || "").replace(/```json/g, "").replace(/```/g, "").trim();
        parsed = JSON.parse(rawResponse);
      } catch (e) {
        parsed = {
          question: "Error generating question.",
          options: null,
          answer: "Error",
          explanation: ""
        };
      }

      const optionsStr = parsed.options ? JSON.stringify(parsed.options) : null;

      db.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)").run(session_id, randomType, parsed.question, optionsStr, parsed.answer, parsed.explanation || null);
      db.prepare("INSERT INTO used_questions (session_id, question) VALUES (?, ?)").run(session_id, parsed.question);

      quizData.type = randomType;
      quizData.question = parsed.question;
      if (parsed.options) quizData.options = parsed.options;
    }

    return res.json(quizData);
  } catch (e) {
    res.status(500).json({ error: "Internal Server Error", message: e.message });
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

    const judgePrompt = `Validate the user answer. Question: "${current.question}". Correct Answer: "${current.answer}". User Answer: "${user_answer}". Output STRICT JSON: {"correct": true, "explanation": "Short feedback in ${langName}"}`;

    let judgeResult = { correct: false, explanation: "" };
    try {
      const judgeResp = await runAI([
        { role: "system", content: "You output strictly JSON." },
        { role: "user", content: judgePrompt }
    ], 700);

      let text = (judgeResp.response || "").replace(/```json|```/g, "").trim();
      judgeResult = JSON.parse(text);
    } catch (e) {
       judgeResult = { correct: false, explanation: "Validation error." };
    }

    const isCorrect = !!judgeResult.correct;
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
    res.status(500).json({ error: "Internal Server Error", message: e.message });
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
    res.status(500).json({ error: "Internal Server Error", message: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
