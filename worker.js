export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const allowedOrigin = "https://quiz.adamdh7.org";

    const corsHeaders = {
      "Access-Control-Allow-Origin": allowedOrigin,
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    };

    function jsonResponse(obj, status = 200, extra = {}) {
      const headers = new Headers({ ...corsHeaders, "Content-Type": "application/json", ...extra });
      return new Response(JSON.stringify(obj), { status, headers });
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (origin && origin !== allowedOrigin) {
      return new Response("Forbidden: Origin not allowed", { status: 403, headers: corsHeaders });
    }

    async function runAIWithTimeout(aiName, args = {}, ms = 7000) {
      const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error("AI timeout")), ms));
      const call = env.AI.run(aiName, args);
      return Promise.race([call, timeout]);
    }

    function base64ToArrayBuffer(base64) {
      const binary = atob(base64);
      const len = binary.length;
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = binary.charCodeAt(i);
      return bytes.buffer;
    }

    try {
      if (url.pathname.startsWith("/r2/")) {
        const key = url.pathname.slice(4);
        const object = await env.server2.get(key);
        if (!object) return new Response("Image Not Found", { status: 404, headers: corsHeaders });
        const headers = new Headers(corsHeaders);
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        headers.set("Cache-Control", "public, max-age=31536000, immutable");
        return new Response(object.body, { headers });
      }

      if (url.pathname === "/quizz") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        if (request.method !== "POST") return jsonResponse({ error: "Method not allowed" }, 405);

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return jsonResponse({ error: "Invalid JSON" }, 400);
        }

        const session_id = (body.session_id || "").trim();
        if (!session_id) return jsonResponse({ error: "session_id required" }, 400);

        const rawLang = (body.lang || "").trim();
        const lang = rawLang ? rawLang.toLowerCase() : null;

        const now = Date.now();
        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) {
          const default_lang = lang || "en";
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct, last_request) VALUES (?, ?, 1, 0, ?)").bind(session_id, default_lang, now).run();
          progress = { language: default_lang, current_step: 1, consecutive_correct: 0, last_request: now };
        } else {
          const lastReq = progress.last_request ? Number(progress.last_request) : 0;
          if (now - lastReq < 300) return jsonResponse({ error: "Too many requests" }, 429);
          await env.server.prepare("UPDATE user_progress SET last_request = ? WHERE session_id = ?").bind(now, session_id).run();
          if (lang) {
            await env.server.prepare("UPDATE user_progress SET language = ? WHERE session_id = ?").bind(lang, session_id).run();
            progress.language = lang;
          }
        }

        const current_step_num = Number(progress.current_step || 1);
        const language = progress.language || "en";

        const langName = {
          en: "English",
          fr: "French",
          es: "Spanish",
          ht: "Haitian Creole"
        }[language] || "English";

        const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
        const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];

        const quizData = {};

        const existing = await env.server.prepare("SELECT * FROM current_quiz WHERE session_id = ?").bind(session_id).first();
        if (existing && existing.q_type && existing.question) {
          quizData.type = existing.q_type;
          quizData.question = existing.question;
          if (existing.options) {
            try { quizData.options = JSON.parse(existing.options); } catch (e) { quizData.options = null; }
          }
          if (existing.image_url) quizData.image_url = existing.image_url;
          return new Response(JSON.stringify(quizData), { headers });
        }

        if (randomType === "IDENTITY_IMAGE") {
          const usedRows = await env.server.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").bind(session_id).all();
          const usedList = (usedRows.results || []).map(r => r.person_name);
          const avoidStr = usedList.length ? `Exclude these: ${usedList.join(", ")}.` : "";

          let personName = "";
          try {
            const prompt = `List up to 3 universally famous historical figures or celebrities (full name only), do not include explanations. ${avoidStr}`;
            const nameResp = await runAIWithTimeout("@cf/meta/llama-3.1-8b-instruct", {
              messages: [
                { role: "system", content: "You are concise." },
                { role: "user", content: prompt }
              ],
              max_tokens: 120
            }, 6000);
            const raw = (nameResp.response || "").replace(/```/g, "").trim();
            const lines = raw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
            personName = lines.length ? lines[0].replace(/^\d+[\).\s-]*/, "").replace(/\.$/, "") : "";
          } catch (e) {
            personName = "";
          }

          if (!personName) personName = "Albert Einstein";

          const cachedImg = await env.server.prepare("SELECT key FROM generated_images WHERE person_name = ? LIMIT 1").bind(personName).first();
          let imageUrl = null;
          if (cachedImg && cachedImg.key) {
            imageUrl = `https://${url.host}/r2/${cachedImg.key}`;
          } else {
            let imageResponse;
            try {
              imageResponse = await runAIWithTimeout("@cf/black-forest-labs/flux-1-schnell", {
                prompt: `Professional portrait of ${personName}, realistic, studio lighting, medium resolution`,
                num_steps: 2
              }, 9000);
            } catch (e) {
              imageResponse = null;
            }

            if (imageResponse && imageResponse.image) {
              const key = `quiz_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
              const buffer = base64ToArrayBuffer(imageResponse.image);
              await env.server2.put(key, buffer, { httpMetadata: { contentType: "image/png" } });
              await env.server.prepare("INSERT INTO generated_images (person_name, key, created_at) VALUES (?, ?, ?)").bind(personName, key, Date.now()).run();
              imageUrl = `https://${url.host}/r2/${key}`;
            } else {
              const fallbackKey = `fallback_${personName.replace(/\s+/g,'_').toLowerCase()}.png`;
              const fallbackObj = await env.server2.get(fallbackKey);
              if (fallbackObj) imageUrl = `https://${url.host}/r2/${fallbackKey}`;
              else imageUrl = null;
            }
          }

          await env.server.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").bind(session_id, personName).run();

          const questionTexts = {
            en: "Who is this person?",
            fr: "Qui est cette personne ?",
            es: "¿Quién es esta persona?",
            ht: "Kiyès moun sa a ye?"
          };
          const questionText = questionTexts[language] || questionTexts.en;

          await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation, created_at) VALUES (?, ?, ?, NULL, ?, ?, NULL, ?)").bind(session_id, "IDENTITY_IMAGE", questionText, imageUrl, personName, Date.now()).run();

          quizData.type = "IDENTITY_IMAGE";
          quizData.question = questionText;
          quizData.image_url = imageUrl;
          return new Response(JSON.stringify(quizData), { headers });
        } else {
          const systemPrompt = `Role: Quiz Generator. Target Language: ${langName}. Difficulty Level: ${current_step_num}. Question Type: ${randomType}. Create one question. Output strict JSON ONLY.`;
          let aiResponse;
          try {
            aiResponse = await runAIWithTimeout("@cf/meta/llama-3.1-8b-instruct", {
              messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: "Generate JSON now." }
              ],
              max_tokens: 700
            }, 7000);
          } catch (e) {
            aiResponse = null;
          }

          let parsed = null;
          if (aiResponse && aiResponse.response) {
            try {
              let raw = (aiResponse.response || "").replace(/```json/g, "").replace(/```/g, "").trim();
              parsed = JSON.parse(raw);
            } catch (e) {
              parsed = null;
            }
          }

          if (!parsed) {
            parsed = {
              question: language === "fr" ? "Erreur génération. Réessayer." : "Error generating question. Please retry.",
              options: null,
              answer: "Error",
              explanation: ""
            };
          }

          const optionsStr = parsed.options ? JSON.stringify(parsed.options) : null;
          await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation, created_at) VALUES (?, ?, ?, ?, NULL, ?, ?, ?)").bind(session_id, randomType, parsed.question, optionsStr, parsed.answer, parsed.explanation || null, Date.now()).run();

          quizData.type = randomType;
          quizData.question = parsed.question;
          if (parsed.options) quizData.options = parsed.options;
          return new Response(JSON.stringify(quizData), { headers });
        }
      }

      if (url.pathname === "/validate") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        if (request.method !== "POST") return jsonResponse({ error: "Method not allowed" }, 405);

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return jsonResponse({ error: "Invalid JSON" }, 400);
        }

        const session_id = (body.session_id || "").trim();
        const user_answer = (body.user_answer || "").trim();
        if (!session_id || !user_answer) return jsonResponse({ error: "session_id and user_answer required" }, 400);

        const current = await env.server.prepare("SELECT * FROM current_quiz WHERE session_id = ?").bind(session_id).first();
        if (!current) return jsonResponse({ error: "No active quiz" }, 400);

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) progress = { language: "en", current_step: 1, consecutive_correct: 0 };

        const langName = {
          en: "English",
          fr: "French",
          es: "Spanish",
          ht: "Haitian Creole"
        }[progress.language] || "English";

        const judgePrompt = `Validate the user's answer. Question: "${current.question}" User Answer: "${user_answer}" Correct Answer: "${current.answer}" Output JSON only: {"correct": boolean, "explanation": "short feedback in ${langName}"}`;
        let judgeResp;
        try {
          judgeResp = await runAIWithTimeout("@cf/meta/llama-3.1-8b-instruct", {
            messages: [
              { role: "system", content: "You are a JSON-only output machine." },
              { role: "user", content: judgePrompt }
            ],
            max_tokens: 300
          }, 6000);
        } catch (e) {
          judgeResp = null;
        }

        let judgeResult = { correct: false, explanation: "" };
        if (judgeResp && judgeResp.response) {
          try {
            let text = (judgeResp.response || "").replace(/```json|```/g, "").trim();
            judgeResult = JSON.parse(text);
          } catch (e) {
            judgeResult = { correct: false, explanation: "" };
          }
        } else {
          judgeResult = { correct: false, explanation: "" };
        }

        const isCorrect = !!judgeResult.correct;
        const explanation = judgeResult.explanation || (isCorrect ? (language === "fr" ? "Correct!" : "Correct!") : `Incorrect. Answer: ${current.answer}`);

        let new_consec = Number(progress.consecutive_correct || 0);
        let new_step = Number(progress.current_step || 1);

        if (isCorrect) {
          new_consec += 1;
          if (new_consec >= 7) {
            new_step += 1;
            new_consec = 0;
          }
          await env.server.prepare("DELETE FROM current_quiz WHERE session_id = ?").bind(session_id).run();
        } else {
          new_consec = 0;
        }

        await env.server.prepare("UPDATE user_progress SET consecutive_correct = ?, current_step = ? WHERE session_id = ?").bind(new_consec, new_step, session_id).run();

        const response = {
          correct: isCorrect,
          explanation: explanation,
          consecutive_correct: new_consec,
          needed_for_next_level: Math.max(0, 7 - new_consec),
          current_step: new_step
        };

        return new Response(JSON.stringify(response), { headers });
      }

      if (url.pathname === "/step") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        if (request.method !== "GET") return jsonResponse({ error: "Method not allowed" }, 405);

        const session_id = url.searchParams.get("session_id");
        if (!session_id) return jsonResponse({ error: "session_id required" }, 400);

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) {
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct, last_request) VALUES (?, 'en', 1, 0, ?)").bind(session_id, Date.now()).run();
          progress = { language: "en", current_step: 1, consecutive_correct: 0 };
        }

        return new Response(JSON.stringify({
          language: progress.language,
          current_step: progress.current_step,
          consecutive_correct: progress.consecutive_correct,
          needed_for_next_level: Math.max(0, 7 - progress.consecutive_correct)
        }), { headers });
      }

    } catch (e) {
      const errorHeaders = { ...corsHeaders, "Content-Type": "application/json" };
      return new Response(JSON.stringify({
        error: "Internal Server Error",
        message: e.message
      }), { status: 500, headers: errorHeaders });
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }
};
