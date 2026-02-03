export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const allowedOrigin = "https://quiz.adamdh7.org";
    const corsHeaders = {
      "Access-Control-Allow-Origin": allowedOrigin,
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    if (origin && origin !== allowedOrigin) {
      return new Response("Forbidden", { status: 403, headers: corsHeaders });
    }

    try {
      if (url.pathname.startsWith("/r2/")) {
        const key = url.pathname.slice(4);
        const object = await env.server2.get(key);
        if (!object) return new Response("Not Found", { status: 404, headers: corsHeaders });
        const headers = new Headers(corsHeaders);
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        headers.set("Cache-Control", "public, max-age=31536000, immutable");
        return new Response(object.body, { headers });
      }

      if (url.pathname === "/quizz") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        if (request.method !== "POST") return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers });

        let body = {};
        try { body = await request.json(); } catch (e) {}
        
        const session_id = body.session_id?.trim();
        if (!session_id) return new Response(JSON.stringify({ error: "session_id required" }), { status: 400, headers });

        const rawLang = body.lang?.trim();
        const lang = rawLang ? rawLang.toLowerCase() : null;

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) {
          const default_lang = lang || "en";
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, ?, 1, 0)").bind(session_id, default_lang).run();
          progress = { language: default_lang, current_step: 1, consecutive_correct: 0 };
        } else if (lang && progress.language !== lang) {
          ctx.waitUntil(env.server.prepare("UPDATE user_progress SET language = ? WHERE session_id = ?").bind(lang, session_id).run());
          progress.language = lang;
        }

        const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";
        const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
        let randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
        let quizData = {};

        try {
          if (randomType === "IDENTITY_IMAGE") {
            const { results: usedRes } = await env.server.prepare("SELECT person_name FROM used_persons WHERE session_id = ? ORDER BY rowid DESC LIMIT 50").bind(session_id).all();
            const usedList = usedRes.map(r => r.person_name).join(", ");
            
            const aiNameResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
              messages: [{ role: "user", content: `Generate 1 famous historical figure name. Exclude: ${usedList}. Output ONLY the name.` }]
            });
            let personName = (aiNameResp.response || "").trim().replace(/\.$/, "") || "Albert Einstein";

            const imageResponse = await env.AI.run("@cf/black-forest-labs/flux-1-schnell", {
              prompt: `Portrait of ${personName}, realistic, 8k`,
              num_steps: 4,
            });

            if (!imageResponse || !imageResponse.image) throw new Error("ImgGenFail");

            const binStr = atob(imageResponse.image);
            const len = binStr.length;
            const bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) bytes[i] = binStr.charCodeAt(i);

            const key = `q_${Date.now()}_${Math.random().toString(36).slice(2)}.png`;
            ctx.waitUntil(env.server2.put(key, bytes.buffer, { httpMetadata: { contentType: "image/png" } }));
            
            ctx.waitUntil(env.server.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").bind(session_id, personName).run());

            const imageUrl = `https://${url.host}/r2/${key}`;
            const qTexts = { en: "Who is this?", fr: "Qui est-ce ?", es: "¿Quién es?", ht: "Kiyès sa?" };
            const qText = qTexts[progress.language] || qTexts.en;

            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)")
              .bind(session_id, randomType, qText, imageUrl, personName).run();

            quizData = { type: randomType, question: qText, image_url: imageUrl };

          } else {
            const sysPrompt = `Task: Create ${randomType} quiz in ${langName}. Level: ${progress.current_step}. JSON Only.
Format: {"question": "Txt", "options": ["A","B"] or null, "answer": "Ans", "explanation": "Exp"}`;
            
            const aiResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
              messages: [{ role: "system", content: sysPrompt }, { role: "user", content: "Generate JSON." }]
            });

            let parsed;
            try { parsed = JSON.parse(aiResp.response.replace(/```json|```/g, "").trim()); } catch(e) { throw new Error("JsonFail"); }

            const optsStr = parsed.options ? JSON.stringify(parsed.options) : null;
            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)")
              .bind(session_id, randomType, parsed.question, optsStr, parsed.answer, parsed.explanation).run();

            quizData = { type: randomType, question: parsed.question, options: parsed.options };
          }
        } catch (err) {
            // FALLBACK SAFE MODE
            randomType = "MCQ";
            const safeQ = {
                en: { q: "Which planet is known as the Red Planet?", o: ["Mars", "Venus", "Jupiter", "Saturn"], a: "Mars" },
                fr: { q: "Quelle planète est la planète rouge ?", o: ["Mars", "Vénus", "Jupiter", "Saturne"], a: "Mars" },
                es: { q: "¿Qué planeta es el planeta rojo?", o: ["Marte", "Venus", "Júpiter", "Saturno"], a: "Marte" },
                ht: { q: "Ki planèt ki wouj la?", o: ["Mas", "Venis", "Jipitè", "Satis"], a: "Mas" }
            }[progress.language] || { q: "Red Planet?", o: ["Mars"], a: "Mars" };

            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)")
                .bind(session_id, "MCQ", safeQ.q, JSON.stringify(safeQ.o), safeQ.a, "Mars is red due to iron oxide.").run();
            
            quizData = { type: "MCQ", question: safeQ.q, options: safeQ.o };
        }

        return new Response(JSON.stringify(quizData), { headers });
      }

      if (url.pathname === "/validate") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        if (request.method !== "POST") return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers });

        let body = {};
        try { body = await request.json(); } catch (e) {}
        const { session_id, user_answer } = body;
        
        if (!session_id || !user_answer) return new Response(JSON.stringify({ error: "Missing data" }), { status: 400, headers });

        const current = await env.server.prepare("SELECT * FROM current_quiz WHERE session_id = ?").bind(session_id).first();
        if (!current) return new Response(JSON.stringify({ error: "No active quiz" }), { status: 400, headers });

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) progress = { language: "en", current_step: 1, consecutive_correct: 0 };

        const prompt = `Compare Answer. Question: "${current.question}". Correct: "${current.answer}". User: "${user_answer}".
Reply JSON: {"correct": boolean, "explanation": "Short feedback in ${progress.language}"}`;

        let isCorrect = false;
        let explanation = "";

        try {
            const aiResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", { messages: [{ role: "user", content: prompt }] });
            const res = JSON.parse(aiResp.response.replace(/```json|```/g, "").trim());
            isCorrect = !!res.correct;
            explanation = res.explanation;
        } catch (e) {
            isCorrect = user_answer.toLowerCase().includes(current.answer.toLowerCase());
            explanation = isCorrect ? "Correct!" : `Incorrect. Answer: ${current.answer}`;
        }

        let { consecutive_correct, current_step } = progress;
        if (isCorrect) {
            consecutive_correct++;
            if (consecutive_correct >= 7) { current_step++; consecutive_correct = 0; }
            ctx.waitUntil(env.server.prepare("DELETE FROM current_quiz WHERE session_id = ?").bind(session_id).run());
        } else {
            consecutive_correct = 0;
        }

        ctx.waitUntil(env.server.prepare("UPDATE user_progress SET consecutive_correct = ?, current_step = ? WHERE session_id = ?").bind(consecutive_correct, current_step, session_id).run());

        return new Response(JSON.stringify({
          correct: isCorrect,
          explanation: explanation || (isCorrect ? "Correct" : "Incorrect"),
          consecutive_correct,
          needed_for_next_level: Math.max(0, 7 - consecutive_correct),
          current_step
        }), { headers });
      }

      if (url.pathname === "/step") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");
        const session_id = new URL(request.url).searchParams.get("session_id");
        if (!session_id) return new Response(JSON.stringify({ error: "session_id required" }), { status: 400, headers });
        
        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) {
            await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, 'en', 1, 0)").bind(session_id).run();
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
      return new Response(JSON.stringify({ error: "Internal Error", message: "Recovered" }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }
    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }
};
