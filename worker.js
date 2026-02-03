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

    if (request.method === "OPTIONS") return new Response(null, { headers: corsHeaders });
    if (origin && origin !== allowedOrigin) return new Response("Forbidden", { status: 403, headers: corsHeaders });

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

        const { current_step: current_step_num, language } = progress;
        const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[language] || "English";
        const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
        let randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];
        let quizData = {};

        try {
          if (randomType === "IDENTITY_IMAGE") {
            const { results: usedRes } = await env.server.prepare("SELECT person_name FROM used_persons WHERE session_id = ? ORDER BY rowid DESC LIMIT 30").bind(session_id).all();
            const usedList = usedRes.map(r => r.person_name).join(", ");
            const avoid = usedList ? `Exclude these: ${usedList}.` : "";
            const personPrompt = `Generate the full name of one universally famous historical figure or celebrity. ${avoid} Output ONLY the name.`;

            const nameResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
              messages: [{ role: "system", content: "You are a precise database assistant." }, { role: "user", content: personPrompt }]
            });

            let personName = (nameResp.response || "").trim().replace(/\.$/, "");
            if (!personName || (usedList && usedList.includes(personName))) personName = "Albert Einstein";

            const imageInputs = { prompt: `Professional portrait of ${personName}, realistic, 8k, studio lighting`, num_steps: 4 };
            const imageResponse = await env.AI.run("@cf/black-forest-labs/flux-1-schnell", imageInputs);
            
            if (!imageResponse?.image) throw new Error("ImgGenFail");

            const binStr = atob(imageResponse.image);
            const len = binStr.length;
            const bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) bytes[i] = binStr.charCodeAt(i);

            const key = `q_${Date.now()}_${Math.random().toString(36).slice(2)}.png`;
            ctx.waitUntil(env.server2.put(key, bytes.buffer, { httpMetadata: { contentType: "image/png" } }));
            ctx.waitUntil(env.server.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)").bind(session_id, personName).run());

            const imageUrl = `https://${url.host}/r2/${key}`;
            const qTexts = { en: "Who is this person?", fr: "Qui est cette personne ?", es: "¿Quién es esta persona?", ht: "Kiyès moun sa a ye?" };
            const questionText = qTexts[language] || qTexts.en;

            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)")
              .bind(session_id, randomType, questionText, imageUrl, personName).run();

            quizData = { type: randomType, question: questionText, image_url: imageUrl };

          } else {
            const systemPrompt = `Role: Quiz Generator.
Target Language: ${langName}.
Difficulty Level: ${current_step_num} (1=easy ... X=hard).
Question Type: ${randomType}.

Instructions:
1. Create a question in ${langName}.
2. If MCQ, provide 4 options. If TRUE_FALSE, options are null.
3. Provide the exact answer.
4. Provide a short explanation in ${langName}.
5. Output strict valid JSON ONLY.

Format:
{
  "question": "Question text here",
  "options": ["A) ...", "B) ..."] or null,
  "answer": "Correct answer text",
  "explanation": "Brief explanation"
}`;

            const aiResponse = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
              messages: [{ role: "system", content: systemPrompt }, { role: "user", content: "Generate JSON now." }]
            });

            let rawResponse = (aiResponse.response || "").replace(/```json|```/g, "").trim();
            let parsed;
            try { parsed = JSON.parse(rawResponse); } catch (e) { throw new Error("JSON Parse Error"); }

            const optionsStr = parsed.options ? JSON.stringify(parsed.options) : null;
            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)")
              .bind(session_id, randomType, parsed.question, optionsStr, parsed.answer, parsed.explanation || null).run();

            quizData = { type: randomType, question: parsed.question, options: parsed.options };
          }
        } catch (err) {
            // Fallback safe mode
            const safeQ = {
                en: { q: "What matches the description?", a: "Data", o: ["Data", "Error", "Null", "Void"] },
                fr: { q: "Que correspond à la description ?", a: "Donnée", o: ["Donnée", "Erreur", "Nul", "Vide"] },
                es: { q: "¿Qué coincide con la descripción?", a: "Datos", o: ["Datos", "Error", "Nulo", "Vacío"] },
                ht: { q: "Kisa ki matche ak deskripsyon an?", a: "Done", o: ["Done", "Erè", "Nil", "Vid"] }
            }[language] || { q: "Select Data", a: "Data", o: ["Data"] };
            
            quizData = { type: "MCQ", question: safeQ.q, options: safeQ.o };
            await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, 'MCQ', ?, ?, NULL, ?, 'System recovery.')")
              .bind(session_id, safeQ.q, JSON.stringify(safeQ.o), safeQ.a).run();
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
        if (!session_id || !user_answer) return new Response(JSON.stringify({ error: "Data required" }), { status: 400, headers });

        const current = await env.server.prepare("SELECT * FROM current_quiz WHERE session_id = ?").bind(session_id).first();
        if (!current) return new Response(JSON.stringify({ error: "No active quiz" }), { status: 400, headers });

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first() || { language: "en", current_step: 1, consecutive_correct: 0 };
        const langName = { en: "English", fr: "French", es: "Spanish", ht: "Haitian Creole" }[progress.language] || "English";

        const judgePrompt = `Validate the user's answer. If an image description is provided, compare the user's input to the person described. Reply "Correct" if it matches, otherwise "Incorrect" : 
Question : "${current.question}"
User Answer : "${user_answer}"
Correct Answer: "${current.answer}"
Mark as 'Correct' if the response is relevant to the question, otherwise 'Incorrect'.
IMPORTANT: Output ONLY valid JSON:
{"correct": boolean, "explanation": "Short feedback in ${langName}"}`;

        let isCorrect = false, explanation = "";
        try {
            const judgeResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
                messages: [{ role: "system", content: "You are a JSON-only output machine." }, { role: "user", content: judgePrompt }]
            });
            const parsed = JSON.parse((judgeResp.response || "").replace(/```json|```/g, "").trim());
            isCorrect = !!parsed.correct;
            explanation = parsed.explanation;
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
      return new Response(JSON.stringify({ error: "Internal Server Error", message: "Recovered" }), { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } });
    }
    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }
};
