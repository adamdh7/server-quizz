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
      return new Response("Forbidden: Origin not allowed", { status: 403, headers: corsHeaders });
    }

    try {
      if (url.pathname.startsWith("/r2/")) {
        const key = url.pathname.slice(4);
        const object = await env.server2.get(key);

        if (!object) {
          return new Response("Image Not Found", { status: 404, headers: corsHeaders });
        }

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

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers });
        }

        const session_id = body.session_id?.trim();
        if (!session_id) return new Response(JSON.stringify({ error: "session_id required" }), { status: 400, headers });

        const rawLang = body.lang?.trim();
        let lang = rawLang ? rawLang.toLowerCase() : null;

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();

        if (!progress) {
          const default_lang = lang || "en";
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, ?, 1, 0)").bind(session_id, default_lang).run();
          progress = { language: default_lang, current_step: 1, consecutive_correct: 0 };
        } else {
          if (lang) {
            await env.server.prepare("UPDATE user_progress SET language = ? WHERE session_id = ?").bind(lang, session_id).run();
            progress.language = lang;
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

        const quizData = {};

        if (randomType === "IDENTITY_IMAGE") {
          const { results: usedRes } = await env.server.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").bind(session_id).all();
          const usedList = usedRes.map(r => r.person_name);

          let personName = "Albert Einstein";
          
          const personPrompt = `Task: Generate a JSON array of 5 universally famous historical figures.
Exclude: ${usedList.join(", ")}.
Output format: ["Name1", "Name2", "Name3", "Name4", "Name5"].
NO other text.`;

          const nameResp = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
            messages: [
              { role: "system", content: "You output valid JSON arrays only." },
              { role: "user", content: personPrompt }
            ]
          });

          let candidates = [];
          try {
             const raw = (nameResp.response || "").replace(/```json|```/g, "").trim();
             candidates = JSON.parse(raw);
          } catch(e) {}

          if (Array.isArray(candidates)) {
             for (const name of candidates) {
                if (!usedList.includes(name)) {
                   personName = name;
                   break;
                }
             }
          }

          const imagePrompt = `Professional portrait of ${personName}, realistic, 8k, studio lighting`;
          
          const extImgResponse = await fetch("https://server4.adamdh7.org/jerere", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ prompt: imagePrompt })
          });

          if (!extImgResponse.ok) {
             return new Response(JSON.stringify({ error: "External image generation failed" }), { status: 500, headers });
          }

          const imgJson = await extImgResponse.json();
          const imageUrl = imgJson.url;

          await env.server.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)")
            .bind(session_id, personName).run();

          const questionTexts = {
            en: "Who is this person?",
            fr: "Qui est cette personne ?",
            es: "¿Quién es esta persona?",
            ht: "Kiyès moun sa a ye?"
          };

          let questionText = questionTexts[language] || questionTexts.en;

          await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)")
            .bind(session_id, randomType, questionText, imageUrl, personName).run();

          quizData.type = randomType;
          quizData.question = questionText;
          quizData.image_url = imageUrl;

        } else {
          
          const systemPrompt = `Task: Generate Quiz Question.
Lang: ${langName}.
Diff: ${current_step_num} As the level increases, the questions should become more difficult, and previously asked questions should not be repeated.
Type: ${randomType}.
JSON Structure: { "question": "texte", "options": ["texte","other texte", "ect"] or null, "answer": "str", "explanation": "str" }.
Strict JSON only.`;

          const aiResponse = await env.AI.run("@cf/meta/llama-3.2-3b-instruct", {
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: "Generate." }
            ],
            max_tokens: 1000
          });

          let rawResponse = aiResponse.response || "";
          let parsed = null;
          try {
            rawResponse = rawResponse.replace(/```json/g, "").replace(/```/g, "").trim();
            parsed = JSON.parse(rawResponse);
          } catch (e) {
            parsed = {
              type: randomType,
              question: "Error generating question.",
              options: null,
              answer: "Error",
              explanation: ""
            };
          }

          const optionsStr = parsed.options ? JSON.stringify(parsed.options) : null;

          await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, ?, NULL, ?, ?)")
            .bind(session_id, randomType, parsed.question, optionsStr, parsed.answer, parsed.explanation || null).run();

          quizData.type = randomType;
          quizData.question = parsed.question;
          if (parsed.options) quizData.options = parsed.options;
        }

        return new Response(JSON.stringify(quizData), { headers });
      }

      if (url.pathname === "/validate") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");

        if (request.method !== "POST") return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers });

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers });
        }

        const session_id = body.session_id?.trim();
        const user_answer = body.user_answer?.trim() || "";
        if (!session_id || !user_answer) return new Response(JSON.stringify({ error: "session_id and user_answer required" }), { status: 400, headers });

        const current = await env.server.prepare("SELECT * FROM current_quiz WHERE session_id = ?").bind(session_id).first();
        if (!current) return new Response(JSON.stringify({ error: "No active quiz" }), { status: 400, headers });

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
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
Correct Answer: "${current.answer}" Validate the user's answer Mark as 'Correct' if the response is relevant to the question, otherwise 'Incorrect
User Answer: "${user_answer}"
Output JSON: {"correct": boolean, "explanation": "Short feedback in ${langName}"}`;

        const judgeResp = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
          messages: [
            { role: "system", content: "You output valid JSON only." },
            { role: "user", content: judgePrompt }
          ],
          max_tokens: 700
        });

        let judgeResult = { correct: false, explanation: "" };
        try {
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
          await env.server.prepare("DELETE FROM current_quiz WHERE session_id = ?").bind(session_id).run();
        } else {
          new_consec = 0;
        }

        await env.server.prepare("UPDATE user_progress SET consecutive_correct = ?, current_step = ? WHERE session_id = ?")
          .bind(new_consec, new_step, session_id).run();

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

        if (request.method !== "GET") return new Response(JSON.stringify({ error: "Method not allowed" }), { status: 405, headers });

        const session_id = url.searchParams.get("session_id");
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
      const errorHeaders = { ...corsHeaders, "Content-Type": "application/json" };
      return new Response(JSON.stringify({ 
        error: "Internal Server Error", 
        message: e.message
      }), { status: 500, headers: errorHeaders });
    }

    return new Response("Not Found", { status: 404, headers: corsHeaders });
  }
};
