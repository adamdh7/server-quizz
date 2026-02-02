export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const allowedOrigin = "https://teste777.pages.dev";

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

      if (url.pathname === "/ai") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");

        if (request.method === "GET") {
          const sess = url.searchParams.get("session_id") || "global";
          try {
            const { results } = await env.server.prepare(
              "SELECT role, content, timestamp FROM messages WHERE session_id = ? ORDER BY id ASC"
            ).bind(sess).all();
            return new Response(JSON.stringify({ messages: results || [] }), { headers });
          } catch (err) {
            return new Response(JSON.stringify({ error: "DB Error", details: err.message }), { status: 500, headers });
          }
        }

        if (request.method === "POST") {
          const body = await request.json();
          const userMessage = body.message?.trim();
          const sess = body.session_id || "global";

          if (!userMessage) return new Response(JSON.stringify({ error: "Empty message" }), { status: 400, headers });

          await env.server.prepare("INSERT INTO messages (role, content, session_id, timestamp) VALUES ('user', ?, ?, ?)")
            .bind(userMessage, sess, new Date().toISOString()).run();

          const { results } = await env.server.prepare("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 30")
            .bind(sess).all();
          
          const context = results ? results.reverse() : [];

          const systemPrompt = "You are Adam_D'H7. Don't mention internal details";
          
          const aiResponse = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [
              { role: "system", content: systemPrompt },
              ...context
            ],
            max_tokens : 2700
          });

          let assistantMessage = "Sorry, I could not generate a response.";
          if (aiResponse && aiResponse.response) {
            assistantMessage = aiResponse.response;
          } else if (aiResponse && aiResponse.text) {
            assistantMessage = aiResponse.text;
          }

          await env.server.prepare("INSERT INTO messages (role, content, session_id, timestamp) VALUES ('assistant', ?, ?, ?)")
            .bind(assistantMessage, sess, new Date().toISOString()).run();

          return new Response(JSON.stringify({ message: assistantMessage }), { headers });
        }
      }

      if (url.pathname === "/jerere") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");

        if (request.method !== "POST") return new Response("Method not allowed", { status: 405, headers });

        const body = await request.json();
        const prompt = body.prompt?.trim();
        
        if (!prompt) return new Response(JSON.stringify({ error: "No prompt provided" }), { status: 400, headers });

        const inputs = {
          prompt: prompt,
          num_steps: 4,
        };

        const aiResponse = await env.AI.run("@cf/black-forest-labs/flux-1-schnell", inputs);

        if (!aiResponse || !aiResponse.image) {
          throw new Error("L'IA n'a pas renvoyé d'image valide.");
        }

        const binaryString = atob(aiResponse.image);
        const len = binaryString.length;
        const bytes = new Uint8Array(len);
        for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
        }

        const key = `img_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
        await env.server2.put(key, bytes.buffer, {
          httpMetadata: { contentType: "image/png" }
        });

        const fullUrl = `https://${url.host}/r2/${key}`;
        return new Response(JSON.stringify({ url: fullUrl }), { headers });
      }

      if (url.pathname === "/calcul") {
        const headers = new Headers(corsHeaders);
        headers.set("Content-Type", "application/json");

        if (request.method !== "POST") {
          return new Response(JSON.stringify({ error: "Méthode non autorisée" }), { status: 405, headers });
        }

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return new Response(JSON.stringify({ error: "JSON invalide" }), { status: 400, headers });
        }

        const calculation = body.calculation?.trim();
        if (!calculation) {
          return new Response(JSON.stringify({ error: "Aucune expression fournie" }), { status: 400, headers });
        }

        try {
          const systemPrompt = `You are an expert polymath specializing in Mathematics, Physics, and all scientific calculations. 
CRITICAL RULES:
1. LANGUAGE: Always respond in the exact same language used by the user. If they ask in French, reply in French; if in Spanish, reply in Spanish.
2. CONTEXT: Thoroughly analyze and incorporate any specific user notes, variables, or constraints provided to tailor the calculation.
3. STEP-BY-STEP LOGIC: Do not just give the answer. Deconstruct the solution into a clear, numbered "logical path." Explain the reasoning and formulas for every step.
4. RIGOR: Use LaTeX for all mathematical formulas and scientific notation.
5. ERROR HANDLING: If the input is syntactically incorrect or physically impossible, explain the error clearly instead of guessing.`;

          const userPrompt = `Analyze and solve the following scientific expression or problem: 

"${calculation}"

Instructions for this task:
- Apply any additional user notes provided in the input.
- Show the full derivation and intermediate steps.
- Conclude with the final answer clearly visible at the bottom as: **Final result: [Result]**

IMPORTANT : You just have 1500 tokens`;

          const aiResponse = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: userPrompt }
            ], 
            max_tokens : 2700
          });

          const analysis = aiResponse.response?.trim() || "Impossible d'analyser l'expression pour le moment.";

          return new Response(JSON.stringify({ result: analysis }), { headers });
        } catch (e) {
          return new Response(JSON.stringify({ error: "Erreur interne lors de l'analyse mathématique" }), { status: 500, headers });
        }
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

        let lang = body.lang?.trim().toLowerCase() || null;

        let progress = await env.server.prepare("SELECT * FROM user_progress WHERE session_id = ?").bind(session_id).first();
        if (!progress) {
          const default_lang = lang || "en";
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, ?, 1, 0)").bind(session_id, default_lang).run();
          progress = { language: default_lang, current_step: 1, consecutive_correct: 0 };
        } else if (lang && lang !== progress.language) {
          await env.server.prepare("UPDATE user_progress SET language = ? WHERE session_id = ?").bind(lang, session_id).run();
          progress.language = lang;
        }

        const current_step_num = progress.current_step;
        const language = progress.language;

        const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
        const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];

        const quizData = {};

        if (randomType === "IDENTITY_IMAGE") {
          const { results: usedRes } = await env.server.prepare("SELECT person_name FROM used_persons WHERE session_id = ?").bind(session_id).all();
          const usedList = usedRes.map(r => r.person_name).join(", ");

          let personName = "";
          let attempts = 0;
          while (attempts < 15) {
            attempts++;
            const avoid = usedList ? ` Do not use any of these names: ${usedList}.` : "";
            const personPrompt = `Output ONLY the full name of one universally famous person (historical figure, scientist, celebrity, leader, artist, etc.).${avoid} No additional text, no quotes, no explanation.`;

            const nameResp = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
              messages: [
                { role: "system", content: "You output exactly and only the requested name." },
                { role: "user", content: personPrompt }
              ]
            });

            const candidate = (nameResp.response || "").trim();
            if (candidate && (!usedList || !usedList.includes(candidate))) {
              personName = candidate;
              break;
            }
          }

          if (!personName) personName = "Marie Curie";

          const imageInputs = {
            prompt: `Highly detailed realistic portrait of ${personName}, professional studio photography, cinematic lighting, sharp focus, 8k resolution`,
            num_steps: 4,
          };

          const imageResponse = await env.AI.run("@cf/black-forest-labs/flux-1-schnell", imageInputs);

          if (!imageResponse || !imageResponse.image) {
            return new Response(JSON.stringify({ error: "Image generation failed" }), { status: 500, headers });
          }

          const binaryString = atob(imageResponse.image);
          const len = binaryString.length;
          const bytes = new Uint8Array(len);
          for (let i = 0; i < len; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }

          const key = `quiz_${Date.now()}_${crypto.randomUUID().split('-')[0]}.png`;
          await env.server2.put(key, bytes.buffer, { httpMetadata: { contentType: "image/png" } });

          const imageUrl = `https://${url.host}/r2/${key}`;

          await env.server.prepare("INSERT INTO used_persons (session_id, person_name) VALUES (?, ?)")
            .bind(session_id, personName).run();

          let questionText = "Who is this person?";
          if (language === "fr") questionText = "Qui est cette personne ?";
          else if (language === "es") questionText = "¿Quién es esta persona?";

          await env.server.prepare("REPLACE INTO current_quiz (session_id, q_type, question, options, image_url, answer, explanation) VALUES (?, ?, ?, NULL, ?, ?, NULL)")
            .bind(session_id, randomType, questionText, imageUrl, personName).run();

          quizData.type = randomType;
          quizData.question = questionText;
          quizData.image_url = imageUrl;

        } else {
          const langName = language === "fr" ? "French" : language === "es" ? "Spanish" : "English";

          const systemPrompt = `You are a precise quiz generator. Generate everything exclusively in ${langName}.
Difficulty level: ${current_step_num} (1 = very easy, higher = advanced topics and harder).

Question type: ${randomType}

STRICT OUTPUT FORMAT - ONLY valid JSON, no markdown, no extra text:

{
  "type": "${randomType}",
  "question": "Full question text in ${langName}. For MCQ include labeled options A) B) C) D) in the question string.",
  "options": ["A) option1", "B) option2", "C) option3", "D) option4"] or null if not MCQ,
  "answer": "Exact correct answer (for MCQ the full 'A) correct text', for TRUE_FALSE 'True' or 'False', for FILL_BLANK the missing word/phrase)",
  "explanation": "Short explanation in ${langName}"
}`;

          const aiResponse = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: "Generate the question now." }
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
              question: "Temporary error generating question.",
              options: null,
              answer: "",
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
          await env.server.prepare("INSERT INTO user_progress (session_id, language, current_step, consecutive_correct) VALUES (?, 'en', 1, 0)").bind(session_id).run();
          progress = { language: "en", current_step: 1, consecutive_correct: 0 };
        }

        const lang = progress.language;
        const langName = lang === "fr" ? "français" : lang === "es" ? "espagnol" : "anglais";

        let optionsText = "";
        if (current.options) {
          try {
            const opts = JSON.parse(current.options);
            optionsText = "Options:\n" + opts.map((o, i) => `${String.fromCharCode(65 + i)}) ${o}`).join("\n");
          } catch {}
        }

        const imageNote = current.image_url ? " (Une image est associée à la question.)" : "";

        const judgePrompt = `Question: ${current.question}
${optionsText}
${imageNote}

Réponse correcte: ${current.answer}

Réponse de l'utilisateur: "${user_answer}"

Juge si la réponse de l'utilisateur est essentiellement correcte (tolère les petites fautes d'orthographe, abréviations, synonymes équivalents).

Réponds UNIQUEMENT avec du JSON valide :

{"correct": true ou false, "explanation": "Explication courte en ${langName}. Si correct félicite, si incorrect donne la bonne réponse."}`;

        const judgeResp = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
          messages: [
            { role: "system", content: "Tu réponds exclusivement avec l'objet JSON demandé, rien d'autre." },
            { role: "user", content: judgePrompt }
          ],
          max_tokens: 600
        });

        let judgeResult = { correct: false, explanation: "" };
        try {
          let text = (judgeResp.response || "").replace(/```json|```/g, "").trim();
          judgeResult = JSON.parse(text);
        } catch (e) {}

        const isCorrect = !!judgeResult.correct;
        const explanation = judgeResult.explanation || (isCorrect 
          ? (lang === "fr" ? "Correct !" : lang === "es" ? "¡Correcto!" : "Correct!") 
          : (lang === "fr" ? `Incorrect. La réponse est : ${current.answer}` : lang === "es" ? `Incorrecto. La respuesta es: ${current.answer}` : `Incorrect. The answer is: ${current.answer}`));

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
