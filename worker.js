export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin");
    const allowedOrigin = "https://tout.adamdh7.org";

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

          const { results } = await env.server.prepare("SELECT role, content FROM messages WHERE session_id = ? ORDER BY id DESC LIMIT 6")
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
4. RIGOR: Use LaTeX for all mathematical formulas and scientific notation.5. ERROR HANDLING: If the input is syntactically incorrect or physically impossible, explain the error clearly instead of guessing.`;

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

        if (request.method !== "POST") return new Response("Method not allowed", { status: 405, headers });

        let body;
        try {
          body = await request.json();
        } catch (e) {
          return new Response(JSON.stringify({ error: "Invalid JSON" }), { status: 400, headers });
        }

        const lang = body.lang || "en";
        const step = body.step || "beginner";
        
        const questionTypes = ["MCQ", "TRUE_FALSE", "FILL_BLANK", "IDENTITY_IMAGE"];
        const randomType = questionTypes[Math.floor(Math.random() * questionTypes.length)];

        if (randomType === "IDENTITY_IMAGE") {
          const personPrompt = `Generate just the name of a very famous historical figure, celebrity, or scientist that is universally recognizable. Output ONLY the name, nothing else.`;
          
          const nameResponse = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [{ role: "system", content: "You are a database helper." }, { role: "user", content: personPrompt }]
          });
          
          const personName = nameResponse.response ? nameResponse.response.trim() : "Albert Einstein";

          const imageInputs = {
            prompt: `A highly detailed, realistic portrait of ${personName}, cinematic lighting, 8k resolution`,
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

          return new Response(JSON.stringify({
            type: "IDENTITY_IMAGE",
            question: lang === "fr" ? "Qui est cette personne ?" : "Who is this person?",
            image_url: imageUrl,
            answer: personName,
            step: step
          }), { headers });

        } else {
          const systemPrompt = `You are a dynamic Quiz Engine API. Your task is to generate a unique, challenging question based on the user's level (step) and requested language.

CONTEXT:
- User Level (Step): ${step} (Adjust difficulty accordingly. Higher steps = harder questions).
- Language: ${lang} (Output content strictly in this language).
- Question Type: ${randomType}

INSTRUCTIONS PER TYPE:
1. If 'MCQ': Provide a question and 4 distinct options (A, B, C, D).
2. If 'TRUE_FALSE': Provide a statement and ask if it is True or False.
3. If 'FILL_BLANK': Provide a sentence with a missing word (represented by '_____').

OUTPUT FORMAT (STRICT JSON ONLY):
You must reply with a valid JSON object containing:
- "type": "${randomType}"
- "question": "The question text here"
- "options": ["Option A", "Option B", "Option C", "Option D"] (Only for MCQ, otherwise null)
- "answer": "The correct answer string" (Used for validation)
- "explanation": "Brief explanation of why this is the answer"

Do not add any markdown, intro, or outro text. Just the raw JSON.`;

          const aiResponse = await env.AI.run("@cf/meta/llama-3.1-8b-instruct", {
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: "Generate the question now." }
            ],
            max_tokens: 1000
          });

          const rawResponse = aiResponse.response || aiResponse.text;
          let jsonResponse;
          
          try {
            const cleanJson = rawResponse.replace(/```json/g, "").replace(/```/g, "").trim();
            jsonResponse = JSON.parse(cleanJson);
          } catch (e) {
            jsonResponse = { 
              type: "ERROR", 
              question: rawResponse, 
              answer: "Error parsing",
              step: step 
            };
          }

          return new Response(JSON.stringify(jsonResponse), { headers });
        }
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
