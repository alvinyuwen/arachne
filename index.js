/**
 * Linq iMessage -> Browserbase web automation agent.
 *
 * Flow: Linq webhook -> ack over iMessage -> Stagehand drives a remote
 * Browserbase browser -> screenshot + summary sent back over iMessage.
 */
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import "dotenv/config";
import axios from "axios";
import express from "express";
import { z } from "zod";
import { Stagehand, browserbase } from "@browserbasehq/stagehand";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, "public");
const SCREENSHOT_NAME = "last_action.png";
const SCREENSHOT_PATH = path.join(PUBLIC_DIR, SCREENSHOT_NAME);

const {
  OPENAI_API_KEY,
  OPENAI_MODEL = "openai/gpt-4o-mini",
  LINQ_API_KEY,
  LINQ_PHONE_NUMBER,
  LINQ_API_URL = "https://api.linqapp.com/api/partner/v3/messages",
  BROWSERBASE_API_KEY,
  BROWSERBASE_PROJECT_ID,
  PUBLIC_BASE_URL,
  PORT = 3000,
  TASK_TIMEOUT_MS = 180000,
} = process.env;

const TASK_TIMEOUT = Number(TASK_TIMEOUT_MS);
const MAX_STEPS = 6;

/* ------------------------------------------------------------------ */
/* Linq outbound                                                       */
/* ------------------------------------------------------------------ */

/**
 * Send a message back over iMessage. `parts` follows Linq's v3 shape:
 * [{ type: "text", value }] and/or [{ type: "media", url }].
 */
async function sendLinq(to, parts) {
  if (!to) {
    console.warn("[linq] no recipient, skipping send:", JSON.stringify(parts));
    return null;
  }
  try {
    const { data } = await axios.post(
      LINQ_API_URL,
      { to: [to], message: { parts } },
      {
        headers: {
          Authorization: `Bearer ${LINQ_API_KEY}`,
          "Content-Type": "application/json",
        },
        timeout: 20000,
      },
    );
    console.log(`[linq] -> ${to}`, parts.map((p) => p.type).join("+"));
    return data;
  } catch (err) {
    const detail = err.response
      ? `${err.response.status} ${JSON.stringify(err.response.data)}`
      : err.message;
    console.error(`[linq] send failed: ${detail}`);
    return null;
  }
}

const sendText = (to, value) => sendLinq(to, [{ type: "text", value }]);

/* ------------------------------------------------------------------ */
/* Webhook parsing                                                     */
/* ------------------------------------------------------------------ */

/**
 * Linq v3 nests the payload under `data`; older/flat test payloads put the
 * fields at the top level. Accept both so curl testing stays trivial.
 */
function parseWebhook(body = {}) {
  const d = body.data ?? body;

  const senderNumber =
    d?.sender_handle?.handle ??
    d?.senderNumber ??
    d?.sender ??
    d?.from ??
    body.senderNumber ??
    body.from ??
    null;

  const messageText =
    (Array.isArray(d?.parts)
      ? d.parts
          .filter((p) => p.type === "text" && p.value)
          .map((p) => p.value)
          .join(" ")
          .trim()
      : "") ||
    d?.messageText ||
    d?.text ||
    d?.message ||
    body.messageText ||
    body.text ||
    "";

  return {
    senderNumber,
    messageText: String(messageText).trim(),
    eventType: body.event_type ?? "message.received",
    direction: d?.direction,
  };
}

/* ------------------------------------------------------------------ */
/* Public URL resolution (ngrok aware)                                 */
/* ------------------------------------------------------------------ */

let cachedPublicUrl = null;

/**
 * Linq must be able to fetch the screenshot, so a localhost path is useless.
 * Prefer PUBLIC_BASE_URL; otherwise ask the local ngrok agent for its https
 * tunnel. Cached because the tunnel URL is stable for the process lifetime.
 */
async function resolvePublicBaseUrl() {
  if (PUBLIC_BASE_URL) return PUBLIC_BASE_URL.replace(/\/+$/, "");
  if (cachedPublicUrl) return cachedPublicUrl;

  try {
    const { data } = await axios.get("http://127.0.0.1:4040/api/tunnels", {
      timeout: 2000,
    });
    const tunnel =
      data.tunnels?.find((t) => t.public_url?.startsWith("https://")) ??
      data.tunnels?.[0];
    if (tunnel?.public_url) {
      cachedPublicUrl = tunnel.public_url.replace(/\/+$/, "");
      console.log(`[ngrok] detected public URL ${cachedPublicUrl}`);
      return cachedPublicUrl;
    }
  } catch {
    /* ngrok not running - fall through */
  }
  return null;
}

/* ------------------------------------------------------------------ */
/* Planning (OpenAI)                                                   */
/* ------------------------------------------------------------------ */

const PLANNER_SYSTEM_PROMPT = [
  "You plan short web-browsing tasks for a browser automation agent.",
  'Reply with JSON only: {"startUrl": string, "steps": string[], "goal": string}.',
  "startUrl must be a full https URL to begin from (use a Google search URL when the task is a lookup).",
  `steps must contain at most ${MAX_STEPS} short imperative browser actions, for example:`,
  '"click the first search result" or "type laptops into the search box and press Enter".',
  "Use an empty steps array when simply loading startUrl already satisfies the request.",
  "goal restates what the user wants in one sentence.",
].join(" ");

/**
 * Turn a free-form iMessage into a starting URL plus a short list of
 * imperative browser steps that Stagehand's act() can execute one by one.
 */
async function planTask(messageText) {
  const { data } = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      // The chat API wants a bare model id; OPENAI_MODEL carries Stagehand's
      // "provider/model" form, so strip the provider prefix here.
      model: OPENAI_MODEL.replace(/^openai\//, ""),
      response_format: { type: "json_object" },
      temperature: 0,
      messages: [
        { role: "system", content: PLANNER_SYSTEM_PROMPT },
        { role: "user", content: messageText },
      ],
    },
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      timeout: 45000,
    },
  );

  const plan = JSON.parse(data.choices[0].message.content);
  return {
    startUrl: plan.startUrl || "https://www.google.com",
    steps: Array.isArray(plan.steps) ? plan.steps.slice(0, MAX_STEPS) : [],
    goal: plan.goal || messageText,
  };
}

/* ------------------------------------------------------------------ */
/* Browser automation (Stagehand + Browserbase)                        */
/* ------------------------------------------------------------------ */

const SummarySchema = z.object({
  summary: z
    .string()
    .describe("A concise 1-3 sentence answer or description of what is on screen"),
});

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out after ${ms}ms`)),
      ms,
    );
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Runs the whole browser task and returns { summary, url, screenshotSaved }.
 * Always tears the remote session down, including on failure.
 */
async function runBrowserTask(messageText) {
  if (!BROWSERBASE_PROJECT_ID) {
    throw new Error(
      "BROWSERBASE_PROJECT_ID is not set in .env - Browserbase cannot create a session without it.",
    );
  }

  const plan = await planTask(messageText);
  console.log(`[plan] ${plan.startUrl} :: ${plan.steps.length} step(s)`);

  let browser;
  let stagehand;
  try {
    browser = await browserbase.launch({
      apiKey: BROWSERBASE_API_KEY,
      projectId: BROWSERBASE_PROJECT_ID,
    });

    stagehand = await Stagehand.create({
      browser,
      model: { modelName: OPENAI_MODEL, apiKey: OPENAI_API_KEY },
    });

    const page = await browser.context.newPage(plan.startUrl);
    await page.waitForLoadState("load").catch(() => {});

    for (const [i, step] of plan.steps.entries()) {
      console.log(`[act ${i + 1}/${plan.steps.length}] ${step}`);
      try {
        await stagehand.act(step, { page, timeout: 60000 });
      } catch (err) {
        // A failed intermediate step is common (layout drift, cookie walls).
        // Keep going so the user still gets a screenshot and a summary.
        console.warn(`[act ${i + 1}] skipped: ${err.message}`);
      }
    }

    // Screenshot first: it reflects the final state even if extraction fails.
    let screenshotSaved = false;
    try {
      const buffer = await page.screenshot({ fullPage: false });
      await fs.writeFile(SCREENSHOT_PATH, Buffer.from(buffer));
      screenshotSaved = true;
      console.log(`[shot] saved ${SCREENSHOT_PATH}`);
    } catch (err) {
      console.warn(`[shot] failed: ${err.message}`);
    }

    let summary;
    try {
      const extracted = await stagehand.extract(
        `Answer this request using what is visible on the page: ${plan.goal}`,
        SummarySchema,
        { page, timeout: 60000 },
      );
      summary = extracted?.data?.summary;
    } catch (err) {
      console.warn(`[extract] failed: ${err.message}`);
    }

    const url = await page.url().catch(() => plan.startUrl);
    return {
      summary: summary || `Finished the task on ${url}.`,
      url,
      screenshotSaved,
    };
  } finally {
    await stagehand?.close().catch(() => {});
    await browser?.close().catch(() => {});
    console.log("[browser] session closed");
  }
}

/* ------------------------------------------------------------------ */
/* Request handling                                                    */
/* ------------------------------------------------------------------ */

async function handleRequest(senderNumber, messageText) {
  // Fire-and-forget ack so the user sees something within a second.
  sendText(senderNumber, "On it! Launching browser automation...");

  try {
    const { summary, url, screenshotSaved } = await withTimeout(
      runBrowserTask(messageText),
      TASK_TIMEOUT,
      "Browser task",
    );

    const parts = [{ type: "text", value: `${summary}\n\nSource: ${url}` }];

    if (screenshotSaved) {
      const base = await resolvePublicBaseUrl();
      if (base) {
        // Cache-buster: the filename is fixed, so without it iMessage would
        // keep showing the previous run's screenshot.
        parts.push({
          type: "media",
          url: `${base}/${SCREENSHOT_NAME}?v=${Date.now()}`,
        });
      } else {
        parts[0].value +=
          "\n\n(Screenshot saved locally, but no public URL is configured - " +
          "start ngrok or set PUBLIC_BASE_URL.)";
      }
    }

    await sendLinq(senderNumber, parts);
  } catch (err) {
    console.error("[task] failed:", err);
    await sendText(
      senderNumber,
      `Sorry - the browser task failed.\n\nReason: ${err.message}\n\nTry rephrasing it, or send a simpler one-step request.`,
    );
  }
}

/* ------------------------------------------------------------------ */
/* Server                                                              */
/* ------------------------------------------------------------------ */

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(PUBLIC_DIR, { maxAge: 0, etag: false }));

app.get("/", (_req, res) =>
  res.json({ ok: true, service: "linq-browser-agent", webhook: "/webhook/linq" }),
);

app.get("/health", async (_req, res) =>
  res.json({
    ok: true,
    agentNumber: LINQ_PHONE_NUMBER ?? null,
    publicBaseUrl: (await resolvePublicBaseUrl()) ?? null,
    browserbaseProjectId: Boolean(BROWSERBASE_PROJECT_ID),
  }),
);

app.post("/webhook/linq", (req, res) => {
  const { senderNumber, messageText, direction } = parseWebhook(req.body);
  console.log(`[webhook] from=${senderNumber} text=${JSON.stringify(messageText)}`);

  // Ack the webhook immediately; Linq retries on slow responses and the
  // browser task takes far longer than any sane webhook timeout.
  res.status(200).json({ received: true });

  if (direction === "outbound") return; // our own echoed message
  if (!senderNumber || !messageText) {
    console.warn("[webhook] ignored: missing sender or text");
    return;
  }

  handleRequest(senderNumber, messageText).catch((err) =>
    console.error("[webhook] unhandled:", err),
  );
});

await fs.mkdir(PUBLIC_DIR, { recursive: true });

app.listen(PORT, () => {
  console.log(`linq-browser-agent listening on http://localhost:${PORT}`);
  console.log("  webhook   POST /webhook/linq");
  console.log(`  artifacts GET  /${SCREENSHOT_NAME}`);
  for (const [name, value] of Object.entries({
    OPENAI_API_KEY,
    LINQ_API_KEY,
    LINQ_PHONE_NUMBER,
    BROWSERBASE_API_KEY,
    BROWSERBASE_PROJECT_ID,
  })) {
    if (!value) console.warn(`  WARNING: ${name} is not set in .env`);
  }
});
