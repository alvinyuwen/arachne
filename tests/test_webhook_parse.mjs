/**
 * Inbound webhook parsing truth set.
 *
 * This exists because of a silent failure. Someone texted an image and got
 * nothing back at all - no reply, no error, no log beyond one line. The parser
 * filtered `parts` down to text, so the media part vanished, and the webhook
 * then dropped the whole message for having no text. Two guards in a row, each
 * reasonable on its own, adding up to a message that was never processed and
 * never mentioned.
 *
 * parseWebhook is read out of index.js rather than copied, so this cannot pass
 * against a version that no longer ships.
 *
 * Run:  node tests/test_webhook_parse.mjs     (no server, no network)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, "..", "index.js"), "utf8");

const start = src.indexOf("function parseWebhook(body = {}) {");
if (start < 0) throw new Error("parseWebhook not found in index.js");
// Balance braces to find the end of the function. Start from the brace that
// opens the BODY, not the `{}` in the default parameter `(body = {})`.
const bodyOpen = src.indexOf("{", src.indexOf(")", src.indexOf("(", start)) );
let depth = 0;
let end = start;
for (let i = bodyOpen; i < src.length; i += 1) {
  if (src[i] === "{") depth += 1;
  else if (src[i] === "}") {
    depth -= 1;
    if (depth === 0) { end = i + 1; break; }
  }
}
const parseWebhook = eval(`(${src.slice(start, end).replace("function parseWebhook", "function")})`);

const results = [];
function check(name, ok, detail = "") {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : `   ${detail}`}`);
}

const from = (parts) => ({ data: { sender_handle: { handle: "+15550001111" }, parts } });
const CDN = "https://cdn.linqapp.com/attachments/abc123.jpg";

/* ---------------------------------------------------------------- */
console.log("text messages must parse exactly as before\n");

{
  const r = parseWebhook(from([{ type: "text", value: "find me a keyboard" }]));
  check("sender is read", r.senderNumber === "+15550001111", r.senderNumber);
  check("text is read", r.messageText === "find me a keyboard", JSON.stringify(r.messageText));
  check("no media means an empty list, not undefined", Array.isArray(r.media) && r.media.length === 0,
    JSON.stringify(r.media));
}
{
  const r = parseWebhook(from([
    { type: "text", value: "two" },
    { type: "text", value: "parts" },
  ]));
  check("multiple text parts still join", r.messageText === "two parts", JSON.stringify(r.messageText));
}
{
  // Fallback shapes the parser has always accepted.
  check("flat body shape still works",
    parseWebhook({ from: "+1555", text: "hello" }).messageText === "hello");
}

/* ---------------------------------------------------------------- */
console.log("\nmedia - the part that was being thrown away\n");

{
  const r = parseWebhook(from([{ type: "media", url: CDN, content_type: "image/jpeg" }]));
  check("a media-only message keeps the url", r.media[0]?.url === CDN, JSON.stringify(r.media));
  check("content type is kept", r.media[0]?.contentType === "image/jpeg", JSON.stringify(r.media[0]));

  // The exact shape that produced silence: no text, but a real request.
  check("media-only yields empty text, which the guard must now allow",
    r.messageText === "" && r.media.length === 1, JSON.stringify(r));
}
{
  const r = parseWebhook(from([
    { type: "text", value: "what is this" },
    { type: "media", url: CDN },
  ]));
  check("a captioned image keeps both", r.messageText === "what is this" && r.media[0]?.url === CDN,
    JSON.stringify(r));
}
{
  const r = parseWebhook(from([
    { type: "media", url: CDN },
    { type: "media", url: "https://cdn.linqapp.com/attachments/def456.png" },
  ]));
  check("several attachments are all kept", r.media.length === 2, JSON.stringify(r.media));
}

/* ---------------------------------------------------------------- */
console.log("\nrubbish in must not become a request\n");

{
  // A media part whose url is not https is not something to hand to a fetcher.
  const r = parseWebhook(from([{ type: "media", url: "file:///etc/passwd" }]));
  check("a non-https media url is dropped", r.media.length === 0, JSON.stringify(r.media));
  check("and that leaves nothing to act on", r.messageText === "" && r.media.length === 0);
}
{
  const r = parseWebhook(from([{ type: "media" }]));
  check("a media part with no url is dropped", r.media.length === 0, JSON.stringify(r.media));
}
{
  const r = parseWebhook({ data: { sender_handle: { handle: "+1555" }, parts: "not an array" } });
  check("a malformed parts field does not throw", r.media.length === 0 && r.messageText === "");
}
{
  const r = parseWebhook({});
  check("an empty body does not throw", r.senderNumber === null && r.media.length === 0);
}
{
  const many = Array.from({ length: 20 }, (_, i) => ({ type: "media", url: `${CDN}?i=${i}` }));
  check("attachments are capped", parseWebhook(from(many)).media.length <= 4,
    String(parseWebhook(from(many)).media.length));
}

console.log();
const passed = results.filter(Boolean).length;
console.log(passed === results.length
  ? `ALL ${results.length} WEBHOOK PARSE CHECKS PASSED`
  : `${results.length - passed}/${results.length} FAILED`);
process.exitCode = passed === results.length ? 0 : 1;
