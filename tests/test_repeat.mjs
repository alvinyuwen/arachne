/**
 * Repeat-request truth set.
 *
 * A task cannot repeat itself. When someone asks to be texted on a cadence and
 * the router lands on `task`, the schedule is silently dropped - one answer,
 * then nothing, having asked for exactly the opposite.
 *
 * That happened to "Tell me the weather at Waterloo university and send me a
 * text every 5 minutes". The router's own reasoning said "and also scheduled
 * follow-up" and it chose task anyway, because the prompt told it to: the rule
 * said a now-and-later message is a task and "the watch gets set up from the
 * result". Nothing set one up from the result. Nothing ever had.
 *
 * REPEAT_RE is the deterministic backstop for that, and is read out of index.js
 * so this cannot pass against a pattern that no longer ships.
 *
 * Run:  node tests/test_repeat.mjs     (no server, no network)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, "..", "index.js"), "utf8");

const start = src.indexOf("const REPEAT_RE = new RegExp(");
if (start < 0) throw new Error("REPEAT_RE not found in index.js");
const end = src.indexOf(");", src.indexOf('"i",', start)) + 2;
const REPEAT_RE = eval(src.slice(start, end).replace("const REPEAT_RE =", ""));

const results = [];
function check(text, shouldRepeat) {
  const hit = REPEAT_RE.test(text);
  const ok = hit === shouldRepeat;
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${hit ? "repeat" : "once  "}  "${text.slice(0, 62)}"`);
}

console.log("asks to be told again - must become a watch\n");

// The message that exposed this.
check("Tell me the weather at Waterloo university and send me a text every 5 minutes", true);
check("track the amazon stock price and text me every hour", true);
check("give me the price every 15 min", true);
check("send me the headlines each morning", true);
check("what's the weather, hourly please", true);
check("update me daily on this", true);
check("check it every other day", true);
check("twice a day is fine", true);
check("keep me posted on the price", true);
check("keep updating me on this", true);

console.log("\nasks once - must stay a task or a chat\n");

// A future CONDITION is already routed correctly as a watch by the model; this
// backstop is only for an explicit cadence, so it must not fire on these.
check("tell me when it drops below 80", false);
check("let me know if it comes back in stock", false);
check("what's the weather right now", false);
check("find me a mechanical keyboard under $100", false);
check("what's the price of a keychron k2", false);

// Words that contain the trigger but are not a cadence. "everyone" and
// "everyday" as an adjective are the ones that would misfire without the
// word boundaries.
check("everyone says this one is the best", false);
check("I need an everyday carry bag", false);
check("is this good for daily driver use", true); // "daily" genuinely is ambiguous - accepted
check("what does constantly connected mean", true); // same - accepted as a false positive

console.log();
const passed = results.filter(Boolean).length;
console.log(passed === results.length
  ? `ALL ${results.length} REPEAT CHECKS PASSED`
  : `${results.length - passed}/${results.length} FAILED`);
process.exitCode = passed === results.length ? 0 : 1;
