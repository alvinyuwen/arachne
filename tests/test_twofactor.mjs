/**
 * Second-factor detection truth set.
 *
 * This exists because of a specific failure. A user submitted their Instagram
 * password through the unlock form, was told "You're in", and then received an
 * email from Instagram with a verification code. The success test was:
 *
 *   signedIn = !stillHasPassword && !block.blocked
 *
 * which infers success from the absence of a password field. On a 2FA page
 * there is no password field, so it read as signed in. The agent then marked
 * the host as authenticated, resumed the task, hit the same login wall, and -
 * because the host was now "signed in" - never offered to help again. When the
 * user texted the code, the router refused it as a credential.
 *
 * Detection is now positive: a code field or code language, checked first and
 * reported as its own outcome rather than as failure, because the password was
 * in fact accepted.
 *
 * TWOFA_RE is read out of index.js so this cannot pass against a pattern that
 * no longer ships. The DOM half (hasCodeField) needs a browser and is covered
 * live by tests/test_unlock.mjs.
 *
 * Run:  node tests/test_twofactor.mjs     (no server needed)
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(path.join(here, "..", "index.js"), "utf8");

const start = src.indexOf("const TWOFA_RE =");
if (start < 0) throw new Error("TWOFA_RE not found in index.js");
const end = src.indexOf(";", src.indexOf("/i", start)) + 1;
const TWOFA_RE = eval(src.slice(start, end).replace("const TWOFA_RE =", ""));

const cases = [
  // Real second-factor prompts. Every one of these must be caught, or the user
  // is told they are signed in while the site waits on a code.
  ["Enter the code we sent to your email", true],
  ["We sent a verification code to j***@gmail.com", true],
  ["Enter the 6-digit login code", true],
  ["Two-factor authentication", true],
  ["Enter the security code from your authenticator app", true],
  ["2FA is required to continue", true],
  ["Check your email for a confirmation code", true],
  ["Confirm it's you", true],
  ["Approve this login from your other device", true],
  ["Enter the authentication code", true],
  ["Check your phone for the code", true],

  // Ordinary signed-in pages. A false positive here strands someone at a code
  // form for a site that never asked for one.
  ["Your inbox is empty", false],
  ["Direct messages", false],
  ["Home Search Explore Reels Messages Notifications Create", false],
  ["Welcome back! Here's what you missed", false],
  ["Settings and privacy", false],
  ["Showing 24 results for power bank", false],
  ["Promo code applied at checkout", false],
  ["Source code available on GitHub", false],
];

let wrong = 0;
for (const [text, shouldMatch] of cases) {
  TWOFA_RE.lastIndex = 0;
  const hit = TWOFA_RE.test(text);
  const ok = hit === shouldMatch;
  if (!ok) wrong += 1;
  console.log(`${ok ? "PASS" : "FAIL"}  ${hit ? "code step" : "signed in"}  "${text.slice(0, 56)}"`);
}

console.log();
console.log(wrong === 0 ? `ALL ${cases.length} SECOND-FACTOR CHECKS PASSED` : `${wrong} FAILED`);
process.exitCode = wrong === 0 ? 0 : 1;
