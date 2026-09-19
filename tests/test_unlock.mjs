/**
 * One-time sign-in form truth set.
 *
 * The form is the only login path that works from a phone: the live view is a
 * single <canvas>, so tapping a field leaves document.activeElement as the
 * canvas and no mobile keyboard opens. This trades that away for the credential
 * passing through this process, so the assertions here are as much about what
 * the server does NOT do with it as about the form working.
 *
 * Driven against a public test login whose credentials are published, so
 * nothing real is ever typed: https://the-internet.herokuapp.com/login
 *
 * Costs two Browserbase sessions and takes about a minute. Not in the fast
 * suite.
 *
 * Run:  node tests/test_unlock.mjs     (server must be running)
 */
const BASE = "http://localhost:3000";
const TOKEN = process.env.DEBUG_TOKEN || "local-dev-debug";

const TEST_LOGIN = "https://the-internet.herokuapp.com/login";
const GOOD = { username: "tomsmith", password: "SuperSecretPassword!" };
const BAD = { username: "not_a_real_user_9f3x", password: "NOT-A-REAL-PASSWORD-zzz9" };

const results = [];
function check(name, ok, detail = "") {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : `  ${detail}`}`);
}

async function mint(url, request = "") {
  const r = await fetch(`${BASE}/debug/unlock`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-debug-token": TOKEN },
    body: JSON.stringify({ from: "+15550007000", url, request }),
  });
  if (!r.ok) throw new Error(`mint HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
  return r.json();
}

const post = (token, body) =>
  fetch(`${BASE}/unlock/${token}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((r) => r.json());

console.log("  ...two real sessions, about a minute\n");

// --- The page itself -------------------------------------------------------
{
  const { token, parked, hasForm } = await mint(TEST_LOGIN);
  check("the session parks on the login page", parked === true);
  check("a sign-in form is detected there", hasForm === true);

  const r = await fetch(`${BASE}/unlock/${token}`);
  const html = await r.text();

  check("the form renders", r.status === 200 && /<form/i.test(html), `status ${r.status}`);

  // Without these a password manager will not offer to fill, which is most of
  // the reason this beats the live view on a phone.
  check("fields are autofill-annotated",
    /autocomplete="username"/.test(html) && /autocomplete="current-password"/.test(html));

  // A page collecting a password should not be talking to anyone else while it
  // does it. No fonts, no CDN, no analytics.
  const external = html.match(/https?:\/\/[^"' )]+/g) ?? [];
  check("the page makes no external requests", external.length === 0, external.join(" "));

  check("it is not cached or referrer-leaked",
    r.headers.get("cache-control") === "no-store" &&
      r.headers.get("referrer-policy") === "no-referrer",
    `${r.headers.get("cache-control")} / ${r.headers.get("referrer-policy")}`);

  // GET must not spend the token, or reloading the page would break it.
  const again = await fetch(`${BASE}/unlock/${token}`);
  check("GET does not spend the token", again.status === 200, `status ${again.status}`);

  const bogus = await fetch(`${BASE}/unlock/${"a".repeat(43)}`);
  check("an unknown token 404s", bogus.status === 404, `status ${bogus.status}`);

  // --- Wrong credentials ---------------------------------------------------
  const fail = await post(token, BAD);
  check("wrong credentials are reported, not swallowed", fail.ok === false, JSON.stringify(fail));
  check("a retry link is offered", typeof fail.retryPath === "string" && fail.retryPath.length > 20,
    String(fail.retryPath));

  // Single use is what makes an internet-facing form acceptable. The token is
  // spent before the fill runs, so even a failed attempt burns it.
  const replay = await post(token, BAD);
  check("the token is single-use even on failure", replay.ok === false &&
    /expired/i.test(replay.reason ?? ""), JSON.stringify(replay));
}

// --- Correct credentials ---------------------------------------------------
//
// This is the assertion that distinguishes "the fill works" from "the fill
// silently did nothing and the page never changed". Without it, a broken
// Input.insertText would look identical to a rejected password.
{
  const { token } = await mint(TEST_LOGIN);
  const ok = await post(token, GOOD);
  check("correct credentials actually sign in", ok.ok === true, JSON.stringify(ok));
}

// --- The credential must not turn up anywhere ------------------------------
{
  const mem = await fetch(`${BASE}/debug/memory?from=${encodeURIComponent("+15550007000")}`, {
    headers: { "x-debug-token": TOKEN },
  }).then((r) => r.json());
  check("the credential is not in conversation memory",
    !JSON.stringify(mem).includes(GOOD.password) && !JSON.stringify(mem).includes(BAD.password),
    JSON.stringify(mem).slice(0, 160));
}

console.log();
console.log("  stdout check: grep the server log for the literal strings");
console.log(`    "${GOOD.password}" and "${BAD.password}"`);
console.log("  Neither should appear. The route logs the outcome, never the values.");

console.log();
const passed = results.filter(Boolean).length;
console.log(passed === results.length
  ? `ALL ${results.length} UNLOCK CHECKS PASSED`
  : `${results.length - passed}/${results.length} FAILED`);
process.exitCode = passed === results.length ? 0 : 1;
