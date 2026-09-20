/**
 * Watch store truth set.
 *
 * The assertion that justifies this file existing at all is "survives a
 * restart". Everything else in the agent is an in-process Map that dies on
 * deploy; a watch that did the same would make the product a lie, and this
 * process was restarted six times during one afternoon of development.
 *
 * Uses a temp database, never the real one.
 *
 * Run:  node tests/test_watch_store.mjs     (no server, no network)
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { openStore } from "../store.js";
import { DAY, HOUR } from "../watch.js";

const results = [];
function check(name, ok, detail = "") {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : `   ${detail}`}`);
}

const dbFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "watchdb-")), "watches.db");
const SENDER = "+15550001234";

const sample = (over = {}) => ({
  sender: SENDER,
  label: "Keychron K2 under $80",
  kind: "numeric",
  metric: "price",
  source: { mode: "pinned", urls: ["https://example.com/keychron"], query: null },
  condition: { op: "lt", value: 80, unit: "CAD" },
  schedule: { everyMs: 6 * HOUR, activeWindow: null, quietHours: { from: 22, to: 7 } },
  lifecycle: { fireMode: "once", renewal: "none", firesCount: 0, maxFires: null, expiresAt: null },
  nextCheckAt: Date.now(),
  ...over,
});

let store = openStore(dbFile);

/* ---------------------------------------------------------------- */
console.log("create and read back\n");

const made = store.create(sample());
check("create returns a row with an id", typeof made?.id === "string" && made.id.length > 0);
check("label round-trips", made.label === "Keychron K2 under $80");
check("status defaults to active", made.status === "active");

// The compound shapes are JSON columns; they must come back as objects, not
// strings, or every caller would have to remember to parse them.
check("source round-trips as an object",
  made.source?.mode === "pinned" && made.source.urls[0] === "https://example.com/keychron",
  JSON.stringify(made.source));
check("condition round-trips as an object", made.condition?.op === "lt" && made.condition.value === 80,
  JSON.stringify(made.condition));
check("schedule round-trips nested objects", made.schedule?.quietHours?.from === 22,
  JSON.stringify(made.schedule));
check("columns are camelCase at the boundary",
  made.nextCheckAt != null && made.next_check_at === undefined,
  Object.keys(made).join(","));

/* ---------------------------------------------------------------- */
console.log("\nupdate\n");

{
  const patched = store.update(made.id, { condition: { op: "lt", value: 70, unit: "CAD" } });
  check("a condition can be changed in place", patched.condition.value === 70);

  const stated = store.update(made.id, { state: { value: 89.99, unit: "CAD" }, failCount: 2 });
  check("observations persist", stated.state?.value === 89.99);
  check("failCount persists", stated.failCount === 2);

  // Anything not on the allow-list is dropped rather than interpolated into SQL.
  const ignored = store.update(made.id, { "id = 'x'; DROP TABLE watches; --": 1, label: "safe" });
  check("unknown patch keys are ignored, not interpolated", ignored.label === "safe");
  check("the table survived that", store.get(made.id) != null);
}

/* ---------------------------------------------------------------- */
console.log("\ndue query - the scheduler's only read\n");

{
  const past = store.create(sample({ label: "due now", nextCheckAt: Date.now() - 1000 }));
  const future = store.create(sample({ label: "not yet", nextCheckAt: Date.now() + DAY }));
  const paused = store.create(
    sample({ label: "paused", nextCheckAt: Date.now() - 1000, status: "paused" }),
  );
  const fired = store.create(
    sample({ label: "fired", nextCheckAt: Date.now() - 1000, status: "fired" }),
  );

  const due = store.due(Date.now(), 50).map((w) => w.label);
  check("a watch past its time is due", due.includes("due now"), due.join(","));
  check("a future watch is not due", !due.includes("not yet"), due.join(","));
  check("a paused watch is never due", !due.includes("paused"), due.join(","));
  check("a fired watch is never due", !due.includes("fired"), due.join(","));

  check("the limit is honoured", store.due(Date.now(), 1).length === 1);

  store.remove(future.id);
  store.remove(paused.id);
  store.remove(fired.id);
  store.remove(past.id);
}

/* ---------------------------------------------------------------- */
console.log("\nper-sender views\n");

{
  const mine = store.listForSender(SENDER);
  check("listing returns this sender's watches", mine.length >= 1 && mine.every((w) => w.sender === SENDER));
  check("another sender sees nothing", store.listForSender("+15559999999").length === 0);

  const before = store.countActive(SENDER);
  store.update(made.id, { status: "cancelled" });
  check("a cancelled watch drops out of the active count",
    store.countActive(SENDER) === before - 1, `${before} -> ${store.countActive(SENDER)}`);
  check("a cancelled watch drops out of the live listing",
    !store.listForSender(SENDER).some((w) => w.id === made.id));
  check("but it is still retrievable by id", store.get(made.id)?.status === "cancelled");
}

/* ---------------------------------------------------------------- */
console.log("\npurging finished watches - what it must NOT delete matters most\n");

{
  const fresh = openStore(dbFile);
  const mk = (label, status) => fresh.create(sample({ label, status }));

  const done = mk("finished and old", "fired");
  const stopped = mk("cancelled and old", "cancelled");
  const live = mk("still active", "active");
  const waiting = mk("waiting on the user", "awaiting_renewal");

  // Age the two terminal ones past the retention window.
  const old = Date.now() - 2 * DAY;
  for (const id of [done.id, stopped.id]) {
    fresh.db.prepare("UPDATE watches SET updated_at = ? WHERE id = ?").run(old, id);
  }

  const removed = fresh.purgeFinished(DAY);
  check("finished and cancelled watches are purged", removed === 2, `removed ${removed}`);
  check("a fired watch is gone", fresh.get(done.id) === null);
  check("a cancelled watch is gone", fresh.get(stopped.id) === null);

  // The two that must survive, and why.
  check("an ACTIVE watch is never purged", fresh.get(live.id) != null);
  check("one awaiting a renewal answer is never purged - it is waiting on a person, not finished",
    fresh.get(waiting.id) != null);

  // Retired just now: "actually, resume that" is a normal thing to say, so the
  // row has to outlive the moment it ended.
  const justEnded = mk("finished a moment ago", "fired");
  check("a watch that just finished is kept for the grace period",
    fresh.purgeFinished(DAY) === 0 && fresh.get(justEnded.id) != null);

  for (const w of [live, waiting, justEnded]) fresh.remove(w.id);
  fresh.close();
}

/* ---------------------------------------------------------------- */
console.log("\nrestart durability - the reason this is on disk at all\n");

{
  const survivor = store.create(sample({ label: "must survive a restart" }));
  store.close();

  // Reopen exactly as a fresh process would.
  store = openStore(dbFile);
  const found = store.get(survivor.id);
  check("the watch is still there after close and reopen", found != null);
  check("its condition survived", found?.condition?.value === 80, JSON.stringify(found?.condition));
  check("its schedule survived", found?.schedule?.everyMs === 6 * HOUR);
  check("it is still due for checking", store.due(Date.now() + DAY, 50).some((w) => w.id === survivor.id));

  // And the schema is idempotent - a second open must not throw or wipe.
  const again = openStore(dbFile);
  check("opening an existing database is idempotent", again.get(survivor.id) != null);
  again.close();
}

store.close();
fs.rmSync(path.dirname(dbFile), { recursive: true, force: true });

console.log();
const passed = results.filter(Boolean).length;
console.log(passed === results.length
  ? `ALL ${results.length} STORE CHECKS PASSED`
  : `${results.length - passed}/${results.length} FAILED`);
process.exitCode = passed === results.length ? 0 : 1;
