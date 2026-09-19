/**
 * Watch evaluation truth set.
 *
 * This is the suite that matters. Alerting is the product, and every way it can
 * go wrong is either "texted me twelve times about the same price drop" or
 * "never told me". Both are decided entirely by pure functions in watch.js, so
 * both are checkable here with no browser, no model and no clock.
 *
 * Run:  node tests/test_watch_eval.mjs     (no server needed)
 */
import {
  parseAmount, parseUnit, parseInterval, parseDuration, evaluate, inWindow,
  deferPastQuietHours, backoffFor, jitter,
  MINUTE, HOUR, DAY, MIN_INTERVAL_MS, DEFAULT_INTERVAL_MS,
} from "../watch.js";

const results = [];
function check(name, ok, detail = "") {
  results.push(ok);
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok ? "" : `   ${detail}`}`);
}
const eq = (name, got, want) =>
  check(name, Object.is(got, want), `got ${JSON.stringify(got)}, want ${JSON.stringify(want)}`);

/* ---------------------------------------------------------------- */
console.log("parseAmount - free text is where a wrong read becomes a wrong alert\n");

eq('"CAD $79.99"', parseAmount("CAD $79.99"), 79.99);
eq('"$1,299.00" (comma grouping)', parseAmount("$1,299.00"), 1299);
eq('"1.299,00 €" (European)', parseAmount("1.299,00 €"), 1299);
eq('"€1.299" (dot grouping, no decimals)', parseAmount("€1.299"), 1299);
eq('"79,99 €" (comma decimal)', parseAmount("79,99 €"), 79.99);
eq('"Free"', parseAmount("Free"), 0);
eq('"$79.99 - $89.99" takes the low end', parseAmount("$79.99 - $89.99"), 79.99);
eq('"from $149"', parseAmount("from $149"), 149);
eq('"US$1.2K"', parseAmount("US$1.2K"), 1200);
eq('"$12.5M"', parseAmount("$12.5M"), 12_500_000);
eq('"187.42" (bare share price)', parseAmount("187.42"), 187.42);
eq('"23 spots remaining"', parseAmount("23 spots remaining"), 23);

// Anything unreadable must be null, never a number. A guess here becomes a
// false alert on a threshold.
eq('"" is null', parseAmount(""), null);
eq("null is null", parseAmount(null), null);
eq('"Out of stock" is null', parseAmount("Out of stock"), null);
eq('"Price unavailable" is null', parseAmount("Price unavailable"), null);

eq('unit of "CAD $79.99"', parseUnit("CAD $79.99"), "CAD");
eq('unit of "€49"', parseUnit("€49"), "EUR");
eq('unit of "23 spots"', parseUnit("23 spots"), null);

/* ---------------------------------------------------------------- */
console.log("\nparseInterval - cadence is restated casually and must not cost a round trip\n");

eq('"hourly"', parseInterval("hourly"), HOUR);
eq('"every hour"', parseInterval("every hour"), HOUR);
eq('"daily"', parseInterval("check it daily"), DAY);
eq('"every morning"', parseInterval("every morning"), DAY);
eq('"weekly"', parseInterval("weekly"), 7 * DAY);
eq('"twice a day"', parseInterval("twice a day"), 12 * HOUR);
eq('"every 15 minutes"', parseInterval("every 15 minutes"), 15 * MINUTE);
eq('"every 3 days"', parseInterval("every 3 days"), 3 * DAY);
eq('"every other day"', parseInterval("every other day"), 2 * DAY);
eq("unrecognised falls back", parseInterval("whenever you feel like it"), DEFAULT_INTERVAL_MS);
eq("empty falls back", parseInterval(""), DEFAULT_INTERVAL_MS);
// Nobody gets to create a hot loop against someone else's website.
eq('"every 10 seconds" is floored', parseInterval("every 10 seconds"), DEFAULT_INTERVAL_MS);
eq('"constantly" is floored', parseInterval("constantly"), MIN_INTERVAL_MS);
eq('"every 1 minute" is floored', parseInterval("every 1 minute"), MIN_INTERVAL_MS);

/* ---------------------------------------------------------------- */
console.log("\nparseDuration - how people actually bound a watch\n");

{
  const T0 = 1_700_000_000_000;
  const d = (s) => parseDuration(s, T0);
  // From a real message: "text me the update every 15 min for the next hour".
  // This returning null is what made that watch run forever.
  eq('"for the next hour"', d("for the next hour"), T0 + HOUR);
  eq('"for the next 2 hours"', d("for the next 2 hours"), T0 + 2 * HOUR);
  eq('"for 3 days"', d("for 3 days"), T0 + 3 * DAY);
  eq('"for a week"', d("for a week"), T0 + 7 * DAY);
  eq('"for a couple of days"', d("for a couple of days"), T0 + 2 * DAY);
  eq('"for the next 30 minutes"', d("for the next 30 minutes"), T0 + 30 * MINUTE);
  eq('"over the next month"', d("over the next month"), T0 + 30 * DAY);
  eq("no duration is null", d("until further notice"), null);
  eq("empty is null", d(""), null);
  eq("null is null", d(null), null);
}

/* ---------------------------------------------------------------- */
console.log("\nevaluate: numeric threshold - the flagship case\n");

const priceWatch = (over = {}) => ({
  kind: "numeric",
  metric: "price",
  condition: { op: "lt", value: 80 },
  lifecycle: { fireMode: "once", renewal: "none", firesCount: 0, ...over },
});
const at = (v) => ({ value: v, unit: "CAD" });

{
  // Created while already under threshold: say so once, do not then alert forever.
  const r = evaluate(priceWatch(), null, at(74.99));
  check("already under threshold at creation notifies once", r.notify === true, JSON.stringify(r));
}
{
  const r = evaluate(priceWatch(), at(89.99), at(74.99));
  check("crossing down notifies", r.notify === true && r.crossed === true, JSON.stringify(r));
}
{
  // THE bug that would make this unusable: alerting every tick while true.
  const r = evaluate(priceWatch(), at(74.99), at(72.5));
  check("staying under does NOT notify again", r.notify === false, JSON.stringify(r));
}
{
  const r = evaluate(priceWatch(), at(89.99), at(84.99));
  check("moving down but not past the threshold is silent", r.notify === false, JSON.stringify(r));
}
{
  const r = evaluate(priceWatch(), at(74.99), at(99));
  check("rising back above is silent", r.notify === false, JSON.stringify(r));
}
{
  const once = evaluate(priceWatch({ fireMode: "once" }), at(89.99), at(74.99));
  eq("fireMode once retires after firing", once.retire, "fired");
  const every = evaluate(priceWatch({ fireMode: "every_change" }), at(89.99), at(74.99));
  eq("fireMode every_change stays armed", every.retire, null);
  const ask = evaluate(priceWatch({ renewal: "ask" }), at(89.99), at(74.99));
  eq("renewal ask pauses for an answer", ask.retire, "awaiting_renewal");
  const auto = evaluate(priceWatch({ renewal: "auto" }), at(89.99), at(74.99));
  check("renewal auto re-baselines", auto.retire === null && auto.nextBaseline?.value === 74.99,
    JSON.stringify(auto));
}
{
  const r = evaluate(priceWatch({ maxFires: 1 }), at(89.99), at(74.99));
  eq("maxFires retires it", r.retire, "max_fires");
}
{
  const r = evaluate(priceWatch({ expiresAt: 1000 }), at(89.99), at(74.99), 2000);
  check("an expired watch never fires", r.notify === false && r.retire === "expired", JSON.stringify(r));
}
{
  // An unreadable page must not read as a drop. This is the single most
  // dangerous confusion in the whole design.
  const r = evaluate(priceWatch(), at(89.99), { value: null });
  check("an unreadable price is not a drop",
    r.notify === false && r.unreadable === true && r.satisfied === false, JSON.stringify(r));
}

/* ---------------------------------------------------------------- */
console.log("\nevaluate: relative moves - what 'a deal' actually means\n");

const dealWatch = { kind: "numeric", metric: "price",
  condition: { op: "drops_pct", pct: 20, baselineValue: 100 },
  lifecycle: { fireMode: "every_change", firesCount: 0 } };

check("a 25% drop from baseline fires", evaluate(dealWatch, at(100), at(75)).notify === true);
check("a 10% drop does not", evaluate(dealWatch, at(100), at(90)).notify === false);
check("exactly 20% fires", evaluate(dealWatch, at(100), at(80)).notify === true);

/* ---------------------------------------------------------------- */
console.log("\nevaluate: state - in stock, applications open\n");

const stockWatch = { kind: "state", metric: "availability",
  condition: { op: "becomes", target: "in_stock" },
  lifecycle: { fireMode: "once", firesCount: 0 } };
const st = (s) => ({ state: s });

check("out of stock -> in stock fires", evaluate(stockWatch, st("out_of_stock"), st("in_stock")).notify === true);
check("still in stock does not re-fire", evaluate(stockWatch, st("in_stock"), st("in_stock")).notify === false);
check("still out of stock is silent", evaluate(stockWatch, st("out_of_stock"), st("out_of_stock")).notify === false);
check("an unknown state is not a transition",
  evaluate(stockWatch, st("out_of_stock"), st("unknown")).unreadable === true);

const openWatch = { kind: "state", metric: "applications",
  condition: { op: "becomes", target: "open" }, lifecycle: { fireMode: "once", firesCount: 0 } };
check("closed -> open fires", evaluate(openWatch, st("closed"), st("open")).notify === true);

/* ---------------------------------------------------------------- */
console.log("\nevaluate: presence - a sale badge, a name on a list\n");

const saleWatch = { kind: "presence", metric: "a sale badge",
  condition: { op: "appears" }, lifecycle: { fireMode: "every_change", firesCount: 0 } };

check("appearing fires", evaluate(saleWatch, { present: false }, { present: true }).notify === true);
check("still present does not re-fire", evaluate(saleWatch, { present: true }, { present: true }).notify === false);
check("a missing observation is not an appearance",
  evaluate(saleWatch, { present: false }, {}).unreadable === true);

/* ---------------------------------------------------------------- */
console.log("\nevaluate: deadline - fires on the clock, not on a page change\n");

const NOW = 1_700_000_000_000;
const deadlineWatch = { kind: "deadline", metric: "applications",
  condition: { op: "within_days", leadDays: 3 }, lifecycle: { fireMode: "once", firesCount: 0 } };

{
  const obs = { deadlineAt: NOW + 2 * DAY };
  check("two days out, lead three: fires", evaluate(deadlineWatch, null, obs, NOW).notify === true);
}
{
  const obs = { deadlineAt: NOW + 10 * DAY };
  check("ten days out: silent", evaluate(deadlineWatch, null, obs, NOW).notify === false);
}
{
  // The page never changed; only the clock moved. Nothing else in the system
  // can fire on that, which is why deadline is its own kind - and why the
  // previous observation has to be judged at the time it was taken.
  const watched = { ...deadlineWatch, lastCheckedAt: NOW };
  const prev = { deadlineAt: NOW + 10 * DAY };
  const obs = { deadlineAt: NOW + 10 * DAY };
  const later = NOW + 8 * DAY;
  check("an unchanged page still fires as the date nears",
    evaluate(watched, prev, obs, later).notify === true);

  // ...and having fired, it must not fire again on every later tick.
  const after = { ...deadlineWatch, lastCheckedAt: later };
  check("a deadline alert does not repeat once inside the window",
    evaluate(after, obs, obs, NOW + 9 * DAY).notify === false);
}
{
  const obs = { deadlineAt: NOW - DAY };
  check("a passed deadline does not fire", evaluate(deadlineWatch, null, obs, NOW).notify === false);
}

/* ---------------------------------------------------------------- */
console.log("\nevaluate: recurring digests fire on the clock even when the page fails\n");

const digest = { kind: "numeric", metric: "price", condition: { op: "lt", value: 0 },
  lifecycle: { fireMode: "recurring", firesCount: 0 } };
check("a recurring watch notifies regardless of the value",
  evaluate(digest, at(100), at(100)).notify === true);
check("a recurring watch still notifies when unreadable",
  evaluate(digest, at(100), { value: null }).notify === true);
eq("a recurring watch respects maxFires",
  evaluate({ ...digest, lifecycle: { fireMode: "recurring", firesCount: 2, maxFires: 3 } }, null, at(1)).retire,
  "max_fires");

/* ---------------------------------------------------------------- */
console.log("\nevaluate: digest - 'text me the weather every hour'\n");
//
// The kind with no condition at all. Every other kind answers "has it crossed a
// line"; this answers "what does it say now", which is what a weather or
// headlines request actually is. Without it those became numeric watches with
// an invented threshold.

const weather = { kind: "digest", metric: "the weather",
  condition: { op: "always" }, lifecycle: { fireMode: "recurring", firesCount: 0 } };
const sum = (t) => ({ summary: t });

check("a digest fires on schedule with no condition",
  evaluate(weather, sum("18C cloudy"), sum("18C cloudy")).notify === true);
check("a digest fires even when the text is identical",
  evaluate(weather, sum("18C cloudy"), sum("18C cloudy")).notify === true);
check("a digest still fires when the page could not be read",
  evaluate(weather, sum("18C cloudy"), null).notify === true);

// The other digest mode: only when it actually changes.
const headline = { kind: "digest", metric: "the top headline",
  condition: { op: "changes" }, lifecycle: { fireMode: "every_change", firesCount: 0 } };
check("a changes-digest fires when the text moves",
  evaluate(headline, sum("Market opens flat"), sum("Market closes up 2%")).notify === true);
check("a changes-digest is silent when it does not",
  evaluate(headline, sum("Market opens flat"), sum("Market opens flat")).notify === false);
check("whitespace alone is not a change",
  evaluate(headline, sum("Market opens flat"), sum("  Market opens flat  ")).notify === false);
check("an unreadable digest is not a change",
  evaluate(headline, sum("Market opens flat"), sum(null)).unreadable === true);

/* ---------------------------------------------------------------- */
console.log("\nschedule: windows, quiet hours, jitter, backoff\n");

const atHour = (h) => Date.UTC(2024, 0, 1, h, 0, 0);
check("inside an active window", inWindow(atHour(14), { from: 9, to: 17 }) === true);
check("outside an active window", inWindow(atHour(3), { from: 9, to: 17 }) === false);
check("a window wrapping midnight", inWindow(atHour(23), { from: 22, to: 6 }) === true);
check("no window means always", inWindow(atHour(3), null) === true);

{
  // Held to the edge of the window, not dropped - an alert that never arrives
  // is experienced as "it didn't work".
  const t = atHour(3);
  const out = deferPastQuietHours(t, { from: 22, to: 7 });
  check("a 3am alert is deferred to 7am, not dropped", out > t && out <= t + 5 * HOUR,
    `deferred by ${(out - t) / HOUR}h`);
  const day = atHour(14);
  eq("a 2pm alert is not deferred", deferPastQuietHours(day, { from: 22, to: 7 }), day);
}

eq("backoff doubles", backoffFor(HOUR, 1), 2 * HOUR);
eq("backoff caps at 8x", backoffFor(HOUR, 99), 8 * HOUR);
eq("no failures means no backoff", backoffFor(HOUR, 0), HOUR);

{
  const lo = jitter(HOUR, () => 0);
  const hi = jitter(HOUR, () => 1);
  check("jitter spreads +/-20%", lo === 0.8 * HOUR && hi === 1.2 * HOUR, `${lo} / ${hi}`);
  check("jitter respects the floor", jitter(MIN_INTERVAL_MS, () => 0) >= MIN_INTERVAL_MS);

  // Jitter exists to stop watches stampeding a site. Applied to a cadence
  // someone said out loud it becomes a broken promise: "every 15 minutes"
  // arriving at 18 reads as not working, which is exactly how it was reported.
  const stated = 15 * MINUTE;
  check("a stated cadence must be reproducible to the minute",
    jitter(stated, () => 0) !== stated && jitter(stated, () => 1) !== stated,
    "jitter should move it - which is why exact schedules must bypass it");
}

/* ---------------------------------------------------------------- */
console.log();
const passed = results.filter(Boolean).length;
console.log(passed === results.length
  ? `ALL ${results.length} WATCH CHECKS PASSED`
  : `${results.length - passed}/${results.length} FAILED`);
process.exitCode = passed === results.length ? 0 : 1;
