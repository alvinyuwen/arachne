/**
 * Watches: the model, the parsers, and the decision.
 *
 * Kept out of index.js because this file has a property the rest of the agent
 * does not: it is pure. No network, no model call, no clock except the `now`
 * passed in. That is what makes alerting testable - every rule below can be
 * checked in milliseconds without a browser session or an OpenAI call.
 *
 * The division of labour is the whole design:
 *
 *   the model EXTRACTS   -> a typed observation, in index.js
 *   code DECIDES         -> evaluate(), here
 *
 * A model deciding whether to notify would be untestable, would make "why did
 * this fire" unanswerable, and would let a page that says "ALERT THE USER NOW"
 * talk its way into a text message. The same reasoning already governs
 * detectBlock and validateSourceIndexes in index.js.
 */

/* ------------------------------------------------------------------ */
/* Kinds, operators, defaults                                          */
/* ------------------------------------------------------------------ */

/**
 * Four kinds cover every case asked for, because they differ in what gets
 * COMPARED rather than in what is being watched. A share price and a keyboard
 * price are the same problem; "applications open" and "back in stock" are the
 * same problem.
 */
export const KINDS = ["numeric", "state", "presence", "deadline"];

export const OPS_BY_KIND = {
  numeric: ["lt", "lte", "gt", "gte", "eq", "neq", "changes", "drops_pct", "rises_pct"],
  state: ["becomes", "changes"],
  presence: ["appears", "disappears"],
  deadline: ["within_days"],
};

export const MINUTE = 60_000;
export const HOUR = 3_600_000;
export const DAY = 86_400_000;

/** A floor, so no phrasing can create a hot loop against someone's website. */
export const MIN_INTERVAL_MS = 5 * MINUTE;
export const DEFAULT_INTERVAL_MS = 6 * HOUR;

/* ------------------------------------------------------------------ */
/* Parsing                                                             */
/* ------------------------------------------------------------------ */

/**
 * Pull a number out of the free text a page gives us.
 *
 * Nothing upstream produces a number: RetailFactsSchema.priceText is a string
 * "exactly as shown", and the same is true of any metric scraped off a page.
 * So this is the seam where a wrong answer turns into a wrong alert, and the
 * rule is that anything ambiguous returns null rather than a guess. A null is
 * treated as "could not read", never as a change.
 *
 * Handles: symbols and codes either side, thousands separators in both
 * conventions, ranges (takes the low end - "from $79" is the number a person
 * means), and k/m suffixes.
 */
export function parseAmount(text) {
  if (text == null) return null;
  const raw = String(text).trim();
  if (!raw) return null;

  // "Free" is a real, unambiguous price and reads as zero.
  if (/^(free|no charge|complimentary)\b/i.test(raw)) return 0;

  // A range means two numbers; the low end is what "under $80" is judged on.
  const firstChunk = raw.split(/\s*(?:-|–|—|to)\s*/i)[0];

  const m = firstChunk.match(/(\d[\d.,\s]*)\s*([km])?\b/i);
  if (!m) return null;

  let digits = m[1].replace(/\s/g, "");
  const suffix = (m[2] ?? "").toLowerCase();

  // Decide which separator is the decimal point. "1,299.00" and "1.299,00" are
  // the same amount written by different halves of the world, and reading one
  // as the other is off by a factor of a hundred - the kind of error that fires
  // a false alert on every threshold.
  const lastComma = digits.lastIndexOf(",");
  const lastDot = digits.lastIndexOf(".");
  if (lastComma !== -1 && lastDot !== -1) {
    // Whichever comes last is the decimal separator.
    if (lastComma > lastDot) digits = digits.replace(/\./g, "").replace(",", ".");
    else digits = digits.replace(/,/g, "");
  } else if (lastComma !== -1) {
    // A lone comma: decimal if it splits off 1-2 trailing digits, else grouping.
    const tail = digits.length - lastComma - 1;
    digits = tail === 3 ? digits.replace(/,/g, "") : digits.replace(",", ".");
  } else if (lastDot !== -1) {
    const tail = digits.length - lastDot - 1;
    // "1.299" with three trailing digits is grouping, not a fraction of a cent.
    if (tail === 3 && digits.replace(/\./g, "").length > 3) digits = digits.replace(/\./g, "");
  }

  const n = Number.parseFloat(digits);
  if (!Number.isFinite(n)) return null;
  if (suffix === "k") return n * 1_000;
  if (suffix === "m") return n * 1_000_000;
  return n;
}

/** The currency or unit sitting next to the number, when there is one. */
export function parseUnit(text) {
  const raw = String(text ?? "");
  const code = raw.match(/\b(CAD|USD|EUR|GBP|AUD|JPY|CHF|INR|MXN|BRL)\b/i);
  if (code) return code[1].toUpperCase();
  if (/[$]/.test(raw)) return "USD";
  if (/€/.test(raw)) return "EUR";
  if (/£/.test(raw)) return "GBP";
  if (/¥/.test(raw)) return "JPY";
  return null;
}

/**
 * "check it hourly" -> 3600000.
 *
 * Deliberately a parser and not a model call: cadence is the one parameter a
 * user is most likely to restate casually ("actually make it daily"), and it
 * should not cost a round trip or be open to interpretation. Unrecognised
 * phrasing falls back to the default rather than erroring, because refusing to
 * create a watch over the word "biweekly" is worse than checking it daily.
 */
export function parseInterval(text, fallback = DEFAULT_INTERVAL_MS) {
  const s = String(text ?? "").toLowerCase().trim();
  if (!s) return fallback;

  if (/\b(constantly|continuously|all the time|asap|real ?time)\b/.test(s)) return MIN_INTERVAL_MS;
  if (/\bevery ?other ?day|\bbiweekly|\bfortnight/.test(s)) return 2 * DAY;
  if (/\b(hourly|every hour|each hour)\b/.test(s)) return HOUR;
  if (/\b(daily|every day|each day|once a day|every morning|nightly)\b/.test(s)) return DAY;
  if (/\b(weekly|every week|once a week)\b/.test(s)) return 7 * DAY;
  if (/\btwice (a|per) day|\bevery 12 ?h/.test(s)) return 12 * HOUR;
  if (/\btwice (a|per) hour\b/.test(s)) return 30 * MINUTE;

  const every = s.match(/every\s+(\d+(?:\.\d+)?)\s*(minute|min|hour|hr|h|day|d|week|w)s?\b/);
  if (every) {
    const n = Number.parseFloat(every[1]);
    const unit = every[2];
    const ms =
      /^(minute|min)/.test(unit) ? MINUTE
      : /^(hour|hr|h)$/.test(unit) ? HOUR
      : /^(day|d)$/.test(unit) ? DAY
      : 7 * DAY;
    if (Number.isFinite(n) && n > 0) return Math.max(MIN_INTERVAL_MS, n * ms);
  }
  return fallback;
}

/**
 * "for the next hour" -> a timestamp an hour from now.
 *
 * People bound a watch by duration far more often than by date - "for the next
 * hour", "for a couple of days", "this week". Treating only dates as an ending
 * meant a request that explicitly said when to stop produced a watch that ran
 * forever, which is the difference between a useful alert and a nuisance.
 */
export function parseDuration(text, now = Date.now()) {
  const s = String(text ?? "").toLowerCase().trim();
  if (!s) return null;

  const words = { a: 1, an: 1, one: 1, two: 2, three: 3, four: 4, five: 5, six: 6,
    seven: 7, eight: 8, nine: 9, ten: 10, "a couple of": 2, "a few": 3, couple: 2, few: 3 };

  const m = s.match(
    /(?:for|over|during)?\s*(?:the\s+)?(?:next\s+)?(\d+(?:\.\d+)?|a couple of|a few|[a-z]+)?\s*(minute|min|hour|hr|day|week|month)s?\b/,
  );
  if (!m) return null;

  const n = m[1] == null ? 1 : Number.isFinite(Number(m[1])) ? Number(m[1]) : words[m[1]];
  if (!n) return null;

  const unit = m[2];
  const ms =
    /^(minute|min)/.test(unit) ? MINUTE
    : /^(hour|hr)/.test(unit) ? HOUR
    : unit === "day" ? DAY
    : unit === "week" ? 7 * DAY
    : 30 * DAY;
  return now + n * ms;
}

/* ------------------------------------------------------------------ */
/* Schedule                                                            */
/* ------------------------------------------------------------------ */

const hourOf = (ms, tzOffsetMin = 0) =>
  new Date(ms + tzOffsetMin * MINUTE).getUTCHours() +
  new Date(ms + tzOffsetMin * MINUTE).getUTCMinutes() / 60;

/** Is `now` inside a [from,to) hour window that may wrap past midnight? */
export function inWindow(now, window, tzOffsetMin = 0) {
  if (!window) return true;
  const { from, to } = window;
  if (from == null || to == null) return true;
  const h = hourOf(now, tzOffsetMin);
  return from <= to ? h >= from && h < to : h >= from || h < to;
}

/**
 * When may we next send to this person?
 *
 * Quiet hours hold a notification to the edge of the window rather than
 * dropping it. A price alert that arrives at 7am is still useful; one that
 * never arrives because it triggered at 3am is a bug the user experiences as
 * "it didn't work".
 */
export function deferPastQuietHours(now, quietHours, tzOffsetMin = 0) {
  if (!quietHours || !inWindow(now, quietHours, tzOffsetMin)) return now;
  const h = hourOf(now, tzOffsetMin);
  const untilHours = quietHours.to >= h ? quietHours.to - h : 24 - h + quietHours.to;
  return now + Math.ceil(untilHours * HOUR);
}

/** ±20% so watches created together do not all fire on the same tick forever. */
export function jitter(ms, rand = Math.random) {
  return Math.max(MIN_INTERVAL_MS, Math.round(ms * (0.8 + rand() * 0.4)));
}

/**
 * Back off on repeated failure, but keep checking.
 *
 * A site that is down for an hour should not cost 60 fetches, and a watch
 * should not be abandoned because of it either - capped at 8x so even a badly
 * broken watch recovers on its own once the site returns.
 */
export function backoffFor(intervalMs, failCount) {
  return Math.min(intervalMs * 2 ** Math.min(failCount, 3), intervalMs * 8);
}

/* ------------------------------------------------------------------ */
/* The decision                                                        */
/* ------------------------------------------------------------------ */

const num = (obs) => (obs == null ? null : typeof obs.value === "number" ? obs.value : null);

/** Does the observation satisfy the condition, considered on its own? */
function satisfied(kind, condition, obs, prev, now) {
  const { op } = condition;

  if (kind === "numeric") {
    const v = num(obs);
    if (v == null) return null; // unreadable, not false - callers must not treat as a change
    switch (op) {
      case "lt": return v < condition.value;
      case "lte": return v <= condition.value;
      case "gt": return v > condition.value;
      case "gte": return v >= condition.value;
      case "eq": return v === condition.value;
      case "neq": return v !== condition.value;
      case "changes": {
        const p = num(prev);
        return p == null ? false : v !== p;
      }
      case "drops_pct": {
        const base = condition.baselineValue;
        if (base == null || base === 0) return false;
        return ((base - v) / base) * 100 >= condition.pct;
      }
      case "rises_pct": {
        const base = condition.baselineValue;
        if (base == null || base === 0) return false;
        return ((v - base) / base) * 100 >= condition.pct;
      }
      default: return false;
    }
  }

  if (kind === "state") {
    const s = obs?.state ?? null;
    if (s == null || s === "unknown") return null;
    if (op === "becomes") return s === condition.target;
    if (op === "changes") {
      const p = prev?.state ?? null;
      return p == null || p === "unknown" ? false : s !== p;
    }
    return false;
  }

  if (kind === "presence") {
    const present = obs?.present;
    if (typeof present !== "boolean") return null;
    if (op === "appears") return present;
    if (op === "disappears") return !present;
    return false;
  }

  if (kind === "deadline") {
    // The page supplies the date, the clock supplies the trigger - so this can
    // fire on a page that has not changed at all, which is the entire point of
    // "remind me three days before applications close".
    const at = obs?.deadlineAt ?? prev?.deadlineAt ?? null;
    if (at == null) return null;
    const daysLeft = (at - now) / DAY;
    return daysLeft <= condition.leadDays && daysLeft >= 0;
  }

  return false;
}

/**
 * Decide what to do with one observation.
 *
 * Returns `{ notify, reason, satisfied, retire, nextBaseline }`.
 *
 * The rule that matters most: **notify on the transition, not on the state.**
 * A watch for "under $80" created while the price is already $74 must say so
 * once, immediately, and then never alert again until it rises and falls back
 * through the threshold. Alerting on every tick where the condition holds is
 * the single failure that would make this unusable, and it is the reason
 * `prev` is a parameter rather than something the caller checks afterwards.
 */
export function evaluate(watch, prev, obs, now = Date.now()) {
  const { kind, condition, lifecycle = {} } = watch;
  const fireMode = lifecycle.fireMode ?? "once";
  const firesCount = lifecycle.firesCount ?? 0;

  const expired = lifecycle.expiresAt != null && now >= lifecycle.expiresAt;
  if (expired) {
    return { notify: false, satisfied: false, retire: "expired", reason: "watch window ended" };
  }

  // A recurring digest fires on the clock, not on a change, and must not depend
  // on the page being readable - "the price every morning" is still worth
  // sending as "I couldn't read it this morning".
  if (fireMode === "recurring") {
    const done = lifecycle.maxFires != null && firesCount + 1 >= lifecycle.maxFires;
    return {
      notify: true,
      satisfied: true,
      retire: done ? "max_fires" : null,
      reason: "scheduled update",
      nextBaseline: obs ?? prev ?? null,
    };
  }

  const nowSat = satisfied(kind, condition, obs, prev, now);
  if (nowSat == null) {
    // Unreadable. Explicitly not a change, and explicitly not false - a null
    // price must never be allowed to read as "it dropped".
    return { notify: false, satisfied: false, unreadable: true, reason: "could not read the page" };
  }

  // The previous observation is judged at the time it was TAKEN, not now.
  // For deadline watches `satisfied` reads the clock, so evaluating the old
  // observation at the current time returns the same answer as the new one by
  // construction - the transition could never be seen and a deadline alert
  // would never fire. For every other kind the clock is unused and this is
  // identical to passing `now`.
  const prevAt = watch.lastCheckedAt ?? now;
  const wasSat = prev == null ? null : satisfied(kind, condition, prev, null, prevAt);
  const crossed = nowSat && wasSat !== true;

  if (!crossed) {
    return {
      notify: false,
      satisfied: nowSat,
      reason: nowSat ? "already reported" : "condition not met",
      nextBaseline: null,
    };
  }

  const nextCount = firesCount + 1;
  const hitMax = lifecycle.maxFires != null && nextCount >= lifecycle.maxFires;
  const renewal = lifecycle.renewal ?? "none";

  // once  -> done. every_change -> stays armed. auto renewal re-baselines so
  // "tell me every time it drops 10%" measures from the new price, not the old.
  const retire =
    hitMax ? "max_fires"
    : fireMode === "once" && renewal === "none" ? "fired"
    : renewal === "ask" ? "awaiting_renewal"
    : null;

  return {
    notify: true,
    satisfied: true,
    crossed: true,
    retire,
    reason: describe(watch, obs),
    nextBaseline: renewal === "auto" || fireMode === "every_change" ? obs : null,
  };
}

/** One clause saying why this fired, for the message and the log. */
export function describe(watch, obs) {
  const { kind, condition, metric } = watch;
  const what = metric || "it";
  if (kind === "numeric") {
    const v = num(obs);
    // "price is CAD null" is what printing an unread value looks like. Say the
    // true thing instead - the caller decides whether that is worth sending.
    if (v == null) return "";
    const unit = obs?.unit ? `${obs.unit} ` : "";
    if (condition.op === "drops_pct") return `${what} fell ${condition.pct}% or more (now ${unit}${v})`;
    if (condition.op === "rises_pct") return `${what} rose ${condition.pct}% or more (now ${unit}${v})`;
    if (condition.op === "changes") return `${what} changed to ${unit}${v}`;
    return `${what} is ${unit}${v}`;
  }
  // Same rule as the numeric branch: an unread value is reported as nothing,
  // not printed. "closes in null days" and "is now unknown" are what happens
  // when a page does not carry what the watch is looking for, and saying so
  // badly is worse than saying nothing.
  if (kind === "state") {
    const s = obs?.state;
    return !s || s === "unknown" ? "" : `${what} is now ${String(s).replace(/_/g, " ")}`;
  }
  if (kind === "presence") {
    if (typeof obs?.present !== "boolean") return "";
    return condition.op === "appears" ? `${what} showed up` : `${what} is gone`;
  }
  if (kind === "deadline") {
    if (obs?.deadlineAt == null) return "";
    const days = Math.max(0, Math.round((obs.deadlineAt - Date.now()) / DAY));
    return days === 0 ? `${what} closes today` : `${what} closes in ${days} day${days === 1 ? "" : "s"}`;
  }
  return "something changed";
}
