/**
 * Durable watch storage.
 *
 * The rest of this agent keeps everything in in-process Maps, deliberately:
 * conversations are a plaintext SMS transcript keyed by phone number, and
 * losing them on restart is a safe failure (index.js, "Conversation memory").
 *
 * Watches are the opposite case. A watch that does not survive a restart is not
 * a feature - the entire promise is "tell me and forget about it", and this
 * process restarts on every deploy. So these go to disk.
 *
 * What lands there is a phone number, a URL, a threshold and a timestamp. Not
 * message content. The file sits beside .env, is gitignored, and is exactly as
 * sensitive as the credentials already there.
 *
 * node:sqlite rather than a JSON file: it is built into Node 22+, so it adds no
 * dependency to a project with five, and "which watches are due" is a query
 * rather than a full-file read, rewrite and fsync on every tick.
 */
import { DatabaseSync } from "node:sqlite";
import crypto from "node:crypto";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS watches (
  id              TEXT PRIMARY KEY,
  sender          TEXT NOT NULL,
  label           TEXT NOT NULL,
  kind            TEXT NOT NULL,
  metric          TEXT,
  source          TEXT NOT NULL,
  condition       TEXT NOT NULL,
  schedule        TEXT NOT NULL,
  lifecycle       TEXT NOT NULL,
  state           TEXT,
  baseline        TEXT,
  status          TEXT NOT NULL,
  next_check_at   INTEGER NOT NULL,
  last_checked_at INTEGER,
  last_notified_at INTEGER,
  fail_count      INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_due ON watches (status, next_check_at);
CREATE INDEX IF NOT EXISTS idx_sender ON watches (sender, status);
`;

const JSON_COLUMNS = ["source", "condition", "schedule", "lifecycle", "state", "baseline"];

/** Rows in, objects out. The compound shapes are JSON so the table can stay put. */
function hydrate(row) {
  if (!row) return null;
  const out = { ...row };
  for (const col of JSON_COLUMNS) {
    out[col] = row[col] == null ? null : safeParse(row[col]);
  }
  // camelCase at the boundary so callers never see snake_case.
  out.nextCheckAt = row.next_check_at;
  out.lastCheckedAt = row.last_checked_at;
  out.lastNotifiedAt = row.last_notified_at;
  out.failCount = row.fail_count;
  out.createdAt = row.created_at;
  delete out.next_check_at;
  delete out.last_checked_at;
  delete out.last_notified_at;
  delete out.fail_count;
  delete out.created_at;
  delete out.updated_at;
  return out;
}

function safeParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    // A corrupt cell must not take down the tick that reads it.
    return null;
  }
}

export function openStore(file) {
  const db = new DatabaseSync(file);
  // WAL so a long-running read on the scheduler tick cannot block a write from
  // an inbound message arriving at the same moment.
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA busy_timeout = 4000");
  db.exec(SCHEMA);

  // Migrations. CREATE TABLE IF NOT EXISTS leaves an existing table alone, so
  // a new column has to be added explicitly - and has to tolerate already
  // being there, because this runs on every open.
  for (const [column, decl] of [["brief", "TEXT"]]) {
    try {
      db.exec(`ALTER TABLE watches ADD COLUMN ${column} ${decl}`);
    } catch {
      /* already present */
    }
  }

  const stmt = {
    insert: db.prepare(`INSERT INTO watches
      (id, sender, label, kind, metric, brief, source, condition, schedule, lifecycle,
       state, baseline, status, next_check_at, last_checked_at, last_notified_at,
       fail_count, created_at, updated_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`),
    byId: db.prepare(`SELECT * FROM watches WHERE id = ?`),
    bySender: db.prepare(
      `SELECT * FROM watches WHERE sender = ? AND status IN ('active','awaiting_renewal','paused')
       ORDER BY created_at DESC`,
    ),
    allBySender: db.prepare(`SELECT * FROM watches WHERE sender = ? ORDER BY created_at DESC`),
    due: db.prepare(
      `SELECT * FROM watches WHERE status = 'active' AND next_check_at <= ?
       ORDER BY next_check_at ASC LIMIT ?`,
    ),
    countActive: db.prepare(
      `SELECT COUNT(*) AS n FROM watches WHERE sender = ? AND status IN ('active','awaiting_renewal')`,
    ),
    del: db.prepare(`DELETE FROM watches WHERE id = ?`),
  };

  function create(w) {
    const now = Date.now();
    const id = w.id ?? crypto.randomBytes(8).toString("hex");
    stmt.insert.run(
      id,
      w.sender,
      w.label,
      w.kind,
      w.metric ?? null,
      w.brief ?? null,
      JSON.stringify(w.source ?? {}),
      JSON.stringify(w.condition ?? {}),
      JSON.stringify(w.schedule ?? {}),
      JSON.stringify(w.lifecycle ?? {}),
      w.state == null ? null : JSON.stringify(w.state),
      w.baseline == null ? null : JSON.stringify(w.baseline),
      w.status ?? "active",
      w.nextCheckAt ?? now,
      w.lastCheckedAt ?? null,
      w.lastNotifiedAt ?? null,
      w.failCount ?? 0,
      now,
      now,
    );
    return get(id);
  }

  const get = (id) => hydrate(stmt.byId.get(id));

  /**
   * Patch a watch.
   *
   * Column names are taken from a fixed allow-list rather than from the caller,
   * because these are interpolated into SQL - a patch key arriving from
   * anywhere near model output must not be able to name a column, or anything
   * else.
   */
  const COLUMNS = {
    label: "label", kind: "kind", metric: "metric", brief: "brief", status: "status",
    source: "source", condition: "condition", schedule: "schedule",
    lifecycle: "lifecycle", state: "state", baseline: "baseline",
    nextCheckAt: "next_check_at", lastCheckedAt: "last_checked_at",
    lastNotifiedAt: "last_notified_at", failCount: "fail_count",
  };

  function update(id, patch) {
    const sets = [];
    const values = [];
    for (const [key, value] of Object.entries(patch)) {
      const col = COLUMNS[key];
      if (!col) continue;
      sets.push(`${col} = ?`);
      values.push(JSON_COLUMNS.includes(col) ? (value == null ? null : JSON.stringify(value)) : value);
    }
    if (!sets.length) return get(id);
    sets.push("updated_at = ?");
    values.push(Date.now(), id);
    db.prepare(`UPDATE watches SET ${sets.join(", ")} WHERE id = ?`).run(...values);
    return get(id);
  }

  return {
    db,
    create,
    get,
    update,
    remove: (id) => stmt.del.run(id),
    /** What this sender would call "my watches" - live ones only. */
    listForSender: (sender) => stmt.bySender.all(sender).map(hydrate),
    listAllForSender: (sender) => stmt.allBySender.all(sender).map(hydrate),
    due: (now = Date.now(), limit = 25) => stmt.due.all(now, limit).map(hydrate),
    countActive: (sender) => stmt.countActive.get(sender)?.n ?? 0,

    /**
     * Delete watches that are finished and have been for a while.
     *
     * A retired watch is not worth keeping forever - it can never fire again,
     * and left alone the table only grows. But deleting the moment one ends is
     * wrong too: "keep watching" after an ask-renewal needs the row, and "stop
     * watching the keyboard" followed by "actually, resume it" is a normal
     * thing to say. So they are retired first and purged after a grace period.
     *
     * awaiting_renewal is deliberately not swept: it is waiting on a person,
     * not finished, and answering "yes" an hour later must still work.
     */
    purgeFinished: (olderThanMs, now = Date.now()) =>
      db
        .prepare(
          `DELETE FROM watches
           WHERE status IN ('fired', 'cancelled', 'failed') AND updated_at < ?`,
        )
        .run(now - olderThanMs).changes ?? 0,

    close: () => db.close(),
  };
}
