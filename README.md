# linq-browser-agent

A web watchdog you talk to by text. Tell it what to keep an eye on, then stop
thinking about it — it checks on its own schedule and texts you when something
actually matters.

```
"find me a mechanical keyboard under $100"     ──▶  answers now
"tell me if one goes under $80"                ──▶  watches, then texts you later
"track the amazon stock price every 15 min"    ──▶  scheduled updates
"tell me when applications open"               ──▶  fires on a state change
"remind me 3 days before the deadline"         ──▶  fires on the clock
```

Two things happen in this process. One answers questions; the other keeps
watching after the conversation ends.

```
iMessage ─▶ Linq webhook ─▶ route ─┬─ chat
                                   ├─ task ────▶ research / browser tier ─▶ reply
                                   ├─ watch ───▶ store a monitoring job
                                   └─ manage ──▶ list / cancel / retune

scheduler (every 60s) ─▶ due watches ─▶ observe ─▶ evaluate ─▶ Linq ─▶ you
                          (SQLite)      (fetch,     (pure code,
                                      browser if      no model)
                                       needed)
```

## Watches

Five kinds cover every case, because they differ in what gets **compared**, not
in subject. A share price and a keyboard price are the same problem.

The first question is whether there is a condition at all. "Text me the weather
every hour" has none — the schedule *is* the request. Those are digests, and
without that kind they became numeric watches with an invented threshold.

| Kind | Compares | Covers |
| --- | --- | --- |
| `digest` | nothing — the schedule is the trigger | the weather, headlines, "the price every 15 min" |
| `numeric` | a number + unit | price, stocks, spots left, follower count |
| `state` | a label transition | in stock, applications open, announced |
| `presence` | did it appear | sale badges, a name on a list |
| `deadline` | a date on the page vs the clock | application cutoffs, early-bird ends |

`metric` is free text (`"NVDA share price"`, `"spots remaining"`), so `numeric`
is not a shopping feature.

**Conditions.** `always changes` for digests, `lt lte gt gte eq neq changes drops_pct rises_pct` for numbers,
`becomes changes` for states, `appears disappears` for presence, `within_days`
for deadlines. `drops_pct` is what "on sale" means when no number is given — a
relative move against a stored baseline.

**Schedules** are parsed, not inferred: `hourly`, `every 15 minutes`, `daily`,
`weekly`, `every 3 days`. Floored at 5 minutes so no phrasing can create a hot
loop against someone's site. Bounded by duration or date — `for the next hour`,
`until Oct 4`. Stock metrics get a market-hours window automatically.

**After firing**: `once` (stop), `every_change` (stay armed), `recurring` (a
scheduled update regardless of change). Renewal is `none`, `auto` (re-baseline)
or `ask` (text you first).

### The model extracts, code decides

The model returns a typed observation. A pure function in `watch.js` compares it
to the stored one and returns `{notify, reason}`. **The model never decides
whether to alert** — it does not know the threshold, has not seen the previous
reading, and is not asked whether anything changed.

Three things follow. Every alerting rule is testable in milliseconds with no
browser and no model call (`tests/test_watch_eval.mjs`, 85 checks). A page that
says "PRICE DROPPED, ALERT THE USER" cannot produce a text message. And "why did
this fire" always has an answer.

### Alerts fire on the transition, not the state

A watch for "under $80" created while the price is already $74 says so **once**,
then stays silent until it rises and falls back through. Alerting on every tick
where the condition holds is the single failure that would make this unusable.

Cooldowns, quiet hours and failed sends all **defer** rather than drop — an
alert that never arrives is indistinguishable from a broken watch. Recurring
digests are exempt from both, because the user chose that cadence.

### Reading pages that need JavaScript

A watch reads via `browserbase.fetch`, which costs one request. When that comes
back empty — a client-rendered SPA returns zero characters — it escalates once
to a real browser session and remembers, so later checks skip the dead fetch.

## Why two tiers

`browserbase.search()` and `browserbase.fetch()` read public pages **without a
browser session** — measured at 0.7s and 1.6s against a 2.7s session launch, with
no anti-bot fight. Reading sources beats driving a browser for almost every
request, so a browser only launches when a task genuinely needs interaction.

## Requirements

- Node.js 18+ (built on v24.15.0)
- Browserbase project, OpenAI key, Linq API key + webhook signing secret
- `ngrok` to expose localhost to Linq (download to this folder as `ngrok.exe`)

## Setup

```bash
npm install
```

| Variable | Purpose |
| --- | --- |
| `OPENAI_API_KEY` | Reasoning and in-page extraction |
| `OPENAI_MODEL` | Stagehand form — **`provider/` prefix required**, e.g. `openai/gpt-5.5` |
| `OPENAI_MODEL_REASONING` | Bare form for direct calls, e.g. `gpt-5.5` |
| `LINQ_API_KEY` | Bearer token for outbound messages |
| `LINQ_WEBHOOK_SECRET` | `whsec_…` signing secret. Unset = unsigned requests accepted |
| `BROWSERBASE_API_KEY` / `BROWSERBASE_PROJECT_ID` | Both required |
| `PUBLIC_BASE_URL` | Blank = auto-detect the running ngrok tunnel |
| `BROWSER_CONCURRENCY` | Max simultaneous Browserbase sessions (default 2) |
| `RATE_LIMIT_PER_HOUR` | Per-sender cap (default 8) |
| `TASK_TIMEOUT_MS` / `RESEARCH_BUDGET_MS` | Time budgets |
| `ARTIFACT_TTL_MS` | How long screenshots stay fetchable (default 1h) |
| `DEBUG_TOKEN` | Enables the `/debug/*` endpoints |
| `WATCH_DB` | SQLite file for monitoring jobs (default `watches.db`) |
| `WATCH_TICK_MS` | How often the scheduler looks for due work (default 60s) |
| `WATCH_CONCURRENCY` | Watches checked at once (default 3) |
| `WATCH_MAX_PER_SENDER` | Active watches per person (default 10) |
| `WATCH_NOTIFY_COOLDOWN_MS` | Floor between two alerts for one watch (default 30m) |
| `WATCH_MAX_FAILS` | Consecutive failures before a watch pauses itself (default 5) |

## Run

```bash
node index.js
./ngrok.exe http 3000 --url https://<your-static-domain>.ngrok-free.dev
```

Point the Linq `message.received` webhook at `https://<host>/webhook/linq`.

## Playbooks

| Task type | Behaviour |
| --- | --- |
| `product_research` | Search review roundups, retailer listings **and forum opinion**, compare, then do a targeted retail lookup per pick for live price, stock and a regional buy link |
| `factual_lookup` | Search, read, answer with cited key facts |
| `social_media` | Public pages only. Never logs in |
| `interactive_browse` | Drives a real browser, one observed step at a time |

## Design notes

**Links cannot be hallucinated.** Synthesis schemas have no URL field. The model
emits a `sourceIndex` integer and code maps it to a real URL from the fetched
corpus, or to a retail URL that came from `search()` results. Anything out of
range is dropped.

**The agent cannot log in.** `detectBlock()` runs before every decision and
before any action — deterministic regexes over URL, title and accessibility
tree, no LLM to talk around. On a wall it screenshots the wall, stops, and falls
back to public sources. The decide-loop schema has no field capable of
expressing a credential, so no code path can type one.

**No blind step lists.** The browser tier observes the live page, picks an index
into the observed actions, and feeds prior step outcomes back in — so it cannot
decide to "click the second result" after already navigating away.

**Retail findings are reconciled in code.** Synthesis runs before the price
lookup, so its caveats are restated deterministically afterwards rather than
being allowed to contradict the links right above them. Sold-out picks are
demoted out of rank 1.

**Artifacts are per-run.** `public/runs/<uuid>/<n>.png`, swept on a TTL.
Concurrent requests can never see each other's screenshots.

**Watches are on disk; conversations are not.** A transcript lost on restart is
a safe failure — the next message gets a clarifying question instead of a wrong
answer. A watch lost on restart means the product does not work, and this
process restarts on every deploy. `watches.db` holds a phone number, a URL and a
threshold, not message content; it is gitignored and is exactly as sensitive as
the `.env` beside it. Storage is `node:sqlite`, built into Node 22+, so the
dependency list stays at five.

**Background checks do not spend the interactive budget.** `RATE_LIMIT_PER_HOUR`
caps what a person can ask for; scheduler work is bounded separately by watch
count and concurrency. Sharing the bucket would let background work lock someone
out of their own agent.

## Testing without iMessage

```bash
curl -X POST http://localhost:3000/debug/run \
  -H "Content-Type: application/json" -H "x-debug-token: $DEBUG_TOKEN" \
  -d '{"text":"best power bank, top 3 with links, I live in Canada"}'
```

Returns the classification, corpus, structured data and rendered message as
JSON, and **sends nothing over iMessage**.

## Endpoints

| Method | Path | |
| --- | --- | --- |
| `POST` | `/webhook/linq` | Signature-verified inbound webhook; acks in <1s |
| `POST` | `/debug/run` | Full pipeline as JSON, no message sent |
| `POST` | `/debug/watch` | Drive the scheduler with `send` stubbed (see below) |
| `GET` | `/health` | Config, models, sessions, queue depth, active watches |
| `GET` | `/runs/<uuid>/<n>.png` | Run screenshots |

### Testing the watchdog without waiting

`POST /debug/watch` takes an `action`: `create` (from a sentence), `list`,
`check` (one watch now), `seed` (plant a prior reading and threshold so a
crossing can be exercised against a live page), and `tick` (force a scheduler
pass). Every one runs with outbound messages captured instead of sent.

```bash
# create, then fake a drop, then watch it fire exactly once
curl -X POST localhost:3000/debug/watch -H "x-debug-token: $DEBUG_TOKEN"   -H "Content-Type: application/json"   -d '{"action":"create","from":"+1555","text":"tell me when <url> drops below $200"}'

curl -X POST localhost:3000/debug/watch -H "x-debug-token: $DEBUG_TOKEN"   -H "Content-Type: application/json"   -d '{"action":"seed","id":"<id>","state":{"value":500},"condition":{"op":"lt","value":300}}'

curl -X POST localhost:3000/debug/watch -H "x-debug-token: $DEBUG_TOKEN"   -H "Content-Type: application/json" -d '{"action":"check","id":"<id>"}'
```

### Tests

```bash
node tests/test_watch_eval.mjs    # 85, no server, no network - the important one
node tests/test_watch_store.mjs   # 27, no server; includes restart durability
node tests/test_redaction.mjs     # 28, no server
node tests/test_routing.mjs       # 35, server running
```

## Notes

- Stagehand v4 has no `stagehand.agent()` and no `env: "BROWSERBASE"`. This uses
  the current surface: `browserbase.launch()` → `Stagehand.create({ browser })`.
- gpt-5 models reject an explicit `temperature`; it is only sent for models that
  accept it.
- `.env` holds live credentials and is gitignored. Rotate if this folder is shared.
