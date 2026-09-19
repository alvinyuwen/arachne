# linq-browser-agent

Turns incoming **Linq iMessage** requests into real web research, and texts back
a structured answer with working links and a screenshot.

```
iMessage ──▶ Linq webhook ──▶ POST /webhook/linq
                                   │  verify signature, rate limit, ack
                                   ▼
                            classify (gpt-5.5)
                        ┌──────────┴──────────┐
                  research tier          browser tier
                  (no browser)          (Stagehand session)
                        │                      │
              search() 3-4 queries      observe → decide → act
              fetch() 6 pages || md     auth wall? stop, never log in
              synthesise → picks               │
              retail lookup per pick    ───────┘ falls back to research
                        │
                        ▼
              render → summary + links + screenshot ──▶ iMessage
```

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
| `DEBUG_TOKEN` | Enables `POST /debug/run` |

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

**The agent cannot log in by itself.** `detectBlock()` runs before every
decision and before any action — deterministic regexes over URL, title and
accessibility tree, no LLM to talk around. On a wall it screenshots the wall,
stops, and falls back to public sources. The decide-loop schema has no field
capable of expressing a credential, so nothing the model decides can type one.

Signing in is a separate, user-initiated path — see **Signing in** below. That
route is the only code in the process that ever handles a credential, it makes
no model call, and the agent loop has no way to reach it.

**No blind step lists.** The browser tier observes the live page, picks an index
into the observed actions, and feeds prior step outcomes back in — so it cannot
decide to "click the second result" after already navigating away.

**Retail findings are reconciled in code.** Synthesis runs before the price
lookup, so its caveats are restated deterministically afterwards rather than
being allowed to contradict the links right above them. Sold-out picks are
demoted out of rank 1.

**Artifacts are per-run.** `public/runs/<uuid>/<n>.png`, swept on a TTL.
Concurrent requests can never see each other's screenshots.

## Signing in

When a task hits a login wall the agent offers a handover. There are two, and
which one you get depends on whether the page has a fillable form.

**One-time form (default).** You get a link to `/unlock/<token>` — a plain page
served by this process with a real `<input type="password">`. You type into it,
it POSTs to the server, the server types the value into the browser already
parked on that login page via CDP `Input.insertText`, and the task resumes on
its own. No reply needed.

**Live view (fallback).** If no form is detected, or no public URL is
configured, you get `debuggerFullscreenUrl` — a screencast of the remote
browser. You drive it yourself and text back `done`.

### Why the form exists

The live view does not work on a phone. It renders as a single `<canvas>`, so
tapping a field leaves `document.activeElement` as the canvas and no mobile
keyboard opens. Verified by emulating an iPhone against a live view:

```
canvases: 1,  typeableElements: 1   (devtools' own UI)
after tapping the password field:  activeTag "CANVAS", isTypeable false
```

Mobile emulation makes it worse, not better. An iPhone UA over a Linux TLS
fingerprint gets the navigation blocked outright; `setDeviceMetricsOverride`
with `mobile: true` makes Instagram serve an app-install interstitial with no
form at all. A desktop UA at a 390×844 viewport renders the real fields, which
is what ships.

### What this costs, stated plainly

The form means **the credential passes through this process.** That is a
deliberate trade, not an oversight. What it buys, against the alternative of
texting it:

| | Texting it | The form |
|---|---|---|
| Apple iMessage infrastructure | sees it | no |
| Linq servers, logs, message history | sees it, durably | no |
| Permanent copy in Messages | yes | no |
| This process | yes | yes |

And it is handled by exactly one route, `POST /unlock/:token`, which makes **no
model call**, writes **nothing to stdout**, stores **nothing in conversation
memory**, and sends **nothing back out over Linq**. The value lives in one
argument object for the duration of the call.

The token is 32 random bytes, single-use, and expires in 10 minutes. It is spent
before the fill runs, so a failed attempt burns it too and a retry gets a fresh
one. `GET` renders the form without spending it, so reloading is safe. The
endpoint is internet-facing by necessity — it has to open from a texted link —
which makes the token the whole access control.

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
| `GET` | `/unlock/<token>` | One-time sign-in form. Public; the token is the access control |
| `POST` | `/unlock/<token>` | Fills the credential into the parked browser, spends the token |
| `POST` | `/debug/run` | Full pipeline as JSON, no message sent |
| `POST` | `/debug/login` | Starts a real handoff, reports it, releases it |
| `POST` | `/debug/unlock` | Mints a token against a real parked session, for tests |
| `GET` | `/health` | Config, models, active sessions, queue depth |
| `GET` | `/runs/<uuid>/<n>.png` | Run screenshots |

### Tests

```bash
node tests/test_redaction.mjs      # 28, no server needed
node tests/test_routing.mjs        # 21, server running
node tests/test_login_handoff.mjs  #  5, costs one Browserbase session
node tests/test_unlock.mjs         # 13, costs two; uses a public test login
```

`test_unlock.mjs` drives `the-internet.herokuapp.com/login`, whose credentials
are published, so no real secret is ever typed. It asserts both directions —
that correct credentials sign in, which is what separates "the fill works" from
"the fill silently did nothing", and that wrong ones are reported rather than
swallowed.

## Notes

- Stagehand v4 has no `stagehand.agent()` and no `env: "BROWSERBASE"`. This uses
  the current surface: `browserbase.launch()` → `Stagehand.create({ browser })`.
- gpt-5 models reject an explicit `temperature`; it is only sent for models that
  accept it.
- `.env` holds live credentials and is gitignored. Rotate if this folder is shared.
