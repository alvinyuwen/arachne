# linq-browser-agent

Turns incoming **Linq iMessage** requests into real web interactions on a remote
**Browserbase** browser, driven by **Stagehand** + **OpenAI**, and messages the
result — a summary plus a screenshot — back to the sender.

```
iMessage ──▶ Linq webhook ──▶ POST /webhook/linq
                                   │
                                   ├─▶ "On it! Launching browser automation..."
                                   │
                                   ├─▶ gpt-4o-mini plans { startUrl, steps[] }
                                   ├─▶ Stagehand act() each step on Browserbase
                                   ├─▶ page.screenshot() → ./public/last_action.png
                                   ├─▶ Stagehand extract() → summary
                                   │
                                   └─▶ summary + screenshot URL back over iMessage
```

## Requirements

- Node.js 18+ (built and checked on v24.15.0)
- A Browserbase project, an OpenAI key, and a Linq API key
- `ngrok` to expose localhost to Linq. Download it into this folder as
  `ngrok.exe` from <https://ngrok.com/download> (gitignored, not committed)

## Setup

```bash
npm install
```

Configuration lives in `.env` (see `.env.example`):

| Variable | Purpose |
| --- | --- |
| `OPENAI_API_KEY` | Planning + Stagehand's reasoning model |
| `OPENAI_MODEL` | Defaults to `openai/gpt-4o-mini` |
| `LINQ_API_KEY` | Bearer token for Linq's outbound message API |
| `LINQ_PHONE_NUMBER` | The agent's own number (reported by `/health`) |
| `LINQ_API_URL` | Defaults to `https://api.linqapp.com/api/partner/v3/messages` |
| `LINQ_WEBHOOK_SECRET` | `whsec_...` signing secret. Unset = unsigned requests accepted |
| `BROWSERBASE_API_KEY` | Browserbase auth |
| `BROWSERBASE_PROJECT_ID` | **Required.** From <https://www.browserbase.com/settings> |
| `PORT` | HTTP port, default `3000` |
| `PUBLIC_BASE_URL` | Public origin for screenshot links. Blank = auto-detect ngrok |
| `TASK_TIMEOUT_MS` | Hard ceiling per browser task, default `180000` |

## Run

```bash
node index.js
```

Then, in a second terminal, expose it:

```bash
./ngrok.exe http 3000
```

The app reads ngrok's local API (`http://127.0.0.1:4040/api/tunnels`) on its own,
so screenshot links resolve to the public tunnel with no extra configuration.
Set `PUBLIC_BASE_URL` instead if you deploy somewhere with a fixed hostname.

Finally, point your Linq webhook at `https://<your-ngrok-host>/webhook/linq`
for the `message.received` event.

### ngrok first-run

ngrok v3 needs an authtoken once per machine:

```bash
./ngrok.exe config add-authtoken <TOKEN>   # from dashboard.ngrok.com
```

## Endpoints

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/webhook/linq` | Linq inbound webhook. Acks in <1s, works in background |
| `GET` | `/health` | Config sanity check + detected public URL |
| `GET` | `/last_action.png` | Latest screenshot, served from `./public` |
| `GET` | `/` | Service banner |

## Webhook security

Inbound webhooks are verified against the [Standard Webhooks](https://www.standardwebhooks.com/)
spec that Linq signs with: HMAC-SHA256 over `{webhook-id}.{webhook-timestamp}.{raw body}`,
keyed by the base64-decoded `whsec_` secret, compared in constant time, with a
300-second replay window. Anything that fails gets a `401`.

This matters because the endpoint is publicly reachable through the tunnel —
without it, anyone who learned the URL could trigger Browserbase sessions and
outbound messages on your account.

Leaving `LINQ_WEBHOOK_SECRET` unset disables the check (and logs a warning at
startup), which is only appropriate for local testing.

## Testing without iMessage

With `LINQ_WEBHOOK_SECRET` unset, a plain request works:

```bash
curl -X POST http://localhost:3000/webhook/linq \
  -H "Content-Type: application/json" \
  -d '{"senderNumber":"+14155559876","messageText":"what is the top story on Hacker News?"}'
```

The webhook accepts both the flat shape above and Linq's real v3 envelope
(`data.sender_handle.handle` + `data.parts[].value`).

With the secret set, requests must carry valid `webhook-id`,
`webhook-timestamp` and `webhook-signature` headers. Note that a request which
passes verification runs a real browser task and bills a Browserbase session.

## Notes

- Stagehand v4 replaced the older `env: "BROWSERBASE"` / `stagehand.agent()` API.
  This app uses the current surface: `browserbase.launch()` → `Stagehand.create({ browser })`
  → `act()` / `extract()`.
- Screenshot links carry a `?v=<timestamp>` cache-buster, because the filename is
  fixed and iMessage would otherwise show the previous run's image.
- Failed intermediate steps are logged and skipped rather than aborting, so the
  sender still gets a screenshot and a summary of wherever the browser ended up.
- `.env` holds live credentials and is gitignored. Rotate the keys if this folder
  is ever shared.
