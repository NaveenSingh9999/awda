# awda

Use opencode from any app over HTTP. Same models, tools, and sessions as the
opencode CLI, behind an OpenAI-style API.

## Why

Opencode's free models only answer inside opencode. If you call
`https://opencode.ai/zen/v1` from your own code, you get this:

```json
{"error": {"type": "FreeTierError",
  "message": "OpenCode's free tier can only be used from within OpenCode"}}
```

Awda talks to a local `opencode serve` instead, which passes the check. Tested:
9 of 11 free models answer through it.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env
```

Start it:

```bash
./awda start          # linux / mac
awda.bat start        # windows
python awda.py start  # anywhere
```

Check it works:

```bash
./awda doctor
curl localhost:4098/health
```

To open it to your network: set `AWDA_HOST=0.0.0.0` in `.env`.
To lock it with a token: set `AWDA_TOKEN=something` in `.env`, then send
`Authorization: Bearer something` with each request.

Stop it:

```bash
./awda stop
```

## What you get

- Chat endpoint that looks like OpenAI's. Point any OpenAI client at it.
- Free models: `space-bunny-free`, `longcat-2.5-preview-free`,
  `mimo-v2.6-flash-free`, `big-pickle`, `ling-3.1-flash-free`,
  `muse-spark-1.3-contributor-free`, `nemotron-3-ultra-free`,
  `nemotron-3.5-lightning-free`, `step-5-preview-free`.
- Pick model, agent, and effort per request. No restarts.
- Send files and images, like `opencode run --file`.
- Streaming that follows the OpenAI format.
- Every reply is JSON, including errors.
- Sessions, so chats remember context.

Two models are dead and blocked on purpose: `exo-free` (removed by opencode)
and `ling-3.0-flash-fin-free` (down upstream). They are hidden from
`/v1/models` and fail fast with a clear error.

## Chat

Normal reply:

```bash
curl -X POST localhost:4098/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "space-bunny-free",
       "messages": [{"role": "user", "content": "Explain event loops"}]}'
```

Streamed reply:

```bash
curl -N -X POST localhost:4098/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "longcat-2.5-preview-free", "stream": true,
       "messages": [{"role": "user", "content": "Hi"}]}'
```

With an image:

```bash
curl -X POST localhost:4098/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "space-bunny-free",
       "messages": [{"role": "user", "content": [
         {"type": "text", "text": "What is in this image?"},
         {"type": "image_url", "image_url": {"url": "data:image/png;base64,..."}}]}]}'
```

Per-request fields:

| Field | What it does |
|---|---|
| `model` | model id, or `provider/model` |
| `provider` | overrides the provider part of `model` |
| `variant` | effort like `low` or `high`; empty = default |
| `agent` | which agent answers, e.g. `general` |
| `tier` | `chat`, `agent`, or `public`; ignored if `agent` is set |
| `session_key` | which chat this belongs to; default `api-default` |
| `title` | names the session |
| `files` | extra file parts for the model |
| `images` | image URLs (`data:` or `http(s)`) |

Plain replies also include `awda_notices` and `awda_tools` next to the normal
OpenAI fields.

## Raw prompt endpoint

`POST /api/prompt` takes plain `text` instead of `messages`. Same extra
fields. Good for bots and scripts:

```bash
curl -X POST localhost:4098/api/prompt \
  -H 'Content-Type: application/json' \
  -d '{"text": "List the python files here using your tools",
       "session_key": "coder", "agent": "general",
       "model": "opencode/mimo-v2.6-flash-free"}'
```

Reply:

```json
{"ok": true, "text": "...", "notices": [], "tools": ["`glob` *.py"],
 "model": "opencode/mimo-v2.6-flash-free", "variant": "low", "agent": "general"}
```

## Sessions and models

```bash
curl localhost:4098/api/sessions
curl -X DELETE localhost:4098/api/session/coder
curl localhost:4098/v1/models
```

Use a different `session_key` per user or channel and each gets its own
remembered chat. Delete a session to start over.

## Python example

```python
import json, urllib.request

BASE = "http://localhost:4098"

def chat(messages, model="space-bunny-free", stream=False, **kw):
    req = urllib.request.Request(
        f"{BASE}/v1/chat/completions",
        data=json.dumps({"messages": messages, "model": model,
                         "stream": stream, **kw}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=300) as r:
        if not stream:
            return json.load(r)["choices"][0]["message"]["content"]
        for line in r:
            line = line.decode().strip()
            if line.startswith("data: ") and line[6:] != "[DONE]":
                d = json.loads(line[6:])
                print(d["choices"][0]["delta"].get("content", ""), end="")

print(chat([{"role": "user", "content": "Reply with exactly: OK"}]))
```

## Uses

- Call free models from any script or app, no API keys.
- Put a bot (Discord, Telegram) in front of it. One `session_key` per
  user gives each person their own chat.
- Let the model use tools (list files, run commands, fetch pages) from
  your own program.
- Share one model endpoint on your network.

## Settings (`.env`)

| Key | Default | What it does |
|---|---|---|
| `AWDA_HOST` | `127.0.0.1` | `0.0.0.0` opens it to your network |
| `AWDA_PORT` | `4098` | port for this API |
| `AWDA_TOKEN` | empty | password; empty means no password |
| `AWDA_CORS` | `*` | which sites may call it from a browser |
| `OPENCODE_PORT` | `4096` | port for the opencode server it runs |
| `OPENCODE_MODEL_PROVIDER` | `opencode` | default provider |
| `OPENCODE_MODEL_ID` | `space-bunny-free` | default model |
| `OPENCODE_MODEL_VARIANT` | `low` | default effort |
| `AWDA_AGENT` | `general` | default agent |
| `AWDA_DEAD_MODELS` | `exo-free,ling-3.0-flash-fin-free` | models to block |
| `AWDA_ALLOW_FILE_URLS` | off | leave off. `file://` parts let callers read server files |

## Files

- `server.py` — the API
- `bridge.py` — talks to `opencode serve` (taken from dex, made faster)
- `awda.py` / `awda` / `awda.bat` — start/stop/status/doctor on any OS
- `config.py` — reads `.env`
