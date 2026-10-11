"""Awda — opencode serve wrapped as an API you can hit from anywhere.

Same streaming core as dex (bridge.py: SSE delta filtering, idle end-of-turn,
per-session locks, permission auto-reject), exposed as OpenAI-compatible HTTP:

    GET  /health
    GET  /v1/models
    POST /v1/chat/completions   (OpenAI shape, stream + non-stream;
                                 per-call: model/provider/variant/agent/tier/
                                 session_key/title/files/images)
    POST /api/prompt            (raw + files/images/title)
    GET  /api/sessions
    DELETE /api/session/{key}

Every response is JSON — including errors:
    {"error": {"type": "...", "message": "...", "status": 400}}

Streaming is exact OpenAI SSE: `data: <chunk>` lines, `data: [DONE]` at the
end. Text deltas are the ONLY thing in `delta.content`, so strict parsers see
clean output. Tool activity and notices ride as SSE comment lines
(`: {"event":"tool",...}`) — valid SSE, ignored by OpenAI parsers, visible to
raw consumers. A mid-stream failure sends `data: {"error": {...}}` instead.

Auth: if AWDA_TOKEN is set, every endpoint except /health requires
`Authorization: Bearer <token>`. Bind AWDA_HOST=0.0.0.0 to serve the LAN.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
import urllib.parse
import uuid

import aiohttp
from aiohttp import web

import bridge
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("awda")

routes = web.RouteTableDef()

AWDA_VERSION = "1.1.0"

# ── JSON-only output ─────────────────────────────────────────────
# Every error leaves this server as {"error": {...}} JSON — never plain text.

_ERROR_TYPES = {
    400: "invalid_request_error",
    401: "authentication_error",
    403: "permission_error",
    404: "not_found_error",
    502: "bad_gateway_error",
    503: "service_unavailable_error",
}


def _cors_headers() -> dict:
    return {
        "Access-Control-Allow-Origin": config.AWDA_CORS,
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    }


@web.middleware
async def _middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        return web.Response(headers=_cors_headers())
    try:
        resp = await handler(request)
    except web.HTTPException as e:
        return web.json_response(
            {"error": {"type": _ERROR_TYPES.get(e.status, "api_error"),
                       "message": e.text, "status": e.status}},
            status=e.status, headers=_cors_headers())
    except asyncio.CancelledError:
        raise
    except Exception as e:  # never leak a traceback as HTML/text
        logger.exception("unhandled error")
        return web.json_response(
            {"error": {"type": "internal_error",
                       "message": f"{type(e).__name__}: {e}", "status": 500}},
            status=500, headers=_cors_headers())
    resp.headers.update(_cors_headers())
    return resp


def _auth_ok(request: web.Request) -> bool:
    if not config.AWDA_TOKEN:
        return True
    return request.headers.get("Authorization", "") == f"Bearer {config.AWDA_TOKEN}"


def _require_auth(handler):
    async def wrapper(request: web.Request):
        if not _auth_ok(request):
            raise web.HTTPUnauthorized(text="bad or missing Bearer token")
        return await handler(request)

    wrapper.__name__ = handler.__name__
    return wrapper


# ── per-call knobs ───────────────────────────────────────────────
def _resolve_call(body: dict) -> dict:
    """Pull every per-call knob out of the request body.

    All optional — anything missing falls back to the server default:
      model:    plain id ("space-bunny-free") or "provider/model"
      provider: explicit override, beats the prefix inside model
      variant:  string or null — null/"" means server default
      agent:    explicit override, beats tier mapping (e.g. "general")
      tier:     chat|agent|public, only used when agent is not given
      session_key / user: session bucket, default "api-default"
      title:    session title (like `opencode run --title`), stored + sent on create
      files:    caller-built opencode parts (sanitized, file:// rejected)
      images:   list of image URLs (data: or http(s)) → file parts
    """
    raw_model = body.get("model")
    if raw_model and "/" in str(raw_model):
        prefix_provider, _, bare = str(raw_model).partition("/")
        model_id, provider_id = bare.strip(), prefix_provider.strip()
    elif raw_model:
        model_id, provider_id = str(raw_model).strip(), config.OPENCODE_MODEL_PROVIDER
    else:
        model_id, provider_id = config.OPENCODE_MODEL_ID, config.OPENCODE_MODEL_PROVIDER
    if body.get("provider"):
        provider_id = str(body["provider"]).strip()
    raw_variant = body.get("variant", None)
    variant = (
        config.OPENCODE_MODEL_VARIANT
        if raw_variant is None or str(raw_variant).strip() == ""
        else str(raw_variant).strip()
    )
    raw_agent = body.get("agent", None)
    agent = (
        None if raw_agent is None or str(raw_agent).strip() == ""
        else str(raw_agent).strip()
    )
    tier = str(body.get("tier", "chat"))
    session_key = str(
        body.get("session_key") or body.get("user") or "api-default"
    )[:128]
    raw_title = body.get("title", None)
    title = (
        None if raw_title is None or str(raw_title).strip() == ""
        else str(raw_title).strip()[:120]
    )
    return {
        "model_id": model_id, "provider_id": provider_id, "variant": variant,
        "agent": agent, "tier": tier, "session_key": session_key,
        "title": title,
        "files": _sanitize_files(body.get("files")),
        "images": body.get("images") or [],
    }


# ── media (files / images, like `opencode run --file`) ──────────
# Cap for server-side fetches of caller-named http(s) media URLs. 8MB matches
# dex's per-file budget.
MEDIA_FETCH_MAX_BYTES = 8 * 1024 * 1024

_IMAGE_EXT = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
}


def _mime_from_data_url(url: str) -> str:
    head = url[5:].split(",", 1)[0]
    return (head.split(";")[0] or "application/octet-stream").lower()


def _filename_for(url: str, mime: str, i: int) -> str:
    path = urllib.parse.urlparse(url).path or ""
    base = path.rsplit("/", 1)[-1][:64] if "/" in path else path[:64]
    if base and "." in base:
        return base
    ext = {v: k for k, v in _IMAGE_EXT.items()}.get(mime, ".bin")
    return f"image-{i}{ext}"


async def _fetch_url_bytes(url: str) -> tuple[bytes, str]:
    """Fetch caller-named http(s) bytes with a hard cap. file:// never allowed."""
    s = await bridge._shared()
    async with s.get(
        url, timeout=aiohttp.ClientTimeout(total=30),
        headers={"User-Agent": "awda/1.0"},
    ) as resp:
        resp.raise_for_status()
        ctype = resp.headers.get("Content-Type", "")
        buf = bytearray()
        async for chunk in resp.content.iter_chunked(65536):
            buf.extend(chunk)
            if len(buf) > MEDIA_FETCH_MAX_BYTES:
                raise web.HTTPBadRequest(
                    text=f"media URL exceeds {MEDIA_FETCH_MAX_BYTES} bytes: {url[:120]}"
                )
        return bytes(buf), ctype


async def _media_part(url: str, i: int) -> dict:
    """One opencode file part from a caller-supplied URL.

    Safety (from dex's attachments.py): a file part tells the *server* to read
    the URL, so user input must never become a file:// URL. Only data: URLs
    (bytes the caller already holds) and explicit http(s) URLs (fetched here
    into capped bytes, then re-encoded as data:) are accepted.
    """
    if url.startswith("file:"):
        raise web.HTTPBadRequest(text="file:// URLs are not accepted — send bytes")
    if url.startswith("data:"):
        mime = _mime_from_data_url(url)
        return {"type": "file", "mime": mime,
                "filename": _filename_for(url, mime, i), "url": url}
    if url.startswith("http://") or url.startswith("https://"):
        raw, ctype = await _fetch_url_bytes(url)
        mime = (ctype or "").split(";")[0].strip().lower() or "application/octet-stream"
        b64 = base64.b64encode(raw).decode()
        return {"type": "file", "mime": mime,
                "filename": _filename_for(url, mime, i),
                "url": f"data:{mime};base64,{b64}"}
    raise web.HTTPBadRequest(text=f"unsupported media URL scheme: {url[:60]}")


def _sanitize_files(files) -> list[dict]:
    """Pass caller-built opencode parts through, minus the file:// hazard."""
    if not files:
        return []
    if not isinstance(files, list):
        raise web.HTTPBadRequest(text="files must be a list of opencode parts")
    out = []
    for p in files:
        if not isinstance(p, dict):
            raise web.HTTPBadRequest(text="each files[] entry must be an object")
        url = str(p.get("url", ""))
        if url.startswith("file:") and not config.AWDA_ALLOW_FILE_URLS:
            raise web.HTTPBadRequest(text="file:// URLs are not accepted — send bytes")
        out.append(p)
    return out


async def _messages_to_parts(messages: list[dict]) -> tuple[str, list[dict]]:
    """Split OpenAI messages into (prompt text, opencode file parts).

    Handles the real-CLI equivalents: plain text, multi-part content with
    image_url entries, and input_image payloads — like `opencode run --file`.
    """
    texts, files = [], []
    for m in messages or []:
        role = m.get("role", "user")
        content = m.get("content", "")
        if isinstance(content, str):
            if content.strip():
                texts.append(f"{role}: {content}" if len(messages) > 1 else content)
            continue
        if isinstance(content, list):
            for part in content:
                if not isinstance(part, dict):
                    texts.append(f"{role}: {part}")
                    continue
                ptype = part.get("type", "")
                if ptype == "text" and part.get("text"):
                    texts.append(part["text"])
                elif ptype in ("image_url", "image"):
                    iu = part.get("image_url", "")
                    url = iu.get("url", "") if isinstance(iu, dict) else (iu or part.get("url", ""))
                    if url:
                        files.append(await _media_part(url, len(files)))
                elif ptype == "input_image":
                    url = part.get("image_url", "")
                    if url:
                        files.append(await _media_part(url, len(files)))
    return "\n\n".join(texts), files


async def _collect_media(body: dict, messages: list[dict]) -> tuple[str, list[dict]]:
    prompt, media = await _messages_to_parts(messages)
    for u in (body.get("images") or []):
        media.append(await _media_part(str(u), len(media)))
    return prompt, media


# ── model list (cached: serve round-trip is the slowest non-LLM call) ──
_MODELS_CACHE = {"at": 0.0, "data": []}
MODELS_TTL = 30.0


async def _live_models() -> list[dict]:
    now = time.monotonic()
    if _MODELS_CACHE["data"] and now - _MODELS_CACHE["at"] < MODELS_TTL:
        return _MODELS_CACHE["data"]
    try:
        s = await bridge._shared()
        async with s.get(
            f"{bridge.SERVER_URL}/config/providers", timeout=20
        ) as resp:
            data = await resp.json()
    except Exception as e:
        raise web.HTTPBadGateway(text=f"opencode serve unreachable: {e}")
    out = []
    for prov in data.get("providers", []):
        pid = prov.get("id", "")
        for mid in (prov.get("models") or {}).keys():
            if mid in config.DEAD_MODELS:
                continue
            out.append({"id": mid, "object": "model", "owned_by": pid})
    _MODELS_CACHE.update(at=now, data=out)
    return out


# ── streaming (exact OpenAI SSE) ─────────────────────────────────
def _chunk(cid: str, created: int, model: str, delta: dict,
           finish: str | None = None) -> bytes:
    return ("data: " + json.dumps(
        {"id": cid, "object": "chat.completion.chunk", "created": created,
         "model": model,
         "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]},
        separators=(",", ":")) + "\n\n").encode()


def _comment(event: str, data: str) -> bytes:
    return (": " + json.dumps({"event": event, "data": data},
                              separators=(",", ":")) + "\n\n").encode()


async def _run_stream(request: web.Request, prompt: str, files: list[dict],
                      call: dict) -> web.StreamResponse:
    """One perfect OpenAI SSE stream. Text ONLY in delta.content."""
    model_id = call["model_id"]
    resp = web.StreamResponse(headers={
        "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no", **_cors_headers()})
    await resp.prepare(request)
    cid = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())
    try:
        await resp.write(_chunk(cid, created, model_id,
                                {"role": "assistant", "content": ""}))
        async for kind, val in bridge.stream_turn(
            prompt, call["session_key"], tier=call["tier"], agent=call["agent"],
            files=files or None, model_id=model_id,
            provider_id=call["provider_id"], variant=call["variant"],
            title=call["title"],
        ):
            if kind == "text":
                await resp.write(_chunk(cid, created, model_id, {"content": val}))
            elif kind == "tool":
                await resp.write(_comment("tool", val))
            elif kind == "notice":
                await resp.write(_comment("notice", val))
        await resp.write(_chunk(cid, created, model_id, {}, "stop"))
        await resp.write(b"data: [DONE]\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass  # client went away — nothing to report to
    except Exception as e:
        logger.exception("stream failed")
        try:
            await resp.write(("data: " + json.dumps(
                {"error": {"type": "stream_error",
                           "message": f"{type(e).__name__}: {e}"}},
                separators=(",", ":")) + "\n\n").encode())
        except Exception:
            pass
    finally:
        try:
            await resp.write_eof()
        except Exception:
            pass
    return resp


# ── routes (all JSON) ────────────────────────────────────────────
@routes.get("/health")
async def health(request: web.Request):
    return web.json_response(
        {
            "ok": True,
            "service": "awda",
            "version": AWDA_VERSION,
            "auth": "on" if config.AWDA_TOKEN else "off",
            "default_model": f"{config.OPENCODE_MODEL_PROVIDER}/{config.OPENCODE_MODEL_ID}",
            "default_variant": config.OPENCODE_MODEL_VARIANT,
            "default_agent": config.AWDA_AGENT,
            "dead_models": sorted(config.DEAD_MODELS),
            "streaming": "openai-sse",
            "errors": "json",
            "per_call": ["model", "provider", "variant", "agent", "tier",
                         "session_key", "title", "files", "images"],
        }
    )


@routes.get("/v1/models")
@_require_auth
async def list_models(request: web.Request):
    return web.json_response({"object": "list", "data": await _live_models()})


@routes.post("/v1/chat/completions")
@_require_auth
async def chat_completions(request: web.Request):
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="expected JSON body")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text="expected JSON object")

    messages = body.get("messages", [])
    if not isinstance(messages, list) or not messages:
        raise web.HTTPBadRequest(text="messages[] is required")
    call = _resolve_call(body)
    if call["model_id"] in config.DEAD_MODELS:
        raise web.HTTPBadRequest(
            text=f"model {call['model_id']} is dead upstream (deprecated/unavailable)"
        )
    prompt, media = await _collect_media(body, messages)
    files = (call["files"] or []) + media

    if bool(body.get("stream")):
        return await _run_stream(request, prompt, files, call)

    text_parts, notices, tools = [], [], []
    async for kind, val in bridge.stream_turn(
        prompt, call["session_key"], tier=call["tier"], agent=call["agent"],
        files=files or None, model_id=call["model_id"],
        provider_id=call["provider_id"], variant=call["variant"],
        title=call["title"],
    ):
        if kind == "text":
            text_parts.append(val)
        elif kind == "notice":
            notices.append(val)
        elif kind == "tool":
            tools.append(val)
    return web.json_response(
        {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": call["model_id"],
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant",
                                "content": "".join(text_parts).strip()
                                or "No response generated."},
                    "finish_reason": "stop",
                }
            ],
            "usage": {},
            "awda_notices": notices,
            "awda_tools": tools,
        }
    )


@routes.post("/api/prompt")
@_require_auth
async def api_prompt(request: web.Request):
    """Raw passthrough: {"text","session_key","tier","model","provider",
    "variant","agent","title","files","images"} — all JSON out."""
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text="expected JSON body")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text="expected JSON object")
    text = str(body.get("text", "")).strip()
    if not text:
        raise web.HTTPBadRequest(text="text is required")
    call = _resolve_call(body)
    if call["model_id"] in config.DEAD_MODELS:
        raise web.HTTPBadRequest(
            text=f"model {call['model_id']} is dead upstream (deprecated/unavailable)"
        )
    files = list(call["files"] or [])
    for u in call["images"]:
        files.append(await _media_part(str(u), len(files)))
    out, notices, tools = [], [], []
    async for kind, val in bridge.stream_turn(
        text, call["session_key"], tier=call["tier"], files=files or None,
        agent=call["agent"], model_id=call["model_id"],
        provider_id=call["provider_id"], variant=call["variant"],
        title=call["title"],
    ):
        if kind == "text":
            out.append(val)
        elif kind == "notice":
            notices.append(val)
        elif kind == "tool":
            tools.append(val)
    return web.json_response(
        {
            "ok": True,
            "text": "".join(out).strip() or "No response generated.",
            "notices": notices,
            "tools": tools,
            "model": f"{call['provider_id']}/{call['model_id']}",
            "variant": call["variant"],
            "agent": call["agent"] or bridge.agent_for(call["tier"]),
        }
    )


@routes.get("/api/sessions")
@_require_auth
async def api_sessions(request: web.Request):
    return web.json_response({"ok": True, "sessions": bridge.list_sessions()})


@routes.delete("/api/session/{key}")
@_require_auth
async def api_session_delete(request: web.Request):
    bridge.clear_session(request.match_info["key"])
    return web.json_response({"ok": True})


def main():
    app = web.Application(middlewares=[_middleware])
    app.add_routes(routes)
    app.on_shutdown.append(_shutdown)
    logger.info(
        "awda %s on http://%s:%d (serve %s, model %s/%s)",
        AWDA_VERSION, config.AWDA_HOST, config.AWDA_PORT,
        bridge.SERVER_URL,
        config.OPENCODE_MODEL_PROVIDER, config.OPENCODE_MODEL_ID,
    )
    web.run_app(app, host=config.AWDA_HOST, port=config.AWDA_PORT, print=None)


async def _shutdown(app):
    try:
        await bridge._close_shared()
    except Exception:
        pass


if __name__ == "__main__":
    main()
