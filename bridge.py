"""Awda ↔ OpenCode bridge (copied from Dex verbatim — it is fast and proven).

## Streaming model

`POST /session/{id}/prompt_async` returns 204 immediately and the answer arrives on
the global `GET /event` SSE stream. This module owns that stream.

Three facts about that stream drive the whole implementation, all established by
capturing a live turn (opencode 1.18.9) rather than from documentation:

1. `message.part.delta` carries deltas for **every** part in a message, including
   `type: "reasoning"`. Filtering by `messageID` alone leaks the model's internal
   monologue into the reply. Deltas are therefore held until the part's type is known
   from `message.part.updated`, then yielded only for `text` parts.

2. For a text part, `message.part.updated` fires with `text: ""` when the part is
   created and once with the **complete** text at the end (observed: 0, then 2133).
   There is no incremental snapshot to fall back on, so correct delta handling is what
   makes live streaming possible at all.

3. Turn completion is `properties.status.type == "idle"`. The old code read
   `properties.type`, which is always absent, and so treated every status event —
   including `busy` — as end-of-turn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import AsyncIterator, Optional

import aiohttp

import config

logger = logging.getLogger("awda.bridge")

SESSION_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sessions")
os.makedirs(SESSION_DIR, exist_ok=True)
MODES_FILE = os.path.join(SESSION_DIR, "modes.json")

SERVER_URL = f"http://127.0.0.1:{config.OPENCODE_PORT}"

# One in-flight request per session. Without this a slow prompt and a quick one race
# on the same opencode session and the loser is silently dropped.
_LOCKS: dict[str, asyncio.Lock] = {}


def _lock_for(key: str) -> asyncio.Lock:
    lock = _LOCKS.get(key)
    if lock is None:
        lock = _LOCKS[key] = asyncio.Lock()
    return lock


# ── local persistence ──────────────────────────────────────────
def _session_file(channel_id: str) -> str:
    return os.path.join(SESSION_DIR, f"{channel_id}.json")


def _load_session(key: str) -> Optional[str]:
    path = _session_file(key)
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f).get("session_id")
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _session_info(key: str) -> dict:
    """Full stored record (session_id + title), for the session list API."""
    path = _session_file(key)
    if os.path.exists(path):
        try:
            with open(path) as f:
                d = json.load(f)
                return {"key": key, "session_id": d.get("session_id"),
                        "title": d.get("title", "")}
        except (json.JSONDecodeError, OSError):
            pass
    return {"key": key, "session_id": None, "title": ""}


def list_sessions() -> list[dict]:
    out = []
    try:
        names = os.listdir(SESSION_DIR)
    except OSError:
        return out
    for name in sorted(names):
        if not name.endswith(".json") or name == "modes.json":
            continue
        out.append(_session_info(name[:-5]))
    return out


def _save_session(key: str, session_id: str, title: str | None = None):
    tmp = _session_file(key) + ".tmp"
    try:
        with open(_session_file(key)) as f:
            prev = json.load(f)
    except (OSError, json.JSONDecodeError):
        prev = {}
    prev.update({"session_id": session_id})
    if title is not None:
        prev["title"] = title
    with open(tmp, "w") as f:
        json.dump(prev, f)
    os.replace(tmp, _session_file(key))


def clear_session(key: str):
    path = _session_file(key)
    if os.path.exists(path):
        os.remove(path)


def _load_modes() -> dict:
    if os.path.exists(MODES_FILE):
        try:
            with open(MODES_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_modes(modes: dict):
    tmp = MODES_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(modes, f, indent=2)
    os.replace(tmp, MODES_FILE)


def get_channel_mode(key: str) -> str:
    return _load_modes().get(key, config.DEFAULT_TIER)


def set_channel_mode(key: str, mode: str):
    modes = _load_modes()
    modes[key] = mode
    _save_modes(modes)


# ── agent tiers ────────────────────────────────────────────────
# `public` is deny-by-default with no allow rules at all. The other two are defined in
# ~/dex/.opencode/opencode.json, which Dex owns — not in the owner's interactive
# opencode config, which is what previously decided this bot's permissions.
# The bot is DeXC: the original ~/.config/opencode/agent/deXC.md, copied in verbatim so
# Dex's own server can use it. Chat and public tiers both run on it; agent mode is the
# separate coding agent.
TIER_AGENT = {
    "public": "deXC",
    "chat": "deXC",
    "agent": "dex-agent",
}

# Human-readable tool summary per tier (bot.py in dex references this;
# it was missing upstream — define it here so /agent-style callers work).
TIER_DESC = {
    "public": "no tools (deny-by-default)",
    "chat": "talk, plan, read-only inspection",
    "agent": "full tools: files, commands, web",
}


def agent_for(tier: str) -> str:
    # Awda overrides the agent via AWDA_AGENT when set (default "general",
    # a built-in that exists in any serve config, not just dex's XDG root).
    override = getattr(config, "AWDA_AGENT", "")
    if override:
        return override
    return TIER_AGENT.get(tier, TIER_AGENT["public"])


def tier_for(is_owner: bool, requested: str) -> str:
    """The single place the owner/non-owner decision is made.

    A non-owner is pinned to `public` no matter what was requested. Modes are a UI
    convenience; this is the security boundary.
    """
    if not is_owner:
        return "public"
    return requested if requested in TIER_AGENT else config.DEFAULT_TIER


# ── server plumbing ────────────────────────────────────────────
def _http() -> aiohttp.ClientSession:
    # Must be a plain function. If this were `async def`, `async with _http() as s`
    # would never await it and every session helper would silently return empty.
    # Kept for the long-lived SSE listener (one dedicated connection per turn)
    # and for backwards compat. Short calls below use the shared session.
    return aiohttp.ClientSession()


# Persistent connection pool for all short serve calls (session CRUD, prompt
# post, permission replies). New TCP+handshake per call was the biggest
# overhead after the fixed 0.7s subscribe sleep — now both are gone.
_SHARED: aiohttp.ClientSession | None = None
_SHARED_LOOP = None


async def _shared() -> aiohttp.ClientSession:
    global _SHARED, _SHARED_LOOP
    loop = asyncio.get_running_loop()
    if _SHARED is None or _SHARED.closed or _SHARED_LOOP is not loop:
        if _SHARED is not None and not _SHARED.closed:
            try:
                await _SHARED.close()
            except Exception:
                pass
        _SHARED = aiohttp.ClientSession()
        _SHARED_LOOP = loop
    return _SHARED


async def _close_shared():
    global _SHARED, _SHARED_LOOP
    if _SHARED is not None and not _SHARED.closed:
        try:
            await _SHARED.close()
        except Exception:
            pass
    _SHARED, _SHARED_LOOP = None, None


async def _create_session(title: str | None = None) -> str:
    s = await _shared()
    async with s.post(
        f"{SERVER_URL}/session",
        json={"title": title} if title else {},
        timeout=aiohttp.ClientTimeout(total=30),
    ) as resp:
        resp.raise_for_status()
        return (await resp.json())["id"]


async def _delete_session(session_id: str):
    try:
        s = await _shared()
        async with s.delete(
            f"{SERVER_URL}/session/{session_id}",
            timeout=aiohttp.ClientTimeout(total=15),
        ):
            pass
    except Exception as e:
        logger.debug("delete session %s: %s", session_id, e)


async def _busy_sessions() -> dict:
    try:
        s = await _shared()
        async with s.get(
            f"{SERVER_URL}/session/status",
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            return await resp.json()
    except Exception as e:
        logger.debug("session status: %s", e)
        return {}


async def reap_stale_sessions() -> int:
    """Delete stored sessions the server still thinks are busy.

    A request killed mid-flight leaves its session busy forever, which silently blocks
    every later message in that channel until the timeout.
    """
    reaped = 0
    busy = await _busy_sessions()
    if not busy:
        return 0
    for name in os.listdir(SESSION_DIR):
        if not name.endswith(".json") or name == "modes.json":
            continue
        key = name[:-5]
        sid = _load_session(key)
        if sid and sid in busy:
            logger.warning("session %s still busy — rotating for %s", sid, key)
            await _delete_session(sid)
            clear_session(key)
            reaped += 1
    return reaped


async def _usable_session(key: str, title: str | None = None) -> str:
    sid = _load_session(key)
    if sid:
        if sid in await _busy_sessions():
            logger.warning("session %s busy — rotating for %s", sid, key)
            await _delete_session(sid)
            clear_session(key)
        else:
            if title is not None:
                _save_session(key, sid, title)
            return sid
    sid = await _create_session(title)
    _save_session(key, sid, title)
    return sid


def _tool_label(part: dict) -> str:
    """A one-line, readable description of what a tool call is doing."""
    tool = part.get("tool") or "tool"
    state = part.get("state") or {}
    inp = state.get("input") or {}
    target = (
        inp.get("filePath") or inp.get("path") or inp.get("pattern")
        or inp.get("command") or inp.get("query") or inp.get("url")
        or inp.get("description") or inp.get("prompt") or ""
    )
    target = " ".join(str(target).split())
    if len(target) > 70:
        target = target[:67] + "..."
    return f"`{tool}` {target}".strip()


class Turn:
    """Classifies parts of one assistant turn so deltas can be trusted.

    This exists because deltas arrive before we know what part they belong to.
    """

    def __init__(self) -> None:
        self.kind: dict[str, str] = {}        # partID -> "text" | "reasoning" | ...
        self.dead: set[str] = set()           # parts whose output is not user-facing
        self.pending: list[tuple[str, str]] = []   # (partID, delta) awaiting a kind
        self.text_parts: list[str] = []       # text partIDs, in the order created
        self.seen_tools: set[str] = set()

    def observe_part(self, part: dict):
        pid, ptype = part.get("id"), part.get("type")
        if not pid:
            return
        self.kind[pid] = ptype
        if ptype == "text":
            if pid not in self.text_parts:
                self.text_parts.append(pid)
            self.pending = [p for p in self.pending if p[0] != pid]
        elif ptype in ("reasoning", "step-start", "step-finish"):
            self.dead.add(pid)
            self.pending = [p for p in self.pending if p[0] != pid]

    def accept(self, part_id: str) -> bool:
        """Is a delta on this part part of the answer the user should see?"""
        if part_id in self.dead:
            return False
        return self.kind.get(part_id) == "text"


# ── the actual call ────────────────────────────────────────────
async def stream_turn(
    text: str,
    session_key: str,
    tier: str = "chat",
    files: Optional[list[dict]] = None,
    model_id: Optional[str] = None,
    provider_id: Optional[str] = None,
    variant: Optional[str] = None,
    agent: Optional[str] = None,
    title: Optional[str] = None,
) -> AsyncIterator[tuple[str, str]]:
    """Stream one turn. Yields (channel, value) pairs.

    Channels:
      text     — user-facing answer fragment (text parts only, never reasoning)
      tool     — a tool is running; value is a human-readable label
      notice   — something the user must know (error, denial, timeout)

    Guarantees:
      * one request per session at a time
      * reasoning parts are never surfaced
      * the turn ends on a real `idle`, not on the first status event
      * a timeout never leaves the session stuck
      * a permission ask is always denied, never auto-approved
    """
    agent = agent or agent_for(tier)
    async with _lock_for(session_key):
        try:
            session_id = await _usable_session(session_key, title)
        except Exception as e:
            yield ("notice", f"Couldn't reach OpenCode: {e}")
            return

        parts: list[dict] = list(files or [])
        parts.append({"type": "text", "text": text})

        payload = {
            "parts": parts,
            "model": {
                "modelID": model_id or config.OPENCODE_MODEL_ID,
                "providerID": provider_id or config.OPENCODE_MODEL_PROVIDER,
            },
            # `variant` is message-scoped in the REST API and is not persisted per
            # session, so it must ride along on every request.
            "variant": variant or config.OPENCODE_MODEL_VARIANT,
            "agent": agent,
        }

        turn = Turn()
        queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        state = {"assistant": None}
        connected = asyncio.Event()

        def put(item):
            loop.call_soon_threadsafe(queue.put_nowait, item)

        async def listen():
            try:
                async with _http() as s:
                    async with s.get(
                        f"{SERVER_URL}/event",
                        headers={"Accept": "text/event-stream"},
                        timeout=aiohttp.ClientTimeout(total=None, sock_read=None),
                    ) as r:
                        # Headers received = subscription attached. Signal the
                        # prompter immediately instead of a fixed 0.7s sleep.
                        loop.call_soon_threadsafe(connected.set)
                        async for raw in r.content:
                            line = raw.decode(errors="ignore").strip()
                            if not line.startswith("data:"):
                                continue
                            try:
                                ev = json.loads(line[5:])
                            except json.JSONDecodeError:
                                continue

                            typ = ev.get("type", "")
                            props = ev.get("properties") or {}
                            if props.get("sessionID") != session_id:
                                continue

                            if typ == "message.updated":
                                info = props.get("info") or {}
                                # NOTE: `info.finish` is NOT a turn terminator. A
                                # captured tool-using turn shows finish=tool-calls
                                # mid-turn, followed by a *second* assistant message
                                # carrying the actual answer. Breaking on `finish`
                                # truncates every agentic reply to its opening line.
                                # Only `session.status -> idle` ends the turn.
                                if info.get("role") == "assistant" and state["assistant"] is None:
                                    state["assistant"] = info.get("id")
                                    put(("assistant", None))

                            elif typ == "message.part.updated":
                                # Not filtered by messageID: each step of a turn is its
                                # own assistant message, so a messageID filter would
                                # discard every part after the first tool call. Parts
                                # are already scoped to our session, and the per-session
                                # lock guarantees no concurrent turn.
                                part = props.get("part") or {}
                                turn.observe_part(part)
                                if part.get("type") == "tool" and part.get("id") not in turn.seen_tools:
                                    turn.seen_tools.add(part.get("id"))
                                    label = _tool_label(part)
                                    if label:
                                        put(("tool", label))

                            elif typ == "message.part.delta":
                                pid = props.get("partID")
                                delta = props.get("delta")
                                if not pid or not isinstance(delta, str) or not delta:
                                    continue
                                if props.get("field") not in (None, "text"):
                                    continue
                                if turn.accept(pid):
                                    put(("text", delta))
                                elif pid not in turn.kind:
                                    # Type unknown yet — hold it rather than guess.
                                    turn.pending.append((pid, delta))

                            elif typ == "session.status":
                                # properties.status.type — NOT properties.type.
                                if (props.get("status") or {}).get("type") == "idle":
                                    put(("done", None))

                            elif typ == "session.error":
                                err = props.get("error") or {}
                                msg = err.get("data", {}).get("message") or err.get("message") or err.get("name")
                                put(("notice", f"OpenCode error: {msg or err}"))

                            elif typ == "permission.asked":
                                # This bot is unattended. An ask can only ever be a
                                # rejection — auto-approving would be a privilege hole.
                                put(("notice", "a tool needed permission — rejected (this bot runs unattended)"))
                                await _reply_permission(session_id, props.get("id"))

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("event stream ended: %s", e)

        listener = asyncio.create_task(listen())
        try:
            # Prompt the moment the SSE subscription is attached (usually
            # <50ms). The timeout is only a fallback — a slow attach must not
            # stall the turn like the old fixed sleep did on every call.
            await asyncio.wait_for(connected.wait(), timeout=5)
        except asyncio.TimeoutError:
            logger.debug("SSE attach slow — prompting anyway")

        try:
            s = await _shared()
            async with s.post(
                f"{SERVER_URL}/session/{session_id}/prompt_async",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    listener.cancel()
                    yield ("notice", f"OpenCode refused the request (HTTP {resp.status}): {body[:300]}")
                    return
        except Exception as e:
            listener.cancel()
            yield ("notice", f"Couldn't start the request: {e}")
            return

        got_text = False
        idle_ticks = 0
        deadline = time.monotonic() + config.OPENCODE_TIMEOUT
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    yield ("notice", f"Timed out after {config.OPENCODE_TIMEOUT}s. Ask again to start fresh.")
                    await _delete_session(session_id)
                    clear_session(session_key)
                    return

                try:
                    kind, val = await asyncio.wait_for(queue.get(), timeout=min(remaining, 15))
                except asyncio.TimeoutError:
                    # A quiet gap during a long tool run is normal. Only the overall
                    # deadline above ends the turn.
                    continue

                if kind == "text":
                    got_text = True
                    idle_ticks = 0
                    yield ("text", val)
                elif kind == "tool":
                    idle_ticks = 0
                    yield ("tool", val)
                elif kind == "notice":
                    idle_ticks = 0
                    yield ("notice", val)
                elif kind == "assistant":
                    idle_ticks = 0
                else:  # done
                    if got_text:
                        break
                    # No text yet: the assistant may still be running tools, so a done
                    # signal alone isn't the end of the turn. Keep listening briefly.
                    idle_ticks += 1
                    if idle_ticks >= 3:
                        break
        finally:
            listener.cancel()
            try:
                await listener
            except BaseException:
                pass

        # Anything still pending belonged to a part we never learned the type of.
        # Surface it rather than silently dropping the user's words.
        if turn.pending and not got_text:
            tail = "".join(d for _, d in turn.pending)
            if tail.strip():
                yield ("text", tail)

        if not got_text:
            yield ("notice", "No response generated.")


async def _reply_permission(session_id: str, request_id: str | None, reply: str = "reject"):
    """Answer a permission ask.

    Verified against the live server: POST /permission/{requestID}/reply with body
    {"reply": "once" | "always" | "reject"}. The requestID must itself start with
    "per". `reject` is the only value an unattended bot may ever send — "once" and
    "always" would be standing grants handed to a model that just asked.
    """
    if not request_id:
        return
    try:
        s = await _shared()
        async with s.post(
            f"{SERVER_URL}/permission/{request_id}/reply",
            json={"reply": reply},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status >= 400:
                logger.debug("permission reject failed: %s", (await resp.text())[:200])
    except Exception as e:
        logger.debug("permission reply failed: %s", e)


async def ask(text: str, session_key: str, tier: str = "chat", files=None,
          model_id: Optional[str] = None, provider_id: Optional[str] = None,
          variant: Optional[str] = None, agent: Optional[str] = None,
          title: Optional[str] = None) -> str:
    """Non-streaming convenience wrapper. Only used by the control API."""
    out = []
    async for ch, val in stream_turn(text, session_key, tier=tier, files=files,
                                     model_id=model_id, provider_id=provider_id,
                                     variant=variant, agent=agent, title=title):
        if ch == "text":
            out.append(val)
    return "".join(out).strip() or "No response generated."