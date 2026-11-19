"""
ShareOpen Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to a ShareOpen instance
via WebSocket and relays messages between ShareOpen users and the
Hermes agent. Provides a Telegram-free, fully local IM experience
with native tool support, multi-agent management, and agent-to-agent
communication.

Configuration in config.yaml::

    gateway:
      platforms:
        shareopen:
          enabled: true
          extra:
            url: "http://localhost:3021"
            token: "your-shareopen-agent-token"
            agent_id: "agent_xxx"    # optional, auto-discovered from token
            reconnect_delay: 5       # seconds

Or via environment variables (overrides config.yaml):
    SHAREOPEN_URL, SHAREOPEN_TOKEN, SHAREOPEN_AGENT_ID, SHAREOPEN_RECONNECT_DELAY
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports: BasePlatformAdapter and friends live in the main repo.
# We import at function/class level to avoid import errors when the plugin
# is discovered but the gateway hasn't been fully initialised yet.
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.session import SessionSource
from gateway.config import PlatformConfig, Platform


# ---------------------------------------------------------------------------
# ShareOpen Adapter
# ---------------------------------------------------------------------------


# Slash-command menu published to ShareOpen on connect (Telegram-parity
# autocomplete). The gateway natively handles every one of these — the menu
# only drives the frontend's "/" list. Keep descriptions in sync with
# `/help` output.
SHARE_COMMANDS = [
  {"name": "help", "description": "Show available commands", "category": "general"},
  {"name": "commands", "description": "Browse all commands and skills (paginated)", "category": "general"},
  {"name": "status", "description": "Show session, model, token, and context info", "category": "general"},
  {"name": "whoami", "description": "Show your slash command access level", "category": "general"},
  {"name": "profile", "description": "Show active profile name and home directory", "category": "general"},
  {"name": "usage", "description": "Show token usage and rate limits", "category": "general"},
  {"name": "context", "description": "Show context window view with usage stats", "category": "general" , "aliases": ["ctx"]},
  {"name": "version", "description": "Show Hermes Agent version", "category": "general" , "aliases": ["ver"]},
  {"name": "new", "description": "Start a new session (fresh ID + history)", "category": "session" , "aliases": ["reset"]},
  {"name": "resume", "description": "Resume a previously-named session", "category": "session" , "parameters": [{"name": "session", "type": "string", "description": "session id or name", "required": True}]},
  {"name": "sessions", "description": "Browse and resume previous sessions", "category": "session" , "parameters": [{"name": "name", "type": "string", "description": "filter by session name", "required": False}]},
  {"name": "title", "description": "Set a title for the current session", "category": "session" , "parameters": [{"name": "title", "type": "string", "description": "new conversation title", "required": True}]},
  {"name": "branch", "description": "Branch the current session", "category": "session" , "parameters": [{"name": "label", "type": "string", "description": "branch label", "required": True}]},
  {"name": "retry", "description": "Retry the last message", "category": "session"},
  {"name": "undo", "description": "Back up N user turns and re-prompt", "category": "session"},
  {"name": "compress", "description": "Compress conversation context", "category": "session"},
  {"name": "save", "description": "Export the current conversation", "category": "session" , "parameters": [{"name": "name", "type": "string", "description": "snapshot name", "required": True}]},
  {"name": "stop", "description": "Kill all running background processes", "category": "control" , "aliases": ["abort"]},
  {"name": "pause", "description": "Pause new work globally; 'off' resumes", "category": "control"},
  {"name": "queue", "description": "Queue a prompt for the next turn", "category": "control"},
  {"name": "steer", "description": "Inject guidance after the next tool call", "category": "control" , "parameters": [{"name": "instruction", "type": "string", "description": "course-correction text", "required": True}]},
  {"name": "busy", "description": "Control message behavior while working", "category": "control"},
  {"name": "approve", "description": "Approve a pending dangerous command", "category": "control"},
  {"name": "deny", "description": "Deny a pending dangerous command", "category": "control"},
  {"name": "model", "description": "Switch model (session-scoped; --global persists)", "category": "model" , "parameters": [{"name": "model", "type": "string", "description": "provider/model", "required": True, "default": "e.g. groq/openai/gpt-oss-120b"}] , "aliases": ["m"]},
  {"name": "personality", "description": "Set a predefined personality", "category": "model" , "parameters": [{"name": "preset", "type": "string", "description": "preset name", "required": True}] , "aliases": ["persona"]},
  {"name": "reasoning", "description": "Manage reasoning effort and display", "category": "model" , "parameters": [{"name": "level", "type": "string", "description": "0 (off) to 4 (max)", "required": False, "default": "2", "options": ["0", "1", "2", "3", "4"]}] , "aliases": ["think"]},
  {"name": "voice", "description": "Toggle voice mode", "category": "model" , "parameters": [{"name": "mode", "type": "string", "description": "toggle voice", "required": False, "options": ["on", "off"]}]},
  {"name": "plan", "description": "Write an implementation plan without executing", "category": "work" , "parameters": [{"name": "task", "type": "string", "description": "task to plan", "required": True}]},
  {"name": "goal", "description": "Set a standing goal worked across turns", "category": "work" , "parameters": [{"name": "goal", "type": "string", "description": "what to accomplish", "required": True}]},
  {"name": "bg", "description": "Run a prompt in a background session", "category": "work" , "parameters": [{"name": "task", "type": "string", "description": "background task", "required": True}]},
  {"name": "agents", "description": "Show active agents and running tasks", "category": "work"},
  {"name": "learn", "description": "Learn a reusable skill from anything", "category": "work"},
  {"name": "diff", "description": "Show git changes in the working directory", "category": "work" , "parameters": [{"name": "ref", "type": "string", "description": "git ref to diff against", "required": False}]},
  {"name": "reload-skills", "description": "Re-scan skills directory", "category": "admin" , "aliases": ["rs"]},
  {"name": "reload-mcp", "description": "Reload MCP servers from config", "category": "admin" , "aliases": ["rmcp"]},
]

# ── Interactive components (P2 parity with the OpenClaw connector) ─────────
# Hermes answers /model & exec-approvals as plain text on non-button platforms;
# these parsers lift that text into ShareOpen's component_message protocol so
# users tap pickers/buttons instead of hand-typing slash commands.

_MODEL_SECTION_RE = re.compile(r"\*\*([a-z0-9._-]+)\*\*\s*`--provider\s+([^`]+)`", re.IGNORECASE)
_INLINE_TOKEN_RE = re.compile(r"`([^`\n]+)`")
_CURRENT_RE = re.compile(r"Current:\s*`?([^`\n]+?)`?(?:\s+on\b|\n|$)", re.IGNORECASE)
# Typed /approve fallback shape (gateway.run._format_exec_approval_fallback):
# native-button platforms never emit the "Reply `/approve` … `/deny`" sentence.
_APPROVAL_MARKERS = ("needs your OK", "`/approve`", "`/deny`")
_NON_CHAT_MODEL_RE = re.compile(r"image|video|tts|embed|whisper|rerank|moderation", re.IGNORECASE)


def parse_model_picker(text):
    """Hermes '/model' provider-section listing -> select component dict (or None)."""
    try:
        src = str(text or "")
        sections = list(_MODEL_SECTION_RE.finditer(src))
        if not sections:
            return None
        cm = _CURRENT_RE.search(src)
        current = cm.group(1).strip() if cm else ""
        options, seen = [], set()
        for i, sec in enumerate(sections):
            provider = sec.group(2).strip()
            start = sec.end()
            end = sections[i + 1].start() if i + 1 < len(sections) else len(src)
            for m in _INLINE_TOKEN_RE.finditer(src[start:end]):
                name = m.group(1).strip()
                if not name or name.startswith("--") or " " in name:
                    continue
                if _NON_CHAT_MODEL_RE.search(name):
                    continue
                key = f"{provider}/{name}"
                if key in seen:
                    continue
                seen.add(key)
                options.append({
                    "value": f"/model {name} --provider {provider}",
                    "label": name + (f" [{provider}]" if provider != current.split("/")[0] else "") + (" ✓" if name == current else ""),
                })
                if len(options) >= 40:
                    break
            if len(options) >= 40:
                break
        if not options:
            return None
        prompt = f"Current: {current} — {len(options)} models" if current else f"Select a model — {len(options)} models"
        return {
            "component_type": "select",
            "component_id": f"mdl_{int(time.time() * 1000)}",
            "config": {"prompt": prompt, "options": options, "buttons": [{"label": "Submit", "value": "__submit__"}]},
        }
    except Exception:
        logger.debug("ShareOpen: parse_model_picker failed", exc_info=True)
        return None


def parse_approval_buttons(text):
    """Exec-approval fallback text -> button component dict (or None)."""
    try:
        src = str(text or "")
        if not all(mk in src for mk in _APPROVAL_MARKERS):
            return None
        buttons = [{"label": "✅ Run once", "value": "/approve"}]
        if "`/approve session`" in src:
            buttons.append({"label": "🔁 This session", "value": "/approve session"})
        if "`/approve always`" in src:
            buttons.append({"label": "🟦 Always allow", "value": "/approve always"})
        buttons.append({"label": "🗑 Deny", "value": "/deny"})
        return {
            "component_type": "button",
            "component_id": f"appr_{int(time.time() * 1000)}",
            "config": {"prompt": "⚠️ Approval needed", "buttons": buttons},
        }
    except Exception:
        logger.debug("ShareOpen: parse_approval_buttons failed", exc_info=True)
        return None


def maybe_build_component(text):
    """Ordered component factory against outgoing reply text."""
    comp = parse_model_picker(text)
    if comp is None:
        comp = parse_approval_buttons(text)
    return comp


class ShareOpenAdapter(BasePlatformAdapter):
    """WebSocket adapter for ShareOpen IM platform.

    Connects to ShareOpen's /ws/agent endpoint and relays messages
    between ShareOpen users and the Hermes agent.
    """

    def __init__(self, config, **kwargs):
        platform = Platform("shareopen")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        # Connection settings (env vars override config.yaml).
        # `extra` is already seeded from the profile-scoped .env by the
        # gateway's config-load path (via _env_enablement); the scoped
        # secret read here covers the direct-construction / CLI path.
        self.base_url = _secret("SHAREOPEN_URL") or extra.get("url", "http://localhost:3021")
        self.token = _secret("SHAREOPEN_TOKEN") or extra.get("token", "")
        self.agent_id = _secret("SHAREOPEN_AGENT_ID") or extra.get("agent_id", "")
        reconnect_delay = _secret("SHAREOPEN_RECONNECT_DELAY") or extra.get("reconnect_delay", 5)
        self.reconnect_delay = int(reconnect_delay)

        # WebSocket URL derivation
        self.ws_url = self._build_ws_url()

        # Runtime state
        self._ws: Optional[Any] = None  # websockets.WebSocketClientProtocol
        self._recv_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._ping_pong_event = asyncio.Event()
        self._auto_reply_agents: Dict[str, bool] = {}  # agent_id -> is_hermes_auto
        self._last_sent_text: str = ""        # streamed-draft/final dedup
        self._sent_component_keys: list = []  # recently emitted component signatures
        self._imported_websockets = False
        self._websockets = None

        # Streaming callbacks (set by gateway)
        self.stream_delta_callback = None
        self.tool_progress_callback = None
        self.tool_start_callback = None
        self.tool_complete_callback = None

    def _build_ws_url(self) -> str:
        """Build WebSocket URL from base URL."""
        url = self.base_url.rstrip("/")
        # Convert http(s):// to ws(s)://
        if url.startswith("https://"):
            ws_base = "wss://" + url[len("https://"):]
        elif url.startswith("http://"):
            ws_base = "ws://" + url[len("http://"):]
        else:
            ws_base = url
        return f"{ws_base}/ws/agent?token={self.token}"

    def _lazy_import_websockets(self):
        """Import websockets library at runtime."""
        if not self._imported_websockets:
            try:
                import websockets
                self._websockets = websockets
                self._imported_websockets = True
            except ImportError:
                logger.error("ShareOpen: websockets package not installed. Run: pip install websockets")
                self._set_fatal_error("missing_dependency", "pip install websockets", retryable=False)
                raise

    @property
    def name(self) -> str:
        return "ShareOpen"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Connect to ShareOpen WebSocket endpoint."""
        if not self.token:
            logger.error("ShareOpen: token must be configured (SHAREOPEN_TOKEN)")
            self._set_fatal_error(
                "config_missing",
                "SHAREOPEN_TOKEN must be set",
                retryable=False,
            )
            return False

        self._lazy_import_websockets()

        try:
            logger.info("ShareOpen: connecting to %s", self.ws_url)
            self._ws = await asyncio.wait_for(
                self._websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # Disable WS-level ping — ShareOpen uses app-level heartbeat
                    ping_timeout=None,
                    close_timeout=5,
                ),
                timeout=15.0,
            )
        except Exception as e:
            logger.error("ShareOpen: connection failed — %s", e)
            self._set_fatal_error("connect_failed", str(e), retryable=True)
            return False

        # Start receive loop
        self._recv_task = asyncio.create_task(self._receive_loop())

        # Start heartbeat (ShareOpen expects periodic pongs)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Publish the slash-command menu BEFORE marking connected: a fire-and-
        # forget create_task here raced the receive loop and silently dropped
        # every frame (never reached the backend). Awaited send is ~36 frames.
        await self._register_commands()

        self._mark_connected()
        logger.warning("ShareOpen: connected successfully (cmd-menu v1)")
        return True

    async def disconnect(self) -> None:
        """Close the WebSocket connection."""
        self._mark_disconnected()

        # Clear callbacks
        self.stream_delta_callback = None
        self.tool_progress_callback = None
        self.tool_start_callback = None
        self.tool_complete_callback = None

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        logger.info("ShareOpen: disconnected")

    # ── Streaming & Progress Support ──────────────────────────────────────

    def supports_draft_streaming(
        self,
        chat_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """ShareOpen supports streaming via WebSocket."""
        return True

    async def send_draft(
        self,
        chat_id: str,
        draft_id: int,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a streaming draft frame (incremental delta to ShareOpen WS).

        ShareOpen's backend ``handleStreamingChunk`` is INCREMENTAL: it does
        ``streamMsg.payload.content += chunk`` and the frontend renders the
        accumulated bubble.  The gateway's ``GatewayStreamConsumer`` passes the
        FULLY ACCUMULATED draft text on every frame (Telegram-style
        progressive edit), so emit only the NEW suffix here — otherwise the
        UI duplicates each character once per frame.

        Tracks the last-sent length per chat_id (dict cleared on disconnect).
        """
        if not self._ws or self._ws.close_code is not None:
            return SendResult(success=False, error="Not connected")
        try:
            if not hasattr(self, "_draft_sent_len"):
                self._draft_sent_len: Dict[str, int] = {}
            prev = self._draft_sent_len.get(chat_id, 0)
            if len(content) < prev:
                # Newer turn with shorter accumulated text — reset and resend.
                prev = 0
            delta = content[prev:]
            self._draft_sent_len[chat_id] = len(content)
            if not delta:
                # Nothing new since last frame — skip the no-op round-trip.
                return SendResult(success=True, message_id=f"draft_{draft_id}")
            msg = {
                "type": "stream_chunk",
                "conversation_id": chat_id,
                "chunk": delta,
                "is_final": False,
            }
            await self._ws.send(json.dumps(msg))
            logger.debug("ShareOpen: sent draft delta (%d new chars) to conv %s", len(delta), chat_id)
            return SendResult(success=True, message_id=f"draft_{draft_id}")
        except Exception as e:
            logger.error("ShareOpen: send_draft failed — %s", e)
            return SendResult(success=False, error=str(e))

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a streaming draft or progress message.

        For ShareOpen, the incremental deltas are ALREADY on the wire via
        ``send_draft`` (which emits suffix-only ``stream_chunk`` frames).  An
        edit-based frame therefore only needs to carry the tail the draft path
        hasn't sent yet — otherwise the typewriter bubble (which APPENDS each
        chunk) renders the accumulated text duplicated.  On finalize the full
        text is authoritative (the client overwrites the bubble with it).
        """
        if not self._ws or self._ws.close_code is not None:
            return SendResult(success=False, error="Not connected")

        try:
            # If it's a draft, send as stream_chunk with is_final
            if message_id.startswith("draft_"):
                if not hasattr(self, "_draft_sent_len"):
                    self._draft_sent_len: Dict[str, int] = {}
                if finalize:
                    delta = content
                else:
                    prev = self._draft_sent_len.get(chat_id, 0)
                    delta = content[prev:] if len(content) >= prev else content
                    self._draft_sent_len[chat_id] = len(content)
                if delta:
                    msg = {
                        "type": "stream_chunk",
                        "conversation_id": chat_id,
                        "chunk": delta,
                        "is_final": finalize,
                    }
                    await self._ws.send(json.dumps(msg))
                    logger.debug("ShareOpen: edited stream chunk to conv %s (finalize=%s, %d chars)", chat_id, finalize, len(delta))
                return SendResult(success=True, message_id=message_id)
            else:
                # Regular message edit - send as agent_message
                msg = {
                    "type": "agent_message",
                    "conversation_id": chat_id,
                    "content": content,
                }
                await self._ws.send(json.dumps(msg))
                return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("ShareOpen: edit_message failed — %s", e)
            return SendResult(success=False, error=str(e))

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Send a message to a ShareOpen conversation."""
        if not self._ws or self._ws.close_code is not None:
            return SendResult(success=False, error="Not connected")

        # Build ShareOpen message format
        # ShareOpen WS expects: {type, conversation_id, sender_id, sender_type,
        #                        receiver_id, receiver_type, payload}
        msg = {
            "type": "agent_message",
            "conversation_id": chat_id,
            "content": content,
        }

        if reply_to:
            msg["reply_to_id"] = reply_to

        if metadata:
            msg["metadata"] = metadata

        try:
            await self._ws.send(json.dumps(msg))
            logger.debug("ShareOpen: sent message to conv %s (%d chars)", chat_id, len(content))
            await self._emit_component(chat_id, content)
            return SendResult(success=True, message_id=str(int(time.time() * 1000)))
        except Exception as e:
            logger.error("ShareOpen: send failed — %s", e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """ShareOpen doesn't have typing indicator yet — no-op."""
        pass

    # ── Slash commands (Telegram-parity menu + gateway-native execution) ──

    async def _register_commands(self) -> None:
        """Publish the command menu so ShareOpen's frontend can list it."""
        if not self._ws or self._ws.close_code is not None:
            return
        for c in SHARE_COMMANDS:
            entry = dict(c)
            entry["parameters"] = c.get("parameters", [])
            entry["aliases"] = c.get("aliases", [])
            try:
                await self._ws.send(json.dumps({"type": "command_register", "command": entry}))
            except Exception as e:
                logger.warning("ShareOpen: command_register send failed: %s", e)
                return
        logger.warning("ShareOpen: published %d slash commands", len(SHARE_COMMANDS))

    async def _emit_component(self, chat_id: str, text: str) -> None:
        """Lift a reply's tappable payload into a component_message frame.

        Dedup: the gateway can deliver the same final twice (draft path + send
        path); each distinct tappable payload fires its component at most once.
        """
        if not self._ws or self._ws.close_code is not None:
            return
        if not text or text == self._last_sent_text:
            return
        self._last_sent_text = text
        comp = maybe_build_component(text)
        if not comp:
            return
        if comp["component_type"] == "button":
            sig = "appr:" + comp["config"]["buttons"][-1]["value"]
        else:
            sig = "mdl:" + comp["config"]["prompt"]
        if sig in self._sent_component_keys:
            return
        self._sent_component_keys.append(sig)
        if len(self._sent_component_keys) > 20:
            self._sent_component_keys = self._sent_component_keys[-20:]
        try:
            await self._ws.send(json.dumps({
                "type": "component_message",
                "conversation_id": chat_id,
                "component_id": comp["component_id"],
                "component_type": comp["component_type"],
                "config": comp["config"],
            }))
            logger.warning("ShareOpen: emitted %s component %s -> %s", comp["component_type"], comp["component_id"], chat_id)
        except Exception as e:
            logger.debug("ShareOpen: component_message send failed: %s", e)

    async def _handle_component_callback(self, data: dict) -> None:
        """Human tapped a component -> replay its value as a slash command."""
        value = str(data.get("value") or "").strip()
        if value.endswith("__submit__"):
            value = value[: -len("__submit__")].strip()
        conv_id = data.get("conversation_id", "")
        user_id = data.get("user_id", "unknown")
        if not value.startswith("/") or not conv_id:
            logger.debug("ShareOpen: component_callback ignored: %r", value)
            return
        self._last_sent_text = ""  # next reply must not be deduped away
        logger.warning("ShareOpen: component_callback %s in %s", value, conv_id)
        await self._dispatch_message(text=value, chat_id=conv_id, user_id=user_id, user_name=user_id)

    async def _send_command_ack(self, invocation_id: str, patch: dict) -> bool:
        if not self._ws or self._ws.close_code is not None:
            return False
        try:
            await self._ws.send(json.dumps({
                "type": "command_response",
                "invocation_id": invocation_id,
                "response": patch,
            }))
            return True
        except Exception as e:
            logger.debug("ShareOpen: command_response send failed: %s", e)
            return False

    async def _handle_command_invocation(self, data: dict) -> None:
        """Replay an invoked slash command through the gateway's native path.

        Mirrors the OpenClaw connector: rebuild '/name arg1 arg2' and hand it
        to _dispatch_message — the gateway intercepts slashes before the agent
        turn and answers through the normal delivery pipeline, so ShareOpen
        gets byte-identical behavior to Telegram.
        """
        invocation_id = data.get("invocation_id", "")
        name = str(data.get("command_name") or "").lstrip("/").strip()
        args = data.get("arguments") or {}
        conv_id = data.get("conversation_id", "")
        user_id = data.get("user_id", "unknown")
        if not name or not conv_id:
            await self._send_command_ack(invocation_id, {"status": "failed", "error": "missing command or conversation"})
            return
        # Values already arrive in parameter order (positional mapping is the
        # frontend's job; the backend echoes dict order = insertion order).
        arg_text = " ".join(str(v) for v in args.values() if v not in ("", None))
        text = f"/{name}" + (f" {arg_text}" if arg_text else "")
        logger.info("ShareOpen: command %s in %s (inv %s)", text, conv_id, invocation_id)
        try:
            await self._dispatch_message(text=text, chat_id=conv_id, user_id=user_id, user_name=user_id)
            # The gateway's reply lands as a normal agent_message; just close
            # the invocation card. No result payload → no duplicate bubble.
            await self._send_command_ack(invocation_id, {"status": "completed"})
        except Exception as e:
            await self._send_command_ack(invocation_id, {"status": "failed", "error": str(e)})

    # ── Media delivery (Telegram-parity attachments) ──────────────────────

    # MEDIA:<http(s) URL> tags. The core extractor deliberately matches only
    # local paths (MEDIA_TAG_CLEANUP_RE), so remote URLs produced by image-gen
    # tools would otherwise leak into the chat text as raw tags. ShareOpen
    # fetches/stores media server-side, so downloading the URL to a temp file
    # here restores full parity with Telegram's MEDIA: delivery for remote
    # outputs (Agnes image tasks return hosted URLs, not local paths).
    _URL_MEDIA_RE = re.compile(
        r'''[`"']{0,3}MEDIA:\s*(?P<url>https?://\S+?\.(?:png|jpe?g|gif|webp|bmp|svg|mp4|mov|webm|mp3|wav|ogg|m4a|aac|flac|pdf|docx?|xlsx?|pptx?|zip|txt|csv))(?=[\s`"'*_,;:)\]}（）〈〉《》：，。；！？、【】]|$)[`"']{0,3}''',
        re.IGNORECASE,
    )
    _URL_MEDIA_MAX_BYTES = 20 * 1024 * 1024  # generous; send_document caps embed at 8MB

    @staticmethod
    def extract_media(content: str):
        media, cleaned = BasePlatformAdapter.extract_media(content)
        if "MEDIA:" not in cleaned or "://" not in cleaned:
            return media, cleaned

        seen = {p for p, _ in media}
        spans = []
        for m in ShareOpenAdapter._URL_MEDIA_RE.finditer(cleaned):
            url = m.group("url")
            # Never touch protected spans the core extractor deliberately
            # preserves (code blocks / JSON examples) — crude guard: skip if
            # the URL was already handed to us by the parent extractor.
            if url in seen:
                continue
            seen.add(url)
            local = ShareOpenAdapter._fetch_remote_media(url)
            if local:
                ext = os.path.splitext(local)[1].lower()
                is_voice = "[[audio_as_voice]]" in content and ext in {".ogg", ".opus", ".mp3", ".m4a", ".wav"}
                media.append((local, is_voice))
                spans.append((m.start(), m.end()))
        for start, end in reversed(spans):
            cleaned = cleaned[:start] + cleaned[end:]
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return media, cleaned

    @staticmethod
    def _fetch_remote_media(url: str) -> Optional[str]:
        """Download a remote MEDIA URL to a stable temp path; None on any failure."""
        try:
            from urllib.parse import urlparse
            host = urlparse(url).hostname or ""
            digest = hashlib.sha1(url.encode()).hexdigest()[:16]
            ext = os.path.splitext(urlparse(url).path)[1][:8] or ".bin"
            cache_dir = os.path.join(tempfile.gettempdir(), "hermes_shareopen_media")
            os.makedirs(cache_dir, exist_ok=True)
            dest = os.path.join(cache_dir, f"web_{digest}{ext}")
            if os.path.isfile(dest) and os.path.getsize(dest) > 0:
                return dest
            req = urllib.request.Request(url, headers={"User-Agent": "HermesShareOpen/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                clen = resp.headers.get("Content-Length")
                if clen and int(clen) > ShareOpenAdapter._URL_MEDIA_MAX_BYTES:
                    logger.warning("ShareOpen: remote media too big (%s bytes): %s", clen, url)
                    return None
                data = resp.read(ShareOpenAdapter._URL_MEDIA_MAX_BYTES + 1)
            if len(data) > ShareOpenAdapter._URL_MEDIA_MAX_BYTES:
                logger.warning("ShareOpen: remote media exceeds cap: %s", url)
                return None
            tmp = dest + ".part"
            with open(tmp, "wb") as fh:
                fh.write(data)
            os.replace(tmp, dest)
            logger.info("ShareOpen: fetched remote media %s (%d bytes) → %s", host, len(data), dest)
            return dest
        except Exception as e:
            # Leave the tag visible so the user still has the raw link.
            logger.warning("ShareOpen: remote media fetch failed %s — %s", url, e)
            return None

    async def _send_attachment_frame(
        self,
        chat_id: str,
        *,
        file_path: Optional[str] = None,
        url: Optional[str] = None,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
    ):
        """Emit one agent_message frame carrying an ``attachments`` array.

        ShareOpen backend (ws-server handleAgentReply /
        handleHermesStreamingMessage) stores the file into media_files and
        renders a media bubble — same taxonomy as user uploads:
        url  -> backend fetches it server-side (SSRF-guarded, size-capped)
        local path -> base64-embedded (capped at 8MB decoded by the backend)
        """
        if not self._ws or self._ws.close_code is not None:
            return SendResult(success=False, error="Not connected")
        att: Dict[str, Any] = {}
        try:
            if url:
                att["url"] = url
            elif file_path:
                import base64
                from pathlib import Path
                from urllib.parse import unquote

                p = Path(unquote(file_path.replace("file://", "")))
                if not p.is_file():
                    return SendResult(success=False, error=f"media not found: {p}")
                size = p.stat().st_size
                if size > 8 * 1024 * 1024:
                    # Too big to embed — deliver as a note instead of failing the reply.
                    att = {}
                    content = caption or ""
                    msg = {
                        "type": "agent_message",
                        "conversation_id": chat_id,
                        "content": (content + f"\n⚠️ 附件过大未发送 ({size // 1024 // 1024}MB, 上限 8MB): {p.name}").strip(),
                    }
                    await self._ws.send(json.dumps(msg))
                    return SendResult(success=True, message_id=str(int(time.time() * 1000)))
                att["data_b64"] = base64.b64encode(p.read_bytes()).decode()
                att["name"] = file_name or p.name
            else:
                return SendResult(success=False, error="no media source")
            if att.get("name") is None and file_name:
                att["name"] = file_name
            msg = {
                "type": "agent_message",
                "conversation_id": chat_id,
                "content": caption or "",
                "attachments": [att],
            }
            await self._ws.send(json.dumps(msg))
            logger.debug("ShareOpen: sent attachment frame to conv %s (%s)", chat_id, att.get("name") or att.get("url"))
            return SendResult(success=True, message_id=str(int(time.time() * 1000)))
        except Exception as e:
            logger.error("ShareOpen: attachment send failed — %s", e)
            return SendResult(success=False, error=str(e))

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_attachment_frame(chat_id, file_path=file_path, caption=caption, file_name=file_name)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_attachment_frame(chat_id, file_path=audio_path, caption=caption)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        return await self._send_attachment_frame(chat_id, file_path=video_path, caption=caption)

    async def send_multiple_images(
        self,
        chat_id: str,
        images,
        metadata: Optional[Dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        for uri, _cap in images or []:
            if uri.startswith("http://") or uri.startswith("https://"):
                await self._send_attachment_frame(chat_id, url=uri)
            else:
                await self._send_attachment_frame(chat_id, file_path=uri)

    async def send_photo(
        self,
        chat_id: str,
        photo_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        if photo_path.startswith("http://") or photo_path.startswith("https://"):
            await self._send_attachment_frame(chat_id, url=photo_path, caption=caption)
        else:
            await self._send_attachment_frame(chat_id, file_path=photo_path, caption=caption)

    # ── Streaming callbacks ───────────────────────────────────────────────

    def set_stream_callbacks(self, stream_delta_callback, tool_progress_callback,
                             tool_start_callback=None, tool_complete_callback=None):
        """Register streaming callbacks from the gateway."""
        self.stream_delta_callback = stream_delta_callback
        self.tool_progress_callback = tool_progress_callback
        self.tool_start_callback = tool_start_callback
        self.tool_complete_callback = tool_complete_callback

    async def _send_stream_chunk(self, conversation_id: str, chunk: str, is_final: bool = False):
        """Send a streaming text chunk to ShareOpen."""
        if not self._ws or self._ws.close_code is not None:
            return
        try:
            msg = {
                "type": "stream_chunk",
                "conversation_id": conversation_id,
                "chunk": chunk,
                "is_final": is_final,
            }
            await self._ws.send(json.dumps(msg))
            logger.debug("ShareOpen: sent stream chunk to conv %s (final=%s)", conversation_id, is_final)
        except Exception as e:
            logger.error("ShareOpen: stream chunk send failed — %s", e)

    async def _send_tool_progress(self, conversation_id: str, event_type: str,
                                  tool_name: str = None, preview: str = None,
                                  args: dict = None, **kwargs):
        """Send tool progress event to ShareOpen."""
        if not self._ws or self._ws.close_code is not None:
            return
        try:
            # [align 2026-11-17] 后端 handleProgressUpdate 契约: {conversation_id, progress, status}.
            # progress 必须 truthy (否则被 L999 !progress 闸丢弃); status 缺省 'in_progress'.
            # 旧帧发的是 event_type/tool_name/preview/... 后端收不到 progress/status → 帧被丢.
            status = "done" if kwargs.get("ok", True) else "error"
            progress_text = preview or tool_name or event_type or ""
            msg = {
                "type": "progress_update",
                "conversation_id": conversation_id,
                "progress": progress_text,
                "status": status,
            }
            await self._ws.send(json.dumps(msg))
            logger.debug("ShareOpen: sent tool progress %s for %s", event_type, tool_name)
        except Exception as e:
            logger.error("ShareOpen: tool progress send failed — %s", e)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "name": chat_id,
            "type": "dm",  # ShareOpen agent chats are 1:1 by default
        }

    # ── Receive loop ──────────────────────────────────────────────────────

    async def _receive_loop(self) -> None:
        """Main receive loop — reads messages from ShareOpen WS."""
        try:
            async for raw_message in self._ws:
                try:
                    data = json.loads(raw_message) if isinstance(raw_message, str) else None
                    if data:
                        await self._handle_message(data)
                except json.JSONDecodeError:
                    logger.warning("ShareOpen: invalid JSON received")
                except Exception as e:
                    logger.warning("ShareOpen: error handling message — %s", e)
        except asyncio.CancelledError:
            raise
        except self._websockets.ConnectionClosed as e:
            logger.warning("ShareOpen: connection closed — code=%s, reason=%s", e.code, e.reason)
        except Exception as e:
            logger.error("ShareOpen: receive loop error — %s", e)
        finally:
            if self.is_connected:
                logger.warning("ShareOpen: connection lost, marking disconnected")
                self._set_fatal_error("connection_lost", "ShareOpen WebSocket closed", retryable=True)
                await self._notify_fatal_error()

    async def _handle_message(self, data: dict) -> None:
        """Process a message from ShareOpen."""
        msg_type = data.get("type", "")

        # ── Ping/heartbeat response ──
        if msg_type == "ping":
            # Send pong back
            if self._ws and self._ws.close_code is None:
                await self._ws.send(json.dumps({"type": "pong"}))
                self._ping_pong_event.set()
            return

        # ── User message ──
        # type can be "message" (from hermes-agent.js /ws/agent), "user_message", or "chat"
        if msg_type in ("user_message", "chat", "message"):
            # For "message" type, content is nested in data.*
            media_info = None
            if msg_type == "message":
                payload = data.get("data", {})
                content = payload.get("message", payload.get("content", ""))
                conversation_id = payload.get("conversation_id", "")
                user_id = payload.get("user_id", "unknown")
                user_name = user_id  # We don't have a name here
                media_info = payload.get("media")
            else:
                content = data.get("content", "") or data.get("payload", "")
                if isinstance(content, dict):
                    content = content.get("content", content.get("text", ""))
                conversation_id = data.get("conversation_id", "")
                user_id = data.get("sender_id", data.get("user_id", "unknown"))
                user_name = data.get("sender_name", data.get("user_name", user_id))
                media_info = data.get("media")

            if (not content and not media_info) or not conversation_id:
                logger.debug("ShareOpen: skipping empty/incomplete message")
                return

            # Track auto-reply agents (hermes type agents)
            agent_id = data.get("agent_id", "")
            if agent_id:
                gateway_type = data.get("gateway_type", "")
                self._auto_reply_agents[agent_id] = (gateway_type == "hermes")

            # [P0 2026-11-03] 群穿透: 后端在群聊 WS 帧上挂了 data.group
            # ({members, recent, mentions, media, self_role}). 群聊时渲染成群上下文
            # 前缀, 让 agent 感知 "我在群里 / 我的角色 / 谁@我 / 最近说了啥".
            # 单聊 group 缺失 → 保持原文, 零成本.
            group = data.get("group") or (data.get("data") or {}).get("group")
            agent_text = content
            if isinstance(group, dict) and group.get("members"):
                role_line = f"[你在本群的角色: {group.get('self_role')}]" if group.get("self_role") else ""
                members = "、".join(
                    f"{(m or {}).get('name') or (m or {}).get('participant_id') or '?'}"
                    f" ({'agent' if ((m or {}).get('type') == 'agent' or (m or {}).get('participant_type') == 'agent') else 'human'}"
                    f"{', 角色: ' + str((m or {}).get('role_desc')) if (m or {}).get('role_desc') else ''})"
                    for m in (group.get("members") or [])
                    if ((m or {}).get("name") or (m or {}).get("participant_id"))
                )
                recent_lines = []
                for r in (group.get("recent") or [])[-10:]:
                    rr = r or {}
                    txt = (rr.get("text") or "").strip()
                    if not txt:
                        continue
                    who = rr.get("from") or rr.get("name") or "?"
                    typ = "agent" if (rr.get("type") == "agent" or rr.get("author_type") == "agent") else "user"
                    recent_lines.append(f"[{typ} {who}]: {txt[:300]}")
                recent_block = "\n".join(recent_lines)
                mentioned = [str((mm or {}).get("name")) for mm in (group.get("mentions") or []) if (mm or {}).get("name")]
                mention_note = f"\n[本条消息 @了你: {', '.join(mentioned)}]" if mentioned else ""
                agent_text = (
                    "[Group context — 你在一个 ShareOpen 群聊里]"
                    f"{role_line}\n"
                    f"群成员: {members}\n"
                    + (f"最近发言:\n{recent_block}\n" if recent_block else "")
                    + mention_note
                    + f"\n\n当前消息: {content}"
                )
                logger.info("ShareOpen: GROUP inbound in %s (members=%d, recent=%d)",
                            conversation_id, len(group.get("members") or []), len(group.get("recent") or []))

            await self._dispatch_message(
                text=agent_text,
                chat_id=conversation_id,
                user_id=user_id,
                user_name=user_name,
                media=media_info,
            )
            return

        # ── Streaming chunks ──
        if msg_type == "stream_chunk":
            # This is for receiving streaming chunks from the gateway (if needed)
            # Currently not used as streaming is pushed from adapter to gateway
            logger.debug("ShareOpen: received stream_chunk (unused)")
            return

        # ── Progress updates ──
        if msg_type == "progress_update":
            logger.debug("ShareOpen: received progress_update (unused)")
            return

        # ── User reactions to agent replies (v1.1.0) ──
        # Backend pushes reaction_added / reaction_removed (incl. offline
        # catchup with catchup=true). Inject as a lightweight feedback note
        # into the active chat session so the agent perceives 👍/👎 on its
        # own messages.
        if msg_type in ("reaction_added", "reaction_removed"):
            emoji = data.get("emoji", "")
            actor = data.get("reacter_name") or data.get("reacter_id") or "a user"
            preview = (data.get("text_preview") or "").strip()
            catchup = " [received while offline]" if data.get("catchup") else ""
            if msg_type == "reaction_added":
                note = f"{actor} reacted {emoji} to your reply"
                if preview:
                    note += f' ("{preview[:60]}…")'
            else:
                note = f"{actor} removed their {emoji} reaction to your reply"
            note += catchup
            logger.info("ShareOpen: user feedback — %s", note)
            conversation_id = data.get("conversation_id", "")
            if conversation_id:
                try:
                    await self._dispatch_message(
                        text=f"[user feedback] {note}",
                        chat_id=conversation_id,
                        user_id=data.get("reacter_id", "unknown"),
                        user_name=actor,
                        media=None,
                    )
                except Exception as e:
                    logger.error("ShareOpen: reaction note dispatch failed: %s", e)
            return

        # ── Interactive component tapped (picker / approval buttons) ──
        if msg_type == "component_callback":
            await self._handle_component_callback(data)
            return

        # ── Slash command invoked from ShareOpen UI ──
        if msg_type == "command_invocation":
            await self._handle_command_invocation(data)
            return

        # ── Registration acks (expected; nothing to do) ──
        if msg_type in ("command_registered", "command_ack"):
            return

        # ── System/status messages ──
        if msg_type in ("connected", "ready", "auth_ok"):
            logger.info("ShareOpen: %s", msg_type)
            return

        # ── Error messages ──
        if msg_type == "error":
            error_msg = data.get("message", data.get("error", "unknown"))
            logger.error("ShareOpen: server error — %s", error_msg)
            return

        logger.debug("ShareOpen: unhandled message type: %s", msg_type)

    async def _dispatch_message(
        self,
        text: str,
        chat_id: str,
        user_id: str,
        user_name: str,
        media: Optional[dict] = None,
    ) -> None:
        """Build a MessageEvent and hand it to the base class handler."""
        if not self._message_handler:
            return

        source = self.build_source(
            chat_id=chat_id,
            chat_name=f"ShareOpen #{chat_id[:8]}",
            chat_type="dm",
            user_id=user_id,
            user_name=user_name,
        )

        # Download inbound media so the vision tool / STT tool can access it
        media_urls = []
        media_types = []
        if media and media.get("url"):
            mtype = media.get("type") or "file"
            local = self._fetch_remote_media(media["url"])
            if local:
                media_urls.append(local)
                media_types.append(mtype)

        mtype_enum = MessageType.TEXT
        if media_urls:
            # Map ShareOpen media types to the gateway enum
            type_map = {"image": MessageType.PHOTO, "audio": MessageType.AUDIO,
                        "video": MessageType.VIDEO, "voice": MessageType.VOICE,
                        "file": MessageType.DOCUMENT, "document": MessageType.DOCUMENT}
            mtype_enum = type_map.get(media_types[0], MessageType.TEXT)

        event = MessageEvent(
            text=text,
            message_type=mtype_enum,
            source=source,
            message_id=str(int(time.time() * 1000)),
            timestamp=__import__("datetime").datetime.now(),
            media_urls=media_urls,
            media_types=media_types,
        )

        await self.handle_message(event)

    # ── Heartbeat ──────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Send periodic pings to keep ShareOpen connection alive."""
        while self.is_connected and self._ws and self._ws.close_code is None:
            try:
                await asyncio.sleep(25)  # Send ping every 25s
                if self._ws and self._ws.close_code is None:
                    self._ping_pong_event.clear()
                    await self._ws.send(json.dumps({"type": "ping"}))
                    # Wait up to 10s for pong
                    try:
                        await asyncio.wait_for(self._ping_pong_event.wait(), timeout=10.0)
                    except asyncio.TimeoutError:
                        logger.warning("ShareOpen: heartbeat timeout")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("ShareOpen: heartbeat error — %s", e)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Scope-aware secret read (multiplex profile support)
# ---------------------------------------------------------------------------
# Under `gateway.multiplex_profiles`, a secondary profile's .env exists only in
# its secret scope — `os.getenv()` (frozen to the launch/default profile) can't
# see it. All platform gate + credential reads go through get_scoped_secret so
# the tom profile's SHAREOPEN_* vars resolve correctly while the default
# profile (which has no ShareOpen config) stays inert.

def _secret(name: str, default: str = "") -> str:
    """Profile-scoped env read; falls back to os.environ for unscoped/CLI paths."""
    try:
        from gateway.platforms._shared import get_scoped_secret
        val = get_scoped_secret(name, default)
    except ImportError:  # plugin imported outside the gateway (tests, CLI)
        val = os.getenv(name, default)
    return str(val or default)


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    """Check if ShareOpen is configured."""
    url = _secret("SHAREOPEN_URL")
    token = _secret("SHAREOPEN_TOKEN")
    return bool(url and token)


def validate_config(config) -> bool:
    """Validate that the platform config has enough info to connect."""
    extra = getattr(config, "extra", {}) or {}
    url = _secret("SHAREOPEN_URL") or extra.get("url", "")
    token = _secret("SHAREOPEN_TOKEN") or extra.get("token", "")
    return bool(url and token)


def interactive_setup() -> None:
    """Interactive `hermes gateway setup` flow for ShareOpen."""
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("ShareOpen")
    existing_url = get_env_value("SHAREOPEN_URL")
    if existing_url:
        print_info(f"ShareOpen: already configured (url: {existing_url})")
        if not prompt_yes_no("Reconfigure ShareOpen?", False):
            return

    print_info("Connect Hermes to a local ShareOpen IM instance.")
    print_info("  ShareOpen provides: multi-agent chat, tool approvals, A2A delegation.")
    print()

    url = prompt(
        "ShareOpen backend URL",
        default=get_env_value("SHAREOPEN_URL") or "http://localhost:3021",
    )
    if not url:
        print_warning("URL is required — skipping ShareOpen setup")
        return
    save_env_value("SHAREOPEN_URL", url.strip())

    token = prompt(
        "ShareOpen agent token (from /api/hermes/create or frontend)",
        password=True,
    )
    if not token:
        print_warning("Token is required — skipping ShareOpen setup")
        return
    save_env_value("SHAREOPEN_TOKEN", token.strip())

    agent_id = prompt(
        "Agent ID (optional — auto-discovered from token if blank)",
        default=get_env_value("SHAREOPEN_AGENT_ID") or "",
    )
    if agent_id:
        save_env_value("SHAREOPEN_AGENT_ID", agent_id.strip())

    print()
    print_success("ShareOpen configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway: hermes gateway restart")


def is_connected(config) -> bool:
    """Check whether ShareOpen is configured."""
    extra = getattr(config, "extra", {}) or {}
    url = _secret("SHAREOPEN_URL") or extra.get("url", "")
    token = _secret("SHAREOPEN_TOKEN") or extra.get("token", "")
    return bool(url and token)


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env vars during gateway config load."""
    url = _secret("SHAREOPEN_URL").strip()
    token = _secret("SHAREOPEN_TOKEN").strip()
    if not (url and token):
        return None
    seed: dict = {
        "url": url,
        "token": token,
    }
    agent_id = _secret("SHAREOPEN_AGENT_ID").strip()
    if agent_id:
        seed["agent_id"] = agent_id
    reconnect_delay = _secret("SHAREOPEN_RECONNECT_DELAY").strip()
    if reconnect_delay:
        try:
            seed["reconnect_delay"] = int(reconnect_delay)
        except ValueError:
            pass
    # Home channel = default conversation for cron deliveries
    home = _secret("SHAREOPEN_HOME_CHANNEL").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": _secret("SHAREOPEN_HOME_CHANNEL_NAME") or home,
        }
    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="shareopen",
        label="ShareOpen",
        adapter_factory=lambda cfg: ShareOpenAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["SHAREOPEN_URL", "SHAREOPEN_TOKEN"],
        install_hint="pip install websockets",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="SHAREOPEN_HOME_CHANNEL",
        emoji="🔷",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are connected to ShareOpen, a local Agent IM platform. "
            "Users interact with you through a chat interface that supports "
            "markdown formatting, code blocks, and rich messages. "
            "You can use all your tools (terminal, file, browser, etc.) "
            "and the responses will be delivered in real-time via WebSocket. "
            "ShareOpen supports multi-agent conversations and tool approval workflows."
        ),
    )
