from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable

from service import db

try:
    from service.config import settings as _settings
except Exception:  # pragma: no cover - fallback for isolated tests/shims
    _settings = None

log = logging.getLogger("SteamFriendRequests")
_OUTGOING_ENV_NAME = "STEAM_OUTGOING_FRIEND_REQUESTS_ENABLED"
_outgoing_status_logged = False


_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS steam_friend_requests(
  steam_id TEXT PRIMARY KEY,
  status TEXT DEFAULT 'pending',
  requested_at INTEGER DEFAULT (strftime('%s','now')),
  last_attempt INTEGER,
  attempts INTEGER DEFAULT 0,
  error TEXT
)
"""


def _ensure_table() -> bool:
    try:
        db.execute(_CREATE_SQL)
        return True
    except Exception:
        log.exception("Failed to ensure steam_friend_requests table exists")
        return False


def _queue_single(steam_id: str, trigger_task: bool = False) -> bool:
    if not steam_id:
        return False
    sid = str(steam_id).strip()
    if not sid:
        return False
    try:
        # 1. Passive Queue (für Retry/Status-Tracking)
        db.execute(
            """
            INSERT INTO steam_friend_requests(steam_id, status)
            VALUES(?, 'pending')
            ON CONFLICT(steam_id) DO UPDATE SET
              status=excluded.status,
              last_attempt=NULL,
              attempts=0,
              error=NULL
            WHERE steam_friend_requests.status != 'sent'
            """,
            (sid,),
        )

        # 2. Active Task (für Sofort-Ausführung)
        if trigger_task:
            payload = json.dumps({"steam_id": sid})
            db.execute(
                "INSERT INTO steam_tasks(type, payload, status) VALUES (?, ?, 'PENDING')",
                ("AUTH_SEND_FRIEND_REQUEST", payload),
            )
        return True
    except Exception:
        log.exception(
            "Failed to queue Steam friend request",
            extra={
                "steam_id_length": len(sid),
                "steam_id_valid": bool(re.fullmatch(r"\d{17,20}", sid)),
            },
        )
        return False


def _outgoing_enabled() -> bool:
    if _settings is not None:
        try:
            return bool(getattr(_settings, "steam_outgoing_friend_requests_enabled", False))
        except Exception:
            return False
    return False


def _log_outgoing_status_once() -> None:
    global _outgoing_status_logged
    if _outgoing_status_logged:
        return
    enabled = _outgoing_enabled()
    source = (
        "settings.steam_outgoing_friend_requests_enabled"
        if _settings is not None
        else f"env:{_OUTGOING_ENV_NAME} (settings unavailable)"
    )
    log.info(
        "Outgoing Steam friend requests are %s (control=%s, env=%s)",
        "ENABLED" if enabled else "DISABLED",
        source,
        _OUTGOING_ENV_NAME,
    )
    _outgoing_status_logged = True


def queue_friend_requests(steam_ids: Iterable[str]) -> bool:
    """Queue outgoing Steam friend requests for the given SteamIDs."""
    if not _outgoing_enabled():
        log.debug("Outgoing Steam friend requests are disabled; skipping queue batch")
        return False
    if not steam_ids:
        return True
    if not _ensure_table():
        return False
    ok = True
    for steam_id in steam_ids:
        if not _queue_single(steam_id, trigger_task=True):
            ok = False
    return ok


def queue_friend_request(steam_id: str) -> bool:
    """Queue a single outgoing Steam friend request."""
    if not _outgoing_enabled():
        log.debug("Outgoing Steam friend requests are disabled; skipping single queue")
        return False
    if not _ensure_table():
        return False
    return _queue_single(steam_id, trigger_task=True)


__all__ = ["queue_friend_request", "queue_friend_requests"]

_log_outgoing_status_once()
