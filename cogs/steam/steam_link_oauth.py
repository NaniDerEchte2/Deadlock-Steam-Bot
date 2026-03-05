# cogs/steam/steam_link_oauth.py
import asyncio
import hashlib
import html
import hmac
import logging
import re
import secrets
import sqlite3
import threading
import time
import uuid
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp
import discord
from aiohttp import web
from discord.ext import commands

from cogs.steam.friend_requests import (
    queue_friend_request,
    queue_friend_requests,
    queue_manual_friend_accept,
)
from cogs.steam.logging_utils import sanitize_log_value
from cogs.steam.steam_master import SteamTaskClient
from service import db
from service.config import settings

log = logging.getLogger("SteamLink")

DISCORD_API = "https://discord.com/api"
STEAM_API_BASE = "https://api.steampowered.com"
STEAM_OPENID_ENDPOINT = "https://steamcommunity.com/openid/login"
OPENID_NS = "http://specs.openid.net/auth/2.0"
IDENTIFIER_SELECT = "http://specs.openid.net/auth/2.0/identifier_select"

# Centralized config via service.config.settings
PUBLIC_BASE_URL = settings.public_base_url.rstrip("/")
STEAM_RETURN_PATH = settings.steam_return_path
STEAM_RETURN_URL = urljoin(PUBLIC_BASE_URL + "/", STEAM_RETURN_PATH.lstrip("/"))

HTTP_HOST = settings.http_host
HTTP_PORT = settings.http_port
# CLIENT_SECRET aus zentralen Settings (kein direkter os.getenv).
CLIENT_SECRET = (settings.discord_oauth_client_secret or "").strip()

# State TTL
STATE_TTL_SEC = 600  # 10 min
STATE_MAX_ENTRIES = 4096
STEAM_LOGIN_LAUNCH_TTL_SEC = 180
FRIEND_CODE_SUBMIT_COOLDOWN_SEC = 12
FRIEND_CODE_DEDUPE_WINDOW_SEC = 120
FRIEND_CODE_TRACK_TTL_SEC = 900
FRIEND_CODE_TRACK_MAX_USERS = 2048
STATE_PRUNE_INTERVAL_SEC = 30
FRIEND_CODE_PRUNE_INTERVAL_SEC = 30
_friend_code_linking_enabled_raw = getattr(settings, "friend_code_linking_enabled", True)
if isinstance(_friend_code_linking_enabled_raw, bool):
    FRIEND_CODE_LINKING_ENABLED = _friend_code_linking_enabled_raw
else:
    FRIEND_CODE_LINKING_ENABLED = (
        str(_friend_code_linking_enabled_raw).strip().lower() not in {"0", "false", "off", "no"}
    )
DISCORD_LOGIN_RATE_WINDOW_SEC = 60
DISCORD_LOGIN_RATE_MAX_REQUESTS = 20
DISCORD_LOGIN_RATE_TRACK_TTL_SEC = 600
DISCORD_LOGIN_RATE_TRACK_MAX_KEYS = 4096

# UI (deutsche Labels)
LINK_COVER_IMAGE = settings.link_cover_image
LINK_COVER_LABEL = settings.link_cover_label
LINK_BUTTON_LABEL = settings.link_button_label
STEAM_BUTTON_LABEL = settings.steam_button_label
STEAM64_BASE = 76561197960265728
MAX_STEAM_ACCOUNT_ID = 2**32 - 1
FRIENDSHIP_VERIFY_TIMEOUT_SEC = 300  # 5 Minuten
FRIENDSHIP_VERIFY_POLL_INTERVAL_SEC = 10
_LAUNCH_TOKEN_FALLBACK_SECRET = secrets.token_bytes(32)
_STEAM_LAUNCH_TOKEN_LOCK = threading.Lock()
# Process-local issued-token cache: unconsumed launch tokens are only accepted once
# and fail closed after expiry or process restart.
_PENDING_STEAM_LAUNCHES: dict[str, int] = {}
_DISCORD_LOGIN_RATE_LOCK = threading.Lock()
_DISCORD_LOGIN_RATE_TRACK: dict[str, list[float]] = {}

# ---------------------------------------------------------------------------
# Öffentliche Schnittstelle für andere Cogs (Welcome-DM, Rules-Panel, etc.)
# ---------------------------------------------------------------------------
__all__ = ("get_public_urls", "start_urls_for", "build_steam_login_start_url")


def _safe_log_repr(value: Any) -> str:
    """Return a repr-style string with control chars escaped for safe logging."""
    return repr(sanitize_log_value(value))


def _friend_code_to_steam_id64(friend_code_raw: str) -> str:
    code = str(friend_code_raw or "").strip().replace(" ", "").replace("-", "")
    if not code:
        raise ValueError("Bitte gib einen Steam-Freundescode ein.")
    if not code.isdigit():
        raise ValueError("Der Steam-Freundescode muss nur aus Zahlen bestehen.")

    account_id = int(code)
    if account_id <= 0:
        raise ValueError("Der Steam-Freundescode muss größer als 0 sein.")
    if account_id > MAX_STEAM_ACCOUNT_ID:
        raise ValueError("Der Steam-Freundescode liegt außerhalb des gültigen Bereichs.")

    steam_id64 = str(STEAM64_BASE + account_id)
    if not re.fullmatch(r"\d{17,20}", steam_id64):
        raise ValueError("Aus dem Freundescode konnte keine gültige SteamID64 erzeugt werden.")
    return steam_id64


def _friend_code_flow_disabled_message() -> str:
    return (
        "❌ Der Freundescode-Flow ist aktuell deaktiviert. "
        "Bitte nutze stattdessen den Steam-Login-Button oder kontaktiere das Team."
    )


def _steam_launch_secret() -> bytes:
    secret = CLIENT_SECRET
    if secret:
        return secret.encode("utf-8")
    try:
        api_key = (settings.steam_api_key.get_secret_value() if settings.steam_api_key else "").strip()
    except Exception:
        api_key = ""
    if api_key:
        return api_key.encode("utf-8")
    return _LAUNCH_TOKEN_FALLBACK_SECRET


def _sign_steam_launch_payload(payload: str) -> str:
    return hmac.new(_steam_launch_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _prune_pending_steam_launches(*, now: int | None = None) -> None:
    cutoff = int(time.time()) if now is None else int(now)
    expired = [payload for payload, exp in _PENDING_STEAM_LAUNCHES.items() if exp <= cutoff]
    for payload in expired:
        _PENDING_STEAM_LAUNCHES.pop(payload, None)


def _remember_steam_launch_payload(payload: str, *, exp: int) -> None:
    now = int(time.time())
    with _STEAM_LAUNCH_TOKEN_LOCK:
        _prune_pending_steam_launches(now=now)
        _PENDING_STEAM_LAUNCHES[payload] = int(exp)


def _consume_steam_launch_payload(payload: str, *, exp: int) -> bool:
    now = int(time.time())
    with _STEAM_LAUNCH_TOKEN_LOCK:
        _prune_pending_steam_launches(now=now)
        tracked_exp = _PENDING_STEAM_LAUNCHES.get(payload)
        if tracked_exp != int(exp) or int(exp) <= now:
            _PENDING_STEAM_LAUNCHES.pop(payload, None)
            return False
        _PENDING_STEAM_LAUNCHES.pop(payload, None)
        return True


def _make_steam_launch_token(uid: int, *, ttl_sec: int = STEAM_LOGIN_LAUNCH_TTL_SEC) -> str:
    now = int(time.time())
    exp = now + max(30, int(ttl_sec))
    nonce = secrets.token_hex(8)
    payload = f"{int(uid)}.{exp}.{nonce}"
    sig = _sign_steam_launch_payload(payload)
    _remember_steam_launch_payload(payload, exp=exp)
    return f"{payload}.{sig}"


def _verify_steam_launch_token(token: str) -> int | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    parts = raw.split(".")
    if len(parts) != 4:
        return None
    uid_raw, exp_raw, nonce, sig = parts
    if not uid_raw.isdigit() or not exp_raw.isdigit():
        return None
    if not re.fullmatch(r"[0-9a-fA-F]{16,64}", nonce):
        return None
    if not re.fullmatch(r"[0-9a-fA-F]{64}", sig):
        return None

    uid = int(uid_raw)
    exp = int(exp_raw)
    if uid <= 0 or exp <= int(time.time()):
        return None

    payload = f"{uid}.{exp}.{nonce}"
    expected = _sign_steam_launch_payload(payload)
    if not hmac.compare_digest(sig.lower(), expected.lower()):
        return None
    if not _consume_steam_launch_payload(payload, exp=exp):
        return None
    return uid


def _discord_login_client_key(request: web.Request) -> str:
    forwarded_for = str(request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded_for:
        client_ip = forwarded_for.split(",", 1)[0].strip()
        if client_ip:
            return client_ip

    remote = str(getattr(request, "remote", "") or "").strip()
    return remote or "unknown"


def _consume_discord_login_budget(request: web.Request) -> bool:
    now_mono = time.monotonic()
    client_key = _discord_login_client_key(request)
    window_cutoff = now_mono - DISCORD_LOGIN_RATE_WINDOW_SEC
    track_cutoff = now_mono - DISCORD_LOGIN_RATE_TRACK_TTL_SEC

    with _DISCORD_LOGIN_RATE_LOCK:
        if _DISCORD_LOGIN_RATE_TRACK:
            stale_keys = [
                key
                for key, stamps in _DISCORD_LOGIN_RATE_TRACK.items()
                if not stamps or stamps[-1] <= track_cutoff
            ]
            for key in stale_keys:
                _DISCORD_LOGIN_RATE_TRACK.pop(key, None)

        stamps = [ts for ts in _DISCORD_LOGIN_RATE_TRACK.get(client_key, []) if ts > window_cutoff]
        if len(stamps) >= DISCORD_LOGIN_RATE_MAX_REQUESTS:
            _DISCORD_LOGIN_RATE_TRACK[client_key] = stamps
            return False

        stamps.append(now_mono)
        _DISCORD_LOGIN_RATE_TRACK[client_key] = stamps

        overflow = len(_DISCORD_LOGIN_RATE_TRACK) - DISCORD_LOGIN_RATE_TRACK_MAX_KEYS
        if overflow > 0:
            oldest_first = sorted(
                _DISCORD_LOGIN_RATE_TRACK.items(),
                key=lambda item: item[1][-1] if item[1] else 0.0,
            )
            for key, _ in oldest_first[:overflow]:
                _DISCORD_LOGIN_RATE_TRACK.pop(key, None)

    return True


def build_steam_login_start_url(uid: int) -> str:
    base = settings.public_base_url.rstrip("/")
    if not base:
        return ""
    token = _make_steam_launch_token(int(uid))
    return f"{base}/steam/login?launch={token}"


class SteamFriendCodeModal(discord.ui.Modal, title="Steam-Freundescode"):
    friend_code = discord.ui.TextInput(
        label="Freundescode",
        placeholder="z. B. 820142646",
        required=True,
        min_length=1,
        max_length=32,
    )

    def __init__(self, *, user_id: int, link_cog: "SteamLink"):
        super().__init__(timeout=300)
        self.user_id = int(user_id)
        self.link_cog = link_cog

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "Nur der ursprüngliche Nutzer kann diesen Freundescode einreichen.",
                ephemeral=True,
            )
            return
        if not FRIEND_CODE_LINKING_ENABLED:
            await interaction.response.send_message(
                _friend_code_flow_disabled_message(),
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self.link_cog._handle_friend_code_submission(
            interaction, str(self.friend_code.value or "")
        )

    async def on_error(self, interaction: discord.Interaction, _error: Exception) -> None:
        log.exception("Friend-Code Modal fehlgeschlagen (user_id=%s)", self.user_id)
        msg = "❌ Beim Verarbeiten des Freundescodes ist ein Fehler aufgetreten. Bitte versuche es erneut."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            log.debug("Konnte Modal-Fehlermeldung nicht senden", exc_info=True)


class LinkPanelView(discord.ui.View):
    def __init__(self, *, user_id: int, steam_url: str, link_cog: "SteamLink"):
        super().__init__(timeout=600)
        self.user_id = int(user_id)
        self.link_cog = link_cog
        self.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label="Steam Account verknüpfen",
                url=steam_url,
            )
        )

    @discord.ui.button(
        label="Freundescode eingeben",
        style=discord.ButtonStyle.secondary,
        emoji="👥",
        custom_id="linkpanel_friend_code",
    )
    async def friend_code(self, interaction: discord.Interaction, _button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "Nur der ursprüngliche Nutzer kann das verwenden.", ephemeral=True
            )
            return
        if not FRIEND_CODE_LINKING_ENABLED:
            await interaction.response.send_message(
                _friend_code_flow_disabled_message(),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(
            SteamFriendCodeModal(user_id=self.user_id, link_cog=self.link_cog)
        )

    @discord.ui.button(
        label="Rank check (Steam)",
        style=discord.ButtonStyle.secondary,
        emoji="📊",
        custom_id="linkpanel_rank_check",
    )
    async def rank_check(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            if interaction.user.id != self.user_id:
                await interaction.followup.send(
                    "Nur der ursprüngliche Nutzer kann das verwenden.", ephemeral=True
                )
                return

            rank_cog = self.link_cog.bot.get_cog("DeadlockFriendRank")
            if rank_cog is None or not hasattr(rank_cog, "_fetch_profile_card"):
                await interaction.followup.send(
                    "Rank-Modul nicht geladen. Bitte Admin informieren.", ephemeral=True
                )
                return

            steam_ids = (
                rank_cog.linked_steam_ids_for_user(int(self.user_id))
                if hasattr(rank_cog, "linked_steam_ids_for_user")
                else None
            )
            if not steam_ids:
                await interaction.followup.send(
                    "Kein verknüpfter Steam-Account gefunden. Bitte zuerst verknüpfen.",
                    ephemeral=True,
                )
                return
            sid = str(steam_ids[0]).strip()
            if not sid:
                await interaction.followup.send("Kein valider Steam-Link gefunden.", ephemeral=True)
                return

            card, data, outcome = await rank_cog._fetch_profile_card(
                {"steam_id": sid}, timeout=45.0
            )
            if outcome.timed_out or not outcome.ok or not isinstance(card, dict):
                await interaction.followup.send(
                    f"Rank-Abfrage fehlgeschlagen ({outcome.error or 'no data'}).",
                    ephemeral=True,
                )
                return

            snap = rank_cog._snapshot_from_profile_card(sid, card, data)
            if not snap:
                await interaction.followup.send(
                    "Keine Rank-Daten in der PlayerCard gefunden.", ephemeral=True
                )
                return

            text = (
                f"**Rank für {sid}**\n"
                f"Tier: {snap.rank_name} (#{snap.rank_value})\n"
                f"Subrank: {snap.subrank or '–'}\n"
                f"Badge: {snap.badge_level or '–'}"
            )
            await interaction.followup.send(text, ephemeral=True)
        except Exception as exc:
            log.debug("Rank check button failed", exc_info=True, extra={"user_id": self.user_id})
            await interaction.followup.send(f"Fehler bei der Rank-Abfrage: {exc}", ephemeral=True)


def _env_client_id(bot: commands.Bot) -> str:
    cid = (settings.discord_oauth_client_id or "").strip()
    if cid:
        return cid
    app_id = getattr(bot, "application_id", None)
    return str(app_id) if app_id else ""


def _env_redirect() -> str:
    # Aus Settings; Fallback: public_base_url/discord/callback
    configured = (settings.discord_oauth_redirect or "").strip()
    if configured:
        return configured
    return settings.public_base_url.rstrip("/") + "/discord/callback"


def get_public_urls() -> dict:
    """
    Quelle der Wahrheit für Start-/Callback-Links.
    UI-Cogs sollten für user-spezifische Startlinks `start_urls_for(uid)` nutzen.
    `steam_openid_start` ist nur der technische Einstiegspunkt und benötigt
    einen gültigen `launch`-Token oder (legacy) `uid`.
    Kein Fallback: fehlt etwas Wesentliches -> ImportError (Start abbrechen).
    """
    base = settings.public_base_url.rstrip("/")
    if not base:
        # entspricht Vorgabe: hart abbrechen, kein Fallback
        raise ImportError("PUBLIC_BASE_URL ist nicht gesetzt – keine öffentlichen URLs verfügbar.")

    urls = {
        # Startpunkte (user-spezifische Links über start_urls_for(uid))
        "discord_start": f"{base}/discord/login",
        "steam_openid_start": f"{base}/steam/login",
        # Callbacks (für Vollständigkeit/Debug)
        "discord_callback": _env_redirect(),
        "steam_return": urljoin(base + "/", STEAM_RETURN_PATH.lstrip("/")),
    }

    # Minimalvalidierung
    for k in (
        "discord_start",
        "steam_openid_start",
        "discord_callback",
        "steam_return",
    ):
        u = urls.get(k)
        if not u or "://" not in u:
            raise ImportError(f"Ungültige URL für {k}: {u!r}")
    return urls


def start_urls_for(uid: int) -> dict:
    """
    Liefert user-spezifische Start-URLs.
    Wird vom SteamLinkStepView (Welcome-DM / Rules-Panel) beim Klick verwendet.
    """
    base = settings.public_base_url.rstrip("/")
    if not base:
        # bewusst nicht hart fehlschlagen – die UI meldet es dem Nutzer ephemer
        return {"discord_start": "", "steam_openid_start": ""}

    u = int(uid)
    return {
        "discord_start": f"{base}/discord/login?uid={u}",
        "steam_openid_start": build_steam_login_start_url(u),
    }


# ----------------------- DB-Schema -------------------------------------------
def _ensure_steam_link_ownership_guardrails() -> None:
    try:
        db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_steam_links_steam_owner
            ON steam_links(steam_id)
            WHERE user_id != 0
            """
        )
    except sqlite3.IntegrityError as exc:
        log.warning(
            "Konnte den eindeutigen Steam-Ownership-Index nicht anlegen "
            "(evtl. historische Duplikate): %s",
            exc,
        )
    except Exception as exc:
        log.warning(
            "Konnte den eindeutigen Steam-Ownership-Index nicht anlegen "
            "(SQLite-Einschränkung oder anderer Fehler): %s",
            exc,
        )

    trigger_statements = [
        "DROP TRIGGER IF EXISTS trg_steam_links_owner_guard_insert",
        "DROP TRIGGER IF EXISTS trg_steam_links_owner_guard_update",
        """
        CREATE TRIGGER trg_steam_links_owner_guard_insert
        BEFORE INSERT ON steam_links
        FOR EACH ROW
        WHEN NEW.user_id != 0
          AND EXISTS (
            SELECT 1
            FROM steam_links
            WHERE steam_id = NEW.steam_id
              AND user_id NOT IN (NEW.user_id, 0)
          )
        BEGIN
          SELECT RAISE(ABORT, 'steam_links ownership conflict');
        END
        """,
        """
        CREATE TRIGGER trg_steam_links_owner_guard_update
        BEFORE UPDATE OF user_id, steam_id ON steam_links
        FOR EACH ROW
        WHEN NEW.user_id != 0
          AND (NEW.user_id != OLD.user_id OR NEW.steam_id != OLD.steam_id)
          AND EXISTS (
            SELECT 1
            FROM steam_links
            WHERE steam_id = NEW.steam_id
              AND user_id NOT IN (NEW.user_id, 0)
          )
        BEGIN
          SELECT RAISE(ABORT, 'steam_links ownership conflict');
        END
        """,
    ]
    for statement in trigger_statements:
        try:
            db.execute(statement)
        except Exception:
            log.warning(
                "Konnte Steam-Ownership-Guardrail nicht sicherstellen.",
                extra={"statement": statement.splitlines()[0].strip() if statement else ""},
                exc_info=True,
            )


def _ensure_schema() -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS steam_links(
          user_id         INTEGER NOT NULL,
          steam_id        TEXT    NOT NULL,
          name            TEXT,
          verified        INTEGER DEFAULT 0,
          primary_account INTEGER DEFAULT 0,
          created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (user_id, steam_id)
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_steam_links_user ON steam_links(user_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_steam_links_steam ON steam_links(steam_id)")
    _ensure_steam_link_ownership_guardrails()


def _find_conflicting_steam_link_user_id(user_id: int, steam_id: str) -> int | None:
    row = db.query_one(
        "SELECT user_id FROM steam_links WHERE steam_id=? AND user_id NOT IN (?, 0) LIMIT 1",
        (str(steam_id), int(user_id)),
    )
    if not row:
        return None
    try:
        return int(row["user_id"])
    except Exception:
        return None


def _save_steam_link_row(
    user_id: int,
    steam_id: str,
    name: str = "",
    verified: int = 0,
    *,
    source: str = "unknown",
) -> dict[str, Any]:
    target_user_id = int(user_id)
    target_steam_id = str(steam_id)
    conflicting_user_id = _find_conflicting_steam_link_user_id(target_user_id, target_steam_id)
    if conflicting_user_id is not None:
        log.warning(
            "Blockiere Steam-Link-Speicherung wegen Ownership-Konflikt "
            "(source=%s, user_id=%s, steam_id=%s, owner_user_id=%s)",
            source,
            target_user_id,
            _safe_log_repr(target_steam_id),
            conflicting_user_id,
        )
        return {
            "saved": False,
            "queue_ok": False,
            "conflicting_user_id": conflicting_user_id,
        }

    try:
        db.execute(
            """
            INSERT INTO steam_links(user_id, steam_id, name, verified)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id, steam_id) DO UPDATE SET
              name=excluded.name,
              verified=CASE WHEN verified > excluded.verified THEN verified ELSE excluded.verified END,
              updated_at=CURRENT_TIMESTAMP
            """,
            (target_user_id, target_steam_id, name or "", int(verified)),
        )
    except Exception:
        conflicting_user_id = _find_conflicting_steam_link_user_id(target_user_id, target_steam_id)
        if conflicting_user_id is not None:
            log.warning(
                "Blockiere Steam-Link-Speicherung nach DB-Ownership-Guardrail "
                "(source=%s, user_id=%s, steam_id=%s, owner_user_id=%s)",
                source,
                target_user_id,
                _safe_log_repr(target_steam_id),
                conflicting_user_id,
            )
            return {
                "saved": False,
                "queue_ok": False,
                "conflicting_user_id": conflicting_user_id,
            }
        raise
    queue_ok = bool(queue_friend_request(target_steam_id))
    manual_queue_ok = None
    if not queue_ok:
        log.warning(
            "Konnte Steam-Freundschaftsanfrage nicht einreihen (steam_id=%s)",
            _safe_log_repr(target_steam_id),
        )
        manual_queue_ok = bool(queue_manual_friend_accept(target_steam_id))
        if manual_queue_ok:
            log.info(
                "Manual-Accept-Fallback eingereiht (steam_id=%s)",
                _safe_log_repr(target_steam_id),
            )
        else:
            log.warning(
                "Manual-Accept-Fallback fehlgeschlagen (steam_id=%s)",
                _safe_log_repr(target_steam_id),
            )
    return {
        "saved": True,
        "queue_ok": queue_ok,
        "manual_queue_ok": manual_queue_ok,
        "conflicting_user_id": None,
    }


# ----------------------- Middleware (Top-Level) -------------------------------
@web.middleware
async def security_headers_mw(request: web.Request, handler):
    try:
        resp = await handler(request)
    except web.HTTPException as ex:
        if ex.status in (404, 405):
            resp = web.Response(
                text="Not Found" if ex.status == 404 else "Method Not Allowed",
                status=ex.status,
                content_type="text/plain",
            )
        else:
            resp = ex
    except Exception:
        log.exception("Unhandled error in request")
        resp = web.Response(
            text=(
                "<html><body style='font-family: system-ui, sans-serif'>"
                "<h3>❌ Unerwarteter Fehler</h3>"
                "<p>Bitte versuche es erneut. Wenn das Problem bleibt, kontaktiere den Admin.</p>"
                "</body></html>"
            ),
            content_type="text/html",
            status=500,
        )

    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    resp.headers["Content-Security-Policy"] = (
        "default-src 'none'; style-src 'unsafe-inline'; "
        "form-action https://steamcommunity.com; base-uri 'none'; frame-ancestors 'none'"
    )
    return resp


# ----------------------- Cog --------------------------------------------------
class SteamLink(commands.Cog):
    """
    Linking-Flow:
      1) /account_verknüpfen → Discord OAuth2 (identify + connections) (Lazy-Start)
      2) Keine Steam-Verknüpfung gefunden → seamless Redirect zu Steam OpenID
      3) /steam/return → SteamID64 extrahieren → speichern
      4) Erfolg → DM an User
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.app = web.Application(middlewares=[security_headers_mw])
        self.tasks = SteamTaskClient(poll_interval=0.5, default_timeout=45.0)
        self.tasks = SteamTaskClient(poll_interval=0.5, default_timeout=45.0)

        # HTTP-Routen
        self.app.router.add_get("/", self.handle_index)
        self.app.router.add_get("/health", self.handle_health)

        # Discord OAuth2 (Lazy-Start + Callback)
        self.app.router.add_get("/discord/login", self.handle_discord_login)
        self.app.router.add_get("/discord/callback", self.handle_discord_callback)

        # Steam OpenID (Lazy-Start + Return)
        self.app.router.add_get("/steam/login", self.handle_steam_login)
        self.app.router.add_get(STEAM_RETURN_PATH, self.handle_steam_return)

        # Kleinkram
        self.app.router.add_get("/favicon.ico", self.handle_favicon)
        self.app.router.add_get("/robots.txt", self.handle_robots)

        self._runner: web.AppRunner | None = None
        self._states: dict[str, dict[str, Any]] = {}  # state -> {uid, ts, context}
        self._state_last_prune_ts = 0.0
        self._friend_code_track: dict[int, dict[str, Any]] = {}
        self._friend_code_track_last_prune_ts = 0.0

    # --------------- Lifecycle -----------------------------------------------
    async def cog_load(self) -> None:
        _ensure_schema()
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

        cid = _env_client_id(self.bot)
        if not cid:
            log.warning(
                "Discord OAuth CLIENT_ID fehlt (DISCORD_OAUTH_CLIENT_ID oder bot.application_id)."
            )
        if not CLIENT_SECRET:
            log.warning("DISCORD_OAUTH_CLIENT_SECRET fehlt – Token-Exchange wird scheitern.")

        if not PUBLIC_BASE_URL:
            log.error("PUBLIC_BASE_URL ist NICHT gesetzt – Start-/Return-Routen brauchen sie.")
        else:
            log.info("Steam OpenID return_to: %s", STEAM_RETURN_URL)

        self._runner = web.AppRunner(self.app)
        await self._runner.setup()

        # Retry logic for port availability during reloads
        max_retries = 5
        retry_delay = 0.5

        for attempt in range(max_retries):
            try:
                site = web.TCPSite(self._runner, host=HTTP_HOST, port=HTTP_PORT)
                await site.start()
                log.info(
                    "OAuth/OpenID Callback-Server läuft auf %s:%s (Discord redirect=%s, STATE_TTL_SEC=%ss)",
                    HTTP_HOST,
                    HTTP_PORT,
                    _env_redirect(),
                    STATE_TTL_SEC,
                )
                return
            except OSError as e:
                # Check for address in use (WinError 10048 or EADDRINUSE)
                import errno

                is_addr_in_use = e.errno == 10048 or e.errno == errno.EADDRINUSE

                if is_addr_in_use and attempt < max_retries - 1:
                    log.debug(
                        "Steam OAuth port %s belegt, versuche es erneut in %ss... (Versuch %s/%s)",
                        HTTP_PORT,
                        retry_delay,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                log.exception(
                    "Konnte OAuth Callback-Server nicht starten (Port belegt oder anderer Fehler)"
                )
                break
            except Exception:
                log.exception("Konnte OAuth Callback-Server nicht starten")
                break

    async def cog_unload(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    # --------------- Helpers --------------------------------------------------
    def _prune_states(self, *, now: float | None = None, force: bool = False) -> None:
        now_ts = time.time() if now is None else float(now)
        if (
            not force
            and len(self._states) < STATE_MAX_ENTRIES
            and (now_ts - self._state_last_prune_ts) < STATE_PRUNE_INTERVAL_SEC
        ):
            return

        if self._states:
            expiry_cutoff = now_ts - STATE_TTL_SEC
            stale_keys = [
                key
                for key, data in self._states.items()
                if float(data.get("ts", 0.0)) <= expiry_cutoff
            ]
            for key in stale_keys:
                self._states.pop(key, None)

        self._state_last_prune_ts = now_ts

    def _mk_state(self, uid: int, context: str = "steam_link") -> str:
        now_ts = time.time()
        self._prune_states(now=now_ts, force=len(self._states) >= STATE_MAX_ENTRIES)
        if len(self._states) >= STATE_MAX_ENTRIES:
            log.warning(
                "State map at capacity after prune (entries=%s, max=%s)",
                len(self._states),
                STATE_MAX_ENTRIES,
            )
            raise RuntimeError("state storage is temporarily saturated")

        s = uuid.uuid4().hex
        self._states[s] = {"uid": int(uid), "ts": now_ts, "context": context}
        return s

    def _pop_state(self, s: str) -> int | None:
        self._prune_states()
        data = self._states.pop(s, None)
        if not data:
            return None
        if time.time() - data["ts"] > STATE_TTL_SEC:
            return None
        return int(data["uid"])

    def _pop_state_data(self, s: str) -> dict | None:
        """Returns full state dict including context, or None if missing/expired."""
        self._prune_states()
        data = self._states.pop(s, None)
        if not data:
            return None
        if time.time() - data["ts"] > STATE_TTL_SEC:
            return None
        return data

    def _prune_friend_code_track(self, *, now: float | None = None, force: bool = False) -> None:
        now_mono = time.monotonic() if now is None else float(now)
        if (
            not force
            and len(self._friend_code_track) < FRIEND_CODE_TRACK_MAX_USERS
            and (now_mono - self._friend_code_track_last_prune_ts) < FRIEND_CODE_PRUNE_INTERVAL_SEC
        ):
            return

        if self._friend_code_track:
            expiry_cutoff = now_mono - FRIEND_CODE_TRACK_TTL_SEC
            stale_user_ids = [
                uid
                for uid, data in self._friend_code_track.items()
                if float(data.get("activity_ts", 0.0)) <= expiry_cutoff
            ]
            for uid in stale_user_ids:
                self._friend_code_track.pop(uid, None)

            overflow = len(self._friend_code_track) - FRIEND_CODE_TRACK_MAX_USERS
            if overflow > 0:
                oldest_first = sorted(
                    self._friend_code_track.items(),
                    key=lambda item: float(item[1].get("activity_ts", 0.0)),
                )
                for uid, _ in oldest_first[:overflow]:
                    self._friend_code_track.pop(uid, None)

        self._friend_code_track_last_prune_ts = now_mono

    def _friend_code_bucket_for_user(self, user_id: int, *, now: float) -> dict[str, Any]:
        uid = int(user_id)
        needs_space = (
            uid not in self._friend_code_track
            and len(self._friend_code_track) >= FRIEND_CODE_TRACK_MAX_USERS
        )
        self._prune_friend_code_track(now=now, force=needs_space)

        data = self._friend_code_track.get(uid)
        if data is None:
            if (
                len(self._friend_code_track) >= FRIEND_CODE_TRACK_MAX_USERS
                and self._friend_code_track
            ):
                oldest_uid = min(
                    self._friend_code_track.items(),
                    key=lambda item: float(item[1].get("activity_ts", 0.0)),
                )[0]
                self._friend_code_track.pop(oldest_uid, None)
            data = {}
            self._friend_code_track[uid] = data
        data["activity_ts"] = now
        return data

    def _friend_code_cooldown_message(self, user_id: int) -> str | None:
        now_mono = time.monotonic()
        data = self._friend_code_bucket_for_user(user_id, now=now_mono)

        elapsed = now_mono - float(data.get("submit_ts", 0.0))
        remaining = FRIEND_CODE_SUBMIT_COOLDOWN_SEC - elapsed
        if remaining > 0:
            wait_sec = max(1, int(remaining + 0.999))
            return (
                "⏳ Bitte warte noch "
                f"{wait_sec} Sekunden, bevor du erneut einen Freundescode einreichst."
            )

        data["submit_ts"] = now_mono
        data["activity_ts"] = now_mono
        return None

    def _friend_code_dedupe_message(self, user_id: int, steam_id: str) -> str | None:
        now_mono = time.monotonic()
        data = self._friend_code_bucket_for_user(user_id, now=now_mono)

        last_steam_id = str(data.get("steam_id", ""))
        last_steam_id_ts = float(data.get("steam_id_ts", 0.0))
        remaining = FRIEND_CODE_DEDUPE_WINDOW_SEC - (now_mono - last_steam_id_ts)
        if last_steam_id == steam_id and remaining > 0:
            wait_sec = max(1, int(remaining + 0.999))
            return (
                "ℹ️ Diesen Freundescode hast du gerade bereits eingereicht. "
                f"Bitte warte noch {wait_sec} Sekunden."
            )

        data["steam_id"] = str(steam_id)
        data["steam_id_ts"] = now_mono
        data["activity_ts"] = now_mono
        return None

    async def _kickoff_profile_card(self, steam_id: str) -> None:
        """
        Fire-and-forget PlayerCard fetch to füllen Rank-Daten direkt nach Verknüpfung.
        """
        if not steam_id or not re.fullmatch(r"\d{17,20}", str(steam_id).strip()):
            return

        async def _run():
            try:
                await self.tasks.run("GC_GET_PROFILE_CARD", {"steam_id": steam_id}, timeout=45.0)
            except Exception:
                log.debug(
                    "ProfileCard prefetch failed",
                    exc_info=True,
                    extra={"steam_id": steam_id},
                )

        asyncio.create_task(_run(), name="steam-link-profilecard")

    def _find_conflicting_steam_link_user(self, user_id: int, steam_id: str) -> int | None:
        return _find_conflicting_steam_link_user_id(user_id, steam_id)

    def _mark_steam_link_verified(self, user_id: int, steam_id: str) -> None:
        try:
            db.execute(
                "UPDATE steam_links SET verified=1, is_steam_friend=1, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND steam_id=?",
                (int(user_id), str(steam_id)),
            )
            return
        except Exception as exc:
            if "is_steam_friend" not in str(exc):
                log.debug(
                    "Konnte is_steam_friend nicht setzen, fallback auf verified-only",
                    exc_info=True,
                )
        try:
            db.execute(
                "UPDATE steam_links SET verified=1, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND steam_id=?",
                (int(user_id), str(steam_id)),
            )
        except Exception:
            log.exception(
                "Konnte Steam-Link nicht als verified markieren (user_id=%s, steam_id=%s)",
                user_id,
                _safe_log_repr(steam_id),
            )

    async def _trigger_verified_role_assignment(self, user_id: int) -> None:
        try:
            verified_cog = self.bot.get_cog("SteamVerifiedRole")
            if verified_cog and hasattr(verified_cog, "assign_verified_role_with_retries"):
                await verified_cog.assign_verified_role_with_retries(int(user_id))
        except Exception:
            log.debug(
                "Sofortige Rollenvergabe nach Verifikation fehlgeschlagen (user_id=%s)",
                user_id,
                exc_info=True,
            )

    async def _poll_friendship_verification(
        self,
        *,
        user_id: int,
        steam_id: str,
        timeout_sec: int = FRIENDSHIP_VERIFY_TIMEOUT_SEC,
        poll_interval_sec: int = FRIENDSHIP_VERIFY_POLL_INTERVAL_SEC,
    ) -> tuple[str, str | None]:
        deadline = time.monotonic() + max(5, int(timeout_sec))
        last_error: str | None = None
        had_successful_check = False

        while time.monotonic() < deadline:
            remaining = max(1.0, deadline - time.monotonic())
            task_timeout = min(20.0, remaining)
            try:
                outcome = await self.tasks.run(
                    "AUTH_CHECK_FRIENDSHIP",
                    {"steam_id": str(steam_id)},
                    timeout=task_timeout,
                )
            except Exception as exc:
                last_error = str(exc)
                log.debug(
                    "AUTH_CHECK_FRIENDSHIP raised (user_id=%s, steam_id=%s)",
                    user_id,
                    _safe_log_repr(steam_id),
                    exc_info=True,
                )
            else:
                if outcome.ok and isinstance(outcome.result, dict):
                    had_successful_check = True
                    data = outcome.result.get("data")
                    if isinstance(data, dict) and bool(data.get("friend")):
                        self._mark_steam_link_verified(user_id, steam_id)
                        await self._trigger_verified_role_assignment(user_id)
                        return "verified", None
                else:
                    last_error = outcome.error or (
                        "Timeout bei der Steam-Freundschaftsprüfung"
                        if outcome.timed_out
                        else "Steam-Freundschaftsprüfung fehlgeschlagen"
                    )

            sleep_for = min(
                max(0, int(poll_interval_sec)),
                max(0.0, deadline - time.monotonic()),
            )
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        if had_successful_check:
            return "timeout", None
        return "error", last_error or "Keine gültige Antwort von Steam erhalten."

    async def _handle_friend_code_submission(
        self, interaction: discord.Interaction, friend_code_raw: str
    ) -> None:
        if not FRIEND_CODE_LINKING_ENABLED:
            await interaction.followup.send(_friend_code_flow_disabled_message(), ephemeral=True)
            return

        user_id = int(interaction.user.id)

        cooldown_msg = self._friend_code_cooldown_message(user_id)
        if cooldown_msg:
            await interaction.followup.send(cooldown_msg, ephemeral=True)
            return

        try:
            steam_id = _friend_code_to_steam_id64(friend_code_raw)
        except ValueError as exc:
            await interaction.followup.send(f"❌ {exc}", ephemeral=True)
            return

        dedupe_msg = self._friend_code_dedupe_message(user_id, steam_id)
        if dedupe_msg:
            await interaction.followup.send(dedupe_msg, ephemeral=True)
            return

        display_name = await self._fetch_persona(steam_id) or await self._discord_at_name(user_id)
        save_result = _save_steam_link_row(
            user_id,
            steam_id,
            display_name,
            verified=0,
            source="friend_code_modal",
        )
        conflicting_user_id = save_result.get("conflicting_user_id")
        if not save_result.get("saved") and conflicting_user_id is not None:
            await interaction.followup.send(
                "❌ Dieser Steam-Account ist bereits mit einem anderen Discord-Konto verknüpft. "
                "Bitte nutze den korrekten Freundescode oder wende dich an das Team.",
                ephemeral=True,
            )
            return

        queue_ok = bool(save_result.get("queue_ok"))
        await self._kickoff_profile_card(steam_id)

        kickoff_hint = (
            "Wir prüfen jetzt bis zu 5 Minuten automatisch, ob die Freundschaft bestätigt wird."
            if queue_ok
            else (
                "⚠️ Die automatische Freundschaftsanfrage konnte nicht gesendet werden.\n"
                "Bitte sende dem Steam-Bot jetzt manuell eine Freundschaftsanfrage "
                "(Freundescode: **820142646**). Wir prüfen danach bis zu 5 Minuten automatisch weiter."
            )
        )
        await interaction.followup.send(
            (
                f"⏳ SteamID64 gespeichert: `{steam_id}`.\n"
                f"{kickoff_hint}"
            ),
            ephemeral=True,
        )

        status, error_text = await self._poll_friendship_verification(
            user_id=user_id,
            steam_id=steam_id,
        )

        if status == "verified":
            suffix = (
                ""
                if queue_ok
                else "\n⚠️ Hinweis: Das Einreihen der Freundschaftsanfrage in die Queue war fehlerhaft."
            )
            await interaction.followup.send(
                (
                    f"✅ Verknüpfung erfolgreich bestätigt.\n"
                    f"SteamID64: `{steam_id}`\n"
                    "Die Steam-Freundschaft wurde erkannt und dein Link ist jetzt verifiziert."
                    f"{suffix}"
                ),
                ephemeral=True,
            )
            return

        if status == "timeout":
            suffix = (
                ""
                if queue_ok
                else (
                    "\n⚠️ Zusätzlich konnte die Freundschaftsanfrage nicht sauber in die Queue gelegt werden.\n"
                    "Bitte sende dem Steam-Bot manuell eine Freundschaftsanfrage "
                    "(Freundescode: **820142646**)."
                )
            )
            await interaction.followup.send(
                (
                    f"⏱️ Zeitlimit erreicht.\n"
                    f"SteamID64: `{steam_id}` wurde gespeichert, aber innerhalb von 5 Minuten "
                    "konnte keine bestätigte Freundschaft erkannt werden.\n"
                    "Bitte nimm die Anfrage in Steam an und starte den Schritt danach erneut."
                    f"{suffix}"
                ),
                ephemeral=True,
            )
            return

        suffix = (
            ""
            if queue_ok
            else (
                "\n⚠️ Die Freundschaftsanfrage konnte nicht in die Queue eingereiht werden.\n"
                "Bitte sende dem Steam-Bot manuell eine Freundschaftsanfrage "
                "(Freundescode: **820142646**)."
            )
        )
        await interaction.followup.send(
            (
                f"❌ Verifikation fehlgeschlagen.\n"
                f"SteamID64: `{steam_id}` wurde gespeichert, aber die Freundschaft konnte nicht geprüft werden.\n"
                f"Fehler: `{error_text or 'unbekannt'}`"
                f"{suffix}"
            ),
            ephemeral=True,
        )

    async def _discord_at_name(self, uid: int) -> str:
        try:
            user = self.bot.get_user(uid) or await self.bot.fetch_user(uid)
            if not user:
                return f"@{uid}"
            at = (
                getattr(user, "global_name", None)
                or getattr(user, "display_name", None)
                or user.name
            )
            at = str(at).strip() if at else str(uid)
            if at.startswith("@"):
                return at
            return f"@{at}"
        except Exception:
            return f"@{uid}"

    async def _cleanup_recent_bot_dms(
        self, user: discord.User | discord.Member, *, limit: int = 25
    ) -> None:
        try:
            dm = user.dm_channel or await user.create_dm()
            bot_id = self.bot.user.id if self.bot.user else None
            if not bot_id:
                return
            async for msg in dm.history(limit=limit):
                if msg.author and msg.author.id == bot_id:
                    try:
                        await msg.delete()
                    except Exception as e:
                        log.debug(
                            "DM-Cleanup scheiterte (user_id=%s): %s",
                            getattr(user, "id", "?"),
                            e,
                            exc_info=True,
                        )
        except Exception as e:
            log.debug(
                "DM-Cleanup übersprungen/fehlgeschlagen (user_id=%s): %s",
                getattr(user, "id", "?"),
                e,
                exc_info=True,
            )

    async def _notify_user_linked(self, user_id: int, steam_ids: list[str]) -> None:
        valid_ids = [
            str(sid).strip()
            for sid in (steam_ids or [])
            if re.fullmatch(r"\d{17,20}", str(sid).strip())
        ]
        if not valid_ids:
            log.debug(
                "Skipping link notification – no valid SteamIDs",
                extra={"user_id": user_id, "steam_id_count": len(steam_ids or [])},
            )
            return
        queue_ok = True
        try:
            queue_ok = bool(queue_friend_requests(valid_ids))
        except Exception:
            queue_ok = False
            ids_for_log = valid_ids
            steam_id_count = len(ids_for_log)
            try:
                safe_user = int(user_id)
            except Exception:
                safe_user = None
            log.exception(
                "Konnte Steam-Freundschaftsanfragen nicht in die Queue legen",
                # Avoid logging raw user-controlled identifiers; keep structured, bounded data only.
                extra={
                    "user_id": safe_user,
                    "steam_id_count": steam_id_count,
                    "steam_ids_invalid": False,
                },
            )
        if not user_id:
            log.debug(
                "Skipping link notification – missing user id",
                extra={"steam_id_count": len(valid_ids)},
            )
            return
        try:
            user = self.bot.get_user(user_id) or await self.bot.fetch_user(user_id)
            if not user:
                return
            await self._cleanup_recent_bot_dms(user, limit=25)
            request_line = (
                "🤝 Wir haben dir eine Freundschaftsanfrage gesendet. "
                "Falls in Steam nichts ankommt, sende dem Steam-Bot manuell eine Anfrage "
                "(Freundescode: **820142646**)."
                if queue_ok
                else (
                    "⚠️ Die automatische Freundschaftsanfrage konnte nicht gesendet werden.\n"
                    "Bitte sende dem Steam-Bot jetzt manuell eine Freundschaftsanfrage "
                    "(Freundescode: **820142646**), damit die Verknüpfung vollständig aktiv wird."
                )
            )
            shine = (
                "✅ **Steam-Verknüpfung erfolgreich.**\n"
                "Diese Verknüpfung ist wichtig für deinen Rang, den Live-Status in den Voice Lanes und die Spielersuche.\n"
                f"{request_line}"
            )
            await user.send(shine)
        except Exception as e:
            log.info("Konnte User-DM nicht senden (id=%s): %s", user_id, e)

    def _build_discord_auth_url(self, uid: int, context: str = "steam_link") -> str:
        client_id = _env_client_id(self.bot)
        redirect_uri = _env_redirect()
        if not client_id:
            raise RuntimeError("DISCORD_OAUTH_CLIENT_ID/bot.application_id nicht gesetzt")
        if not redirect_uri:
            raise RuntimeError("DISCORD_OAUTH_REDIRECT/PUBLIC_BASE_URL/HTTP_HOST fehlen")

        # Turnier context only needs identify scope; steam_link needs connections too
        scope = "identify" if context == "turnier" else "identify connections"
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scope,
            "prompt": "consent",
            "state": self._mk_state(uid, context=context),
        }
        return f"{DISCORD_API}/oauth2/authorize?{urlencode(params)}"

    # Öffentliche Helper (für andere Cogs über Bot-Instanz, optional)
    def discord_start_url_for(self, uid: int) -> str:
        if not PUBLIC_BASE_URL:
            return ""
        return f"{PUBLIC_BASE_URL}/discord/login?uid={int(uid)}"

    def steam_start_url_for(self, uid: int) -> str:
        if not PUBLIC_BASE_URL:
            return ""
        return build_steam_login_start_url(int(uid))

    # Rückwärtskompatibel
    def build_discord_link_for(self, uid: int) -> str:
        try:
            return self._build_discord_auth_url(int(uid))
        except Exception:
            log.exception("build_discord_link_for failed (uid=%s)", uid)
            return ""

    def build_steam_openid_for(self, uid: int) -> str:
        try:
            s = self._mk_state(int(uid))
            return self._build_steam_login_url(s)
        except Exception:
            log.exception("build_steam_openid_for failed (uid=%s)", uid)
            return ""

    # ---------- Discord OAuth helpers ----------------------------------------
    async def _discord_token_exchange(self, code: str) -> dict | None:
        client_id = _env_client_id(self.bot)
        redirect_uri = _env_redirect()
        if not client_id or not CLIENT_SECRET:
            return None

        data = {
            "client_id": client_id,
            "client_secret": CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with aiohttp.ClientSession() as s:
            async with s.post(f"{DISCORD_API}/oauth2/token", data=data, headers=headers) as r:
                if r.status != 200:
                    log.warning("Discord OAuth exchange failed (HTTP %s).", r.status)
                    return None
                return await r.json()

    async def _discord_fetch_user(self, access_token: str) -> dict | None:
        """Fetch the authenticated Discord user's basic info (/users/@me)."""
        headers = {"Authorization": f"Bearer {access_token}"}
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DISCORD_API}/users/@me", headers=headers) as r:
                if r.status != 200:
                    log.warning("Discord @me API failed (HTTP %s).", r.status)
                    return None
                return await r.json()

    async def _discord_fetch_connections(self, access_token: str) -> list[dict] | None:
        headers = {"Authorization": f"Bearer {access_token}"}
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{DISCORD_API}/users/@me/connections", headers=headers) as r:
                if r.status != 200:
                    log.warning("Discord Connections-API fehlgeschlagen (HTTP %s).", r.status)
                    return None
                return await r.json()

    async def _resolve_vanity(self, vanity: str) -> str | None:
        key = (settings.steam_api_key.get_secret_value() if settings.steam_api_key else "").strip()
        if not key:
            return None
        url = f"{STEAM_API_BASE}/ISteamUser/ResolveVanityURL/v0001/"
        params = {"key": key, "vanityurl": vanity}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params, timeout=10) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
                    resp = data.get("response", {})
                    if resp.get("success") == 1:
                        sid = resp.get("steamid")
                        if sid and re.fullmatch(r"\d{17,20}", sid):
                            return sid
        except Exception:
            return None
        return None

    async def _resolve_steam_input(self, raw: str) -> str | None:
        s = (raw or "").strip()
        if not s:
            return None

        # 1) 17–20-stellige ID direkt (tolerant, wie gewünscht)
        if re.fullmatch(r"\d{17,20}", s):
            return s

        # 2) URL?
        try:
            u = urlparse(s)
        except Exception:
            u = None

        if u and (u.hostname or "").lower():
            host = (u.hostname or "").lower().strip(".")
            if host == "steamcommunity.com" or host.endswith(".steamcommunity.com"):
                path = (u.path or "").rstrip("/")
                m = re.search(r"/profiles/(\d{17,20})$", path)
                if m:
                    return m.group(1)
                m = re.search(r"/id/([^/]+)$", path)
                if m:
                    return await self._resolve_vanity(m.group(1))

        # 3) nackter Vanity-Kandidat
        if re.fullmatch(r"[A-Za-z0-9_.\-]+", s):
            return await self._resolve_vanity(s)

        return None

    async def _fetch_persona(self, steam_id: str) -> str | None:
        key = (settings.steam_api_key.get_secret_value() if settings.steam_api_key else "").strip()
        if not key:
            return None
        url = f"{STEAM_API_BASE}/ISteamUser/GetPlayerSummaries/v0002/"
        params = {"key": key, "steamids": steam_id}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params, timeout=10) as r:
                    if r.status != 200:
                        return None
                    data = await r.json()
                    players = data.get("response", {}).get("players", [])
                    if players:
                        return players[0].get("personaname") or None
        except Exception:
            return None
        return None

    # ---- Steam OpenID helpers -----------------------------------------------
    def _require_public_base(self) -> None:
        if not PUBLIC_BASE_URL:
            raise RuntimeError("PUBLIC_BASE_URL ist nicht gesetzt.")

    def _steam_return_to(self, state: str) -> str:
        self._require_public_base()
        return f"{urljoin(PUBLIC_BASE_URL + '/', STEAM_RETURN_PATH.lstrip('/'))}?{urlencode({'state': state})}"

    def _steam_realm(self) -> str:
        self._require_public_base()
        return PUBLIC_BASE_URL

    def _build_steam_login_url(self, state: str) -> str:
        self._require_public_base()
        params = {
            "openid.ns": OPENID_NS,
            "openid.mode": "checkid_setup",
            "openid.return_to": self._steam_return_to(state),
            "openid.realm": self._steam_realm(),
            "openid.identity": IDENTIFIER_SELECT,
            "openid.claimed_id": IDENTIFIER_SELECT,
        }
        url = f"{STEAM_OPENID_ENDPOINT}?{urlencode(params)}"
        safe = url.replace(state, "[state]")
        log.info("Steam OpenID URL (safe): %s", safe)
        return url

    async def _verify_steam_openid(self, request: web.Request) -> str | None:
        query = dict(request.query)
        if query.get("openid.mode") != "id_res":
            return None

        verify_params = query.copy()
        verify_params["openid.mode"] = "check_authentication"

        async with aiohttp.ClientSession() as session:
            async with session.post(STEAM_OPENID_ENDPOINT, data=verify_params, timeout=15) as resp:
                body = await resp.text()
                if resp.status != 200 or "is_valid:true" not in body:
                    log.warning("Steam OpenID verify fehlgeschlagen (HTTP %s).", resp.status)
                    return None

        claimed_id = query.get("openid.claimed_id", "")
        m = re.search(r"/openid/id/(\d+)$", claimed_id)
        sid = m.group(1) if m else None
        if sid and re.fullmatch(r"\d{17,20}", sid):  # tolerant
            return sid
        return None

    # --------------- HTTP-Handler --------------------------------------------
    async def handle_index(self, request: web.Request) -> web.Response:
        html_doc = (
            "<html><body style='font-family: system-ui, sans-serif'>"
            "<h2>Deadlock Bot - Link Service</h2>"
            "<p>✅ Server läuft. Nutze im Discord <code>/account_verknüpfen</code> "
            "oder <code>/steam setprimary</code>.</p>"
            "<p><a href='/health'>Health-Check</a></p>"
            "</body></html>"
        )
        return web.Response(text=html_doc, content_type="text/html")

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "ts": int(time.time())})

    async def handle_favicon(self, request: web.Request) -> web.Response:
        return web.Response(status=204)

    async def handle_robots(self, request: web.Request) -> web.Response:
        return web.Response(text="User-agent: *\nDisallow: /\n", content_type="text/plain")

    async def handle_discord_login(self, request: web.Request) -> web.Response:
        if not _consume_discord_login_budget(request):
            return web.Response(text="too many login attempts, please retry later", status=429)

        context = request.query.get("context", "steam_link")
        if context == "turnier":
            uid = 0  # Will be determined after Discord OAuth; not needed for turnier flow
        else:
            uid_q = request.query.get("uid")
            if not uid_q or not uid_q.isdigit():
                return web.Response(text="missing uid", status=400)
            uid = int(uid_q)
        try:
            url = self._build_discord_auth_url(uid, context=context)
            raise web.HTTPFound(location=url)
        except web.HTTPFound:
            raise
        except RuntimeError as exc:
            log.warning("discord/login state allocation failed: %s", exc)
            return web.Response(text="link flow is temporarily busy, please retry", status=503)
        except Exception as e:
            log.exception("discord/login failed: %s", e)
            return web.Response(text="failed to start oauth", status=500)

    async def handle_discord_callback(self, request: web.Request) -> web.Response:
        code = request.query.get("code")
        state_key = request.query.get("state")
        if not code or not state_key:
            return web.Response(text="missing code/state", status=400)

        state_data = self._pop_state_data(state_key)
        if not state_data:
            return web.Response(text="invalid/expired state", status=400)

        uid = int(state_data["uid"])
        context = str(state_data.get("context", "steam_link"))

        token = await self._discord_token_exchange(code)
        if not token:
            return web.Response(text="token exchange failed", status=400)

        at = token.get("access_token")
        if not at:
            return web.Response(text="no access_token", status=400)

        if context != "turnier":
            user_info = await self._discord_fetch_user(at)
            if not user_info:
                return web.Response(text="user fetch failed", status=400)
            try:
                actual_uid = int(user_info["id"])
            except (KeyError, TypeError, ValueError):
                log.warning("Discord callback returned invalid user payload (state_uid=%s)", uid)
                return web.Response(text="invalid user payload", status=400)
            if actual_uid != uid:
                log.warning(
                    "Discord OAuth user mismatch (state_uid=%s, actual_uid=%s)",
                    uid,
                    actual_uid,
                )
                return web.Response(text="discord user mismatch", status=403)

        # ── Turnier context: issue one-time token, redirect to turnier site ──
        if context == "turnier":
            try:
                user_info = await self._discord_fetch_user(at)
                if not user_info:
                    return web.Response(text="user fetch failed", status=400)
                actual_uid = int(user_info["id"])
                display_name = str(
                    user_info.get("global_name") or user_info.get("username") or str(actual_uid)
                ).strip()
                from cogs.customgames.tournament_store import create_auth_token_async

                auth_token = await create_auth_token_async(actual_uid, display_name)
                turnier_url = f"https://turnier.earlysalty.com/auth/complete?token={auth_token}"
                raise web.HTTPFound(location=turnier_url)
            except web.HTTPFound:
                raise
            except Exception:
                log.exception("Turnier OAuth callback fehlgeschlagen")
                return web.Response(
                    text=(
                        "<html><body style='font-family: system-ui, sans-serif'>"
                        "<h3>❌ Anmeldung fehlgeschlagen</h3>"
                        "<p>Bitte versuche es erneut.</p>"
                        "</body></html>"
                    ),
                    content_type="text/html",
                    status=500,
                )

        # ── Default steam_link context ──
        conns = await self._discord_fetch_connections(at)
        if conns is None:
            return web.Response(text="connections fetch failed", status=400)

        saved_ids = await self._save_steam_links_from_discord(uid, conns)
        if saved_ids:
            await self._notify_user_linked(uid, saved_ids)
            html_doc = (
                "<html><body style='font-family: system-ui, sans-serif'>"
                "<h3>✅ Verknüpfung abgeschlossen</h3>"
                "<p>Steam-Account(s) wurden gespeichert.</p>"
                "<p>Du kannst dieses Fenster jetzt schließen.</p>"
                "</body></html>"
            )
            return web.Response(text=html_doc, content_type="text/html")

        # Seamless Redirect zu Steam OpenID
        try:
            steam_state = self._mk_state(uid)
            steam_login = self._build_steam_login_url(steam_state)
            raise web.HTTPFound(location=steam_login)
        except web.HTTPFound:
            raise
        except RuntimeError:
            log.warning("Discord callback: state allocation failed during Steam redirect")
            return web.Response(
                text=(
                    "<html><body style='font-family: system-ui, sans-serif'>"
                    "<h3>⚠️ Link-Service ist kurzzeitig ausgelastet</h3>"
                    "<p>Bitte starte den Verknüpfungsprozess in Discord in wenigen Sekunden erneut.</p>"
                    "</body></html>"
                ),
                content_type="text/html",
                status=503,
            )
        except Exception:
            log.exception("Discord callback: Weiterleitung zu Steam fehlgeschlagen")
            return web.Response(
                text=(
                    "<html><body style='font-family: system-ui, sans-serif'>"
                    "<h3>❌ Weiterleitung fehlgeschlagen</h3>"
                    "<p>Bitte starte den Link-Vorgang in Discord erneut.</p>"
                    "</body></html>"
                ),
                content_type="text/html",
                status=500,
            )

    async def handle_steam_login(self, request: web.Request) -> web.Response:
        launch = request.query.get("launch")
        if launch:
            if any(key != "launch" for key in request.query.keys()):
                return web.Response(text="invalid launch", status=400)
            uid = _verify_steam_launch_token(launch)
            if not uid:
                return web.Response(text="invalid launch", status=400)
            try:
                s = self._mk_state(uid)
                login_url = self._build_steam_login_url(s)
                raise web.HTTPFound(location=login_url)
            except web.HTTPFound:
                raise
            except RuntimeError as exc:
                log.warning("Steam login state allocation failed: %s", exc)
                return web.Response(
                    text="steam login temporarily busy, please retry shortly",
                    status=503,
                )
            except Exception:
                log.exception("Steam login redirect failed")
                return web.Response(text="failed to start steam login", status=500)

        # Legacy compatibility path: old links used /steam/login?uid=<discord_id>.
        # Keep this secure by forcing users through Discord OAuth first.
        uid_q = request.query.get("uid")
        if uid_q:
            if any(key != "uid" for key in request.query.keys()):
                return web.Response(text="invalid launch", status=400)
            if not uid_q.isdigit():
                return web.Response(text="invalid uid", status=400)
            legacy_uid = int(uid_q)
            if legacy_uid <= 0:
                return web.Response(text="invalid uid", status=400)
            log.info("Legacy steam/login uid flow used; redirecting to Discord OAuth.")
            raise web.HTTPFound(location=f"/discord/login?uid={legacy_uid}")

        return web.Response(text="missing launch", status=400)

    async def handle_steam_return(self, request: web.Request) -> web.Response:
        try:
            if request.method == "HEAD" or request.query.get("healthcheck") == "1":
                return web.Response(text="steam return ok", status=200)

            state = request.query.get("state", "")
            uid = self._pop_state(state)
            if not uid:
                return web.Response(text="invalid/expired state", status=400)

            steam_id = await self._verify_steam_openid(request)
            if not steam_id:
                return web.Response(text="OpenID validation failed", status=400)

            display_name = await self._fetch_persona(steam_id) or await self._discord_at_name(uid)
            # Verified=0, da wir erst die Freundschaftsanfrage abwarten wollen
            save_result = _save_steam_link_row(
                uid,
                steam_id,
                display_name,
                verified=0,
                source="steam_openid_return",
            )
            if not save_result.get("saved") and save_result.get("conflicting_user_id") is not None:
                return web.Response(
                    text=(
                        "<html><body style='font-family: system-ui, sans-serif'>"
                        "<h3>❌ Verknüpfung blockiert</h3>"
                        "<p>Diese SteamID ist bereits mit einem anderen Discord-Konto verknüpft.</p>"
                        "<p>Bitte verwende den korrekten Steam-Account oder kontaktiere das Team.</p>"
                        "</body></html>"
                    ),
                    content_type="text/html",
                    status=409,
                )
            await self._kickoff_profile_card(steam_id)
            await self._notify_user_linked(uid, [steam_id])

            steam_id_safe = html.escape(steam_id, quote=True)
            body = (
                "<h3>✅ Verknüpfung abgeschlossen</h3>"
                f"<p>Deine SteamID64 ist: <b>{steam_id_safe}</b>.</p>"
                "<p>Du kannst dieses Fenster schließen und zu Discord zurückkehren.</p>"
            )
            return web.Response(text=body, content_type="text/html")

        except Exception:
            log.exception("Fehler im Steam-Return")
            return web.Response(
                text=(
                    "<html><body style='font-family: system-ui, sans-serif'>"
                    "<h3>❌ Unerwarteter Fehler</h3>"
                    "<p>Bitte versuche es erneut. Wenn das Problem bleibt, kontaktiere den Admin.</p>"
                    "</body></html>"
                ),
                content_type="text/html",
                status=500,
            )

    # --------------- Connections → SteamIDs ----------------------------------
    async def _save_steam_links_from_discord(self, uid: int, conns: list[dict]) -> list[str]:
        saved: list[str] = []
        if not conns:
            return saved

        for c in conns:
            try:
                if str(c.get("type", "")).lower() != "steam":
                    continue

                sid_raw = str(c.get("id") or "").strip()
                steam_id: str | None = None

                if re.fullmatch(r"\d{17,20}", sid_raw):
                    steam_id = sid_raw
                else:
                    name_or_vanity = str(c.get("name") or "").strip()
                    steam_id = await self._resolve_steam_input(
                        sid_raw
                    ) or await self._resolve_steam_input(name_or_vanity)

                if not steam_id:
                    meta = c.get("metadata") or {}
                    meta_sid = str(meta.get("steam_id") or "").strip()
                    if re.fullmatch(r"\d{17,20}", meta_sid):
                        steam_id = meta_sid

                if not steam_id:
                    log.info(
                        "Ignoriere Verbindung ohne gültige SteamID: %s",
                        _safe_log_repr(c),
                    )
                    continue

                persona = await self._fetch_persona(steam_id) or (c.get("name") or "")
                if not persona:
                    persona = await self._discord_at_name(uid)

                # Wir setzen verified=0, damit der User erst Freund werden muss
                save_result = _save_steam_link_row(
                    uid,
                    steam_id,
                    persona,
                    verified=0,
                    source="discord_connections",
                )
                if not save_result.get("saved"):
                    continue
                saved.append(steam_id)
                await self._kickoff_profile_card(steam_id)

            except Exception:
                log.exception(
                    "Fehler beim Speichern der Steam-Verknüpfung: user_id=%s, conn=%s",
                    uid,
                    _safe_log_repr(c),
                )

        return saved

    # --------------- Commands -------------------------------------------------
    async def _defer_if_needed(self, ctx: commands.Context) -> None:
        if getattr(ctx, "interaction", None) and not ctx.interaction.response.is_done():
            try:
                await ctx.interaction.response.defer(ephemeral=True)
            except Exception as e:
                log.debug(
                    "Defer fehlgeschlagen (ctx.user_id=%s): %s",
                    getattr(getattr(ctx, "author", None), "id", "?"),
                    e,
                    exc_info=True,
                )

    async def _send_ephemeral(
        self,
        ctx: commands.Context,
        content: str | None = None,
        *,
        embed: discord.Embed | None = None,
        view: discord.ui.View | None = None,
    ) -> None:
        c = content if content is not None else discord.utils.MISSING
        e = embed if embed is not None else discord.utils.MISSING
        v = view if view is not None else discord.utils.MISSING

        if getattr(ctx, "interaction", None) and not ctx.interaction.response.is_done():
            await ctx.interaction.response.send_message(c, embed=e, view=v, ephemeral=True)
        elif getattr(ctx, "interaction", None):
            await ctx.interaction.followup.send(c, embed=e, view=v, ephemeral=True)
        else:
            await ctx.reply(
                c if c is not discord.utils.MISSING else "",
                embed=e if e is not discord.utils.MISSING else None,
                view=view,
            )

    async def _send_account_link_panel(self, ctx: commands.Context) -> None:
        desc = (
            "Verknüpfe deinen Steam-Account in zwei Schritten:\n"
            "1. Klicke auf den Steam-Login-Button und melde dich bei Steam an.\n"
            "2. Nutze den Button **Freundescode eingeben**.\n"
            "   Wir senden automatisch eine Freundschaftsanfrage und prüfen bis zu 5 Minuten "
            "auf Bestätigung.\n\n"
            "**Datenschutz-Kurzinfo:**\n"
            "- Discord erhält aus diesem Schritt keine zusätzlichen Daten.\n"
            "- Wir speichern nur die technisch nötigen IDs (Discord-ID und SteamID64).\n"
            "- Wir erhalten keine Passwörter oder sonstige Zugangsdaten.\n"
            "- Es werden keine Daten an Dritte weitergegeben.\n\n"
            "**Open Source:**\n"
            "Unser Source Code ist öffentlich: <https://github.com/NaniDerEchte2/Deadlock-Bots>\n\n"
            "Wichtig: Schicke dem Steam-Bot direkt eine Freundschaftsanfrage "
            "Freundescode **820142646** – sonst wird die Verknüpfung nicht aktiv."
        )
        embed = discord.Embed(
            title="Account verknüpfen",
            description=desc,
            color=discord.Color.green(),
        )
        if LINK_COVER_IMAGE:
            embed.set_image(url=LINK_COVER_IMAGE)
        embed.set_author(name=LINK_COVER_LABEL)

        if not PUBLIC_BASE_URL:
            await self._send_ephemeral(
                ctx, "⚠️ PUBLIC_BASE_URL fehlt - Start-Links können nicht gebaut werden."
            )
            return

        def _fresh_link_view() -> LinkPanelView:
            steam_start_url = build_steam_login_start_url(ctx.author.id)
            return LinkPanelView(
                user_id=ctx.author.id,
                steam_url=steam_start_url,
                link_cog=self,
            )

        if getattr(ctx, "interaction", None):
            await self._send_ephemeral(ctx, embed=embed, view=_fresh_link_view())
            return

        if ctx.guild is None:
            await ctx.reply(embed=embed, view=_fresh_link_view())
            return

        try:
            await ctx.author.send(embed=embed, view=_fresh_link_view())
        except discord.Forbidden:
            await ctx.reply(
                "Aus Sicherheitsgründen sende ich den Steam-Link nicht öffentlich. "
                "Ich konnte dir keine DM schicken. Bitte aktiviere DMs oder nutze `/account_verknüpfen`."
            )
            return
        except Exception as e:
            log.info(
                "Konnte sicheren Steam-Link nicht per DM senden (user_id=%s): %s",
                getattr(ctx.author, "id", "?"),
                e,
            )
            await ctx.reply(
                "Aus Sicherheitsgründen sende ich den Steam-Link nicht öffentlich. "
                "Bitte nutze `/account_verknüpfen` oder versuche es in einer DM."
            )
            return

        await ctx.reply("Ich habe dir den Steam-Link aus Sicherheitsgründen per DM geschickt.")

    @discord.app_commands.allowed_installs(guilds=True, users=True)
    @discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    @commands.hybrid_command(
        name="account_verknüpfen",
        description="Zeigt den Steam-OpenID-Link zur Account-Verknüpfung",
    )
    async def account_verknuepfen(self, ctx: commands.Context) -> None:
        await self._send_account_link_panel(ctx)

    @discord.app_commands.allowed_installs(guilds=True, users=True)
    @discord.app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
    @commands.hybrid_group(
        name="steam", description="Steam-Links verwalten", invoke_without_command=True
    )
    async def steam(self, ctx: commands.Context) -> None:
        # Fallback: ohne Subcommand direkt den Link-Flow starten
        if ctx.invoked_subcommand is None:
            await self._send_account_link_panel(ctx)

    @steam.command(
        name="link",
        with_app_command=False,
        description="Legacy-Alias für die Account-Verknüpfung",
    )
    async def steam_link(self, ctx: commands.Context) -> None:
        await self._send_account_link_panel(ctx)

    @steam.command(
        name="link_steam",
        with_app_command=False,
        description="Legacy-Alias für die Account-Verknüpfung",
    )
    async def steam_link_steam(self, ctx: commands.Context) -> None:
        await self._send_account_link_panel(ctx)

    @steam.command(name="links", description="Zeigt deine gespeicherten Steam-Links")
    async def steam_links(self, ctx: commands.Context) -> None:
        rows = db.query_all(
            "SELECT steam_id, name, verified, primary_account "
            "FROM steam_links WHERE user_id=? "
            "ORDER BY primary_account DESC, updated_at DESC",
            (ctx.author.id,),
        )
        if not rows:
            await self._send_ephemeral(
                ctx, "Keine Steam-Links gefunden. Nutze `/account_verknüpfen`."
            )
            return
        lines = []
        for r in rows:
            sid = r["steam_id"]
            nm = r["name"] or "-"
            chk = " ✅" if r["verified"] else ""
            prim = " [primary]" if r["primary_account"] else ""
            lines.append(f"- **{sid}** ({nm}){chk}{prim}")
        await self._send_ephemeral(ctx, "Deine verknüpften Accounts:\n" + "\n".join(lines))

    @steam.command(
        name="whoami",
        description="Prüft ID/Vanity/Profil-Link und zeigt Persona + SteamID",
    )
    async def steam_whoami(self, ctx: commands.Context, steam: str) -> None:
        await self._defer_if_needed(ctx)
        try:
            sid = await asyncio.wait_for(self._resolve_steam_input(steam), timeout=8)
        except TimeoutError:
            await self._send_ephemeral(ctx, "⚠️ Steam/Netzwerk langsam. Bitte nochmal versuchen.")
            return

        if not sid:
            await self._send_ephemeral(ctx, "❌ Konnte aus deiner Eingabe keine SteamID bestimmen.")
            return

        try:
            persona = await asyncio.wait_for(self._fetch_persona(sid), timeout=8)
        except TimeoutError:
            persona = None

        if persona:
            await self._send_ephemeral(ctx, f"✅ **{persona}** -> SteamID64: `{sid}`")
        else:
            await self._send_ephemeral(ctx, f"SteamID64: `{sid}` (Persona nicht abrufbar)")

    @steam.command(
        name="setprimary",
        description="Markiert einen bestehenden Steam-Account als Primär (akzeptiert ID/Vanity/Link).",
    )
    async def steam_setprimary(
        self, ctx: commands.Context, steam: str, name: str | None = None
    ) -> None:
        await self._defer_if_needed(ctx)
        try:
            sid = await asyncio.wait_for(self._resolve_steam_input(steam), timeout=8)
        except TimeoutError:
            await self._send_ephemeral(ctx, "⚠️ Steam/Netzwerk langsam. Bitte nochmal versuchen.")
            return

        if not sid:
            await self._send_ephemeral(
                ctx,
                "❌ Ungültige Eingabe. Erwarte SteamID64, Vanity oder steamcommunity-Link.",
            )
            return

        existing = db.query_one(
            "SELECT steam_id, name FROM steam_links WHERE user_id=? AND steam_id=?",
            (ctx.author.id, sid),
        )
        if not existing:
            await self._send_ephemeral(
                ctx,
                "Kein gespeicherter Steam-Link gefunden. Bitte zuerst `/account_verknüpfen` nutzen.",
            )
            return

        try:
            persona = await asyncio.wait_for(self._fetch_persona(sid), timeout=8)
        except TimeoutError:
            persona = None

        if name or persona or existing["name"]:
            display_name = name or persona or existing["name"]
            db.execute(
                "UPDATE steam_links SET name=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND steam_id=?",
                (display_name, ctx.author.id, sid),
            )

        db.execute("UPDATE steam_links SET primary_account=0 WHERE user_id=?", (ctx.author.id,))
        db.execute(
            "UPDATE steam_links SET primary_account=1, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND steam_id=?",
            (ctx.author.id, sid),
        )
        await self._send_ephemeral(ctx, f"✅ Primär gesetzt: `{sid}`")

    @steam.command(
        name="unlink",
        description="Entfernt einen Steam-Link (ID/Vanity/Profil-Link möglich)",
    )
    async def steam_unlink(self, ctx: commands.Context, steam: str) -> None:
        sid = await self._resolve_steam_input(steam)
        if not sid and re.fullmatch(r"\d{17,20}", steam or ""):
            sid = steam
        if not sid:
            await self._send_ephemeral(
                ctx,
                "❌ Ungültige Eingabe. Erwarte SteamID64, Vanity oder steamcommunity-Link.",
            )
            return
        db.execute(
            "DELETE FROM steam_links WHERE user_id=? AND steam_id=?",
            (ctx.author.id, sid),
        )
        await self._send_ephemeral(ctx, f"Entfernt: `{sid}`")


async def setup(bot: commands.Bot):
    await bot.add_cog(SteamLink(bot))
