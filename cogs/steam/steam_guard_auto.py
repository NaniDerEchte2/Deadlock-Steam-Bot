"""Steam Guard Scheduled Token Refresh.

Automatically refreshes Steam refresh tokens every 80 days by monitoring
email for Steam Guard codes during the scheduled renewal process.
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta

from discord.ext import commands, tasks

from cogs.steam.token_vault import (
    get_refresh_token_age_days,
    refresh_token_path as token_vault_refresh_token_path,
    refresh_token_exists,
)
from service import db

log = logging.getLogger(__name__)
_STEAM_ACCOUNT_ENV_NAMES = ("STEAM_BOT_USERNAME", "STEAM_LOGIN", "STEAM_ACCOUNT")
_STEAM_PASSWORD_ENV_NAMES = ("STEAM_BOT_PASSWORD", "STEAM_PASSWORD")
_SENSITIVE_GUARD_TASK_TIMEOUT_S = 120


def _refresh_token_path():
    return token_vault_refresh_token_path()


def _first_configured_env(names: tuple[str, ...]) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _load_steam_runtime_state() -> dict[str, object]:
    try:
        with db.get_conn() as conn:
            row = conn.execute(
                "SELECT payload FROM standalone_bot_state WHERE bot = 'steam' LIMIT 1"
            ).fetchone()
    except Exception:
        log.debug("Failed to read Steam runtime state for refresh preflight", exc_info=True)
        return {}

    if not row:
        return {}

    try:
        payload = json.loads(row[0])
    except Exception:
        log.debug("Failed to decode Steam runtime state for refresh preflight", exc_info=True)
        return {}

    if not isinstance(payload, dict):
        return {}

    runtime = payload.get("runtime")
    return runtime if isinstance(runtime, dict) else {}


class SteamGuardAutoConfig:
    """Configuration for Steam Guard email automation."""

    IMAP_SERVER = os.getenv("STEAM_GUARD_IMAP_SERVER", "imap.ionos.de")
    IMAP_PORT = int(os.getenv("STEAM_GUARD_IMAP_PORT", "993"))
    EMAIL_ADDRESS = os.getenv("STEAM_GUARD_EMAIL")

    # Try Windows Credential Manager first, then env variable
    EMAIL_PASSWORD = None

    @classmethod
    def _load_email_password(cls):
        """Load email password from Windows Credential Manager or environment."""
        if cls.EMAIL_PASSWORD:
            return cls.EMAIL_PASSWORD

        # Try keyring (Windows Credential Manager)
        try:
            import keyring

            password = keyring.get_password("DeadlockBot", "STEAM_EMAIL_ACCOUNT_PASSWORD")
            if password:
                cls.EMAIL_PASSWORD = password
                return password
        except ImportError:
            pass  # keyring not installed
        except Exception as e:
            log.debug(f"Could not load from keyring: {e}")

        # Fallback to environment variable
        password = os.getenv("STEAM_EMAIL_ACCOUNT_PASSWORD")
        if password:
            cls.EMAIL_PASSWORD = password
        return password

    # Refresh token every N days (before 90-day expiry)
    TOKEN_REFRESH_INTERVAL_DAYS = int(os.getenv("STEAM_TOKEN_REFRESH_DAYS", "80"))

    # How often to check email when guard is pending (only during refresh)
    POLL_INTERVAL_SECONDS = int(os.getenv("STEAM_GUARD_POLL_INTERVAL", "10"))

    # How old can the email be (avoid using old codes)
    MAX_EMAIL_AGE_MINUTES = int(os.getenv("STEAM_GUARD_MAX_EMAIL_AGE", "5"))

    @classmethod
    def is_configured(cls) -> bool:
        """Check if email automation is properly configured."""
        email_password = cls._load_email_password()
        return bool(cls.EMAIL_ADDRESS and email_password)


class SteamGuardEmailMonitor:
    """Monitors email for Steam Guard codes."""

    # Regex to extract Steam Guard code from email body
    # Primary pattern for explicit code mentions
    CODE_PATTERN = re.compile(
        r"(?:Your Steam Guard code is|Guard[- ]code|Steam[- ]Guard[- ]Code)[:\s]+([A-Z0-9]{5})",
        re.IGNORECASE,
    )

    # Fallback: Find any 5-char code that's all uppercase + digits
    # (Steam codes are always uppercase with mix of letters and numbers)
    GENERIC_CODE_PATTERN = re.compile(r"\b([A-Z0-9]{5})\b")

    def __init__(self, config: SteamGuardAutoConfig):
        self.config = config
        self._last_processed_uid: str | None = None

    def fetch_latest_steam_code(self) -> str | None:
        """
        Connect to IMAP, search for recent Steam Guard emails,
        extract and return the code.

        Returns:
            The 5-character Steam Guard code, or None if not found.
        """
        if not self.config.is_configured():
            log.warning("Steam Guard email not configured - skipping email check")
            return None

        try:
            # Get password (from keyring or env)
            email_password = self.config._load_email_password()
            if not email_password:
                log.error("Email password not found in Credential Manager or environment")
                return None

            # Connect to IMAP server
            log.debug(f"Connecting to {self.config.IMAP_SERVER}:{self.config.IMAP_PORT}")
            mail = imaplib.IMAP4_SSL(self.config.IMAP_SERVER, self.config.IMAP_PORT)
            mail.login(self.config.EMAIL_ADDRESS, email_password)
            mail.select("INBOX")

            # Search for Steam emails from last N minutes
            cutoff_time = datetime.now() - timedelta(minutes=self.config.MAX_EMAIL_AGE_MINUTES)
            date_str = cutoff_time.strftime("%d-%b-%Y")

            # Search criteria: from Steam, recent, unseen
            search_criteria = f'(FROM "noreply@steampowered.com" SINCE {date_str} UNSEEN)'
            log.debug(f"Searching with criteria: {search_criteria}")

            status, messages = mail.search(None, search_criteria)

            if status != "OK" or not messages[0]:
                log.debug("No new Steam Guard emails found")
                mail.logout()
                return None

            # Get the most recent email
            email_ids = messages[0].split()
            latest_id = email_ids[-1]

            # Skip if we already processed this email
            if latest_id == self._last_processed_uid:
                log.debug(f"Email {latest_id} already processed")
                mail.logout()
                return None

            # Fetch email
            status, msg_data = mail.fetch(latest_id, "(RFC822)")
            if status != "OK":
                mail.logout()
                return None

            # Parse email
            email_body = msg_data[0][1]
            email_message = email.message_from_bytes(email_body)

            # Extract text content
            body_text = self._extract_email_body(email_message)
            log.debug("Fetched Steam Guard email body", extra={"length": len(body_text)})

            # Search for Steam Guard code
            match = self.CODE_PATTERN.search(body_text)
            code = None

            if match:
                code = match.group(1)
            else:
                # Fallback: Find the LAST uppercase-only 5-char code in email
                # (Steam codes appear at the end, German words appear earlier)
                all_matches = self.GENERIC_CODE_PATTERN.findall(body_text)
                # Filter: must have at least one digit AND one letter, all uppercase
                for potential_code in reversed(all_matches):
                    if (
                        potential_code.isupper()
                        and any(c.isdigit() for c in potential_code)
                        and any(c.isalpha() for c in potential_code)
                    ):
                        code = potential_code
                        break

            if code:
                log.info("✅ Found Steam Guard code in email")
                self._last_processed_uid = latest_id
                mail.logout()
                return code

            log.warning("Steam email found but no code extracted")
            mail.logout()
            return None

        except imaplib.IMAP4.error as e:
            log.error(f"IMAP authentication failed: {e}")
            return None
        except Exception as e:
            log.error(f"Failed to fetch Steam Guard code from email: {e}", exc_info=True)
            return None

    def _extract_email_body(self, email_message) -> str:
        """Extract plain text body from email message."""
        body = ""

        if email_message.is_multipart():
            for part in email_message.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        break
                    except Exception:
                        continue
        else:
            try:
                body = email_message.get_payload(decode=True).decode("utf-8", errors="ignore")
            except Exception:
                log.debug("Failed to decode non-multipart email body", exc_info=True)

        return body


class SteamGuardAuto(commands.Cog):
    """Automatic Steam token refresh with email-based Guard code submission."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.config = SteamGuardAutoConfig()
        self.monitor = SteamGuardEmailMonitor(self.config)
        self._guard_check_active = False
        self._refresh_in_progress = False

        if self.config.is_configured():
            log.info(f"Steam Guard automation configured for {self.config.EMAIL_ADDRESS}")
            log.info(
                f"Token refresh scheduled every {self.config.TOKEN_REFRESH_INTERVAL_DAYS} days"
            )
            self.check_steam_guard.start()
            self.schedule_token_refresh.start()
        else:
            log.warning(
                "Steam Guard email automation NOT configured. "
                "Set STEAM_GUARD_EMAIL and STEAM_EMAIL_ACCOUNT_PASSWORD environment variables."
            )

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        self.check_steam_guard.cancel()
        if hasattr(self, "schedule_token_refresh"):
            self.schedule_token_refresh.cancel()

    def _get_token_age_days(self) -> int | None:
        """Get age of current refresh token in days."""
        return get_refresh_token_age_days(_refresh_token_path())

    def _expire_stale_guard_code_tasks(self) -> int:
        """Fail closed on orphaned AUTH_GUARD_CODE tasks that still carry a payload."""
        now = int(time.time())
        cutoff = now - _SENSITIVE_GUARD_TASK_TIMEOUT_S
        stale_error = (
            "Task stale: sensitiver Guard-Code nach "
            f"{_SENSITIVE_GUARD_TASK_TIMEOUT_S}s verworfen"
        )

        try:
            with db.get_conn() as conn:
                before = conn.total_changes
                conn.execute(
                    """
                    UPDATE steam_tasks
                       SET status = 'FAILED',
                           payload = NULL,
                           error = CASE
                               WHEN error IS NULL OR error = '' THEN ?
                               ELSE error
                           END,
                           finished_at = ?,
                           updated_at = ?
                     WHERE type = 'AUTH_GUARD_CODE'
                       AND status = 'PENDING'
                       AND payload IS NOT NULL
                       AND updated_at < ?
                    """,
                    (stale_error, now, now, cutoff),
                )
                conn.commit()
                return int(conn.total_changes - before)
        except Exception:
            log.debug("Failed to expire stale AUTH_GUARD_CODE tasks", exc_info=True)
            return 0

    def _credential_login_preflight(self) -> tuple[bool, str | None]:
        """Verify that a forced credential login can run before disrupting the session."""
        runtime = _load_steam_runtime_state()
        worker_account = str(runtime.get("account_name") or "").strip()
        worker_password_ready = bool(runtime.get("account_password_configured"))
        if runtime:
            if not worker_account:
                return (
                    False,
                    "standalone_bot_state/runtime.account_name is empty; worker credentials are "
                    "not ready for force_credentials login",
                )
            if not worker_password_ready:
                return (
                    False,
                    "standalone_bot_state/runtime.account_password_configured is false; "
                    "worker password is not ready for force_credentials login",
                )
            return True, None

        local_account = _first_configured_env(_STEAM_ACCOUNT_ENV_NAMES)
        if not local_account:
            return (
                False,
                "missing Steam account name in standalone_bot_state/runtime.account_name "
                "and local env",
            )

        password = _first_configured_env(_STEAM_PASSWORD_ENV_NAMES)
        if not password:
            return (
                False,
                "missing credential password in local env "
                "(STEAM_BOT_PASSWORD or STEAM_PASSWORD)",
            )

        return True, None

    async def _trigger_token_refresh(self):
        """Trigger a token refresh by re-authenticating."""
        log.info("🔄 Starting scheduled Steam token refresh...")
        self._refresh_in_progress = True

        try:
            preflight_ok, preflight_error = self._credential_login_preflight()
            if not preflight_ok:
                log.error(
                    "Aborting scheduled Steam token refresh before logout: %s",
                    preflight_error,
                )
                return

            with db.get_conn() as conn:
                task_id = conn.execute(
                    "INSERT INTO steam_tasks(type, payload, status) VALUES(?, ?, 'PENDING')",
                    ("AUTH_LOGOUT", None),
                ).lastrowid
                conn.commit()

            log.info(f"Enqueued AUTH_LOGOUT task #{task_id}")
            await asyncio.sleep(3)

            token_path = _refresh_token_path()
            log.info("Keeping existing refresh token in storage until a replacement is confirmed")

            # Start new login (this will trigger Steam Guard email)
            with db.get_conn() as conn:
                task_id = conn.execute(
                    "INSERT INTO steam_tasks(type, payload, status) VALUES(?, ?, 'PENDING')",
                    ("AUTH_LOGIN", json.dumps({"force_credentials": True})),
                ).lastrowid
                conn.commit()

            log.info(
                f"✅ Enqueued AUTH_LOGIN task #{task_id} - email monitor will handle Guard code"
            )

            # Email monitoring loop will automatically handle the Guard code
            # Wait for login to complete (timeout after 5 minutes)
            for _ in range(30):  # 30 * 10 = 5 minutes max
                await asyncio.sleep(10)

                # Check if we have a new token
                if refresh_token_exists(token_path):
                    age = self._get_token_age_days()
                    if age is not None and age == 0:  # Fresh token
                        log.info("✅ Token refresh completed successfully!")
                        self._refresh_in_progress = False
                        return

            log.warning("Token refresh timed out after 5 minutes")

        except Exception as e:
            log.error(f"Token refresh failed: {e}", exc_info=True)
        finally:
            self._refresh_in_progress = False

    @tasks.loop(hours=24)
    async def schedule_token_refresh(self):
        """Check daily if token needs refresh."""
        try:
            age = self._get_token_age_days()

            if age is None:
                log.info("⚠️ No refresh token found - triggering initial login")
                await self._trigger_token_refresh()
                return

            log.debug(
                f"Token age: {age} days (refresh at {self.config.TOKEN_REFRESH_INTERVAL_DAYS} days)"
            )

            if age >= self.config.TOKEN_REFRESH_INTERVAL_DAYS:
                log.info(f"⚠️ Token is {age} days old - triggering automatic refresh")
                await self._trigger_token_refresh()
            elif age >= self.config.TOKEN_REFRESH_INTERVAL_DAYS - 7:
                log.info(f"ℹ️ Token will expire soon (age: {age} days)")

        except Exception as e:
            log.error(f"Error in token refresh scheduler: {e}", exc_info=True)

    @schedule_token_refresh.before_loop
    async def before_schedule_token_refresh(self):
        """Wait for bot to be ready before starting loop."""
        await self.bot.wait_until_ready()
        # Check immediately on startup
        await asyncio.sleep(10)

    @commands.command(name="steam_token_refresh")
    @commands.has_permissions(administrator=True)
    async def cmd_manual_token_refresh(self, ctx: commands.Context):
        """Manually trigger a Steam token refresh."""
        age = self._get_token_age_days()
        if age is None:
            await ctx.reply("❌ No refresh token found")
            return

        await ctx.reply(f"🔄 Starting token refresh (current age: {age} days)...")

        # Trigger refresh in background
        asyncio.create_task(self._trigger_token_refresh())

        await ctx.reply(
            "✅ Token refresh initiated. Check logs for progress. "
            "You'll receive a Steam Guard email shortly."
        )

    @tasks.loop(seconds=10)
    async def check_steam_guard(self):
        """
        Periodically check if Steam Guard is required and attempt
        to auto-submit code from email.
        """
        try:
            expired = self._expire_stale_guard_code_tasks()
            if expired > 0:
                log.warning(
                    "Expired stale AUTH_GUARD_CODE tasks before polling",
                    extra={
                        "count": expired,
                        "max_age_seconds": _SENSITIVE_GUARD_TASK_TIMEOUT_S,
                    },
                )

            # Skip completely if Steam is already logged in – avoid needless polling
            with db.get_conn() as conn:
                row_state = conn.execute(
                    "SELECT payload FROM standalone_bot_state WHERE bot = 'steam' LIMIT 1"
                ).fetchone()
            if row_state:
                try:
                    state = json.loads(row_state[0])
                    runtime = state.get("runtime", {})
                    if runtime.get("logged_on"):
                        if self._guard_check_active:
                            log.debug("Guard check disabled because logged_on=True")
                            self._guard_check_active = False
                        return
                except Exception:
                    pass

            # Check standalone_bot_state for guard_required
            with db.get_conn() as conn:
                cursor = conn.execute(
                    "SELECT payload FROM standalone_bot_state WHERE bot = 'steam' LIMIT 1"
                )
                row = cursor.fetchone()

            if not row:
                return

            state = json.loads(row[0])
            runtime = state.get("runtime", {})
            guard_required = runtime.get("guard_required")

            # No guard needed
            if not guard_required:
                if self._guard_check_active:
                    log.info("Steam Guard no longer required")
                    self._guard_check_active = False
                return

            # Guard is needed and it's email type
            guard_type = guard_required.get("type")
            if guard_type != "email":
                if not self._guard_check_active:
                    log.info(
                        f"Steam Guard required but type is '{guard_type}' (not email) - skipping automation"
                    )
                    self._guard_check_active = True
                return

            if not self._guard_check_active:
                log.info("🔍 Steam Guard (email) is pending - starting email monitoring")
                self._guard_check_active = True

            # Fetch code from email
            code = await asyncio.to_thread(self.monitor.fetch_latest_steam_code)

            if not code:
                log.debug("No Steam Guard code found in email yet")
                return

            # Submit code via Steam task
            log.info("📧 Auto-submitting Steam Guard code from email")

            with db.get_conn() as task_conn:
                cursor = task_conn.execute(
                    "INSERT INTO steam_tasks(type, payload, status) VALUES(?, ?, 'PENDING')",
                    ("AUTH_GUARD_CODE", json.dumps({"code": code})),
                )
                task_id = cursor.lastrowid
                task_conn.commit()

            log.info(f"✅ Enqueued AUTH_GUARD_CODE task #{task_id}")

            # Wait a bit for task to process
            await asyncio.sleep(5)

            # Check if it worked
            with db.get_conn() as result_conn:
                cursor = result_conn.execute(
                    "SELECT status, error FROM steam_tasks WHERE id = ? LIMIT 1",
                    (task_id,),
                )
                result_row = cursor.fetchone()

            if result_row:
                status, error = result_row
                if status == "DONE":
                    log.info("✅ Steam Guard code accepted! Login should complete soon.")
                    self._guard_check_active = False
                elif status == "FAILED":
                    log.error(f"❌ Steam Guard code rejected: {error}")
                    # Keep monitoring in case we need to retry

        except Exception as e:
            log.error(f"Error in Steam Guard check loop: {e}", exc_info=True)

    @check_steam_guard.before_loop
    async def before_check_steam_guard(self):
        """Wait for bot to be ready before starting loop."""
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    """Load the SteamGuardAuto cog."""
    await bot.add_cog(SteamGuardAuto(bot))
