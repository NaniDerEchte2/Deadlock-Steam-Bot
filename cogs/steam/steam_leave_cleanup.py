from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable

import discord
from discord.ext import commands, tasks

from cogs import privacy_core as privacy
from cogs.steam.steam_master import SteamTaskClient
from service import db
from service.config import settings

log = logging.getLogger(__name__)


def _safe_int_setting(name: str, default: int, *, minimum: int = 1) -> int:
    raw = getattr(settings, name, default)
    try:
        value = int(raw)
    except Exception:
        value = int(default)
    return max(minimum, value)


STEAM_POLL_MIN_INTERVAL_SEC = _safe_int_setting("steam_poll_min_interval_sec", 86400)
STEAM_POLL_BATCH_SIZE = min(100, _safe_int_setting("steam_poll_batch_size", 25))


class SteamLeaveCleanup(commands.Cog):
    """Bereinigt Steam-Links und Freundschaften, wenn User den Server verlassen."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.tasks = SteamTaskClient(poll_interval=0.5, default_timeout=20.0)
        self._recent: dict[int, float] = {}
        self._last_relationship_probe: float = 0.0
        self._poll_min_interval_sec = STEAM_POLL_MIN_INTERVAL_SEC
        self._poll_batch_size = STEAM_POLL_BATCH_SIZE

    async def cog_load(self) -> None:
        self._ensure_poll_state_table()
        self.reconcile_orphaned_links.start()

    def cog_unload(self) -> None:
        self.reconcile_orphaned_links.cancel()

    def _is_recent(self, user_id: int, window: float = 20.0) -> bool:
        now = time.monotonic()
        last = self._recent.get(user_id)
        self._recent[user_id] = now
        # keep cache small
        if len(self._recent) > 500:
            cutoff = now - window
            self._recent = {uid: ts for uid, ts in self._recent.items() if ts >= cutoff}
        return bool(last and now - last < window)

    def _ensure_poll_state_table(self) -> None:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS steam_cleanup_poll_state(
              user_id INTEGER PRIMARY KEY,
              last_polled_at INTEGER NOT NULL,
              last_result TEXT NOT NULL,
              last_error TEXT,
              miss_count INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL
            )
            """
        )
        for alter_stmt in (
            "ALTER TABLE steam_cleanup_poll_state ADD COLUMN miss_count INTEGER NOT NULL DEFAULT 0",
        ):
            try:
                db.execute(alter_stmt)
            except Exception as exc:
                if "duplicate column name" not in str(exc).lower():
                    log.debug("SteamLeaveCleanup: schema alter skipped/failed: %s", exc)
        db.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_steam_cleanup_poll_state_polled
              ON steam_cleanup_poll_state(last_polled_at, updated_at)
            """
        )

    def _select_poll_candidates(self, *, now_ts: int) -> list[int]:
        cutoff = int(now_ts) - int(self._poll_min_interval_sec)
        rows = db.query_all(
            """
            SELECT links.user_id
              FROM (
                SELECT DISTINCT user_id
                  FROM steam_links
                 WHERE user_id != 0
              ) AS links
              LEFT JOIN steam_cleanup_poll_state AS state
                ON state.user_id = links.user_id
             WHERE COALESCE(state.last_polled_at, 0) <= ?
             ORDER BY COALESCE(state.last_polled_at, 0) ASC, links.user_id ASC
             LIMIT ?
            """,
            (cutoff, int(self._poll_batch_size)),
        )
        out: list[int] = []
        for row in rows:
            try:
                out.append(int(row["user_id"]))
            except Exception:
                continue
        return out

    def _upsert_poll_state(
        self,
        user_id: int,
        *,
        now_ts: int,
        result: str,
        miss_count: int = 0,
        error: str | None = None,
    ) -> None:
        db.execute(
            """
            INSERT INTO steam_cleanup_poll_state(
              user_id, last_polled_at, last_result, last_error, miss_count, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              last_polled_at = excluded.last_polled_at,
              last_result = excluded.last_result,
              last_error = excluded.last_error,
              miss_count = excluded.miss_count,
              updated_at = excluded.updated_at
            """,
            (
                int(user_id),
                int(now_ts),
                str(result),
                error,
                max(0, int(miss_count)),
                int(now_ts),
            ),
        )

    def _poll_miss_count(self, user_id: int) -> int:
        row = db.query_one(
            """
            SELECT miss_count
              FROM steam_cleanup_poll_state
             WHERE user_id = ?
             LIMIT 1
            """,
            (int(user_id),),
        )
        if not row:
            return 0
        try:
            return max(0, int(row["miss_count"] or 0))
        except Exception:
            return 0

    def _confirmed_left(self, user_id: int, guild_id: int) -> bool:
        """
        Verifiziert einen Leave über den vorhandenen member_events Tracker.
        Nur wenn das letzte Event ein leave/ban war, wird aufgeräumt.
        """
        row = db.query_one(
            """
            SELECT event_type
              FROM member_events
             WHERE user_id = ? AND guild_id = ?
             ORDER BY timestamp DESC
             LIMIT 1
            """,
            (int(user_id), int(guild_id)),
        )
        if not row:
            return False
        event = str(row["event_type"] or "").lower()
        return event in ("leave", "ban")

    async def _handle_leave_event(
        self,
        user_id: int,
        guild_id: int | None,
        display_name: str | None,
        reason: str,
    ) -> None:
        if self._is_recent(user_id):
            return
        await self._cleanup_user(user_id, guild_id, display_name, reason)

    async def _cleanup_user(
        self,
        user_id: int,
        guild_id: int | None,
        display_name: str | None,
        reason: str,
    ) -> bool:
        try:
            if privacy.is_opted_out(int(user_id)):
                return False

            rows = db.query_all(
                """
                SELECT user_id, steam_id, name, verified, primary_account,
                       deadlock_rank, deadlock_rank_name, deadlock_subrank,
                       deadlock_badge_level, deadlock_rank_updated_at,
                       created_at, updated_at
                  FROM steam_links
                 WHERE user_id=?
                """,
                (int(user_id),),
            )
            if not rows:
                return False

            steam_ids = [str(r["steam_id"]).strip() for r in rows if r and r["steam_id"]]
            if not steam_ids:
                return False

            rel_map = await self._fetch_relationships(steam_ids)

            left_at = int(time.time())
            async with db.transaction() as conn:
                for r in rows:
                    steam_id = str(r["steam_id"]).strip()
                    if not steam_id:
                        continue
                    conn.execute(
                        """
                        INSERT INTO steam_links_archive(
                            user_id, steam_id, name, verified, primary_account,
                            deadlock_rank, deadlock_rank_name, deadlock_subrank, deadlock_badge_level,
                            deadlock_rank_updated_at, created_at, updated_at,
                            left_at, guild_id, leave_reason, display_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(user_id, steam_id) DO UPDATE SET
                            name=excluded.name,
                            -- keep relationship snapshot for debugging cleanup issues
                            leave_reason=excluded.leave_reason,
                            verified=excluded.verified,
                            primary_account=excluded.primary_account,
                            deadlock_rank=excluded.deadlock_rank,
                            deadlock_rank_name=excluded.deadlock_rank_name,
                            deadlock_subrank=excluded.deadlock_subrank,
                            deadlock_badge_level=excluded.deadlock_badge_level,
                            deadlock_rank_updated_at=excluded.deadlock_rank_updated_at,
                            created_at=COALESCE(steam_links_archive.created_at, excluded.created_at),
                            updated_at=excluded.updated_at,
                            left_at=excluded.left_at,
                            guild_id=excluded.guild_id,
                            display_name=COALESCE(excluded.display_name, steam_links_archive.display_name)
                        """,
                        (
                            int(user_id),
                            steam_id,
                            r["name"],
                            r["verified"],
                            r["primary_account"],
                            r["deadlock_rank"],
                            r["deadlock_rank_name"],
                            r["deadlock_subrank"],
                            r["deadlock_badge_level"],
                            r["deadlock_rank_updated_at"],
                            r["created_at"],
                            r["updated_at"],
                            left_at,
                            int(guild_id) if guild_id else None,
                            f"{reason} | rel={rel_map.get(steam_id)}",
                            display_name,
                        ),
                    )

                conn.execute("DELETE FROM steam_links WHERE user_id=?", (int(user_id),))
                conn.executemany(
                    "DELETE FROM steam_friend_requests WHERE steam_id=?",
                    [(sid,) for sid in steam_ids],
                )

            await self._unfriend_all(steam_ids)
            log.info(
                "Steam cleanup after leave: user=%s, steam_ids=%d, reason=%s",
                user_id,
                len(steam_ids),
                reason,
            )
            return True
        except Exception as exc:  # noqa: BLE001 - we want full context in logs
            log.warning("Steam cleanup failed for user %s: %s", user_id, exc, exc_info=True)
            return False

    async def _unfriend_all(self, steam_ids: Iterable[str]) -> None:
        unique_ids = sorted({sid for sid in steam_ids if sid})
        if not unique_ids:
            return

        futures = [
            self.tasks.run("AUTH_REMOVE_FRIEND", {"steam_id": sid}, timeout=25.0)
            for sid in unique_ids
        ]

        results = await asyncio.gather(*futures, return_exceptions=True)
        failed = 0
        for res in results:
            if isinstance(res, Exception):
                failed += 1
                continue
            if not getattr(res, "ok", False):
                failed += 1
        if failed:
            log.warning(
                "Steam unfriend tasks finished with %d/%d failures",
                failed,
                len(unique_ids),
            )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        if member.bot:
            return
        await self._handle_leave_event(member.id, member.guild.id, member.display_name, "leave")

    @commands.Cog.listener()
    async def on_raw_member_remove(self, payload: discord.RawMemberRemoveEvent) -> None:
        user = payload.user
        if user and user.bot:
            return
        user_id = getattr(payload, "user_id", None) or (user.id if user else None)
        if not user_id:
            return
        display = getattr(user, "display_name", None) or getattr(user, "name", None)
        await self._handle_leave_event(int(user_id), payload.guild_id, display, "raw_leave")

    @tasks.loop(hours=1)
    async def reconcile_orphaned_links(self) -> None:
        await self.bot.wait_until_ready()
        guild_id = settings.guild_id
        if not guild_id:
            return
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            log.debug("SteamLeaveCleanup: Guild %s not found for reconciliation", guild_id)
            return

        self._ensure_poll_state_table()
        now_ts = int(time.time())
        candidates = self._select_poll_candidates(now_ts=now_ts)
        if not candidates:
            return

        cleaned = 0
        for uid in candidates:
            prior_miss_count = self._poll_miss_count(uid)
            member = guild.get_member(uid)
            if member is not None:
                self._upsert_poll_state(uid, now_ts=now_ts, result="present", miss_count=0)
                continue

            try:
                member = await guild.fetch_member(uid)
            except discord.NotFound:
                confirmed_by_event = self._confirmed_left(uid, guild.id)
                miss_count = prior_miss_count + 1
                if confirmed_by_event or miss_count >= 2:
                    reason = (
                        "reconcile_event"
                        if confirmed_by_event
                        else "reconcile_poll_missing_confirmed"
                    )
                    changed = await self._cleanup_user(uid, guild.id, None, reason)
                    cleaned += int(changed)
                    self._upsert_poll_state(
                        uid,
                        now_ts=now_ts,
                        result="cleaned" if changed else "confirmed_missing",
                        miss_count=0,
                    )
                else:
                    self._upsert_poll_state(
                        uid,
                        now_ts=now_ts,
                        result="missing",
                        miss_count=miss_count,
                    )
                continue
            except discord.HTTPException as exc:  # rate limits / temporary failures
                log.debug("SteamLeaveCleanup: fetch_member failed for %s: %s", uid, exc)
                self._upsert_poll_state(
                    uid,
                    now_ts=now_ts,
                    result="error",
                    miss_count=prior_miss_count,
                    error=str(exc)[:500],
                )
                continue
            except Exception as exc:  # noqa: BLE001
                self._upsert_poll_state(
                    uid,
                    now_ts=now_ts,
                    result="error",
                    miss_count=prior_miss_count,
                    error=str(exc)[:500],
                )
                continue
            else:
                if member is None:
                    self._upsert_poll_state(
                        uid,
                        now_ts=now_ts,
                        result="missing",
                        miss_count=prior_miss_count + 1,
                    )
                else:
                    self._upsert_poll_state(uid, now_ts=now_ts, result="present", miss_count=0)

        if cleaned:
            log.info(
                "SteamLeaveCleanup poll reconcile cleaned %d user(s) [batch=%d, interval=%ds]",
                cleaned,
                len(candidates),
                self._poll_min_interval_sec,
            )

    @reconcile_orphaned_links.before_loop
    async def _wait_ready(self) -> None:
        await self.bot.wait_until_ready()

    async def _fetch_relationships(self, steam_ids: Iterable[str]) -> dict[str, str]:
        """
        Bot-seitige Prüfung der aktuellen Beziehung (inkl. Pending) direkt via Steam-Task.
        Hard-throttled auf 1x/24h, um Steam nicht unnötig zu belasten.
        """
        now = time.time()
        if now - self._last_relationship_probe < 24 * 60 * 60:
            return {}
        self._last_relationship_probe = now

        out: dict[str, str] = {}
        tasks = []
        for sid in {s for s in steam_ids if s}:
            tasks.append(self.tasks.run("AUTH_CHECK_FRIENDSHIP", {"steam_id": sid}, timeout=15.0))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) or not getattr(res, "ok", False):
                continue
            data = (res.result or {}).get("data") if isinstance(res.result, dict) else None
            if not data:
                continue
            sid = str(data.get("steam_id64") or "").strip()
            rel = data.get("relationship_name") or str(data.get("relationship") or "")
            if sid:
                out[sid] = rel
        return out


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(SteamLeaveCleanup(bot))
