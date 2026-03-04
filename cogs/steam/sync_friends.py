"""
Synchronize Steam Bot friends to database.

This module provides functionality to sync all current friends of the Steam bot
into the steam_links table, ensuring all bot friends are tracked in the database.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import discord
from discord import app_commands
from discord.ext import commands, tasks

from cogs.steam.steam_master import SteamTaskClient
from service import db
from service.config import settings

log = logging.getLogger(__name__)

MIN_DISCORD_SNOWFLAKE = 10_000_000_000_000_000


def _safe_int_setting(name: str, default: int, *, minimum: int = 1) -> int:
    raw = getattr(settings, name, default)
    try:
        value = int(raw)
    except Exception:
        value = int(default)
    return max(minimum, value)


STEAM_POLL_MIN_INTERVAL_SEC = _safe_int_setting("steam_poll_min_interval_sec", 86400)
STEAM_UNFOLLOW_MISS_THRESHOLD = _safe_int_setting("steam_unfollow_miss_threshold", 2)
STEAM_POLL_BATCH_SIZE = min(100, _safe_int_setting("steam_poll_batch_size", 25))


def _ensure_unfollow_tracking_tables() -> None:
    with db.get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS steam_friendship_miss_tracker(
              steam_id TEXT PRIMARY KEY,
              user_id INTEGER NOT NULL,
              miss_count INTEGER NOT NULL DEFAULT 0,
              last_polled_at INTEGER NOT NULL,
              last_seen_friend_at INTEGER,
              last_miss_at INTEGER,
              last_action_at INTEGER,
              updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS steam_role_cleanup_pending(
              user_id INTEGER PRIMARY KEY,
              reason TEXT NOT NULL,
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT,
              created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
              updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_steam_friendship_miss_tracker_user
              ON steam_friendship_miss_tracker(user_id, last_polled_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_steam_friendship_miss_tracker_polled
              ON steam_friendship_miss_tracker(last_polled_at, miss_count)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_steam_role_cleanup_pending_updated
              ON steam_role_cleanup_pending(updated_at)
            """
        )


def _save_steam_friend_to_db(steam_id64: str, discord_id: int | None = None) -> set[int]:
    """
    Save a Steam friend to the database.

    Args:
        steam_id64: The Steam ID64 of the friend
        discord_id: Optional Discord ID if known (defaults to 0 for unknown)

    Returns:
        Set der betroffenen Discord-IDs (falls bekannt), um Sofort-Rollenvergabe anzustoßen.
    """
    affected: set[int] = set()
    uid = int(discord_id) if discord_id else 0

    with db.get_conn() as conn:
        # Check if this steam_id already exists with a real Discord ID
        existing = conn.execute(
            "SELECT user_id FROM steam_links WHERE steam_id = ? AND user_id != 0 LIMIT 1",
            (steam_id64,),
        ).fetchone()

        if existing:
            # Already linked to a Discord account, just update verified + friend status
            conn.execute(
                """
                UPDATE steam_links
                SET verified = 1, is_steam_friend = 1, updated_at = CURRENT_TIMESTAMP
                WHERE steam_id = ?
                """,
                (steam_id64,),
            )
            log.info(
                "Updated existing steam_link(s): steam=%s",
                steam_id64,
            )
            rows = conn.execute(
                "SELECT DISTINCT user_id FROM steam_links WHERE steam_id=? AND user_id != 0",
                (steam_id64,),
            ).fetchall()
            for r in rows:
                try:
                    affected.add(int(r["user_id"]))
                except Exception:
                    continue
        else:
            # New friend or unlinked friend
            conn.execute(
                """
                INSERT INTO steam_links(user_id, steam_id, name, verified, is_steam_friend)
                VALUES(?, ?, ?, ?, 1)
                ON CONFLICT(user_id, steam_id) DO UPDATE SET
                  verified=1,
                  is_steam_friend=1,
                  updated_at=CURRENT_TIMESTAMP
                """,
                (uid, steam_id64, "", 1),
            )
            log.info("Saved new steam_link: steam=%s, discord=%s", steam_id64, uid)
            if uid:
                affected.add(uid)

    return affected


def _user_has_verified_friend_link(conn, user_id: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
          FROM steam_links
         WHERE user_id = ?
           AND verified = 1
           AND is_steam_friend = 1
         LIMIT 1
        """,
        (int(user_id),),
    ).fetchone()
    return bool(row)


async def sync_all_friends(tasks: SteamTaskClient | None = None) -> dict:
    """
    Synchronize all current Steam bot friends to the database.

    Args:
        tasks: Optional SteamTaskClient instance (creates new one if not provided)

    Returns:
        Dictionary with sync results:
        - success: bool
        - count: int (number of friends synced)
        - error: Optional str
    """
    if tasks is None:
        tasks = SteamTaskClient(poll_interval=0.5, default_timeout=30.0)

    try:
        _ensure_unfollow_tracking_tables()

        # Request friends list from Node.js service
        log.info("Requesting friends list from Steam service...")
        outcome = await tasks.run("AUTH_GET_FRIENDS_LIST", timeout=30.0)

        if not outcome.ok:
            error_msg = outcome.error or "Failed to get friends list"
            log.error("Failed to get friends list: %s", error_msg)
            return {
                "success": False,
                "count": 0,
                "cleared_count": 0,
                "fully_unfollowed_user_ids": [],
                "polled_users_count": 0,
                "misses_observed": 0,
                "miss_resets": 0,
                "error": error_msg,
            }

        if not outcome.result or not isinstance(outcome.result, dict):
            log.error("Invalid result format from AUTH_GET_FRIENDS_LIST")
            return {
                "success": False,
                "count": 0,
                "cleared_count": 0,
                "fully_unfollowed_user_ids": [],
                "polled_users_count": 0,
                "misses_observed": 0,
                "miss_resets": 0,
                "error": "Invalid result format",
            }

        data = outcome.result.get("data", {})
        friends = data.get("friends", [])
        if not isinstance(friends, list):
            log.error("Invalid friends format from AUTH_GET_FRIENDS_LIST")
            return {
                "success": False,
                "count": 0,
                "cleared_count": 0,
                "fully_unfollowed_user_ids": [],
                "polled_users_count": 0,
                "misses_observed": 0,
                "miss_resets": 0,
                "error": "Invalid friends format",
            }

        if not friends:
            log.warning("No friends found in Steam bot's friend list")
        else:
            log.info("Found %d friends, syncing to database...", len(friends))

        # Sync each friend to database
        synced = 0
        newly_verified_ids: set[int] = set()
        friend_steam_ids_set: set[str] = set()
        for friend in friends:
            steam_id64_raw = friend.get("steam_id64")
            if not steam_id64_raw:
                continue
            steam_id64 = str(steam_id64_raw).strip()
            if not steam_id64:
                continue
            friend_steam_ids_set.add(steam_id64)

            try:
                ids = _save_steam_friend_to_db(steam_id64)
                newly_verified_ids.update(ids)
                synced += 1
            except Exception as e:
                log.error("Failed to save friend %s: %s", steam_id64, e)

        log.info("Synced %d/%d friends to database", synced, len(friends))

        now = int(time.time())
        cutoff = now - STEAM_POLL_MIN_INTERVAL_SEC
        fully_unfollowed_user_ids: list[int] = []
        misses_observed = 0
        miss_resets = 0
        polled_users_count = 0
        cleared = 0

        with db.get_conn() as conn:
            if friend_steam_ids_set:
                ids_json = json.dumps(sorted(friend_steam_ids_set))
                reset_info = conn.execute(
                    """
                    UPDATE steam_friendship_miss_tracker
                       SET miss_count = 0,
                           last_seen_friend_at = ?,
                           updated_at = ?
                     WHERE steam_id IN (SELECT value FROM json_each(?))
                       AND miss_count > 0
                    """,
                    (now, now, ids_json),
                )
                miss_resets = max(int(reset_info.rowcount or 0), 0)

            # Pro User nur eine Steam-ID-Kandidatur je Lauf (bevorzugt primary_account)
            candidate_rows = conn.execute(
                """
                SELECT steam_id, user_id
                  FROM steam_links
                 WHERE verified = 1
                   AND is_steam_friend = 1
                   AND user_id >= ?
                 ORDER BY user_id ASC, primary_account DESC, updated_at DESC
                """,
                (MIN_DISCORD_SNOWFLAKE,),
            ).fetchall()

            selected_candidates: list[tuple[int, str]] = []
            seen_users: set[int] = set()
            for row in candidate_rows:
                user_id_raw = row["user_id"] if row else None
                steam_id_raw = row["steam_id"] if row else None
                if user_id_raw is None or not steam_id_raw:
                    continue
                user_id = int(user_id_raw)
                if user_id in seen_users:
                    continue

                last_poll_row = conn.execute(
                    """
                    SELECT MAX(last_polled_at) AS last_polled_at
                      FROM steam_friendship_miss_tracker
                     WHERE user_id = ?
                    """,
                    (user_id,),
                ).fetchone()
                last_polled_at = int(last_poll_row["last_polled_at"] or 0) if last_poll_row else 0
                if last_polled_at > cutoff:
                    continue

                seen_users.add(user_id)
                selected_candidates.append((user_id, str(steam_id_raw).strip()))
                if len(selected_candidates) >= STEAM_POLL_BATCH_SIZE:
                    break

            if selected_candidates:
                log.info(
                    "Unfollow reconcile poll: selected=%d, min_interval=%ds, threshold=%d",
                    len(selected_candidates),
                    STEAM_POLL_MIN_INTERVAL_SEC,
                    STEAM_UNFOLLOW_MISS_THRESHOLD,
                )

            for user_id, steam_id in selected_candidates:
                if not steam_id:
                    continue

                polled_users_count += 1
                tracker_row = conn.execute(
                    """
                    SELECT miss_count, last_action_at
                      FROM steam_friendship_miss_tracker
                     WHERE steam_id = ?
                    """,
                    (steam_id,),
                ).fetchone()
                previous_miss_count = (
                    int(tracker_row["miss_count"] or 0) if tracker_row else 0
                )
                last_action_at = (
                    int(tracker_row["last_action_at"])
                    if tracker_row and tracker_row["last_action_at"] is not None
                    else None
                )

                if steam_id in friend_steam_ids_set:
                    if previous_miss_count > 0:
                        miss_resets += 1
                    conn.execute(
                        """
                        INSERT INTO steam_friendship_miss_tracker(
                          steam_id, user_id, miss_count, last_polled_at,
                          last_seen_friend_at, last_miss_at, last_action_at, updated_at
                        )
                        VALUES (?, ?, 0, ?, ?, NULL, ?, ?)
                        ON CONFLICT(steam_id) DO UPDATE SET
                          user_id = excluded.user_id,
                          miss_count = 0,
                          last_polled_at = excluded.last_polled_at,
                          last_seen_friend_at = excluded.last_seen_friend_at,
                          last_miss_at = NULL,
                          last_action_at = COALESCE(steam_friendship_miss_tracker.last_action_at, excluded.last_action_at),
                          updated_at = excluded.updated_at
                        """,
                        (steam_id, user_id, now, now, last_action_at, now),
                    )
                    continue

                misses_observed += 1
                miss_count = previous_miss_count + 1
                action_triggered = False
                reason = (
                    f"Steam unfollow confirmed (miss_count={miss_count}, "
                    f"threshold={STEAM_UNFOLLOW_MISS_THRESHOLD})"
                )

                if miss_count >= STEAM_UNFOLLOW_MISS_THRESHOLD:
                    result = conn.execute(
                        """
                        UPDATE steam_links
                           SET verified = 0,
                               is_steam_friend = 0,
                               updated_at = CURRENT_TIMESTAMP
                         WHERE user_id = ?
                           AND steam_id = ?
                           AND verified = 1
                           AND is_steam_friend = 1
                        """,
                        (user_id, steam_id),
                    )
                    changed = max(int(result.rowcount or 0), 0)
                    if changed > 0:
                        action_triggered = True
                        cleared += changed
                        last_action_at = now
                        if not _user_has_verified_friend_link(conn, user_id):
                            fully_unfollowed_user_ids.append(user_id)
                            conn.execute(
                                """
                                INSERT INTO steam_role_cleanup_pending(
                                  user_id, reason, attempts, last_error, created_at, updated_at
                                )
                                VALUES (?, ?, 0, NULL, ?, ?)
                                ON CONFLICT(user_id) DO UPDATE SET
                                  reason = excluded.reason,
                                  last_error = NULL,
                                  updated_at = excluded.updated_at
                                """,
                                (user_id, reason, now, now),
                            )
                        else:
                            log.debug(
                                "Skipping role cleanup enqueue for user=%s because verified friend link(s) remain",
                                user_id,
                            )

                conn.execute(
                    """
                    INSERT INTO steam_friendship_miss_tracker(
                      steam_id, user_id, miss_count, last_polled_at,
                      last_seen_friend_at, last_miss_at, last_action_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
                    ON CONFLICT(steam_id) DO UPDATE SET
                      user_id = excluded.user_id,
                      miss_count = excluded.miss_count,
                      last_polled_at = excluded.last_polled_at,
                      last_seen_friend_at = excluded.last_seen_friend_at,
                      last_miss_at = excluded.last_miss_at,
                      last_action_at = excluded.last_action_at,
                      updated_at = excluded.updated_at
                    """,
                    (steam_id, user_id, miss_count, now, now, last_action_at, now),
                )

                if action_triggered:
                    log.debug(
                        "Unfollow miss-threshold action applied: user=%s miss_count=%s",
                        user_id,
                        miss_count,
                    )

        fully_unfollowed_user_ids = sorted(set(fully_unfollowed_user_ids))

        return {
            "success": True,
            "count": synced,
            "cleared_count": cleared,
            "fully_unfollowed_user_ids": fully_unfollowed_user_ids,
            "newly_verified_ids": list(newly_verified_ids),
            "polled_users_count": polled_users_count,
            "misses_observed": misses_observed,
            "miss_resets": miss_resets,
            "error": None,
        }

    except Exception as e:
        log.exception("Failed to sync friends")
        return {
            "success": False,
            "count": 0,
            "cleared_count": 0,
            "fully_unfollowed_user_ids": [],
            "polled_users_count": 0,
            "misses_observed": 0,
            "miss_resets": 0,
            "error": str(e),
        }


class SteamFriendsSync(commands.Cog):
    """Commands for syncing Steam bot friends to database."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.tasks = SteamTaskClient(poll_interval=0.5, default_timeout=30.0)

    async def cog_load(self) -> None:
        _ensure_unfollow_tracking_tables()
        self.periodic_sync.start()
        # Kein eigener Guild-Sync im cog_load:
        # Der zentrale Sync in main_bot.setup_hook soll die komplette Commandliste
        # veröffentlichen, um Teilmengen bei Rate-Limits zu vermeiden.

    def cog_unload(self) -> None:
        self.periodic_sync.cancel()

    async def _sync_guild_commands(self, guild_obj: discord.Object) -> None:
        try:
            synced = await asyncio.wait_for(self.bot.tree.sync(guild=guild_obj), timeout=20.0)
            log.info(
                "SteamFriendsSync: Guild-Command sync abgeschlossen (%d commands)", len(synced)
            )
        except asyncio.TimeoutError:
            log.warning("SteamFriendsSync: Guild-Command-Sync Timeout (>20s) – wird übersprungen")
        except Exception as exc:
            log.warning("SteamFriendsSync: Guild-Command-Sync fehlgeschlagen: %s", exc)

    async def _apply_verified_role_updates(
        self, newly_verified_ids: list[int] | None
    ) -> tuple[int, int]:
        role_changes = 0
        immediate = 0
        guild = self.bot.get_guild(settings.guild_id) if settings.guild_id else None
        role = guild.get_role(settings.verified_role_id) if guild else None
        try:
            verified_cog = self.bot.get_cog("SteamVerifiedRole")
            if verified_cog is not None:
                tasks_local = []
                for uid in newly_verified_ids or []:

                    async def run(uid_local: int):
                        nonlocal immediate
                        try:
                            ok = await verified_cog.assign_verified_role_with_retries(
                                uid_local, guild=guild, role=role
                            )
                            if ok:
                                immediate += 1
                        except Exception as exc:  # noqa: PERF203
                            log.warning(
                                "Assign verified (immediate) failed for %s: %s", uid_local, exc
                            )

                    tasks_local.append(asyncio.create_task(run(int(uid))))
                if tasks_local:
                    await asyncio.gather(*tasks_local, return_exceptions=True)
                role_changes = int(await verified_cog._run_once() or 0)
        except Exception as exc:
            log.warning("Rolle-Update nach Sync fehlgeschlagen: %s", exc)
        return immediate, role_changes

    async def _drain_pending_role_cleanup(self, *, limit: int = 200) -> dict[str, int]:
        targets: list[tuple[int, str, int]] = []
        with db.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT user_id, reason, attempts
                  FROM steam_role_cleanup_pending
                 ORDER BY updated_at ASC, user_id ASC
                 LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            for row in rows:
                try:
                    user_id = int(row["user_id"])
                except Exception:
                    continue
                if user_id < MIN_DISCORD_SNOWFLAKE:
                    continue
                reason = str(row["reason"] or "Steam unfollow cleanup")
                attempts = int(row["attempts"] or 0)
                targets.append((user_id, reason, attempts))

        if not targets:
            return {"processed": 0, "completed": 0, "deferred": 0, "failed": 0}

        verified_cog = self.bot.get_cog("SteamVerifiedRole")
        rank_cog = self.bot.get_cog("DeadlockFriendRank")
        has_verified_cleanup = bool(
            verified_cog and hasattr(verified_cog, "remove_verified_role_for_user")
        )
        has_rank_cleanup = bool(rank_cog and hasattr(rank_cog, "remove_rank_roles_for_users"))
        if not has_verified_cleanup or not has_rank_cleanup:
            log.warning(
                "Role cleanup deferred: SteamVerifiedRole/DeadlockFriendRank unavailable (verified=%s rank=%s pending=%d)",
                has_verified_cleanup,
                has_rank_cleanup,
                len(targets),
            )
            return {
                "processed": len(targets),
                "completed": 0,
                "deferred": len(targets),
                "failed": 0,
            }

        completed = 0
        failed = 0
        now = int(time.time())
        for user_id, reason, attempts in targets:
            ok_verified = False
            ok_rank = False
            error_text = None
            with db.get_conn() as conn:
                if _user_has_verified_friend_link(conn, user_id):
                    conn.execute(
                        "DELETE FROM steam_role_cleanup_pending WHERE user_id = ?",
                        (user_id,),
                    )
                    completed += 1
                    continue
            try:
                ok_verified = bool(
                    await verified_cog.remove_verified_role_for_user(
                        user_id,
                        reason=reason,
                    )
                )
            except Exception as exc:
                error_text = f"verified_cleanup_failed: {exc}"

            if error_text is None and not ok_verified:
                error_text = "verified_cleanup_not_completed"

            if error_text is None and ok_verified:
                try:
                    await rank_cog.remove_rank_roles_for_users([user_id], reason=reason)
                    ok_rank = True
                except Exception as exc:
                    error_text = f"rank_cleanup_failed: {exc}"

            if ok_verified and ok_rank:
                with db.get_conn() as conn:
                    conn.execute(
                        "DELETE FROM steam_role_cleanup_pending WHERE user_id = ?",
                        (user_id,),
                    )
                completed += 1
                continue

            failed += 1
            with db.get_conn() as conn:
                conn.execute(
                    """
                    UPDATE steam_role_cleanup_pending
                       SET attempts = ?,
                           last_error = ?,
                           updated_at = ?
                     WHERE user_id = ?
                    """,
                    (
                        max(0, int(attempts)) + 1,
                        (error_text or "cleanup_incomplete")[:500],
                        now,
                        user_id,
                    ),
                )

        return {
            "processed": len(targets),
            "completed": completed,
            "deferred": 0,
            "failed": failed,
        }

    @tasks.loop(hours=6)
    async def periodic_sync(self) -> None:
        """Synchronisiert Steam-Freunde + gedrosseltes Unfollow-Reconcile."""
        log.info("Periodischer Steam-Freunde-Sync gestartet...")
        result = await sync_all_friends(self.tasks)
        if not result["success"]:
            log.warning("Periodischer Sync fehlgeschlagen: %s", result.get("error"))
            return

        immediate, role_changes = await self._apply_verified_role_updates(
            result.get("newly_verified_ids", [])
        )
        pending_cleanup = await self._drain_pending_role_cleanup()

        log.info(
            "Periodischer Sync abgeschlossen: friends=%d immediate=%d role_changes=%d polled=%d misses=%d resets=%d unfollow_actions=%d pending_cleanup=%s",
            result["count"],
            immediate,
            role_changes,
            int(result.get("polled_users_count", 0) or 0),
            int(result.get("misses_observed", 0) or 0),
            int(result.get("miss_resets", 0) or 0),
            int(result.get("cleared_count", 0) or 0),
            pending_cleanup,
        )

    @periodic_sync.before_loop
    async def before_periodic_sync(self) -> None:
        await self.bot.wait_until_ready()

    @app_commands.command(
        name="sync_steam_friends",
        description="(Admin) Synchronisiert Steam-Freundesliste und aktualisiert Verified-Rollen.",
    )
    @app_commands.checks.has_permissions(administrator=True)
    async def slash_sync_friends(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True, thinking=True)

        # 1. Steam-Freunde syncen
        result = await sync_all_friends(self.tasks)

        if not result["success"]:
            error = result.get("error", "Unbekannter Fehler")
            await interaction.followup.send(
                f"❌ Steam-Sync fehlgeschlagen: `{error}`", ephemeral=True
            )
            return

        synced = result["count"]
        cleared = result.get("cleared_count", 0)

        # 2. Verified-Rollen sofort aktualisieren (add + remove)
        immediate, role_changes = await self._apply_verified_role_updates(
            result.get("newly_verified_ids", [])
        )
        pending_cleanup = await self._drain_pending_role_cleanup()

        lines = [
            f"**Steam-Freunde synchronisiert:** {synced}",
            f"**Sofort verifizierte Rollen vergeben:** {immediate}",
            f"**Polling geprüft (User):** {int(result.get('polled_users_count', 0) or 0)}",
            f"**Misses erkannt:** {int(result.get('misses_observed', 0) or 0)}",
        ]
        if cleared:
            lines.append(f"**Unfollow bestätigt (unverified gesetzt):** {cleared}")
        if result.get("miss_resets", 0):
            lines.append(f"**Miss-Resets (wieder Freund):** {int(result.get('miss_resets', 0) or 0)}")
        lines.append(f"**Rollen-Updates:** {role_changes}")
        lines.append(
            "**Pending Role-Cleanup:** "
            f"processed={pending_cleanup.get('processed', 0)}, "
            f"completed={pending_cleanup.get('completed', 0)}, "
            f"deferred={pending_cleanup.get('deferred', 0)}, "
            f"failed={pending_cleanup.get('failed', 0)}"
        )

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @commands.command(name="sync_steam_friends")
    @commands.has_permissions(administrator=True)
    async def cmd_sync_friends(self, ctx: commands.Context) -> None:
        """Synchronize all Steam bot friends to the database."""
        async with ctx.typing():
            result = await sync_all_friends(self.tasks)

        if result["success"]:
            await ctx.reply(
                f"✅ Synced {result['count']} Steam friends to database.",
                mention_author=False,
            )
        else:
            error = result.get("error", "Unknown error")
            await ctx.reply(f"❌ Failed to sync friends: {error}", mention_author=False)


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(SteamFriendsSync(bot))
