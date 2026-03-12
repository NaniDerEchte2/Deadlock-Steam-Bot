from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from importlib.util import find_spec
from pathlib import Path
from typing import Any


TEST_USER_ID = 100_000_000_000_000_123


class _DbShim:
    sqlite3 = sqlite3

    def __init__(self, db_path: Path) -> None:
        self._db_path = str(db_path)

    class _ConnContext:
        def __init__(self, db_path: str) -> None:
            self._db_path = db_path
            self._conn: sqlite3.Connection | None = None

        def __enter__(self) -> sqlite3.Connection:
            conn = sqlite3.connect(self._db_path)
            conn.row_factory = sqlite3.Row
            self._conn = conn
            return conn

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            if self._conn is None:
                return
            try:
                if exc_type is None:
                    self._conn.commit()
                else:
                    self._conn.rollback()
            finally:
                self._conn.close()

    def get_conn(self) -> "_DbShim._ConnContext":
        return self._ConnContext(self._db_path)


class _FakeSteamTaskClient:
    def __init__(self, result: dict[str, Any] | None = None, *_: Any, **__: Any) -> None:
        self._result = result or {"data": {"friends": []}}

    async def run(self, *_: Any, **__: Any) -> Any:
        return types.SimpleNamespace(ok=True, result=self._result, error=None)


class _FakeVerifiedCog:
    def __init__(self, *, should_succeed: bool = True, exc: Exception | None = None) -> None:
        self.should_succeed = should_succeed
        self.exc = exc
        self.calls: list[tuple[int, str]] = []

    async def remove_verified_role_for_user(self, user_id: int, *, reason: str) -> bool:
        self.calls.append((int(user_id), str(reason)))
        if self.exc is not None:
            raise self.exc
        return self.should_succeed


class _FakeRankCog:
    def __init__(self, *, exc: Exception | None = None) -> None:
        self.exc = exc
        self.calls: list[tuple[list[int], str]] = []

    async def remove_rank_roles_for_users(self, user_ids: list[int], *, reason: str) -> int:
        self.calls.append(([int(uid) for uid in user_ids], str(reason)))
        if self.exc is not None:
            raise self.exc
        return len(user_ids)


class _FakeBot:
    def __init__(self, cogs: dict[str, Any] | None = None) -> None:
        self._cogs = dict(cogs or {})

    def get_cog(self, name: str) -> Any | None:
        return self._cogs.get(name)

    def get_guild(self, _guild_id: int) -> Any | None:
        return None


@unittest.skipUnless(find_spec("discord") is not None, "discord.py is required for this integration test")
class SyncFriendsCleanupTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temp_db_file = tempfile.NamedTemporaryFile(
            prefix="sync_friends_test_",
            suffix=".sqlite3",
            delete=False,
        )
        self._temp_db_file.close()
        self._db_path = Path(self._temp_db_file.name)

        self._original_modules: dict[str, Any] = {}
        for name in (
            "service",
            "service.config",
            "cogs.steam.steam_master",
            "cogs.steam.sync_friends",
        ):
            self._original_modules[name] = sys.modules.get(name)

        service_pkg = types.ModuleType("service")
        service_pkg.db = _DbShim(self._db_path)
        sys.modules["service"] = service_pkg

        config_mod = types.ModuleType("service.config")
        config_mod.settings = types.SimpleNamespace(
            steam_poll_min_interval_sec=1,
            steam_unfollow_miss_threshold=1,
            steam_poll_batch_size=25,
            guild_id=0,
            verified_role_id=0,
        )
        service_pkg.config = config_mod
        sys.modules["service.config"] = config_mod

        steam_master_mod = types.ModuleType("cogs.steam.steam_master")
        steam_master_mod.SteamTaskClient = _FakeSteamTaskClient
        sys.modules["cogs.steam.steam_master"] = steam_master_mod

        sys.modules.pop("cogs.steam.sync_friends", None)
        self.mod = importlib.import_module("cogs.steam.sync_friends")
        self._create_minimal_tables()
        self.mod._ensure_unfollow_tracking_tables()

    def tearDown(self) -> None:
        sys.modules.pop("cogs.steam.sync_friends", None)
        for name, original in self._original_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        try:
            os.unlink(self._db_path)
        except OSError:
            pass

    def _create_minimal_tables(self) -> None:
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steam_links(
                    user_id INTEGER NOT NULL,
                    steam_id TEXT NOT NULL,
                    name TEXT,
                    verified INTEGER DEFAULT 0,
                    is_steam_friend INTEGER DEFAULT 0,
                    primary_account INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(user_id, steam_id)
                )
                """
            )

    def _insert_link(
        self,
        *,
        user_id: int,
        steam_id: str,
        verified: int,
        is_steam_friend: int,
        primary_account: int = 0,
    ) -> None:
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO steam_links(
                    user_id, steam_id, name, verified, is_steam_friend, primary_account
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), str(steam_id), "", int(verified), int(is_steam_friend), int(primary_account)),
            )

    def _get_pending_row(self, user_id: int) -> sqlite3.Row | None:
        with self.mod.db.get_conn() as conn:
            return conn.execute(
                """
                SELECT user_id, reason, attempts, last_error
                  FROM steam_role_cleanup_pending
                 WHERE user_id = ?
                """,
                (int(user_id),),
            ).fetchone()

    async def test_sync_all_friends_queues_cleanup_for_single_active_unfollow(self) -> None:
        self._insert_link(
            user_id=TEST_USER_ID,
            steam_id="76561198000000001",
            verified=1,
            is_steam_friend=1,
            primary_account=1,
        )

        result = await self.mod.sync_all_friends(_FakeSteamTaskClient())

        self.assertTrue(result["success"])
        self.assertEqual(result["cleared_count"], 1)
        self.assertEqual(result["fully_unfollowed_user_ids"], [TEST_USER_ID])

        with self.mod.db.get_conn() as conn:
            row = conn.execute(
                """
                SELECT verified, is_steam_friend
                  FROM steam_links
                 WHERE user_id = ? AND steam_id = ?
                """,
                (TEST_USER_ID, "76561198000000001"),
            ).fetchone()
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(int(row["verified"]), 0)
        self.assertEqual(int(row["is_steam_friend"]), 0)

        pending = self._get_pending_row(TEST_USER_ID)
        self.assertIsNotNone(pending)
        assert pending is not None
        self.assertEqual(int(pending["attempts"]), 0)
        self.assertIsNone(pending["last_error"])

    async def test_sync_all_friends_skips_cleanup_when_other_active_link_remains(self) -> None:
        self._insert_link(
            user_id=TEST_USER_ID,
            steam_id="76561198000000002",
            verified=1,
            is_steam_friend=1,
            primary_account=1,
        )
        self._insert_link(
            user_id=TEST_USER_ID,
            steam_id="76561198000000003",
            verified=1,
            is_steam_friend=1,
            primary_account=0,
        )

        result = await self.mod.sync_all_friends(_FakeSteamTaskClient())

        self.assertTrue(result["success"])
        self.assertEqual(result["cleared_count"], 1)
        self.assertEqual(result["fully_unfollowed_user_ids"], [])
        self.assertIsNone(self._get_pending_row(TEST_USER_ID))

        with self.mod.db.get_conn() as conn:
            missing_row = conn.execute(
                """
                SELECT verified, is_steam_friend
                  FROM steam_links
                 WHERE user_id = ? AND steam_id = ?
                """,
                (TEST_USER_ID, "76561198000000002"),
            ).fetchone()
            remaining_row = conn.execute(
                """
                SELECT verified, is_steam_friend
                  FROM steam_links
                 WHERE user_id = ? AND steam_id = ?
                """,
                (TEST_USER_ID, "76561198000000003"),
            ).fetchone()
        self.assertIsNotNone(missing_row)
        self.assertIsNotNone(remaining_row)
        assert missing_row is not None
        assert remaining_row is not None
        self.assertEqual(int(missing_row["verified"]), 0)
        self.assertEqual(int(missing_row["is_steam_friend"]), 0)
        self.assertEqual(int(remaining_row["verified"]), 1)
        self.assertEqual(int(remaining_row["is_steam_friend"]), 1)

    def test_enqueue_role_cleanup_pending_preserves_existing_attempt_metadata(self) -> None:
        self._insert_link(
            user_id=TEST_USER_ID,
            steam_id="76561198000000004",
            verified=0,
            is_steam_friend=0,
            primary_account=1,
        )
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO steam_role_cleanup_pending(
                    user_id, reason, attempts, last_error, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 1, 1)
                """,
                (TEST_USER_ID, "old reason", 3, "boom"),
            )
            queued = self.mod._enqueue_role_cleanup_pending(
                conn,
                TEST_USER_ID,
                "new reason",
                now=5,
            )

        self.assertTrue(queued)
        pending = self._get_pending_row(TEST_USER_ID)
        self.assertIsNotNone(pending)
        assert pending is not None
        self.assertEqual(str(pending["reason"]), "new reason")
        self.assertEqual(int(pending["attempts"]), 3)
        self.assertEqual(str(pending["last_error"]), "boom")

    async def test_drain_pending_role_cleanup_runs_verified_and_rank_cleanup(self) -> None:
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO steam_role_cleanup_pending(
                    user_id, reason, attempts, last_error, created_at, updated_at
                )
                VALUES (?, ?, 0, NULL, 1, 1)
                """,
                (TEST_USER_ID, "cleanup now"),
            )

        verified_cog = _FakeVerifiedCog()
        rank_cog = _FakeRankCog()
        cog = self.mod.SteamFriendsSync(
            _FakeBot(
                {
                    "SteamVerifiedRole": verified_cog,
                    "DeadlockFriendRank": rank_cog,
                }
            )
        )

        result = await cog._drain_pending_role_cleanup()

        self.assertEqual(
            result,
            {"processed": 1, "completed": 1, "deferred": 0, "failed": 0},
        )
        self.assertEqual(verified_cog.calls, [(TEST_USER_ID, "cleanup now")])
        self.assertEqual(rank_cog.calls, [([TEST_USER_ID], "cleanup now")])
        self.assertIsNone(self._get_pending_row(TEST_USER_ID))

    async def test_drain_pending_role_cleanup_keeps_entry_on_rank_failure(self) -> None:
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO steam_role_cleanup_pending(
                    user_id, reason, attempts, last_error, created_at, updated_at
                )
                VALUES (?, ?, 0, NULL, 1, 1)
                """,
                (TEST_USER_ID, "cleanup fail"),
            )

        cog = self.mod.SteamFriendsSync(
            _FakeBot(
                {
                    "SteamVerifiedRole": _FakeVerifiedCog(),
                    "DeadlockFriendRank": _FakeRankCog(exc=RuntimeError("rank boom")),
                }
            )
        )

        result = await cog._drain_pending_role_cleanup()

        self.assertEqual(
            result,
            {"processed": 1, "completed": 0, "deferred": 0, "failed": 1},
        )
        pending = self._get_pending_row(TEST_USER_ID)
        self.assertIsNotNone(pending)
        assert pending is not None
        self.assertEqual(int(pending["attempts"]), 1)
        self.assertIn("rank_cleanup_failed", str(pending["last_error"]))
