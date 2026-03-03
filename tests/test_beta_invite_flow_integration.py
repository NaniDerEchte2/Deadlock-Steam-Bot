from __future__ import annotations

import asyncio
import importlib
import os
import sqlite3
import sys
import tempfile
import types
import unittest
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path
from typing import Any


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


@dataclass(slots=True)
class _FakeOutcome:
    ok: bool
    result: dict[str, Any] | None = None
    status: str = "ok"
    error: str | None = None


class _FakeSteamTaskClient:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    async def run(self, *_: Any, **__: Any) -> _FakeOutcome:
        return _FakeOutcome(ok=False, error="not configured")


class _FakeBot:
    def add_view(self, *_: Any, **__: Any) -> None:
        return None


class _FakeResponse:
    def is_done(self) -> bool:
        return True


class _FakeUser:
    def __init__(self, discord_id: int, name: str = "Tester") -> None:
        self.id = discord_id
        self.name = name
        self.global_name = name
        self.display_name = name
        self.discriminator = "0"
        self.mention = f"<@{discord_id}>"

    async def send(self, *_: Any, **__: Any) -> None:
        return None


class _FakeInteraction:
    def __init__(self, user: _FakeUser) -> None:
        self.user = user
        self.response = _FakeResponse()


class _FakeGuild:
    def __init__(self, guild_id: int, members: dict[int, _FakeUser]) -> None:
        self.id = guild_id
        self._members = dict(members)

    def get_member(self, user_id: int) -> _FakeUser | None:
        return self._members.get(int(user_id))

    async def fetch_member(self, user_id: int) -> _FakeUser:
        member = self._members.get(int(user_id))
        if member is None:
            raise LookupError(f"user_id={user_id} not found")
        return member


@unittest.skipUnless(find_spec("discord") is not None, "discord.py is required for this integration test")
class BetaInviteFlowIntegrationTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._temp_db_file = tempfile.NamedTemporaryFile(prefix="beta_invite_test_", suffix=".sqlite3", delete=False)
        self._temp_db_file.close()
        self._db_path = Path(self._temp_db_file.name)

        self._original_modules: dict[str, Any] = {}
        for name in (
            "service",
            "cogs.welcome_dm",
            "cogs.welcome_dm.base",
            "cogs.steam.steam_master",
            "cogs.steam.beta_invite",
        ):
            self._original_modules[name] = sys.modules.get(name)

        service_pkg = types.ModuleType("service")
        service_pkg.db = _DbShim(self._db_path)
        sys.modules["service"] = service_pkg

        welcome_base_mod = types.ModuleType("cogs.welcome_dm.base")
        welcome_pkg = types.ModuleType("cogs.welcome_dm")
        welcome_pkg.base = welcome_base_mod
        sys.modules["cogs.welcome_dm"] = welcome_pkg
        sys.modules["cogs.welcome_dm.base"] = welcome_base_mod

        steam_master_mod = types.ModuleType("cogs.steam.steam_master")
        steam_master_mod.SteamTaskClient = _FakeSteamTaskClient
        sys.modules["cogs.steam.steam_master"] = steam_master_mod

        sys.modules.pop("cogs.steam.beta_invite", None)
        self.mod = importlib.import_module("cogs.steam.beta_invite")
        self._create_minimal_tables()
        self.mod._ensure_pending_payments_table()

    def tearDown(self) -> None:
        sys.modules.pop("cogs.steam.beta_invite", None)
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
                CREATE TABLE IF NOT EXISTS beta_invite_intent (
                    discord_id INTEGER PRIMARY KEY,
                    intent TEXT NOT NULL,
                    decided_at INTEGER NOT NULL,
                    locked INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steam_beta_invites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_id INTEGER NOT NULL UNIQUE,
                    steam_id64 TEXT NOT NULL,
                    account_id INTEGER,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    friend_requested_at INTEGER,
                    friend_confirmed_at INTEGER,
                    invite_sent_at INTEGER,
                    last_notified_at INTEGER,
                    created_at INTEGER DEFAULT (strftime('%s','now')),
                    updated_at INTEGER DEFAULT (strftime('%s','now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS beta_invite_tickets (
                    discord_id INTEGER PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                    updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                    closed_at INTEGER
                )
                """
            )

    async def test_invite_only_flow_end_to_end(self) -> None:
        discord_id = 123_456_789
        steam_id64 = "76561198000000000"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        intent = self.mod._persist_intent_once(discord_id, self.mod.INTENT_INVITE_ONLY)
        self.assertEqual(intent.intent, self.mod.INTENT_INVITE_ONLY)
        self.assertTrue(intent.locked)

        first_token = self.mod._register_pending_payment(discord_id, "Tester")
        self.assertTrue(first_token.startswith("DDL-"))
        self.assertTrue(self.mod._mark_payment_confirmed(discord_id, "Tester"))
        self.assertTrue(self.mod._consume_payment_for_invite(discord_id))
        self.assertFalse(self.mod._consume_payment_for_invite(discord_id))

        second_token = self.mod._register_pending_payment(discord_id, "Tester")
        self.assertNotEqual(first_token, second_token, "Nach Consume muss ein neuer Token erzeugt werden")
        self.assertTrue(self.mod._mark_payment_confirmed(discord_id, "Tester"))
        self.assertTrue(self.mod._consume_payment_for_invite(discord_id))

        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        self.assertEqual(record.status, self.mod.STATUS_PENDING)

        self.mod._queue_manual_friend_accept(steam_id64)
        with self.mod.db.get_conn() as conn:
            row = conn.execute(
                "SELECT status FROM steam_friend_requests WHERE steam_id = ?",
                (steam_id64,),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "manual")

        waiting_record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_WAITING,
            friend_requested_at=1,
            last_error=None,
        )
        self.assertIsNotNone(waiting_record)
        assert waiting_record is not None

        flow = self.mod.BetaInviteFlow(_FakeBot())

        async def _run_task(*_: Any, **__: Any) -> _FakeOutcome:
            return _FakeOutcome(
                ok=True,
                result={
                    "data": {
                        "friend": True,
                        "relationship_name": "Friend",
                        "friend_source": "event",
                        "account_id": account_id,
                    }
                },
            )

        flow.tasks = types.SimpleNamespace(run=_run_task)
        flow._trace_user_action = lambda *_args, **_kwargs: None

        async def _noop(*_args: Any, **_kwargs: Any) -> None:
            return None

        flow._response_send_message = _noop
        flow._response_edit_message = _noop
        flow._edit_original_response = _noop
        flow._followup_send = _noop
        flow._response_defer = _noop
        flow._await_animation_task = _noop
        flow._animate_processing = _noop
        flow._sync_verified_on_friendship = _noop
        flow._mark_ticket_completed = lambda *_args, **_kwargs: None

        invite_send_calls: list[int] = []

        async def _send_invite_after_friend(*_args: Any, **_kwargs: Any) -> bool:
            invite_send_calls.append(1)
            return True

        flow._send_invite_after_friend = _send_invite_after_friend

        interaction = _FakeInteraction(_FakeUser(discord_id))
        await flow.handle_confirmation(interaction, waiting_record.id)
        self.assertEqual(len(invite_send_calls), 1, "Confirm-Step muss Invite-Send auslösen")

    async def test_kofi_webhook_does_not_auto_match_username(self) -> None:
        discord_id = 555_111
        self.mod._register_pending_payment(discord_id, "Alice")

        flow = self.mod.BetaInviteFlow(_FakeBot())
        guild = _FakeGuild(42, members={})
        flow._main_guild = lambda: guild
        notifications: list[str] = []

        async def _notify(message: str) -> None:
            notifications.append(message)

        flow._notify_log_channel = _notify

        result = await flow.handle_kofi_webhook({"data": {"message": "@Alice"}})
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "token_not_found")
        self.assertTrue(
            any("Keine Auto-Zuordnung per Username mehr aktiv" in msg for msg in notifications),
            "Webhook muss klar auf manuellen Review statt Username-Auto-Match hinweisen",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
