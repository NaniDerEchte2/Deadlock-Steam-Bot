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
    timed_out: bool = False
    task_id: int = 1


class _FakeSteamTaskClient:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    async def run(self, *_: Any, **__: Any) -> _FakeOutcome:
        return _FakeOutcome(ok=False, error="not configured")


class _FakeBot:
    def __init__(
        self,
        *,
        guilds: dict[int, Any] | None = None,
        users: dict[int, Any] | None = None,
    ) -> None:
        self._guilds = dict(guilds or {})
        self._users = dict(users or {})

    def add_view(self, *_: Any, **__: Any) -> None:
        return None

    async def wait_until_ready(self) -> None:
        return None

    def get_guild(self, guild_id: int) -> Any | None:
        return self._guilds.get(int(guild_id))

    def get_user(self, user_id: int) -> Any | None:
        return self._users.get(int(user_id))

    async def fetch_user(self, user_id: int) -> Any | None:
        return self._users.get(int(user_id))

    def get_channel(self, _channel_id: int) -> Any | None:
        return None

    async def fetch_channel(self, _channel_id: int) -> Any | None:
        return None

    def get_cog(self, _name: str) -> Any | None:
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
        self.bot = False

    async def send(self, *_: Any, **__: Any) -> None:
        return None


class _FakeInteraction:
    def __init__(self, user: _FakeUser, channel: Any | None = None) -> None:
        self.user = user
        self.channel = channel
        self.response = _FakeResponse()


class _FakeChannel:
    def __init__(self, channel_id: int) -> None:
        self.id = channel_id


class _FakeGuild:
    def __init__(self, guild_id: int, members: dict[int, _FakeUser]) -> None:
        self.id = guild_id
        self._members = dict(members)
        self.ban_calls: list[int] = []

    def get_member(self, user_id: int) -> _FakeUser | None:
        return self._members.get(int(user_id))

    async def fetch_member(self, user_id: int) -> _FakeUser:
        member = self._members.get(int(user_id))
        if member is None:
            raise LookupError(f"user_id={user_id} not found")
        return member

    async def ban(self, target: Any, **__: Any) -> None:
        target_id = getattr(target, "id", target)
        self.ban_calls.append(int(target_id))


class _FakeMember(_FakeUser):
    def __init__(self, discord_id: int, guild: _FakeGuild, name: str = "Tester") -> None:
        super().__init__(discord_id, name=name)
        self.guild = guild


class _FakeDiscordApiResponse:
    status = 404
    reason = "Not Found"
    headers: dict[str, str] = {}


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
                    scheduled_invite_at INTEGER,
                    dispatch_attempts INTEGER NOT NULL DEFAULT 0,
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS steam_links (
                    user_id INTEGER NOT NULL,
                    steam_id TEXT NOT NULL,
                    verified INTEGER DEFAULT 0,
                    primary_account INTEGER DEFAULT 0,
                    updated_at INTEGER DEFAULT (strftime('%s','now'))
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

    async def test_community_flow_queues_after_confirmed_friendship(self) -> None:
        discord_id = 123_123_123
        ticket_channel_id = 3_333
        steam_id64 = "76561198000000042"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO beta_invite_tickets(discord_id, guild_id, channel_id, status)
                VALUES(?, 1, ?, ?)
                """,
                (discord_id, ticket_channel_id, self.mod.BETA_TICKET_STATUS_OPEN),
            )

        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        waiting_record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_WAITING,
            friend_requested_at=1,
            last_error=None,
        )
        assert waiting_record is not None

        flow = self.mod.BetaInviteFlow(_FakeBot())
        task_calls: list[str] = []
        captured_messages: list[str] = []
        log_messages: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            if task_type == "AUTH_CHECK_FRIENDSHIP":
                return _FakeOutcome(
                    ok=True,
                    status="DONE",
                    result={
                        "data": {
                            "friend": True,
                            "relationship_name": "Friend",
                            "account_id": account_id,
                        }
                    },
                )
            raise AssertionError(f"Unexpected task {task_type}")

        async def _noop(*_args: Any, **_kwargs: Any) -> None:
            return None

        async def _capture_edit(_interaction: Any, *, content: str | None = None, **__: Any) -> None:
            if content:
                captured_messages.append(content)

        async def _notify(message: str) -> None:
            log_messages.append(message)

        flow.tasks = types.SimpleNamespace(run=_run_task)
        flow._response_edit_message = _noop
        flow._edit_original_response = _capture_edit
        flow._followup_send = _noop
        flow._await_animation_task = _noop
        flow._animate_processing = _noop
        flow._sync_verified_on_friendship = _noop
        flow._notify_log_channel = _notify

        interaction = _FakeInteraction(
            _FakeUser(discord_id, name="CommunityUser"),
            channel=_FakeChannel(ticket_channel_id),
        )
        now_before = int(self.mod.time.time())
        await flow.handle_confirmation(interaction, waiting_record.id)

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                """
                SELECT status, scheduled_invite_at, dispatch_attempts, friend_confirmed_at
                  FROM steam_beta_invites
                 WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()
            ticket_row = conn.execute(
                "SELECT status FROM beta_invite_tickets WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, ["AUTH_CHECK_FRIENDSHIP"])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_QUEUED_COMMUNITY_DELAY)
        self.assertEqual(int(invite_row["dispatch_attempts"]), 0)
        self.assertGreaterEqual(int(invite_row["friend_confirmed_at"]), now_before)
        self.assertGreaterEqual(
            int(invite_row["scheduled_invite_at"]),
            now_before + self.mod.COMMUNITY_INVITE_DELAY_SECONDS - 5,
        )
        self.assertLessEqual(
            int(invite_row["scheduled_invite_at"]),
            now_before + self.mod.COMMUNITY_INVITE_DELAY_SECONDS + 5,
        )
        self.assertIsNotNone(ticket_row)
        self.assertEqual(ticket_row["status"], self.mod.BETA_TICKET_STATUS_COMPLETED)
        self.assertTrue(
            any("15 Minuten bis 2 Stunden" in message for message in captured_messages),
            "Community-Queue soll sofort die neue Erfolgscopy zeigen",
        )
        self.assertTrue(
            any("BetaInvite queue" in message for message in log_messages),
            "Queueing muss einen Admin-Logeintrag erzeugen",
        )

    async def test_dispatcher_claims_due_community_invite_exclusively_across_instances(self) -> None:
        discord_id = 456_456_456
        steam_id64 = "76561198000000043"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        member = _FakeUser(discord_id, name="QueuedUser")
        guild = _FakeGuild(42, members={discord_id: member})
        flow = self.mod.BetaInviteFlow(_FakeBot(users={discord_id: member}))
        competing_flow = self.mod.BetaInviteFlow(_FakeBot(users={discord_id: member}))
        flow._main_guild = lambda: guild
        competing_flow._main_guild = lambda: guild

        task_calls: list[str] = []
        status_during_task: list[str] = []
        log_messages: list[str] = []
        send_started = asyncio.Event()
        allow_send_finish = asyncio.Event()

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            with self.mod.db.get_conn() as conn:
                row = conn.execute(
                    "SELECT status FROM steam_beta_invites WHERE discord_id = ?",
                    (discord_id,),
                ).fetchone()
            status_during_task.append(str(row["status"]))
            send_started.set()
            await allow_send_finish.wait()
            return _FakeOutcome(
                ok=True,
                status="DONE",
                result={"data": {"response": {"ok": True}}},
            )

        async def _notify(message: str) -> None:
            log_messages.append(message)

        flow.tasks = types.SimpleNamespace(run=_run_task)
        flow._notify_log_channel = _notify
        competing_flow.tasks = types.SimpleNamespace(run=_run_task)
        competing_flow._notify_log_channel = _notify

        first_dispatch_task = asyncio.create_task(flow._dispatch_due_community_invites())
        await asyncio.wait_for(send_started.wait(), timeout=2.0)
        await competing_flow._dispatch_due_community_invites()
        allow_send_finish.set()
        await first_dispatch_task
        await competing_flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                """
                SELECT status, dispatch_attempts, invite_sent_at
                  FROM steam_beta_invites
                 WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, ["AUTH_SEND_PLAYTEST_INVITE"])
        self.assertEqual(status_during_task, [self.mod.STATUS_DISPATCHING_COMMUNITY_DELAY])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_INVITE_SENT)
        self.assertEqual(int(invite_row["dispatch_attempts"]), 1)
        self.assertIsNotNone(invite_row["invite_sent_at"])
        self.assertTrue(
            any("BetaInvite dispatch_success" in message for message in log_messages),
            "Dispatcher-Erfolg muss geloggt werden",
        )

    async def test_dispatcher_reclaims_stale_dispatch_claim_after_restart(self) -> None:
        discord_id = 456_456_999
        steam_id64 = "76561198000000053"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_DISPATCHING_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=1,
            last_error="worker crashed",
        )
        assert record is not None

        stale_updated_at = (
            int(self.mod.time.time()) - self.mod.COMMUNITY_DISPATCH_CLAIM_TIMEOUT_SECONDS - 5
        )
        with self.mod.db.get_conn() as conn:
            conn.execute(
                "UPDATE steam_beta_invites SET updated_at = ? WHERE id = ?",
                (stale_updated_at, record.id),
            )

        member = _FakeUser(discord_id, name="RecoveredUser")
        guild = _FakeGuild(42, members={discord_id: member})
        flow = self.mod.BetaInviteFlow(_FakeBot(users={discord_id: member}))
        flow._main_guild = lambda: guild

        task_calls: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            return _FakeOutcome(
                ok=True,
                status="DONE",
                result={"data": {"response": {"ok": True}}},
            )

        flow.tasks = types.SimpleNamespace(run=_run_task)

        await flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                """
                SELECT status, dispatch_attempts, last_error
                  FROM steam_beta_invites
                 WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, ["AUTH_SEND_PLAYTEST_INVITE"])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_INVITE_SENT)
        self.assertEqual(int(invite_row["dispatch_attempts"]), 2)
        self.assertIsNone(invite_row["last_error"])

    async def test_dispatcher_marks_error_without_endless_resend(self) -> None:
        discord_id = 789_789_789
        steam_id64 = "76561198000000044"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        member = _FakeUser(discord_id, name="FailUser")
        guild = _FakeGuild(42, members={discord_id: member})
        flow = self.mod.BetaInviteFlow(_FakeBot(users={discord_id: member}))
        flow._main_guild = lambda: guild

        task_calls: list[str] = []
        log_messages: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            return _FakeOutcome(
                ok=False,
                status="FAILED",
                error="bridge failed",
                result={"error": "bridge failed"},
            )

        async def _notify(message: str) -> None:
            log_messages.append(message)

        flow.tasks = types.SimpleNamespace(run=_run_task)
        flow._notify_log_channel = _notify

        await flow._dispatch_due_community_invites()
        await flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                """
                SELECT status, dispatch_attempts, last_error
                  FROM steam_beta_invites
                 WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, ["AUTH_SEND_PLAYTEST_INVITE"])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_ERROR)
        self.assertEqual(int(invite_row["dispatch_attempts"]), 1)
        self.assertIn("bridge failed", str(invite_row["last_error"]))
        self.assertTrue(
            any("BetaInvite dispatch_error" in message for message in log_messages),
            "Dispatcher-Fehler muss geloggt werden",
        )

    async def test_dispatcher_cancels_when_member_events_show_leave_even_if_member_present(self) -> None:
        discord_id = 741_852_963
        steam_id64 = "76561198000000046"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=100,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS member_events(
                    user_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO member_events(user_id, guild_id, event_type, timestamp)
                VALUES(?, ?, 'leave', ?)
                """,
                (discord_id, 42, int(self.mod.time.time())),
            )

        member = _FakeUser(discord_id, name="RejoinedUser")
        guild = _FakeGuild(42, members={discord_id: member})
        flow = self.mod.BetaInviteFlow(_FakeBot(users={discord_id: member}))
        flow._main_guild = lambda: guild

        task_calls: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            return _FakeOutcome(ok=True, status="DONE", result={"ok": True})

        flow.tasks = types.SimpleNamespace(run=_run_task)

        await flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                "SELECT status, last_error FROM steam_beta_invites WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, [])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_CANCELLED)
        self.assertIn("member_events", str(invite_row["last_error"]))
        self.assertEqual(guild.ban_calls, [discord_id])

    async def test_dispatcher_defers_on_membership_lookup_error(self) -> None:
        discord_id = 654_987_321
        steam_id64 = "76561198000000047"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        guild = _FakeGuild(42, members={})

        async def _fail_fetch_member(_user_id: int) -> _FakeUser:
            raise RuntimeError("discord down")

        guild.fetch_member = _fail_fetch_member  # type: ignore[method-assign]

        flow = self.mod.BetaInviteFlow(_FakeBot())
        flow._main_guild = lambda: guild
        task_calls: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            return _FakeOutcome(ok=True, status="DONE", result={"ok": True})

        flow.tasks = types.SimpleNamespace(run=_run_task)

        await flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                "SELECT status, dispatch_attempts FROM steam_beta_invites WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, [])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_QUEUED_COMMUNITY_DELAY)
        self.assertEqual(int(invite_row["dispatch_attempts"]), 0)
        self.assertEqual(guild.ban_calls, [])

    async def test_dispatcher_cancels_on_confirmed_discord_not_found(self) -> None:
        discord_id = 654_987_322
        steam_id64 = "76561198000000054"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        guild = _FakeGuild(42, members={})

        async def _missing_fetch_member(_user_id: int) -> _FakeUser:
            raise self.mod.discord.NotFound(_FakeDiscordApiResponse(), "missing")

        guild.fetch_member = _missing_fetch_member  # type: ignore[method-assign]

        flow = self.mod.BetaInviteFlow(_FakeBot())
        flow._main_guild = lambda: guild
        task_calls: list[str] = []

        async def _run_task(task_type: str, *_: Any, **__: Any) -> _FakeOutcome:
            task_calls.append(task_type)
            return _FakeOutcome(ok=True, status="DONE", result={"ok": True})

        flow.tasks = types.SimpleNamespace(run=_run_task)

        await flow._dispatch_due_community_invites()

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                "SELECT status, last_error FROM steam_beta_invites WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()

        self.assertEqual(task_calls, [])
        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_CANCELLED)
        self.assertIn("nicht mehr im Main-Guild", str(invite_row["last_error"]))
        self.assertEqual(guild.ban_calls, [discord_id])

    async def test_leave_before_dispatch_cancels_queue_and_bans_user(self) -> None:
        discord_id = 888_111_222
        steam_id64 = "76561198000000045"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_QUEUED_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) + 3600,
            dispatch_attempts=0,
            last_error=None,
        )
        assert record is not None

        guild = _FakeGuild(42, members={})
        member = _FakeMember(discord_id, guild, name="LeavingUser")
        flow = self.mod.BetaInviteFlow(_FakeBot())
        log_messages: list[str] = []

        async def _notify(message: str) -> None:
            log_messages.append(message)

        flow._notify_log_channel = _notify

        await flow.on_member_remove(member)

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                """
                SELECT status, last_error
                  FROM steam_beta_invites
                 WHERE discord_id = ?
                """,
                (discord_id,),
            ).fetchone()

        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_CANCELLED)
        self.assertIn("verlassen", str(invite_row["last_error"]).lower())
        self.assertEqual(guild.ban_calls, [discord_id])
        self.assertTrue(
            any("BetaInvite cancel" in message for message in log_messages),
            "Leave-Cancel muss geloggt werden",
        )

    async def test_leave_during_dispatch_claim_cancels_and_bans_user(self) -> None:
        discord_id = 888_111_333
        steam_id64 = "76561198000000055"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        self.mod._persist_intent_once(discord_id, self.mod.INTENT_COMMUNITY)
        record = self.mod._create_or_reset_invite(discord_id, steam_id64, account_id)
        record = self.mod._update_invite(
            record.id,
            status=self.mod.STATUS_DISPATCHING_COMMUNITY_DELAY,
            friend_confirmed_at=1,
            scheduled_invite_at=int(self.mod.time.time()) - 60,
            dispatch_attempts=1,
            last_error=None,
        )
        assert record is not None

        guild = _FakeGuild(42, members={})
        member = _FakeMember(discord_id, guild, name="LeavingDispatchUser")
        flow = self.mod.BetaInviteFlow(_FakeBot())

        await flow.on_member_remove(member)

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                "SELECT status, last_error FROM steam_beta_invites WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()

        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_CANCELLED)
        self.assertIn("verlassen", str(invite_row["last_error"]).lower())
        self.assertEqual(guild.ban_calls, [discord_id])

    async def test_ticket_manual_friend_flow_restores_persistent_ticket_step(self) -> None:
        discord_id = 987_654_321
        ticket_channel_id = 4_242
        steam_id64 = "76561198000000001"
        account_id = self.mod.steam64_to_account_id(steam_id64)

        with self.mod.db.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO steam_links(user_id, steam_id, verified, primary_account, updated_at)
                VALUES(?, ?, 1, 1, strftime('%s','now'))
                """,
                (discord_id, steam_id64),
            )
            conn.execute(
                """
                INSERT INTO beta_invite_tickets(discord_id, guild_id, channel_id, status)
                VALUES(?, 1, ?, ?)
                """,
                (discord_id, ticket_channel_id, self.mod.BETA_TICKET_STATUS_OPEN),
            )

        flow = self.mod.BetaInviteFlow(_FakeBot())
        flow._trace_user_action = lambda *_args, **_kwargs: None

        async def _run_task(*_: Any, **__: Any) -> _FakeOutcome:
            return _FakeOutcome(
                ok=True,
                status="DONE",
                result={
                    "data": {
                        "friend": False,
                        "relationship_name": "RequestRecipient",
                        "friend_source": "manual",
                        "account_id": account_id,
                    }
                },
            )

        flow.tasks = types.SimpleNamespace(run=_run_task)

        edit_calls: list[dict[str, Any]] = []
        followup_calls: list[dict[str, Any]] = []

        async def _capture_edit(_interaction: Any, *, content: str | None = None, view: Any | None = None) -> None:
            edit_calls.append({"content": content, "view": view})

        async def _capture_followup(
            _interaction: Any,
            content: str,
            *,
            ephemeral: bool = False,
            view: Any | None = None,
        ) -> None:
            followup_calls.append(
                {"content": content, "view": view, "ephemeral": ephemeral}
            )

        flow._edit_original_response = _capture_edit
        flow._followup_send = _capture_followup

        interaction = _FakeInteraction(
            _FakeUser(discord_id),
            channel=_FakeChannel(ticket_channel_id),
        )
        await flow._process_invite_request(interaction)

        self.assertEqual(
            len(edit_calls),
            1,
            "Ticket-Flow soll den bestehenden Ticket-Schritt aktualisieren statt einen neuen Followup-Step zu erzeugen",
        )
        self.assertEqual(len(followup_calls), 0, "Im Ticket-Flow darf kein kurzlebiger Followup-Button mehr entstehen")
        self.assertIsInstance(edit_calls[0]["view"], self.mod.BetaInviteFriendHintView)

        with self.mod.db.get_conn() as conn:
            invite_row = conn.execute(
                "SELECT status, last_error FROM steam_beta_invites WHERE discord_id = ?",
                (discord_id,),
            ).fetchone()
            friend_row = conn.execute(
                "SELECT status FROM steam_friend_requests WHERE steam_id = ?",
                (steam_id64,),
            ).fetchone()

        self.assertIsNotNone(invite_row)
        self.assertEqual(invite_row["status"], self.mod.STATUS_WAITING)
        self.assertEqual(invite_row["last_error"], "Warte auf manuelle Steam-Freundschaft.")
        self.assertIsNotNone(friend_row)
        self.assertEqual(friend_row["status"], "manual")

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

    def test_resolve_kofi_verification_token_uses_windows_vault_fallback(self) -> None:
        if self.mod.os.name != "nt":
            self.skipTest("Windows-only vault fallback")

        env_before = os.environ.pop("KOFI_VERIFICATION_TOKEN", None)
        keyring_before = sys.modules.get("keyring")

        def _fake_get_password(service: str, user: str) -> str | None:
            if (service, user) == ("DeadlockBot", "KOFI_VERIFICATION_TOKEN"):
                return "vault-token-123"
            return None

        fake_keyring = types.SimpleNamespace(get_password=_fake_get_password)
        sys.modules["keyring"] = fake_keyring
        try:
            token = self.mod._resolve_kofi_verification_token()
        finally:
            if env_before is None:
                os.environ.pop("KOFI_VERIFICATION_TOKEN", None)
            else:
                os.environ["KOFI_VERIFICATION_TOKEN"] = env_before

            if keyring_before is None:
                sys.modules.pop("keyring", None)
            else:
                sys.modules["keyring"] = keyring_before

        self.assertEqual(token, "vault-token-123")

    def test_kofi_health_access_filter_suppresses_healthcheck_requests(self) -> None:
        record = self.mod.logging.makeLogRecord(
            {
                "name": "uvicorn.access",
                "levelno": self.mod.logging.INFO,
                "msg": '%s - "%s %s HTTP/%s" %d',
                "args": ("127.0.0.1:12345", "GET", "/kofi-health", "1.1", 200),
            }
        )

        self.assertTrue(self.mod._is_kofi_health_access_log(record))
        self.assertFalse(self.mod._DropKofiHealthAccessLogFilter().filter(record))

    def test_kofi_health_access_filter_keeps_regular_requests(self) -> None:
        record = self.mod.logging.makeLogRecord(
            {
                "name": "uvicorn.access",
                "levelno": self.mod.logging.INFO,
                "msg": '%s - "%s %s HTTP/%s" %d',
                "args": ("127.0.0.1:12345", "POST", "/kofi-webhook", "1.1", 200),
            }
        )

        self.assertFalse(self.mod._is_kofi_health_access_log(record))
        self.assertTrue(self.mod._DropKofiHealthAccessLogFilter().filter(record))


if __name__ == "__main__":
    unittest.main(verbosity=2)
