from __future__ import annotations

import importlib
import json
import sys
import time
import types
import unittest
from dataclasses import dataclass
from importlib.util import find_spec
from typing import Any


class _DbShim:
    def __init__(self) -> None:
        self.bridge_state_row: dict[str, Any] | None = self._healthy_bridge_state_row()

    async def execute_async(self, *_: Any, **__: Any) -> None:
        return None

    async def query_one_async(self, *_: Any, **__: Any) -> dict[str, Any] | None:
        row = self.bridge_state_row
        return dict(row) if isinstance(row, dict) else row

    async def query_all_async(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return []

    async def executemany_async(self, *_: Any, **__: Any) -> None:
        return None

    @staticmethod
    def _healthy_bridge_state_row() -> dict[str, Any]:
        return {
            "heartbeat": int(time.time()),
            "payload": json.dumps({"runtime": {"logged_on": True, "logging_in": False}}),
        }


@dataclass(slots=True)
class _FakeOutcome:
    task_id: int
    status: str
    result: dict[str, Any] | None
    error: str | None = None
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.status.upper() == "DONE" and not self.timed_out


class _ScriptedSteamTaskClient:
    scripted_wait_outcomes: list[_FakeOutcome] = []
    last_instance: "_ScriptedSteamTaskClient | None" = None

    def __init__(self, *_: Any, **__: Any) -> None:
        self.enqueue_calls: list[tuple[str, dict[str, Any] | None]] = []
        self.wait_calls: list[tuple[int, float | None]] = []
        self._wait_outcomes = list(type(self).scripted_wait_outcomes)
        self._next_task_id = 1
        type(self).last_instance = self

    def enqueue(self, task_type: str, payload: dict[str, Any] | None = None) -> int:
        self.enqueue_calls.append((task_type, payload))
        return self._next_task_id

    async def wait(self, task_id: int, *, timeout: float | None = None) -> _FakeOutcome:
        self.wait_calls.append((task_id, timeout))
        if not self._wait_outcomes:
            raise AssertionError("No scripted wait outcome configured")
        return self._wait_outcomes.pop(0)

    async def run(self, *_: Any, **__: Any) -> _FakeOutcome:
        raise AssertionError("DeadlockFriendRank should enqueue and wait explicitly in this test")


class _FakeBot:
    guilds: list[Any] = []


@unittest.skipUnless(find_spec("discord") is not None, "discord.py is required for this integration test")
class DeadlockFriendRankTaskWaitTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._original_modules: dict[str, Any] = {}
        for name in (
            "service",
            "cogs.steam.steam_master",
            "cogs.steam.deadlock_friend_rank",
        ):
            self._original_modules[name] = sys.modules.get(name)

        service_pkg = types.ModuleType("service")
        service_pkg.db = _DbShim()
        sys.modules["service"] = service_pkg

        steam_master_mod = types.ModuleType("cogs.steam.steam_master")
        steam_master_mod.SteamTaskClient = _ScriptedSteamTaskClient
        steam_master_mod.SteamTaskOutcome = _FakeOutcome
        sys.modules["cogs.steam.steam_master"] = steam_master_mod

        sys.modules.pop("cogs.steam.deadlock_friend_rank", None)
        self.mod = importlib.import_module("cogs.steam.deadlock_friend_rank")

    def tearDown(self) -> None:
        sys.modules.pop("cogs.steam.deadlock_friend_rank", None)
        for name, original in self._original_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original
        _ScriptedSteamTaskClient.scripted_wait_outcomes = []
        _ScriptedSteamTaskClient.last_instance = None

    def _set_bridge_state(
        self,
        *,
        heartbeat: int | None = None,
        runtime: dict[str, Any] | None = None,
        payload: Any | None = None,
    ) -> None:
        db = sys.modules["service"].db
        if payload is None:
            payload = {"runtime": runtime or {"logged_on": True, "logging_in": False}}
        db.bridge_state_row = {
            "heartbeat": int(time.time()) if heartbeat is None else int(heartbeat),
            "payload": json.dumps(payload) if not isinstance(payload, str) else payload,
        }

    async def test_fetch_bot_friend_ids_extends_pending_timeout_before_success(self) -> None:
        _ScriptedSteamTaskClient.scripted_wait_outcomes = [
            _FakeOutcome(task_id=1, status="PENDING", result=None, timed_out=True),
            _FakeOutcome(
                task_id=1,
                status="DONE",
                result={
                    "data": {
                        "friends": [
                            {"steam_id64": "76561198000000000"},
                            {"steam_id64": "76561198000000001"},
                            {"steam_id64": "invalid"},
                        ]
                    }
                },
            ),
        ]

        cog = self.mod.DeadlockFriendRank(_FakeBot())
        friend_ids = await cog._fetch_bot_friend_ids()

        self.assertEqual(friend_ids, {"76561198000000000", "76561198000000001"})
        client = _ScriptedSteamTaskClient.last_instance
        self.assertIsNotNone(client)
        assert client is not None
        self.assertEqual(client.enqueue_calls, [("AUTH_GET_FRIENDS_LIST", None)])
        self.assertEqual(
            client.wait_calls,
            [
                (1, self.mod.FRIEND_LIST_TASK_TIMEOUT),
                (1, self.mod.FRIEND_LIST_TASK_GRACE_TIMEOUT),
            ],
        )

    async def test_run_friend_rank_sync_skips_when_bridge_heartbeat_is_stale(self) -> None:
        stale_heartbeat = int(time.time()) - self.mod.STEAM_BRIDGE_HEARTBEAT_STALE_SECONDS - 5
        self._set_bridge_state(
            heartbeat=stale_heartbeat,
            runtime={"logged_on": True, "logging_in": False},
        )

        cog = self.mod.DeadlockFriendRank(_FakeBot())

        with self.assertRaisesRegex(
            self.mod.SteamBridgeUnavailableError,
            r"Steam bridge heartbeat is stale",
        ):
            await cog._run_friend_rank_sync(trigger="loop")

        client = _ScriptedSteamTaskClient.last_instance
        self.assertIsNotNone(client)
        assert client is not None
        self.assertEqual(client.enqueue_calls, [])
        self.assertEqual(client.wait_calls, [])

    async def test_fetch_bot_friend_ids_raises_after_grace_timeout(self) -> None:
        _ScriptedSteamTaskClient.scripted_wait_outcomes = [
            _FakeOutcome(task_id=1, status="RUNNING", result=None, timed_out=True),
            _FakeOutcome(task_id=1, status="RUNNING", result=None, timed_out=True),
        ]

        cog = self.mod.DeadlockFriendRank(_FakeBot())

        with self.assertRaisesRegex(
            RuntimeError,
            r"AUTH_GET_FRIENDS_LIST timed out after 120\.0s \(Task #1, status=RUNNING\)",
        ):
            await cog._fetch_bot_friend_ids()

        client = _ScriptedSteamTaskClient.last_instance
        self.assertIsNotNone(client)
        assert client is not None
        self.assertEqual(
            client.wait_calls,
            [
                (1, self.mod.FRIEND_LIST_TASK_TIMEOUT),
                (1, self.mod.FRIEND_LIST_TASK_GRACE_TIMEOUT),
            ],
        )
