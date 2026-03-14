from __future__ import annotations

import importlib
import sys
import types
import unittest
from importlib.util import find_spec
from unittest import mock


class _DbStub:
    def execute(self, *_: object, **__: object) -> None:
        return None


class _Settings(types.SimpleNamespace):
    def __getattr__(self, _name: str) -> object:
        return None


class _FakeSteamTaskClient:
    def __init__(self, *_: object, **__: object) -> None:
        pass


@unittest.skipUnless(
    find_spec("discord") is not None and find_spec("aiohttp") is not None,
    "discord.py and aiohttp are required for this integration test",
)
class SteamLinkLaunchTokenTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original_modules: dict[str, object] = {}
        for name in (
            "service",
            "service.config",
            "cogs.steam.steam_master",
            "cogs.steam.steam_link_oauth",
        ):
            self._original_modules[name] = sys.modules.get(name)

        service_pkg = types.ModuleType("service")
        service_pkg.db = _DbStub()
        sys.modules["service"] = service_pkg

        config_mod = types.ModuleType("service.config")
        config_mod.settings = _Settings(
            public_base_url="https://link.example.test",
            steam_return_path="/steam/return",
            http_host="127.0.0.1",
            http_port=8888,
            discord_oauth_client_secret="discord-secret",
            steam_api_key=None,
            link_cover_image="",
            link_cover_label="link.example.test",
            link_button_label="Via Discord verknüpfen",
            steam_button_label="Direkt bei Steam anmelden",
            friend_code_linking_enabled=True,
            steam_login_launch_ttl_sec=900,
        )
        service_pkg.config = config_mod
        sys.modules["service.config"] = config_mod

        steam_master_mod = types.ModuleType("cogs.steam.steam_master")
        steam_master_mod.SteamTaskClient = _FakeSteamTaskClient
        sys.modules["cogs.steam.steam_master"] = steam_master_mod

        sys.modules.pop("cogs.steam.steam_link_oauth", None)
        self.mod = importlib.import_module("cogs.steam.steam_link_oauth")

    def tearDown(self) -> None:
        sys.modules.pop("cogs.steam.steam_link_oauth", None)
        for name, original in self._original_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original

    def test_build_steam_login_start_url_uses_signed_launch_token(self) -> None:
        url = self.mod.build_steam_login_start_url(123456789)

        self.assertIn("launch=", url)
        self.assertNotIn("uid=", url)

    def test_build_steam_login_start_url_issues_new_token_on_each_render(self) -> None:
        first = self.mod.build_steam_login_start_url(123456789)
        second = self.mod.build_steam_login_start_url(123456789)

        self.assertNotEqual(first, second)

    def test_launch_token_uses_900_second_ttl_and_expires(self) -> None:
        with mock.patch.object(self.mod.time, "time", return_value=1000):
            token = self.mod._make_steam_launch_token(123456789)

        parts = token.split(".")
        self.assertEqual(len(parts), 4)
        self.assertEqual(int(parts[1]), 1900)

        with mock.patch.object(self.mod.time, "time", return_value=1901):
            self.assertIsNone(self.mod._verify_steam_launch_token(token))


if __name__ == "__main__":
    unittest.main(verbosity=2)
