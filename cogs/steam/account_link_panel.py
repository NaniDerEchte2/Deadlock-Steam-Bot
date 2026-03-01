"""
Steam-Account-Verknüpfen Panel Cog.

Postet eine persistente Panel-Message mit einem Button.
Beim Klick bekommt der User seine persönlichen Link-Buttons (ephemeral).

Admin-Command: /publish_steam_panel  →  postet/editiert das Panel
"""

from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

from service.config import settings

log = logging.getLogger(__name__)

GUILD_ID = settings.guild_id

# Wird nach dem ersten /publish_steam_panel gesetzt und beim Bot-Restart
# aus dem kv_store geladen (optional – Panel funktioniert auch ohne Persist).
_PANEL_CUSTOM_ID = "steam_link_panel:open"


# ---------------------------------------------------------------------------
# Persistente Panel-View
# ---------------------------------------------------------------------------


class SteamLinkPanelView(discord.ui.View):
    """Persistente View die in der Panel-Message sitzt."""

    def __init__(self):
        super().__init__(timeout=None)  # persistent

    @discord.ui.button(
        label="Steam Account verknüpfen 🔗",
        style=discord.ButtonStyle.success,
        custom_id=_PANEL_CUSTOM_ID,
    )
    async def open_link(self, interaction: discord.Interaction, _button: discord.ui.Button):
        from cogs.steam.steam_link_oauth import (
            LINK_COVER_IMAGE,
            LINK_COVER_LABEL,
            PUBLIC_BASE_URL,
            STEAM_BUTTON_LABEL,
        )

        if not PUBLIC_BASE_URL:
            await interaction.response.send_message(
                "⚠️ PUBLIC_BASE_URL fehlt – Verknüpfung nicht möglich.", ephemeral=True
            )
            return

        uid = interaction.user.id
        desc = (
            "Verknüpfe deinen Steam-Account direkt über Steam OpenID.\n\n"
            "Das ist wichtig für korrekten Rang, den Live-Status in den Voice Lanes und die Spielersuche.\n\n"
            "Wichtig: Sende dem Steam-Bot direkt eine Freundschaftsanfrage (Freundescode **820142646**), "
            "sonst kann die Verknüpfung nicht abgeschlossen werden.\n\n"
            "Unser Source Code ist öffentlich: <https://github.com/NaniDerEchte2/Deadlock-Bots>"
        )
        embed = discord.Embed(
            title="Account verknüpfen", description=desc, color=discord.Color.green()
        )
        if LINK_COVER_IMAGE:
            embed.set_image(url=LINK_COVER_IMAGE)
        embed.set_author(name=LINK_COVER_LABEL)

        view = discord.ui.View()
        view.add_item(
            discord.ui.Button(
                style=discord.ButtonStyle.link,
                label=STEAM_BUTTON_LABEL,
                url=f"{PUBLIC_BASE_URL}/steam/login?uid={uid}",
            )
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(
        label="Rank check 📊",
        style=discord.ButtonStyle.secondary,
        custom_id="steam_link_panel:rankcheck",
    )
    async def rank_check(self, interaction: discord.Interaction, _button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            rank_cog = interaction.client.get_cog("DeadlockFriendRank")
            if rank_cog is None or not hasattr(rank_cog, "linked_steam_ids_for_user"):
                await interaction.followup.send(
                    "Rank-Modul nicht geladen. Bitte Admin informieren.", ephemeral=True
                )
                return

            steam_ids = rank_cog.linked_steam_ids_for_user(int(interaction.user.id))
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

            rank_label = snap.rank_name or "Unbekannt"
            subrank_label = snap.subrank if snap.subrank is not None else "–"
            account_label = snap.account_id if getattr(snap, "account_id", None) else "–"
            text = (
                f"**Rank für {sid}**\n"
                f"Rank: {rank_label}\n"
                f"Subrank: {subrank_label}\n"
                f"Account-ID: {account_label}"
            )
            await interaction.followup.send(text, ephemeral=True)
        except Exception:
            log.exception("Rank check button failed (user_id=%s)", interaction.user.id)
            await interaction.followup.send(
                "Unerwarteter Fehler bei der Rank-Abfrage.", ephemeral=True
            )


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------


class SteamLinkPanel(commands.Cog):
    """Verwaltet das persistente Steam-Account-Verknüpfen-Panel."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        self.bot.add_view(SteamLinkPanelView())
        log.info("SteamLinkPanel geladen (persistent view aktiv).")

    # ------------------------------------------------------------------
    # Admin-Command: Panel posten / editieren
    # ------------------------------------------------------------------

    @app_commands.command(
        name="publish_steam_panel",
        description="(Admin) Steam-Verknüpfen-Panel in diesem Channel posten / aktualisieren",
    )
    @app_commands.guilds(discord.Object(id=GUILD_ID))
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        message_id="ID einer bestehenden Message die editiert werden soll (optional)"
    )
    async def publish_steam_panel(
        self,
        interaction: discord.Interaction,
        message_id: str | None = None,
    ):
        embed = discord.Embed(
            title="🔗 Steam Account verknüpfen",
            description=(
                "Verknüpfe deinen Steam-Account\n\n"
                "**Was wird gespeichert?**\n"
                "```txt\n"
                "user_id: 662995601738170389\n"
                "steam_id: 76561199026913891\n"
                "name: EarlySalty\n"
                "verified: 1\n"
                "primary_account: 0\n"
                "updated_at: 2026-02-27 02:59:42\n"
                "deadlock_rank_name: Ascendant\n"
                "is_steam_friend: 1\n"
                "```\n"
                "**Datenschutz - Was wir nicht Speichern:**\n"
                "- Wir speichern **keine Passwörter**\n"
                "- Wir speichern **keine Login-Tokens** oder Session-Cookies\n"
                "- Wir speichern **keine Zahlungsdaten**\n"
                "- Wir speichern nur technisch nötige Daten  Discord-ID, SteamID64, Verknüpfungsstatus, Rank-Infos\n"
                "- Es werden **keine Daten an Dritte weitergegeben**\n"
                "- Du authentifizierst dich direkt bei Steam (OpenID), nicht über Passwort-Eingabe bei uns\n\n"
                "**Open Source:** Unser Source Code ist öffentlich: <https://github.com/NaniDerEchte2/Deadlock-Bots>\n\n"
                "**Was bringt das?**\n"
                "- Dein Rang wird automatisch auf dem Server angezeigt\n"
                "- Der Live-Status in den Voice Lanes funktioniert\n"
                "- Du wirst in der Spieler-Suche korrekt eingestuft\n\n"
                "**Wichtig:** Klick auf den Button und sende dem Steam-Bot eine Freundschaftsanfrage "
                "(Freundescode **820142646**)."
            ),
            color=0x00AEEF,
        )
        view = SteamLinkPanelView()

        # Bestehende Message editieren?
        if message_id:
            try:
                mid = int(message_id)
                msg = await interaction.channel.fetch_message(mid)
                await msg.edit(embed=embed, view=view)
                await interaction.response.send_message("✅ Panel aktualisiert.", ephemeral=True)
                return
            except (ValueError, discord.NotFound):
                await interaction.response.send_message(
                    "❌ Message nicht gefunden. Neues Panel wird gepostet.", ephemeral=True
                )
            except discord.Forbidden:
                await interaction.response.send_message(
                    "❌ Keine Berechtigung diese Message zu editieren.", ephemeral=True
                )
                return

        # Neue Message posten
        await interaction.channel.send(embed=embed, view=view)
        if not interaction.response.is_done():
            await interaction.response.send_message("✅ Panel gepostet.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(SteamLinkPanel(bot))
