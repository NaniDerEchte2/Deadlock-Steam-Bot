"""
Wiederverwendbares Steam-Account-Verknüpfen UI.

Stellt Embed + View mit Steam-Link-Button bereit.
Kann von überall importiert werden: Onboarding, Rang-Auswahl, etc.

Verwendung:
    from cogs.steam.account_link_ui import make_link_embed, make_link_view

    embed = make_link_embed()
    view  = make_link_view(interaction.user.id)
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
"""

from __future__ import annotations

import logging

import discord

log = logging.getLogger(__name__)

__all__ = ("make_link_embed", "make_link_view", "send_link_panel")

# ---------------------------------------------------------------------------
# Embed
# ---------------------------------------------------------------------------


def make_link_embed(
    *,
    title: str = "🔗 Steam Account verknüpfen",
    show_why: bool = True,
) -> discord.Embed:
    """Erstellt das Embed für das Account-Verknüpfen-Panel."""
    lines: list[str] = []

    if show_why:
        lines += [
            "**Warum verknüpfen?**",
            "Dein Rang wird automatisch auf dem Server angezeigt und der Live-Status in den Voice Lanes funktioniert nur mit verknüpftem Account.",
            "",
        ]

    lines += [
        "**Datenschutz-Kurzinfo:**",
        "- Discord erhält aus diesem Schritt keine zusätzlichen Daten.",
        "- Wir speichern nur die technisch nötigen IDs (Discord-ID und SteamID64).",
        "- Wir erhalten keine Passwörter oder sonstige Zugangsdaten.",
        "- Es werden keine Daten an Dritte weitergegeben.",
        "",
        "**Open Source:**",
        "Unser Source Code ist öffentlich: <https://github.com/NaniDerEchte2/Deadlock-Bots>",
        "",
        "**So geht's:**",
        "Klick auf den Steam-Button unten. Danach **musst du dem Steam-Bot eine Freundschaftsanfrage senden** "
        "Freundescode **820142646**, sonst wird die Verknüpfung nicht aktiv.",
    ]

    return discord.Embed(
        title=title,
        description="\n".join(lines),
        color=0x00AEEF,
    )


# ---------------------------------------------------------------------------
# View
# ---------------------------------------------------------------------------


def make_link_view(user_id: int, *, timeout: float | None = 3600) -> discord.ui.View:
    """
    Erstellt einen View mit Steam-Link-Button für die Steam-Verknüpfung.

    Gibt immer einen View zurück – ohne Button wenn URL nicht generiert
    werden können (z. B. PUBLIC_BASE_URL nicht gesetzt).
    """
    steam_url = _get_steam_url(user_id)

    view = discord.ui.View(timeout=timeout)

    if steam_url:
        view.add_item(
            discord.ui.Button(
                label="Direkt bei Steam anmelden",
                style=discord.ButtonStyle.link,
                url=steam_url,
                emoji="🎮",
                row=0,
            )
        )

    if not steam_url:
        log.warning(
            "account_link_ui: Keine Steam-URL generiert (PUBLIC_BASE_URL gesetzt?) für user_id=%s",
            user_id,
        )

    return view


# ---------------------------------------------------------------------------
# Convenience send
# ---------------------------------------------------------------------------


async def send_link_panel(
    target: discord.Interaction | discord.abc.Messageable,
    user_id: int,
    *,
    ephemeral: bool = True,
    title: str = "🔗 Steam Account verknüpfen",
    show_why: bool = True,
    content: str | None = None,
) -> None:
    """
    Sendet Embed + View in einem Aufruf.

    target kann eine discord.Interaction oder ein Messageable (Channel/Thread) sein.
    """
    embed = make_link_embed(title=title, show_why=show_why)
    view = make_link_view(user_id)

    if isinstance(target, discord.Interaction):
        if target.response.is_done():
            await target.followup.send(content=content, embed=embed, view=view, ephemeral=ephemeral)
        else:
            await target.response.send_message(
                content=content, embed=embed, view=view, ephemeral=ephemeral
            )
    else:
        await target.send(content=content, embed=embed, view=view)


# ---------------------------------------------------------------------------
# Interna
# ---------------------------------------------------------------------------


def _get_steam_url(user_id: int) -> str:
    """Gibt die Steam-Login-URL zurück. Leerer String bei Fehler."""
    from service.config import settings

    base = settings.public_base_url.rstrip("/")
    if not base:
        return ""

    uid = int(user_id)
    return f"{base}/steam/login?uid={uid}"
