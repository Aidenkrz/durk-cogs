from __future__ import annotations

import asyncio
import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import tasks
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.durk-cogs.government")

DAY_SECONDS = 24 * 60 * 60
TERM_SECONDS = 14 * DAY_SECONDS
MIN_PARTY_MEMBERS = 5
PARTY_CHANNEL_MIN_MEMBERS = 6
CONSTITUTION_VERSION = 2
MAX_ICON_BYTES = 256 * 1024
MAX_PARTY_NAME = 50
MAX_PARTY_SLOGAN = 100
MAX_PARTY_DESCRIPTION = 500
MAX_PARTY_MANIFESTO = 1000
MAX_LAW_TITLE = 100
MAX_LAW_TEXT = 3500

IMMUTABLE_LAWS = (
    "No law or government action may violate Discord's Terms of Service, "
    "Community Guidelines, or applicable law.",
    "The government may not interfere with actions necessary to protect the "
    "server, its members, or its infrastructure.",
    "The server owner retains final authority when required by Discord, law, "
    "safety, or technical necessity.",
    "Each President may choose one in-game change to be implemented during "
    "their term, provided it is technically possible and does not violate "
    "these Immutable Laws.",
    "These Immutable Laws cannot be amended, suspended, or repealed. Any "
    "conflicting law is automatically void.",
)

DEFAULT_LAWS = (
    (
        "Presidential Term",
        "The President is elected for a two-week term.",
    ),
    (
        "Vice Presidential Appointment",
        "After being elected, the President appoints a Vice President to serve for the remainder of the term.",
    ),
    (
        "Presidential Powers",
        "The President may propose laws, administer approved laws, appoint officials, and organize government activities.",
    ),
    (
        "Presidential Succession",
        "The Vice President assists the President and assumes the presidency if the President cannot serve.",
    ),
    (
        "Public Lawmaking",
        "Proposed laws must receive at least 24 hours of public discussion before being decided by a majority vote.",
    ),
    (
        "Constitutional Amendments",
        "These government rules may be amended by a two-thirds public vote, except for the Immutable Laws.",
    ),
)

COLOR_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def unix_now() -> int:
    return int(utcnow().timestamp())


def parse_color(value: str) -> int:
    match = COLOR_RE.fullmatch((value or "").strip())
    if not match:
        raise ValueError("Color must be a six-digit hex value such as `#5865F2`.")
    return int(match.group(1), 16)


def detect_image_type(data: bytes) -> Optional[str]:
    """Recognize the static image types accepted for Discord role icons."""
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if data.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "webp"
    return None


def clean_party_name(value: str) -> str:
    name = " ".join((value or "").split())
    if not 2 <= len(name) <= MAX_PARTY_NAME:
        raise ValueError(
            f"Party names must be between 2 and {MAX_PARTY_NAME} characters."
        )
    if any(ord(char) < 32 for char in name):
        raise ValueError("Party names cannot contain control characters.")
    return name


def clean_profile_field(
    value: Optional[str], *, label: str, maximum: int
) -> Optional[str]:
    """Validate an optional profile value. A single dash clears the field."""
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "-":
        return ""
    if not cleaned:
        raise ValueError(f"{label} cannot be empty. Use `-` to clear it.")
    if len(cleaned) > maximum:
        raise ValueError(f"{label} may not exceed {maximum} characters.")
    return cleaned


class Government(commands.Cog):
    """Political parties, elections, offices, and constitutional lawmaking."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=0xC0715717A710, force_registration=True
        )
        self.config.register_guild(
            channel_id=None,
            laws_channel_id=None,
            constitution_message_id=None,
            constitution_version=0,
            law_message_ids={},
            default_laws_seeded=False,
            parties={},
            president_role_id=None,
            vice_president_role_id=None,
            party_leader_role_id=None,
            party_channel_category_id=None,
            president_id=None,
            vice_president_id=None,
            term_started_at=None,
            term_ends_at=None,
            active_election=None,
            election_history=[],
            laws={},
            next_law_number=1,
        )
        self._locks: Dict[int, asyncio.Lock] = {}
        self._data_path: Path = cog_data_path(self)

    async def cog_load(self) -> None:
        self._data_path.mkdir(parents=True, exist_ok=True)
        self.maintenance_loop.start()

    async def cog_unload(self) -> None:
        self.maintenance_loop.cancel()

    def _lock(self, guild_id: int) -> asyncio.Lock:
        return self._locks.setdefault(guild_id, asyncio.Lock())

    def _polls_api(self):
        cog = self.bot.get_cog("Polls")
        return getattr(cog, "api", None) if cog is not None else None

    async def _require_admin(self, interaction: discord.Interaction) -> bool:
        member = interaction.user
        allowed = (
            isinstance(member, discord.Member)
            and member.guild_permissions.administrator
        ) or await self.bot.is_owner(member)
        if not allowed:
            await interaction.response.send_message(
                "This command requires the **Administrator** permission.",
                ephemeral=True,
            )
        return allowed

    @staticmethod
    async def _reply(
        interaction: discord.Interaction, content: str, *, ephemeral: bool = True
    ) -> None:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content, ephemeral=ephemeral)

    @staticmethod
    def _guild(interaction: discord.Interaction) -> Optional[discord.Guild]:
        return interaction.guild

    @staticmethod
    def _find_party(
        parties: Dict[str, Dict[str, Any]], query: str
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        wanted = (query or "").strip().casefold()
        if wanted in parties:
            return wanted, parties[wanted]
        for party_id, party in parties.items():
            if str(party.get("name", "")).casefold() == wanted:
                return party_id, party
        return None, None

    @staticmethod
    def _party_for_user(
        parties: Dict[str, Dict[str, Any]], user_id: int
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        for party_id, party in parties.items():
            if user_id in [int(uid) for uid in party.get("member_ids", [])]:
                return party_id, party
        return None, None

    def _icon_path(self, guild_id: int, party_id: str, image_type: str) -> Path:
        return self._data_path / f"{guild_id}-{party_id}.{image_type}"

    @staticmethod
    def _party_channel_name(name: str, party_id: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", name.casefold()).strip("-")
        slug = slug[:80].strip("-") or "party"
        return f"{slug}-{party_id[:6]}"[:100]

    @staticmethod
    async def _read_icon(attachment: discord.Attachment) -> Tuple[bytes, str]:
        if attachment.size > MAX_ICON_BYTES:
            raise ValueError("Party icons may not exceed 256 KiB.")
        data = await attachment.read()
        if len(data) > MAX_ICON_BYTES:
            raise ValueError("Party icons may not exceed 256 KiB.")
        image_type = detect_image_type(data)
        if image_type is None:
            raise ValueError("Party icons must be a PNG, JPEG, or static WebP image.")
        return data, image_type

    async def _ensure_office_roles(
        self, guild: discord.Guild
    ) -> Tuple[discord.Role, discord.Role]:
        settings = await self.config.guild(guild).all()
        president = guild.get_role(settings.get("president_role_id") or 0)
        vice = guild.get_role(settings.get("vice_president_role_id") or 0)
        if president is None:
            president = await guild.create_role(
                name="President",
                permissions=discord.Permissions.none(),
                colour=discord.Colour.gold(),
                reason="Government cog office role",
            )
            await self.config.guild(guild).president_role_id.set(president.id)
        if vice is None:
            vice = await guild.create_role(
                name="Vice President",
                permissions=discord.Permissions.none(),
                colour=discord.Colour.light_grey(),
                reason="Government cog office role",
            )
            await self.config.guild(guild).vice_president_role_id.set(vice.id)
        return president, vice

    async def _sync_party_leader_role(
        self, guild: discord.Guild, parties: Dict[str, Dict[str, Any]]
    ) -> Tuple[Optional[discord.Role], Optional[str]]:
        """Give the shared role only to leaders of currently qualified parties."""
        role_id = await self.config.guild(guild).party_leader_role_id()
        role = guild.get_role(int(role_id or 0))
        try:
            if role is None:
                role = await guild.create_role(
                    name="Party Leader",
                    permissions=discord.Permissions.none(),
                    colour=discord.Colour.purple(),
                    reason="Government cog shared party-leader role",
                )
                await self.config.guild(guild).party_leader_role_id.set(role.id)
            else:
                await role.edit(
                    name="Party Leader",
                    permissions=discord.Permissions.none(),
                    reason="Synchronize government Party Leader role",
                )

            qualified_ids = {
                int(party["leader_id"])
                for party in parties.values()
                if len(party.get("member_ids", [])) >= MIN_PARTY_MEMBERS
                and party.get("leader_id")
                and int(party["leader_id"])
                in {int(user_id) for user_id in party.get("member_ids", [])}
                and guild.get_member(int(party["leader_id"])) is not None
            }
            for member in list(role.members):
                if member.id not in qualified_ids:
                    await member.remove_roles(
                        role, reason="No longer leads a qualifying party"
                    )
            for user_id in qualified_ids:
                member = guild.get_member(user_id)
                if member is not None and role not in member.roles:
                    await member.add_roles(
                        role, reason="Leads a party with at least five members"
                    )
            return role, None
        except discord.Forbidden:
            return None, (
                "I cannot manage the Party Leader role. Check my Manage Roles "
                "permission and role position."
            )
        except discord.HTTPException as exc:
            log.warning(
                "Could not synchronize Party Leader role in %s: %s", guild.id, exc
            )
            return None, "Discord rejected the Party Leader role update."

    async def _ensure_party_role(
        self, guild: discord.Guild, party_id: str, parties: Dict[str, Dict[str, Any]]
    ) -> Tuple[Optional[discord.Role], Optional[str]]:
        party = parties[party_id]
        if len(party.get("member_ids", [])) < MIN_PARTY_MEMBERS:
            return None, None

        role = guild.get_role(int(party.get("role_id") or 0))
        icon_path = Path(party["icon_path"])
        try:
            icon = icon_path.read_bytes()
        except OSError:
            return (
                None,
                "The party icon file is missing; its leader must upload it again.",
            )

        try:
            if role is None:
                role = await guild.create_role(
                    name=party["name"],
                    permissions=discord.Permissions.none(),
                    colour=discord.Colour(int(party["color"])),
                    display_icon=icon,
                    reason="Party reached five members",
                )
                party["role_id"] = role.id
                await self.config.guild(guild).parties.set(parties)
            else:
                await role.edit(
                    name=party["name"],
                    permissions=discord.Permissions.none(),
                    colour=discord.Colour(int(party["color"])),
                    display_icon=icon,
                    reason="Synchronize government party role",
                )

            member_ids = {int(user_id) for user_id in party.get("member_ids", [])}
            for member in list(role.members):
                if member.id not in member_ids:
                    await member.remove_roles(
                        role, reason="Not a member of this government party"
                    )
            for user_id in member_ids:
                member = guild.get_member(user_id)
                if member is not None and role not in member.roles:
                    await member.add_roles(role, reason="Government party membership")
            return role, None
        except discord.Forbidden:
            return (
                None,
                "I cannot manage the party role. Check my Manage Roles permission and role position.",
            )
        except discord.HTTPException as exc:
            log.warning("Could not create/update party role in %s: %s", guild.id, exc)
            if "ROLE_ICONS" not in guild.features:
                return (
                    None,
                    "This server does not currently support role icons, so the required party role could not be created.",
                )
            return (
                None,
                "Discord rejected the party role or icon. Try a different icon and run reconcile.",
            )

    async def _ensure_party_channel(
        self, guild: discord.Guild, party_id: str, parties: Dict[str, Dict[str, Any]]
    ) -> Tuple[Optional[discord.TextChannel], Optional[str]]:
        party = parties[party_id]
        if len(party.get("member_ids", [])) < PARTY_CHANNEL_MIN_MEMBERS:
            return None, None

        category_id = await self.config.guild(guild).party_channel_category_id()
        if not category_id:
            return (
                None,
                "An administrator has not configured the party-channel category.",
            )
        category = guild.get_channel(int(category_id))
        if not isinstance(category, discord.CategoryChannel):
            await self.config.guild(guild).party_channel_category_id.set(None)
            return None, "The configured party-channel category no longer exists."

        role = guild.get_role(int(party.get("role_id") or 0))
        if role is None:
            return None, "The party role must be created before its private channel."

        overwrites: Dict[Any, discord.PermissionOverwrite] = {
            guild.default_role: discord.PermissionOverwrite(
                view_channel=False,
                send_messages=False,
                read_message_history=False,
            ),
            role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                add_reactions=True,
                attach_files=True,
                embed_links=True,
                use_external_emojis=True,
            ),
        }
        if guild.me is not None:
            overwrites[guild.me] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
                manage_messages=True,
            )

        channel = guild.get_channel(int(party.get("channel_id") or 0))
        try:
            if isinstance(channel, discord.TextChannel):
                await channel.edit(
                    name=self._party_channel_name(party["name"], party_id),
                    category=category,
                    overwrites=overwrites,
                    topic=f"Private channel for {party['name']} • Party ID: {party_id}",
                    reason="Synchronize private government party channel",
                )
            else:
                channel = await category.create_text_channel(
                    self._party_channel_name(party["name"], party_id),
                    overwrites=overwrites,
                    topic=f"Private channel for {party['name']} • Party ID: {party_id}",
                    reason="Party exceeded five members",
                )
                party["channel_id"] = channel.id
                await self.config.guild(guild).parties.set(parties)
            return channel, None
        except discord.Forbidden:
            return None, (
                "I cannot create or secure the private party channel. "
                "Check my Manage Channels permission."
            )
        except discord.HTTPException:
            log.exception("Could not create/update party channel for %s", party_id)
            return None, "Discord rejected the private party-channel update."

    async def _remove_role(
        self, member: discord.Member, role_id: Optional[int]
    ) -> None:
        role = member.guild.get_role(int(role_id or 0))
        if role is not None and role in member.roles:
            try:
                await member.remove_roles(
                    role, reason="Government office or party ended"
                )
            except discord.HTTPException:
                log.exception("Could not remove role %s from %s", role.id, member.id)

    async def _expire_term(
        self, guild: discord.Guild, settings: Dict[str, Any]
    ) -> None:
        for key, role_key in (
            ("president_id", "president_role_id"),
            ("vice_president_id", "vice_president_role_id"),
        ):
            member = guild.get_member(int(settings.get(key) or 0))
            if member is not None:
                await self._remove_role(member, settings.get(role_key))
        group = self.config.guild(guild)
        await group.president_id.set(None)
        await group.vice_president_id.set(None)
        await group.term_started_at.set(None)
        await group.term_ends_at.set(None)
        channel = guild.get_channel(int(settings.get("channel_id") or 0))
        if channel is not None:
            try:
                await channel.send(
                    "The presidential term has ended. Both offices are now vacant."
                )
            except discord.HTTPException:
                pass

    @staticmethod
    def _founding_constitution_embed() -> discord.Embed:
        embed = discord.Embed(
            title="Founding Constitution",
            description="The permanent foundation of the server government.",
            colour=discord.Colour.gold(),
        )
        embed.add_field(
            name="Immutable Laws",
            value="\n".join(
                f"{index}. {law}" for index, law in enumerate(IMMUTABLE_LAWS, 1)
            ),
            inline=False,
        )
        embed.set_footer(text="The Immutable Laws cannot be repealed or amended.")
        return embed

    async def _seed_default_laws(self, guild: discord.Guild) -> int:
        """Create the amendable founding rules as normal enacted law records."""
        settings = await self.config.guild(guild).all()
        if settings.get("default_laws_seeded"):
            return 0
        laws = settings.get("laws") or {}
        next_number = int(settings.get("next_law_number") or 1)
        for title, text in DEFAULT_LAWS:
            law_id = str(next_number)
            laws[law_id] = {
                "title": title,
                "text": text,
                "kind": "amendment",
                "action": "enact",
                "target_law_id": None,
                "proposer_id": 0,
                "status": "enacted",
                "created_at": None,
                "decided_at": None,
                "approve_votes": None,
                "reject_votes": None,
                "source": "founding_constitution",
                "channel_id": settings.get("channel_id"),
                "message_id": None,
                "thread_id": None,
                "poll_id": None,
            }
            next_number += 1
        await self.config.guild(guild).laws.set(laws)
        await self.config.guild(guild).next_law_number.set(next_number)
        await self.config.guild(guild).default_laws_seeded.set(True)
        return len(DEFAULT_LAWS)

    @staticmethod
    def _current_law_embed(law_id: str, law: Dict[str, Any]) -> discord.Embed:
        embed = discord.Embed(
            title=f"Law {law_id}: {law['title']}",
            description=law["text"],
            colour=discord.Colour.green(),
        )
        if law.get("source") == "founding_constitution":
            kind = "Founding law - two-thirds vote required to change it"
        elif law.get("kind") == "amendment":
            kind = "Constitutional law - two-thirds vote required"
        else:
            kind = "Ordinary law - simple majority required"
        embed.add_field(name="Type", value=kind)
        if law.get("action") == "amend" and law.get("target_law_id"):
            embed.add_field(name="Replaces", value=f"Law {law['target_law_id']}")
        elif law.get("action") == "repeal" and law.get("target_law_id"):
            embed.add_field(name="Repeals", value=f"Law {law['target_law_id']}")
        if law.get("approve_votes") is not None:
            embed.add_field(
                name="Ratification vote",
                value=(
                    f"{law['approve_votes']} approve / " f"{law['reject_votes']} reject"
                ),
            )
        decided_at = law.get("decided_at")
        embed.add_field(
            name="Enacted",
            value=f"<t:{int(decided_at)}:F>" if decided_at else "Before records began",
            inline=False,
        )
        return embed

    async def _sync_current_laws(self, guild: discord.Guild) -> Optional[str]:
        """Mirror every currently enacted law into the read-only laws channel."""
        settings = await self.config.guild(guild).all()
        channel = guild.get_channel(int(settings.get("laws_channel_id") or 0))
        if not isinstance(channel, discord.TextChannel):
            return "The current-laws channel is missing. Run government setup again."

        laws = settings.get("laws") or {}
        current = {
            law_id: law
            for law_id, law in laws.items()
            if law.get("status") == "enacted"
        }
        stored_ids = settings.get("law_message_ids") or {}
        new_ids: Dict[str, int] = {}

        constitution_id = settings.get("constitution_message_id")
        if constitution_id:
            try:
                constitution_message = channel.get_partial_message(int(constitution_id))
                await constitution_message.edit(
                    embed=self._founding_constitution_embed(), content=None
                )
            except discord.NotFound:
                constitution_id = None
            except discord.HTTPException:
                log.exception("Could not update the founding constitution message")
                constitution_id = None
        if not constitution_id:
            try:
                constitution_message = await channel.send(
                    embed=self._founding_constitution_embed(),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                constitution_id = constitution_message.id
            except discord.HTTPException:
                log.exception("Could not publish the founding constitution")
                return f"I could not publish the Founding Constitution in {channel.mention}."
        await self.config.guild(guild).constitution_message_id.set(int(constitution_id))

        for law_id, message_id in stored_ids.items():
            if law_id in current:
                continue
            try:
                await channel.get_partial_message(int(message_id)).delete()
            except discord.NotFound:
                pass
            except discord.HTTPException:
                log.exception("Could not remove old law message %s", message_id)

        for law_id, law in sorted(current.items(), key=lambda pair: int(pair[0])):
            embed = self._current_law_embed(law_id, law)
            message_id = stored_ids.get(law_id)
            if message_id:
                try:
                    message = channel.get_partial_message(int(message_id))
                    await message.edit(embed=embed, content=None)
                    new_ids[law_id] = int(message_id)
                    continue
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    log.exception("Could not update law message %s", message_id)
            try:
                message = await channel.send(
                    embed=embed, allowed_mentions=discord.AllowedMentions.none()
                )
                new_ids[law_id] = message.id
            except discord.HTTPException:
                log.exception("Could not publish current Law %s", law_id)
                return f"I could not publish Law {law_id} in {channel.mention}."

        await self.config.guild(guild).law_message_ids.set(new_ids)
        await self.config.guild(guild).constitution_version.set(CONSTITUTION_VERSION)
        return None

    async def _make_laws_channel(
        self, guild: discord.Guild, government_channel: discord.TextChannel
    ) -> discord.TextChannel:
        configured_id = await self.config.guild(guild).laws_channel_id()
        configured = guild.get_channel(int(configured_id or 0))
        if isinstance(configured, discord.TextChannel):
            channel = configured
        else:
            overwrites: Dict[Any, discord.PermissionOverwrite] = {
                guild.default_role: discord.PermissionOverwrite(
                    send_messages=False,
                    add_reactions=False,
                    create_public_threads=False,
                    create_private_threads=False,
                    send_messages_in_threads=False,
                )
            }
            if guild.me is not None:
                overwrites[guild.me] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    embed_links=True,
                    read_message_history=True,
                    manage_messages=True,
                )
            channel = await guild.create_text_channel(
                "current-laws",
                category=government_channel.category,
                topic="Read-only register of all currently enacted server laws.",
                overwrites=overwrites,
                reason="Government cog current-laws register",
            )
            await self.config.guild(guild).laws_channel_id.set(channel.id)
        return channel

    async def _set_laws_channel_read_only(
        self, guild: discord.Guild, channel: discord.TextChannel
    ) -> None:
        await channel.set_permissions(
            guild.default_role,
            send_messages=False,
            add_reactions=False,
            create_public_threads=False,
            create_private_threads=False,
            send_messages_in_threads=False,
            reason="Keep current-laws register read-only",
        )
        if guild.me is not None:
            await channel.set_permissions(
                guild.me,
                view_channel=True,
                send_messages=True,
                embed_links=True,
                read_message_history=True,
                manage_messages=True,
                reason="Allow Government cog to maintain current laws",
            )

    async def _post_law_discussion(
        self,
        guild: discord.Guild,
        settings: Dict[str, Any],
        *,
        proposer_id: int,
        title: str,
        text: str,
        kind: str,
        action: str = "enact",
        target_law_id: Optional[str] = None,
    ) -> str:
        channel = guild.get_channel(int(settings.get("channel_id") or 0))
        if channel is None:
            raise ValueError(
                "An administrator must first run `/government admin setup`."
            )
        number = int(settings.get("next_law_number") or 1)
        law_id = str(number)
        end = unix_now() + DAY_SECONDS
        embed = discord.Embed(
            title=f"Law {law_id}: {title}",
            description=text,
            colour=discord.Colour.orange(),
        )
        embed.add_field(
            name="Stage",
            value=f"Public discussion until <t:{end}:F> (<t:{end}:R>)",
            inline=False,
        )
        embed.add_field(
            name="Decision",
            value=(
                "Two-thirds public vote"
                if kind == "amendment"
                else "Simple-majority public vote"
            ),
        )
        if action == "repeal" and target_law_id is not None:
            embed.add_field(name="Would repeal", value=f"Law {target_law_id}")
        elif action == "amend" and target_law_id is not None:
            embed.add_field(name="Would replace", value=f"Law {target_law_id}")
        embed.set_footer(
            text="Immutable Laws always prevail. Administrators may void conflicting proposals."
        )
        message = await channel.send(
            embed=embed, allowed_mentions=discord.AllowedMentions.none()
        )
        thread_id = None
        if isinstance(channel, discord.TextChannel):
            try:
                thread = await message.create_thread(
                    name=f"Discussion: Law {law_id} - {title}"[:100]
                )
                thread_id = thread.id
            except discord.HTTPException:
                pass

        laws = settings.get("laws") or {}
        laws[law_id] = {
            "title": title,
            "text": text,
            "kind": kind,
            "action": action,
            "target_law_id": target_law_id,
            "proposer_id": proposer_id,
            "status": "discussion",
            "created_at": unix_now(),
            "discussion_ends_at": end,
            "channel_id": channel.id,
            "message_id": message.id,
            "thread_id": thread_id,
            "poll_id": None,
        }
        await self.config.guild(guild).laws.set(laws)
        await self.config.guild(guild).next_law_number.set(number + 1)
        return law_id

    async def _start_law_vote(
        self, guild: discord.Guild, law_id: str, law: Dict[str, Any]
    ) -> None:
        api = self._polls_api()
        if api is None:
            raise RuntimeError("The Polls cog must be loaded.")
        channel = guild.get_channel(int(law.get("channel_id") or 0))
        if channel is None:
            raise RuntimeError("The configured government channel no longer exists.")
        requirement = (
            "two-thirds" if law["kind"] == "amendment" else "a simple majority"
        )
        action = law.get("action", "enact")
        is_repeal = action == "repeal"
        is_amendment = action == "amend"
        target_law_id = law.get("target_law_id")
        poll = await api.create_poll(
            guild=guild,
            channel=channel,
            author_id=int(law["proposer_id"]),
            question=(
                f"Law {law_id}: repeal Law {target_law_id}?"
                if is_repeal
                else (
                    f"Law {law_id}: amend Law {target_law_id}?"
                    if is_amendment
                    else f"Law {law_id}: {law['title']} - approve?"
                )
            ),
            options=(
                ["Approve repeal", "Retain existing law"]
                if is_repeal
                else (
                    ["Approve amendment", "Retain existing law"]
                    if is_amendment
                    else ["Approve", "Reject"]
                )
            ),
            duration_seconds=DAY_SECONDS,
            hide_voters=False,
            hide_tally_until_close=True,
            max_choices=1,
        )
        async with self.config.guild(guild).laws() as laws:
            current = laws.get(law_id)
            if current is None or current.get("status") != "discussion":
                return
            current["status"] = "voting"
            current["poll_id"] = poll.id
            current["vote_started_at"] = unix_now()
        await channel.send(
            f"The required discussion period for **Law {law_id}** has ended. "
            f"The 24-hour vote is now open and requires {requirement} to pass.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    async def _finish_law_vote(self, guild: discord.Guild, poll: Any) -> None:
        api = self._polls_api()
        if api is None:
            return
        counts = await api.get_vote_counts(poll.id)
        approve = counts[0] if counts else 0
        reject = counts[1] if len(counts) > 1 else 0
        result: Optional[Tuple[str, Dict[str, Any]]] = None
        async with self.config.guild(guild).laws() as laws:
            for law_id, law in laws.items():
                if law.get("poll_id") != poll.id or law.get("status") != "voting":
                    continue
                total = approve + reject
                if law.get("kind") == "amendment":
                    passed = total > 0 and approve * 3 >= total * 2
                else:
                    passed = approve > reject
                action = law.get("action", "enact")
                if passed and action in {"repeal", "amend"}:
                    target = laws.get(str(law.get("target_law_id")))
                    if target is not None and target.get("status") == "enacted":
                        target["status"] = (
                            "repealed" if action == "repeal" else "amended"
                        )
                        target[f"{target['status']}_at"] = unix_now()
                        target[f"{target['status']}_by"] = law_id
                        law["status"] = "executed" if action == "repeal" else "enacted"
                    else:
                        law["status"] = "moot"
                else:
                    law["status"] = "enacted" if passed else "rejected"
                law["decided_at"] = unix_now()
                law["approve_votes"] = approve
                law["reject_votes"] = reject
                result = law_id, dict(law)
                break
        if result is None:
            return
        law_id, law = result
        dashboard_warning = await self._sync_current_laws(guild)
        channel = guild.get_channel(int(law.get("channel_id") or 0))
        if channel is not None:
            threshold = (
                "two-thirds threshold" if law["kind"] == "amendment" else "majority"
            )
            await channel.send(
                f"**Law {law_id}: {law['title']}** was **{law['status']}** "
                f"({approve} approve, {reject} reject; {threshold})."
                + (f"\n⚠️ {dashboard_warning}" if dashboard_warning else ""),
                allowed_mentions=discord.AllowedMentions.none(),
            )

    async def _finish_election(self, guild: discord.Guild, poll: Any) -> None:
        settings = await self.config.guild(guild).all()
        election = settings.get("active_election")
        if not election or election.get("poll_id") != poll.id:
            return
        api = self._polls_api()
        if api is None:
            return
        counts = await api.get_vote_counts(poll.id)
        candidates = election.get("candidates", [])
        winner: Optional[Dict[str, Any]] = None
        outcome = "no_votes"
        if counts and max(counts, default=0) > 0:
            high = max(counts)
            winning_indexes = [
                index for index, count in enumerate(counts) if count == high
            ]
            if len(winning_indexes) == 1 and winning_indexes[0] < len(candidates):
                winner = candidates[winning_indexes[0]]
                outcome = "elected"
            else:
                outcome = "tie"

        elected_member: Optional[discord.Member] = None
        if winner is not None:
            elected_member = guild.get_member(int(winner["leader_id"]))
            parties = settings.get("parties") or {}
            current_party = parties.get(winner["party_id"])
            if (
                elected_member is None
                or current_party is None
                or int(current_party.get("leader_id") or 0) != elected_member.id
            ):
                elected_member = None
                outcome = "candidate_unavailable"

        old_president = guild.get_member(int(settings.get("president_id") or 0))
        old_vice = guild.get_member(int(settings.get("vice_president_id") or 0))
        if elected_member is not None:
            try:
                president_role, vice_role = await self._ensure_office_roles(guild)
                if old_president and old_president.id != elected_member.id:
                    await self._remove_role(old_president, president_role.id)
                if old_vice:
                    await self._remove_role(old_vice, vice_role.id)
                await elected_member.add_roles(
                    president_role, reason="Won presidential election"
                )
            except discord.HTTPException:
                log.exception(
                    "Could not assign elected President in guild %s", guild.id
                )
                elected_member = None
                outcome = "role_error"
            else:
                started = unix_now()
                await self.config.guild(guild).president_id.set(elected_member.id)
                await self.config.guild(guild).vice_president_id.set(None)
                await self.config.guild(guild).term_started_at.set(started)
                await self.config.guild(guild).term_ends_at.set(started + TERM_SECONDS)

        record = {
            "poll_id": poll.id,
            "ended_at": unix_now(),
            "outcome": outcome,
            "winner_id": elected_member.id if elected_member else None,
            "counts": counts,
            "candidates": candidates,
        }
        async with self.config.guild(guild).election_history() as history:
            history.append(record)
            del history[:-20]
        await self.config.guild(guild).active_election.set(None)

        channel = guild.get_channel(int(settings.get("channel_id") or 0))
        if channel is not None:
            if elected_member is not None:
                await channel.send(
                    f"{elected_member.mention} has been elected **President**. "
                    f"Their two-week term ends <t:{unix_now() + TERM_SECONDS}:R>."
                )
            elif outcome == "tie":
                await channel.send(
                    "The presidential election ended in a tie. An administrator must start a new election."
                )
            elif outcome == "no_votes":
                await channel.send(
                    "The presidential election ended without votes. No President was elected."
                )
            elif outcome == "role_error":
                await channel.send(
                    "The election had a winner, but I could not assign the President role. Check my role hierarchy, run reconcile, and start a new election."
                )
            else:
                await channel.send(
                    "The winning party's candidate is no longer eligible. No President was elected."
                )

    @tasks.loop(minutes=1)
    async def maintenance_loop(self) -> None:
        all_guilds = await self.config.all_guilds()
        now = unix_now()
        for guild_id, settings in all_guilds.items():
            guild = self.bot.get_guild(int(guild_id))
            if guild is None:
                continue
            try:
                await self._maintain_guild(guild, settings, now)
            except Exception:
                # One broken guild or deleted Discord object must not stop the
                # loop from maintaining every other government.
                log.exception("Government maintenance failed for guild %s", guild.id)

    async def _maintain_guild(
        self, guild: discord.Guild, settings: Dict[str, Any], now: int
    ) -> None:
        async with self._lock(guild.id):
            seeded = await self._seed_default_laws(guild)
            if seeded:
                settings = await self.config.guild(guild).all()
            if (
                settings.get("laws_channel_id")
                and int(settings.get("constitution_version") or 0)
                != CONSTITUTION_VERSION
            ):
                await self._sync_current_laws(guild)
            term_end = settings.get("term_ends_at")
            if term_end and int(term_end) <= now:
                await self._expire_term(guild, settings)

            # Polls may have closed while this cog was unloaded. Recover their
            # terminal state instead of relying solely on events.
            api = self._polls_api()
            election = settings.get("active_election")
            if api is not None and election:
                election_poll = await api.get_poll(election.get("poll_id"))
                if election_poll is None or election_poll.status == "cancelled":
                    await self.config.guild(guild).active_election.set(None)
                elif election_poll.status == "closed":
                    await self._finish_election(guild, election_poll)

            if api is not None:
                for law_id, law in (settings.get("laws") or {}).items():
                    if law.get("status") != "voting" or not law.get("poll_id"):
                        continue
                    law_poll = await api.get_poll(law["poll_id"])
                    if law_poll is None or law_poll.status == "cancelled":
                        async with self.config.guild(guild).laws() as laws:
                            current = laws.get(law_id)
                            if current and current.get("status") == "voting":
                                current["status"] = "cancelled"
                                current["decided_at"] = now
                    elif law_poll.status == "closed":
                        await self._finish_law_vote(guild, law_poll)

            for law_id, law in (settings.get("laws") or {}).items():
                if (
                    law.get("status") == "discussion"
                    and int(law.get("discussion_ends_at") or 0) <= now
                ):
                    try:
                        await self._start_law_vote(guild, law_id, law)
                    except Exception:
                        log.exception(
                            "Could not start vote for law %s in %s", law_id, guild.id
                        )

    @maintenance_loop.before_loop
    async def before_maintenance_loop(self) -> None:
        await self.bot.wait_until_ready()

    # Events from Polls are the authoritative end of elections and law votes.
    @commands.Cog.listener()
    async def on_poll_closed(self, poll: Any) -> None:
        guild = self.bot.get_guild(int(getattr(poll, "guild_id", 0)))
        if guild is None:
            return
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            election = settings.get("active_election")
            if election and election.get("poll_id") == getattr(poll, "id", None):
                await self._finish_election(guild, poll)
                return
            await self._finish_law_vote(guild, poll)

    @commands.Cog.listener()
    async def on_poll_cancelled(self, poll: Any) -> None:
        guild = self.bot.get_guild(int(getattr(poll, "guild_id", 0)))
        if guild is None:
            return
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            election = settings.get("active_election")
            if election and election.get("poll_id") == getattr(poll, "id", None):
                await self.config.guild(guild).active_election.set(None)
            async with self.config.guild(guild).laws() as laws:
                for law in laws.values():
                    if (
                        law.get("poll_id") == getattr(poll, "id", None)
                        and law.get("status") == "voting"
                    ):
                        law["status"] = "cancelled"
                        law["decided_at"] = unix_now()

    @commands.Cog.listener()
    async def on_poll_deleted(self, poll: Any) -> None:
        await self.on_poll_cancelled(poll)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        guild = member.guild
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            parties = settings.get("parties") or {}
            changed = False
            for party in parties.values():
                members = [int(uid) for uid in party.get("member_ids", [])]
                if member.id not in members:
                    continue
                members.remove(member.id)
                party["member_ids"] = members
                if int(party.get("leader_id") or 0) == member.id:
                    party["leader_id"] = members[0] if members else None
                changed = True
            if changed:
                await self.config.guild(guild).parties.set(parties)
                await self._sync_party_leader_role(guild, parties)

            if int(settings.get("president_id") or 0) == member.id:
                vice = guild.get_member(int(settings.get("vice_president_id") or 0))
                if vice is not None:
                    president_role, vice_role = await self._ensure_office_roles(guild)
                    await self._remove_role(vice, vice_role.id)
                    await vice.add_roles(
                        president_role, reason="Presidential succession"
                    )
                    await self.config.guild(guild).president_id.set(vice.id)
                    await self.config.guild(guild).vice_president_id.set(None)
                    channel = guild.get_channel(int(settings.get("channel_id") or 0))
                    if channel:
                        await channel.send(
                            f"{vice.mention} has succeeded to the presidency."
                        )
                else:
                    await self.config.guild(guild).president_id.set(None)
                    await self.config.guild(guild).term_started_at.set(None)
                    await self.config.guild(guild).term_ends_at.set(None)
            elif int(settings.get("vice_president_id") or 0) == member.id:
                await self.config.guild(guild).vice_president_id.set(None)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role) -> None:
        async with self._lock(role.guild.id):
            settings = await self.config.guild(role.guild).all()
            if settings.get("president_role_id") == role.id:
                await self.config.guild(role.guild).president_role_id.set(None)
            if settings.get("vice_president_role_id") == role.id:
                await self.config.guild(role.guild).vice_president_role_id.set(None)
            if settings.get("party_leader_role_id") == role.id:
                await self.config.guild(role.guild).party_leader_role_id.set(None)
            parties = settings.get("parties") or {}
            changed = False
            for party in parties.values():
                if party.get("role_id") == role.id:
                    party["role_id"] = None
                    changed = True
            if changed:
                await self.config.guild(role.guild).parties.set(parties)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel) -> None:
        async with self._lock(channel.guild.id):
            settings = await self.config.guild(channel.guild).all()
            group = self.config.guild(channel.guild)
            if int(settings.get("laws_channel_id") or 0) == channel.id:
                await group.laws_channel_id.set(None)
                await group.constitution_message_id.set(None)
                await group.law_message_ids.set({})
            if int(settings.get("party_channel_category_id") or 0) == channel.id:
                await group.party_channel_category_id.set(None)
            parties = settings.get("parties") or {}
            changed = False
            for party in parties.values():
                if int(party.get("channel_id") or 0) == channel.id:
                    party["channel_id"] = None
                    changed = True
            if changed:
                await group.parties.set(parties)

    @commands.Cog.listener()
    async def on_member_update(
        self, before: discord.Member, after: discord.Member
    ) -> None:
        added_role_ids = {role.id for role in after.roles} - {
            role.id for role in before.roles
        }
        if not added_role_ids:
            return
        parties = await self.config.guild(after.guild).parties()
        for party in parties.values():
            role_id = int(party.get("role_id") or 0)
            if role_id not in added_role_ids:
                continue
            if after.id in {int(uid) for uid in party.get("member_ids", [])}:
                continue
            role = after.guild.get_role(role_id)
            if role is not None:
                try:
                    await after.remove_roles(
                        role, reason="Private party roles are limited to party members"
                    )
                except discord.HTTPException:
                    log.warning(
                        "Could not remove unauthorized party role %s from %s",
                        role_id,
                        after.id,
                    )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if message.guild is None or message.author.bot:
            return
        laws_channel_id = await self.config.guild(message.guild).laws_channel_id()
        if message.channel.id != int(laws_channel_id or 0):
            return
        try:
            await message.delete()
        except discord.HTTPException:
            log.warning(
                "Could not remove a non-law message from current-laws channel %s",
                message.channel.id,
            )

    government = app_commands.Group(
        name="government", description="Server government and constitution"
    )
    party = app_commands.Group(
        name="party", description="Political party commands", parent=government
    )
    law = app_commands.Group(
        name="law", description="Propose and inspect laws", parent=government
    )
    admin = app_commands.Group(
        name="admin", description="Government administration", parent=government
    )

    @government.command(
        name="constitution", description="Display the founding constitution"
    )
    async def constitution(self, interaction: discord.Interaction) -> None:
        embed = self._founding_constitution_embed()
        if interaction.guild is not None:
            laws = await self.config.guild(interaction.guild).laws()
            amendments = [
                f"Law {law_id}: {law['title']}"
                for law_id, law in laws.items()
                if law.get("kind") == "amendment"
                and law.get("status") == "enacted"
                and law.get("source") != "founding_constitution"
            ]
            if amendments:
                embed.add_field(
                    name="Ratified Amendments",
                    value="\n".join(amendments[-8:]),
                    inline=False,
                )
        await interaction.response.send_message(embed=embed)

    @government.command(
        name="status", description="Show current offices and election status"
    )
    async def government_status(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        settings = await self.config.guild(guild).all()
        president = guild.get_member(int(settings.get("president_id") or 0))
        vice = guild.get_member(int(settings.get("vice_president_id") or 0))
        embed = discord.Embed(
            title="Government Status", colour=discord.Colour.blurple()
        )
        embed.add_field(
            name="President", value=president.mention if president else "Vacant"
        )
        embed.add_field(name="Vice President", value=vice.mention if vice else "Vacant")
        term_end = settings.get("term_ends_at")
        embed.add_field(
            name="Term",
            value=f"Ends <t:{term_end}:R>" if term_end else "No active term",
        )
        election = settings.get("active_election")
        embed.add_field(
            name="Election",
            value=(
                f"Poll `{election['poll_id']}` is open" if election else "None active"
            ),
        )
        embed.add_field(name="Parties", value=str(len(settings.get("parties") or {})))
        laws_channel = guild.get_channel(int(settings.get("laws_channel_id") or 0))
        embed.add_field(
            name="Current laws",
            value=(
                laws_channel.mention
                if isinstance(laws_channel, discord.TextChannel)
                else "Not configured"
            ),
        )
        enacted = sum(
            1
            for item in (settings.get("laws") or {}).values()
            if item.get("status") == "enacted"
        )
        embed.add_field(name="Enacted laws", value=str(enacted))
        await interaction.response.send_message(embed=embed)

    @party.command(name="create", description="Create a party and become its leader")
    @app_commands.describe(
        name="Party name",
        color="Hex color such as #5865F2",
        icon="PNG, JPEG, or WebP up to 256 KiB",
        slogan="Optional party slogan (up to 100 characters)",
        description="Optional party description (up to 500 characters)",
        manifesto="Optional party manifesto (up to 1,000 characters)",
    )
    async def party_create(
        self,
        interaction: discord.Interaction,
        name: str,
        color: str,
        icon: discord.Attachment,
        slogan: Optional[str] = None,
        description: Optional[str] = None,
        manifesto: Optional[str] = None,
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        try:
            clean_name = clean_party_name(name)
            color_value = parse_color(color)
            icon_data, image_type = await self._read_icon(icon)
            clean_slogan = clean_profile_field(
                slogan, label="Slogan", maximum=MAX_PARTY_SLOGAN
            )
            clean_description = clean_profile_field(
                description,
                label="Description",
                maximum=MAX_PARTY_DESCRIPTION,
            )
            clean_manifesto = clean_profile_field(
                manifesto, label="Manifesto", maximum=MAX_PARTY_MANIFESTO
            )
        except ValueError as exc:
            return await self._reply(interaction, str(exc))
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            if self._party_for_user(parties, interaction.user.id)[0] is not None:
                return await self._reply(
                    interaction, "You already belong to a party. Leave it first."
                )
            if any(
                p.get("name", "").casefold() == clean_name.casefold()
                for p in parties.values()
            ):
                return await self._reply(
                    interaction, "A party with that name already exists."
                )
            party_id = secrets.token_hex(4)
            icon_path = self._icon_path(guild.id, party_id, image_type)
            icon_path.write_bytes(icon_data)
            parties[party_id] = {
                "name": clean_name,
                "leader_id": interaction.user.id,
                "member_ids": [interaction.user.id],
                "color": color_value,
                "icon_path": str(icon_path),
                "slogan": clean_slogan or "",
                "description": clean_description or "",
                "manifesto": clean_manifesto or "",
                "role_id": None,
                "channel_id": None,
                "created_at": unix_now(),
            }
            await self.config.guild(guild).parties.set(parties)
        await self._reply(
            interaction,
            f"Created **{clean_name}** (`{party_id}`). Its permissionless role will be created at {MIN_PARTY_MEMBERS} members. Use `/government party edit` to update its profile.",
        )

    @party.command(name="join", description="Join a party; you may belong to only one")
    @app_commands.describe(party_name="Party name or ID")
    async def party_join(
        self, interaction: discord.Interaction, party_name: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not isinstance(interaction.user, discord.Member):
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            if self._party_for_user(parties, interaction.user.id)[0] is not None:
                return await self._reply(
                    interaction, "You already belong to a party. Leave it first."
                )
            party_id, selected = self._find_party(parties, party_name)
            if selected is None or party_id is None:
                return await self._reply(
                    interaction,
                    "That party does not exist. Use `/government party list`.",
                )
            selected.setdefault("member_ids", []).append(interaction.user.id)
            if not selected.get("leader_id"):
                selected["leader_id"] = interaction.user.id
            await self.config.guild(guild).parties.set(parties)
            role, warning = await self._ensure_party_role(guild, party_id, parties)
            _, leader_warning = await self._sync_party_leader_role(guild, parties)
            _, channel_warning = await self._ensure_party_channel(
                guild, party_id, parties
            )
            if role is not None and role not in interaction.user.roles:
                try:
                    await interaction.user.add_roles(
                        role, reason="Joined government party"
                    )
                except discord.HTTPException:
                    warning = "You joined, but I could not assign the party role."
            warning = warning or leader_warning or channel_warning
            message = f"You joined **{selected['name']}** ({len(selected['member_ids'])} members)."
            if warning:
                message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @party.command(name="leave", description="Leave your current party")
    async def party_leave(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None or not isinstance(interaction.user, discord.Member):
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._party_for_user(parties, interaction.user.id)
            if selected is None or party_id is None:
                return await self._reply(interaction, "You do not belong to a party.")
            members = [
                int(uid)
                for uid in selected.get("member_ids", [])
                if int(uid) != interaction.user.id
            ]
            selected["member_ids"] = members
            if int(selected.get("leader_id") or 0) == interaction.user.id:
                selected["leader_id"] = members[0] if members else None
            await self.config.guild(guild).parties.set(parties)
            await self._remove_role(interaction.user, selected.get("role_id"))
            _, warning = await self._sync_party_leader_role(guild, parties)
        message = f"You left **{selected['name']}**."
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @party.command(name="list", description="List all political parties")
    async def party_list(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        parties = await self.config.guild(guild).parties()
        if not parties:
            return await self._reply(
                interaction, "No parties have been formed yet.", ephemeral=False
            )
        lines = []
        ordered_parties = sorted(
            parties.items(), key=lambda pair: pair[1]["name"].casefold()
        )
        for party_id, item in ordered_parties[:25]:
            leader = guild.get_member(int(item.get("leader_id") or 0))
            active = (
                "active role"
                if guild.get_role(int(item.get("role_id") or 0))
                else "forming"
            )
            line = (
                f"**{item['name']}** (`{party_id}`) - {len(item.get('member_ids', []))} members, "
                f"leader: {leader.mention if leader else 'vacant'}, {active}"
            )
            slogan = item.get("slogan", "").strip()
            if slogan:
                line += f"\n*“{slogan[:60]}{'…' if len(slogan) > 60 else ''}”*"
            if len("\n".join(lines + [line])) > 3900:
                break
            lines.append(line)
        embed = discord.Embed(
            title="Political Parties",
            description="\n".join(lines),
            colour=discord.Colour.blurple(),
        )
        if len(lines) < len(ordered_parties):
            embed.set_footer(
                text=f"Showing {len(lines)} of {len(ordered_parties)} parties"
            )
        await interaction.response.send_message(embed=embed)

    @party.command(
        name="info", description="Show a party's icon, color, leader, and members"
    )
    @app_commands.describe(party_name="Party name or ID")
    async def party_info(
        self, interaction: discord.Interaction, party_name: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        parties = await self.config.guild(guild).parties()
        party_id, selected = self._find_party(parties, party_name)
        if party_id is None or selected is None:
            return await self._reply(
                interaction,
                "That party does not exist. Use `/government party list`.",
            )

        leader = guild.get_member(int(selected.get("leader_id") or 0))
        role = guild.get_role(int(selected.get("role_id") or 0))
        color_value = int(selected.get("color") or 0)
        member_ids = [int(user_id) for user_id in selected.get("member_ids", [])]
        member_lines = []
        for user_id in member_ids:
            member = guild.get_member(user_id)
            member_lines.append(
                member.mention
                if member is not None
                else f"Unknown member (`{user_id}`)"
            )

        embed = discord.Embed(
            title=selected["name"],
            description=(
                f"*“{selected['slogan']}”*" if selected.get("slogan") else None
            ),
            colour=discord.Colour(color_value),
        )
        embed.add_field(name="Color", value=f"`#{color_value:06X}`", inline=True)
        embed.add_field(
            name="Leader", value=leader.mention if leader else "Vacant", inline=True
        )
        embed.add_field(
            name="Party role",
            value=(
                role.mention
                if role is not None
                else f"Forms at {MIN_PARTY_MEMBERS} members"
            ),
            inline=True,
        )
        created_at = selected.get("created_at")
        embed.add_field(
            name="Founded",
            value=(
                f"<t:{int(created_at)}:D>" if created_at else "Before records began"
            ),
            inline=True,
        )
        if selected.get("description"):
            embed.add_field(name="About", value=selected["description"], inline=False)
        if selected.get("manifesto"):
            embed.add_field(name="Manifesto", value=selected["manifesto"], inline=False)

        chunks: List[str] = []
        current_lines: List[str] = []
        displayed = 0
        for line in member_lines:
            candidate = "\n".join(current_lines + [line])
            if len(candidate) > 1000 and current_lines:
                if len(chunks) >= 2:
                    break
                chunks.append("\n".join(current_lines))
                current_lines = [line]
            else:
                current_lines.append(line)
            displayed += 1
        if current_lines and len(chunks) < 3:
            chunks.append("\n".join(current_lines))
        if not chunks:
            chunks = ["No members"]
        for index, chunk in enumerate(chunks):
            embed.add_field(
                name=(
                    f"Members ({len(member_ids)})"
                    if index == 0
                    else "Members (continued)"
                ),
                value=chunk,
                inline=False,
            )
        embed.set_footer(
            text=(
                f"Party ID: {party_id} • Showing {displayed} of {len(member_ids)} members"
                if displayed < len(member_ids)
                else f"Party ID: {party_id}"
            )
        )

        icon_file: Optional[discord.File] = None
        icon_path = Path(selected.get("icon_path", ""))
        if icon_path.is_file():
            filename = f"party-icon{icon_path.suffix.lower()}"
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")
        if icon_file is not None:
            await interaction.response.send_message(
                embed=embed,
                file=icon_file,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        else:
            await interaction.response.send_message(
                embed=embed,
                allowed_mentions=discord.AllowedMentions.none(),
            )

    @party.command(
        name="edit", description="Change your party's appearance or public profile"
    )
    @app_commands.describe(
        color="New hex color",
        icon="New PNG, JPEG, or WebP icon",
        slogan="New slogan, or - to clear it",
        description="New description, or - to clear it",
        manifesto="New manifesto, or - to clear it",
    )
    async def party_edit(
        self,
        interaction: discord.Interaction,
        color: Optional[str] = None,
        icon: Optional[discord.Attachment] = None,
        slogan: Optional[str] = None,
        description: Optional[str] = None,
        manifesto: Optional[str] = None,
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        if all(
            value is None for value in (color, icon, slogan, description, manifesto)
        ):
            return await self._reply(
                interaction,
                "Provide a new color, icon, slogan, description, or manifesto.",
            )
        try:
            new_color = parse_color(color) if color is not None else None
            icon_result = await self._read_icon(icon) if icon is not None else None
            new_slogan = clean_profile_field(
                slogan, label="Slogan", maximum=MAX_PARTY_SLOGAN
            )
            new_description = clean_profile_field(
                description,
                label="Description",
                maximum=MAX_PARTY_DESCRIPTION,
            )
            new_manifesto = clean_profile_field(
                manifesto, label="Manifesto", maximum=MAX_PARTY_MANIFESTO
            )
        except ValueError as exc:
            return await self._reply(interaction, str(exc))
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._party_for_user(parties, interaction.user.id)
            if (
                selected is None
                or party_id is None
                or int(selected.get("leader_id") or 0) != interaction.user.id
            ):
                return await self._reply(
                    interaction, "Only your party's leader can edit it."
                )
            if new_color is not None:
                selected["color"] = new_color
            if new_slogan is not None:
                selected["slogan"] = new_slogan
            if new_description is not None:
                selected["description"] = new_description
            if new_manifesto is not None:
                selected["manifesto"] = new_manifesto
            if icon_result is not None:
                data, image_type = icon_result
                old_path = Path(selected["icon_path"])
                new_path = self._icon_path(guild.id, party_id, image_type)
                new_path.write_bytes(data)
                selected["icon_path"] = str(new_path)
                if old_path != new_path:
                    old_path.unlink(missing_ok=True)
            await self.config.guild(guild).parties.set(parties)
            warning = None
            if new_color is not None or icon_result is not None:
                _, warning = await self._ensure_party_role(guild, party_id, parties)
        message = f"Updated **{selected['name']}**."
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @party.command(name="transfer", description="Transfer leadership to a party member")
    async def party_transfer(
        self, interaction: discord.Interaction, member: discord.Member
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        if member.bot or member.id == interaction.user.id:
            return await self._reply(
                interaction, "Choose another human member of your party."
            )
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._party_for_user(parties, interaction.user.id)
            target_party_id, _ = self._party_for_user(parties, member.id)
            if (
                selected is None
                or party_id is None
                or int(selected.get("leader_id") or 0) != interaction.user.id
            ):
                return await self._reply(
                    interaction,
                    "Only your party's current leader can transfer leadership.",
                )
            if target_party_id != party_id:
                return await self._reply(
                    interaction, "The new leader must belong to your party."
                )
            election = await self.config.guild(guild).active_election()
            if election and any(
                candidate.get("party_id") == party_id
                for candidate in election.get("candidates", [])
            ):
                return await self._reply(
                    interaction,
                    "Leadership cannot change while the party is on an active ballot.",
                )
            selected["leader_id"] = member.id
            await self.config.guild(guild).parties.set(parties)
            _, warning = await self._sync_party_leader_role(guild, parties)
        message = (
            f"Transferred leadership of **{selected['name']}** to {member.mention}."
        )
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message, ephemeral=False)

    @party.command(name="disband", description="Permanently disband the party you lead")
    async def party_disband(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._party_for_user(parties, interaction.user.id)
            if (
                selected is None
                or party_id is None
                or int(selected.get("leader_id") or 0) != interaction.user.id
            ):
                return await self._reply(
                    interaction, "Only your party's leader can disband it."
                )
            election = await self.config.guild(guild).active_election()
            if election and any(
                item.get("party_id") == party_id
                for item in election.get("candidates", [])
            ):
                return await self._reply(
                    interaction,
                    "A party cannot disband while it is on an active election ballot.",
                )
            party_channel = guild.get_channel(int(selected.get("channel_id") or 0))
            if isinstance(party_channel, discord.TextChannel):
                try:
                    await party_channel.delete(reason="Government party disbanded")
                except discord.Forbidden:
                    return await self._reply(
                        interaction,
                        "I cannot delete the private party channel; check my permissions.",
                    )
            role = guild.get_role(int(selected.get("role_id") or 0))
            if role is not None:
                try:
                    await role.delete(reason="Government party disbanded")
                except discord.Forbidden:
                    return await self._reply(
                        interaction,
                        "I cannot delete the party role; check my role position.",
                    )
            Path(selected["icon_path"]).unlink(missing_ok=True)
            del parties[party_id]
            await self.config.guild(guild).parties.set(parties)
            _, warning = await self._sync_party_leader_role(guild, parties)
        message = f"Disbanded **{selected['name']}** and removed its role and private channel."
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @government.command(
        name="appoint", description="President: appoint the Vice President"
    )
    async def appoint(
        self, interaction: discord.Interaction, member: discord.Member
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if int(settings.get("president_id") or 0) != interaction.user.id:
                return await self._reply(
                    interaction,
                    "Only the sitting President can appoint the Vice President.",
                )
            if int(settings.get("term_ends_at") or 0) <= unix_now():
                return await self._reply(
                    interaction, "The presidential term has ended."
                )
            if member.bot or member.id == interaction.user.id:
                return await self._reply(
                    interaction, "Choose another human member of this server."
                )
            _, vice_role = await self._ensure_office_roles(guild)
            previous = guild.get_member(int(settings.get("vice_president_id") or 0))
            if previous is not None and previous.id != member.id:
                await self._remove_role(previous, vice_role.id)
            try:
                await member.add_roles(vice_role, reason="Appointed Vice President")
            except discord.Forbidden:
                return await self._reply(
                    interaction,
                    "I cannot assign the Vice President role; check my role position.",
                )
            await self.config.guild(guild).vice_president_id.set(member.id)
        await self._reply(
            interaction,
            f"Appointed {member.mention} as **Vice President**.",
            ephemeral=False,
        )

    @law.command(
        name="propose",
        description="President: begin a law's required 24-hour discussion",
    )
    @app_commands.choices(
        kind=[
            app_commands.Choice(
                name="Ordinary law (simple majority)", value="ordinary"
            ),
            app_commands.Choice(
                name="Constitutional amendment (two-thirds)", value="amendment"
            ),
        ]
    )
    async def law_propose(
        self,
        interaction: discord.Interaction,
        title: str,
        text: str,
        kind: app_commands.Choice[str],
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        title = " ".join(title.split())
        text = text.strip()
        if (
            not title
            or len(title) > MAX_LAW_TITLE
            or not text
            or len(text) > MAX_LAW_TEXT
        ):
            return await self._reply(
                interaction,
                f"Use a 1–{MAX_LAW_TITLE} character title and 1–{MAX_LAW_TEXT} character text.",
            )
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if int(settings.get("president_id") or 0) != interaction.user.id:
                return await self._reply(
                    interaction, "Only the sitting President can propose laws."
                )
            if int(settings.get("term_ends_at") or 0) <= unix_now():
                return await self._reply(
                    interaction, "The presidential term has ended."
                )
            try:
                law_id = await self._post_law_discussion(
                    guild,
                    settings,
                    proposer_id=interaction.user.id,
                    title=title,
                    text=text,
                    kind=kind.value,
                )
            except ValueError as exc:
                return await self._reply(interaction, str(exc))
            except discord.Forbidden:
                return await self._reply(
                    interaction, "I cannot post in the configured government channel."
                )
        await self._reply(
            interaction,
            f"Law {law_id} is now in public discussion. Its 24-hour vote will start automatically.",
        )

    @law.command(
        name="repeal",
        description="President: propose removing a currently enacted law",
    )
    async def law_repeal(
        self, interaction: discord.Interaction, law_id: str, reason: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        target_id = law_id.strip()
        reason = reason.strip()
        if not reason or len(reason) > MAX_LAW_TEXT - 150:
            return await self._reply(
                interaction,
                f"Give a repeal reason between 1 and {MAX_LAW_TEXT - 150} characters.",
            )
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if int(settings.get("president_id") or 0) != interaction.user.id:
                return await self._reply(
                    interaction,
                    "Only the sitting President can propose repealing a law.",
                )
            if int(settings.get("term_ends_at") or 0) <= unix_now():
                return await self._reply(
                    interaction, "The presidential term has ended."
                )
            laws = settings.get("laws") or {}
            target = laws.get(target_id)
            if target is None or target.get("status") != "enacted":
                return await self._reply(
                    interaction, "That law is not currently enacted."
                )
            if any(
                law.get("action") in {"repeal", "amend"}
                and str(law.get("target_law_id")) == target_id
                and law.get("status") in {"discussion", "voting"}
                for law in laws.values()
            ):
                return await self._reply(
                    interaction,
                    "That law already has an active amendment or repeal proposal.",
                )
            title = f"Repeal Law {target_id}: {target['title']}"[:MAX_LAW_TITLE]
            text = (
                f"This proposal would repeal Law {target_id}, "
                f"**{target['title']}**.\n\n**Reason:** {reason}"
            )
            try:
                repeal_id = await self._post_law_discussion(
                    guild,
                    settings,
                    proposer_id=interaction.user.id,
                    title=title,
                    text=text,
                    kind=target.get("kind", "ordinary"),
                    action="repeal",
                    target_law_id=target_id,
                )
            except ValueError as exc:
                return await self._reply(interaction, str(exc))
            except discord.Forbidden:
                return await self._reply(
                    interaction, "I cannot post in the configured government channel."
                )
        await self._reply(
            interaction,
            f"Repeal proposal Law {repeal_id} is now in public discussion. If it passes, Law {target_id} will be removed from current laws.",
        )

    @law.command(
        name="amend",
        description="President: propose replacing a currently enacted law",
    )
    async def law_amend(
        self,
        interaction: discord.Interaction,
        law_id: str,
        new_title: str,
        new_text: str,
    ) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        target_id = law_id.strip()
        new_title = " ".join(new_title.split())
        new_text = new_text.strip()
        if (
            not new_title
            or len(new_title) > MAX_LAW_TITLE
            or not new_text
            or len(new_text) > MAX_LAW_TEXT
        ):
            return await self._reply(
                interaction,
                f"Use a 1–{MAX_LAW_TITLE} character title and 1–{MAX_LAW_TEXT} character replacement text.",
            )
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if int(settings.get("president_id") or 0) != interaction.user.id:
                return await self._reply(
                    interaction,
                    "Only the sitting President can propose amending a law.",
                )
            if int(settings.get("term_ends_at") or 0) <= unix_now():
                return await self._reply(
                    interaction, "The presidential term has ended."
                )
            laws = settings.get("laws") or {}
            target = laws.get(target_id)
            if target is None or target.get("status") != "enacted":
                return await self._reply(
                    interaction, "That law is not currently enacted."
                )
            if any(
                law.get("action") in {"repeal", "amend"}
                and str(law.get("target_law_id")) == target_id
                and law.get("status") in {"discussion", "voting"}
                for law in laws.values()
            ):
                return await self._reply(
                    interaction,
                    "That law already has an active amendment or repeal proposal.",
                )
            try:
                amendment_id = await self._post_law_discussion(
                    guild,
                    settings,
                    proposer_id=interaction.user.id,
                    title=new_title,
                    text=new_text,
                    kind=target.get("kind", "ordinary"),
                    action="amend",
                    target_law_id=target_id,
                )
            except ValueError as exc:
                return await self._reply(interaction, str(exc))
            except discord.Forbidden:
                return await self._reply(
                    interaction, "I cannot post in the configured government channel."
                )
        await self._reply(
            interaction,
            f"Amendment proposal Law {amendment_id} is now in public discussion. If it passes, it will replace Law {target_id} in current laws.",
        )

    @law.command(name="list", description="List recent law proposals and outcomes")
    async def law_list(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        laws = await self.config.guild(guild).laws()
        if not laws:
            return await self._reply(
                interaction, "No laws have been proposed.", ephemeral=False
            )
        ordered = sorted(laws.items(), key=lambda pair: int(pair[0]), reverse=True)[:15]
        lines = [
            f"**{law_id}. {law['title']}** - `{law['status']}` ({law['kind']})"
            for law_id, law in ordered
        ]
        embed = discord.Embed(
            title="Law Register",
            description="\n".join(lines),
            colour=discord.Colour.blurple(),
        )
        await interaction.response.send_message(embed=embed)

    @law.command(name="show", description="Show the full text and status of a law")
    async def law_show(self, interaction: discord.Interaction, law_id: str) -> None:
        guild = self._guild(interaction)
        if guild is None:
            return await self._reply(
                interaction, "This command can only be used in a server."
            )
        law = (await self.config.guild(guild).laws()).get(law_id.strip())
        if law is None:
            return await self._reply(interaction, "That law does not exist.")
        embed = discord.Embed(
            title=f"Law {law_id}: {law['title']}",
            description=law["text"],
            colour=discord.Colour.blurple(),
        )
        embed.add_field(name="Status", value=law["status"])
        embed.add_field(name="Type", value=law["kind"])
        if law.get("approve_votes") is not None:
            embed.add_field(
                name="Vote",
                value=f"{law['approve_votes']} approve / {law['reject_votes']} reject",
                inline=False,
            )
        await interaction.response.send_message(embed=embed)

    @admin.command(
        name="setup", description="Set the government channel and create office roles"
    )
    async def admin_setup(
        self, interaction: discord.Interaction, channel: discord.TextChannel
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        if self._polls_api() is None:
            return await self._reply(
                interaction, "Load the `Polls` cog before setting up Government."
            )
        await interaction.response.defer(ephemeral=True, thinking=True)
        warnings: List[str] = []
        try:
            await self._ensure_office_roles(guild)
            _, warning = await self._sync_party_leader_role(
                guild, await self.config.guild(guild).parties()
            )
        except discord.HTTPException:
            return await self._reply(
                interaction,
                "I need Manage Roles and my role must be above the office roles.",
            )
        await self.config.guild(guild).channel_id.set(channel.id)
        seeded = await self._seed_default_laws(guild)
        if seeded:
            warnings.append(f"Added {seeded} amendable founding laws.")
        if warning:
            warnings.append(warning)
        laws_channel: Optional[discord.TextChannel] = None
        try:
            laws_channel = await self._make_laws_channel(guild, channel)
            await self._set_laws_channel_read_only(guild, laws_channel)
            dashboard_warning = await self._sync_current_laws(guild)
            if dashboard_warning:
                warnings.append(dashboard_warning)
        except discord.HTTPException:
            warnings.append(
                "I could not create/configure `current-laws`; grant me Manage Channels and run setup again."
            )
        message = (
            f"Government channel set to {channel.mention}; permissionless office "
            "roles are ready."
        )
        if laws_channel is not None:
            message += f" Current laws are mirrored in {laws_channel.mention}."
        if warnings:
            message += "\n" + "\n".join(f"⚠️ {item}" for item in warnings)
        await self._reply(interaction, message)

    @admin.command(
        name="set-laws-channel",
        description="Use an existing channel as the read-only current-laws register",
    )
    async def admin_set_laws_channel(
        self, interaction: discord.Interaction, channel: discord.TextChannel
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._seed_default_laws(guild)
        settings = await self.config.guild(guild).all()
        old_channel = guild.get_channel(int(settings.get("laws_channel_id") or 0))
        if isinstance(old_channel, discord.TextChannel):
            managed_message_ids = list((settings.get("law_message_ids") or {}).values())
            if settings.get("constitution_message_id"):
                managed_message_ids.append(settings["constitution_message_id"])
            for message_id in managed_message_ids:
                try:
                    await old_channel.get_partial_message(int(message_id)).delete()
                except discord.NotFound:
                    pass
                except discord.HTTPException:
                    log.exception(
                        "Could not remove old current-law message %s", message_id
                    )
        try:
            await self._set_laws_channel_read_only(guild, channel)
            await self.config.guild(guild).laws_channel_id.set(channel.id)
            await self.config.guild(guild).constitution_message_id.set(None)
            await self.config.guild(guild).law_message_ids.set({})
            warning = await self._sync_current_laws(guild)
        except discord.HTTPException:
            return await self._reply(
                interaction,
                "I could not configure that channel. Grant me Manage Channels, Send Messages, Embed Links, and Manage Messages.",
            )
        message = (
            f"{channel.mention} is now the read-only current-laws register. "
            "Existing enacted laws were backfilled."
        )
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @admin.command(
        name="set-party-category",
        description="Set the category used for private qualifying-party channels",
    )
    async def admin_set_party_category(
        self, interaction: discord.Interaction, category: discord.CategoryChannel
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        warnings: List[str] = []
        created_or_synced = 0
        async with self._lock(guild.id):
            await self.config.guild(guild).party_channel_category_id.set(category.id)
            parties = await self.config.guild(guild).parties()
            for party_id, party in parties.items():
                if len(party.get("member_ids", [])) < PARTY_CHANNEL_MIN_MEMBERS:
                    continue
                role, role_warning = await self._ensure_party_role(
                    guild, party_id, parties
                )
                if role_warning:
                    warnings.append(f"{party['name']}: {role_warning}")
                if role is None:
                    continue
                channel, channel_warning = await self._ensure_party_channel(
                    guild, party_id, parties
                )
                if channel is not None:
                    created_or_synced += 1
                if channel_warning:
                    warnings.append(f"{party['name']}: {channel_warning}")
        message = (
            f"Private party channels will be created in **{category.name}** once "
            f"a party has {PARTY_CHANNEL_MIN_MEMBERS} members. "
            f"Created or synchronized {created_or_synced} channel(s)."
        )
        if warnings:
            message += "\n" + "\n".join(f"⚠️ {item}" for item in warnings[:10])
        await self._reply(interaction, message)

    @admin.command(
        name="rename-party", description="Rename a party and its managed resources"
    )
    async def admin_rename_party(
        self, interaction: discord.Interaction, party_name: str, new_name: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        try:
            clean_name = clean_party_name(new_name)
        except ValueError as exc:
            return await self._reply(interaction, str(exc))
        await interaction.response.defer(ephemeral=True, thinking=True)
        warnings: List[str] = []
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._find_party(parties, party_name)
            if party_id is None or selected is None:
                return await self._reply(interaction, "That party does not exist.")
            if any(
                other_id != party_id
                and party.get("name", "").casefold() == clean_name.casefold()
                for other_id, party in parties.items()
            ):
                return await self._reply(
                    interaction, "A party with that name already exists."
                )
            election = await self.config.guild(guild).active_election()
            if election and any(
                candidate.get("party_id") == party_id
                for candidate in election.get("candidates", [])
            ):
                return await self._reply(
                    interaction,
                    "A party cannot be renamed while it is on an active ballot.",
                )
            old_name = selected["name"]
            selected["name"] = clean_name
            await self.config.guild(guild).parties.set(parties)

            role = guild.get_role(int(selected.get("role_id") or 0))
            if role is not None:
                try:
                    await role.edit(name=clean_name, reason="Party renamed by admin")
                except discord.HTTPException:
                    warnings.append("I could not rename the party role.")
            channel = guild.get_channel(int(selected.get("channel_id") or 0))
            if isinstance(channel, discord.TextChannel):
                try:
                    await channel.edit(
                        name=self._party_channel_name(clean_name, party_id),
                        topic=f"Private channel for {clean_name} • Party ID: {party_id}",
                        reason="Party renamed by admin",
                    )
                except discord.HTTPException:
                    warnings.append("I could not rename the private party channel.")
        message = f"Renamed **{old_name}** to **{clean_name}**."
        if warnings:
            message += "\n" + "\n".join(f"⚠️ {item}" for item in warnings)
        await self._reply(interaction, message)

    @admin.command(
        name="delete-party",
        description="Permanently delete a party and its managed resources",
    )
    async def admin_delete_party(
        self, interaction: discord.Interaction, party_name: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            parties = await self.config.guild(guild).parties()
            party_id, selected = self._find_party(parties, party_name)
            if party_id is None or selected is None:
                return await self._reply(interaction, "That party does not exist.")
            election = await self.config.guild(guild).active_election()
            if election and any(
                candidate.get("party_id") == party_id
                for candidate in election.get("candidates", [])
            ):
                return await self._reply(
                    interaction,
                    "A party cannot be deleted while it is on an active ballot.",
                )
            channel = guild.get_channel(int(selected.get("channel_id") or 0))
            try:
                if isinstance(channel, discord.TextChannel):
                    await channel.delete(reason="Party deleted by government admin")
                role = guild.get_role(int(selected.get("role_id") or 0))
                if role is not None:
                    await role.delete(reason="Party deleted by government admin")
            except discord.Forbidden:
                return await self._reply(
                    interaction,
                    "I cannot delete that party's role or channel. Check my permissions and role position.",
                )
            Path(selected["icon_path"]).unlink(missing_ok=True)
            del parties[party_id]
            await self.config.guild(guild).parties.set(parties)
            _, warning = await self._sync_party_leader_role(guild, parties)
        message = f"Deleted **{selected['name']}**, its role, and its private channel."
        if warning:
            message += f"\n⚠️ {warning}"
        await self._reply(interaction, message)

    @admin.command(
        name="start-election", description="Start a 24-hour presidential election"
    )
    async def admin_start_election(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        api = self._polls_api()
        if api is None:
            return await self._reply(interaction, "The Polls cog must be loaded.")
        await interaction.response.defer(ephemeral=True, thinking=True)
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if settings.get("active_election"):
                return await self._reply(
                    interaction, "A presidential election is already active."
                )
            term_end = int(settings.get("term_ends_at") or 0)
            if settings.get("president_id") and term_end > unix_now() + DAY_SECONDS:
                return await self._reply(
                    interaction,
                    f"The current term has more than 24 hours remaining (<t:{term_end}:R>).",
                )
            channel = guild.get_channel(int(settings.get("channel_id") or 0))
            if channel is None:
                return await self._reply(
                    interaction, "Run `/government admin setup` first."
                )
            candidates = []
            options = []
            for party_id, party in (settings.get("parties") or {}).items():
                leader = guild.get_member(int(party.get("leader_id") or 0))
                if (
                    len(party.get("member_ids", [])) < MIN_PARTY_MEMBERS
                    or leader is None
                    or leader.bot
                ):
                    continue
                candidates.append(
                    {
                        "party_id": party_id,
                        "leader_id": leader.id,
                        "party_name": party["name"],
                    }
                )
                options.append(party["name"])
            if len(candidates) < 2:
                return await self._reply(
                    interaction,
                    "At least two parties with five members and an active leader are required.",
                )
            if len(candidates) > 25:
                return await self._reply(
                    interaction,
                    "Discord polls support at most 25 options; reduce the qualified field before starting the election.",
                )
            poll = await api.create_poll(
                guild=guild,
                channel=channel,
                author_id=interaction.user.id,
                question="Presidential Election - choose a party",
                options=options,
                duration_seconds=DAY_SECONDS,
                hide_voters=True,
                hide_tally_until_close=True,
                max_choices=1,
            )
            await self.config.guild(guild).active_election.set(
                {
                    "poll_id": poll.id,
                    "started_at": unix_now(),
                    "ends_at": int(poll.closes_at.timestamp()),
                    "candidates": candidates,
                }
            )
        await self._reply(
            interaction,
            f"Presidential election `{poll.id}` started. Voting closes <t:{int(poll.closes_at.timestamp())}:R>.",
        )

    @admin.command(
        name="vacate",
        description="Vacate an office; the VP succeeds a vacated President",
    )
    @app_commands.choices(
        office=[
            app_commands.Choice(name="President", value="president"),
            app_commands.Choice(name="Vice President", value="vice"),
        ]
    )
    async def admin_vacate(
        self, interaction: discord.Interaction, office: app_commands.Choice[str]
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        async with self._lock(guild.id):
            settings = await self.config.guild(guild).all()
            if office.value == "vice":
                vice = guild.get_member(int(settings.get("vice_president_id") or 0))
                if vice:
                    await self._remove_role(
                        vice, settings.get("vice_president_role_id")
                    )
                await self.config.guild(guild).vice_president_id.set(None)
                message = "The Vice Presidency is now vacant."
            else:
                president = guild.get_member(int(settings.get("president_id") or 0))
                vice = guild.get_member(int(settings.get("vice_president_id") or 0))
                if president:
                    await self._remove_role(
                        president, settings.get("president_role_id")
                    )
                if vice:
                    president_role, vice_role = await self._ensure_office_roles(guild)
                    await self._remove_role(vice, vice_role.id)
                    await vice.add_roles(
                        president_role, reason="Presidential succession"
                    )
                    await self.config.guild(guild).president_id.set(vice.id)
                    await self.config.guild(guild).vice_president_id.set(None)
                    message = f"{vice.mention} succeeded to the Presidency for the remainder of the term."
                else:
                    await self.config.guild(guild).president_id.set(None)
                    await self.config.guild(guild).term_started_at.set(None)
                    await self.config.guild(guild).term_ends_at.set(None)
                    message = "The Presidency is now vacant; there was no Vice President to succeed."
        await self._reply(interaction, message, ephemeral=False)

    @admin.command(
        name="void-law",
        description="Void a proposal that conflicts with immutable safety rules",
    )
    async def admin_void_law(
        self, interaction: discord.Interaction, law_id: str, reason: str
    ) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        law_id = law_id.strip()
        async with self._lock(guild.id):
            laws = await self.config.guild(guild).laws()
            law = laws.get(law_id)
            if law is None:
                return await self._reply(interaction, "That law does not exist.")
            if law.get("status") in {"void", "rejected", "cancelled"}:
                return await self._reply(
                    interaction, f"That law is already `{law.get('status')}`."
                )
            poll_id = law.get("poll_id")
            law["status"] = "void"
            law["void_reason"] = reason[:1000]
            law["decided_at"] = unix_now()
            await self.config.guild(guild).laws.set(laws)
            api = self._polls_api()
            if poll_id and api:
                await api.close_poll(poll_id)
            dashboard_warning = await self._sync_current_laws(guild)
            channel = guild.get_channel(int(law.get("channel_id") or 0))
            if channel:
                await channel.send(
                    f"**Law {law_id}: {law['title']}** is automatically void under the Immutable Laws.\nReason: {reason[:1000]}",
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        message = f"Law {law_id} was marked void."
        if dashboard_warning:
            message += f"\n⚠️ {dashboard_warning}"
        await self._reply(interaction, message)

    @admin.command(
        name="reconcile", description="Repair and resync party and office roles"
    )
    async def admin_reconcile(self, interaction: discord.Interaction) -> None:
        guild = self._guild(interaction)
        if guild is None or not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        warnings: List[str] = []
        async with self._lock(guild.id):
            try:
                president_role, vice_role = await self._ensure_office_roles(guild)
                settings = await self.config.guild(guild).all()
                president = guild.get_member(int(settings.get("president_id") or 0))
                vice = guild.get_member(int(settings.get("vice_president_id") or 0))
                if president is not None and president_role not in president.roles:
                    await president.add_roles(
                        president_role, reason="Reconcile government office role"
                    )
                if vice is not None and vice_role not in vice.roles:
                    await vice.add_roles(
                        vice_role, reason="Reconcile government office role"
                    )
            except discord.HTTPException as exc:
                warnings.append(f"Office roles: {exc}")
            parties = await self.config.guild(guild).parties()
            for party_id in list(parties):
                _, warning = await self._ensure_party_role(guild, party_id, parties)
                if warning:
                    warnings.append(f"{parties[party_id]['name']}: {warning}")
                _, channel_warning = await self._ensure_party_channel(
                    guild, party_id, parties
                )
                if channel_warning:
                    warnings.append(
                        f"{parties[party_id]['name']} channel: {channel_warning}"
                    )
            _, warning = await self._sync_party_leader_role(guild, parties)
            if warning:
                warnings.append(f"Party Leader role: {warning}")
            laws_channel_id = await self.config.guild(guild).laws_channel_id()
            laws_channel = guild.get_channel(int(laws_channel_id or 0))
            if isinstance(laws_channel, discord.TextChannel):
                try:
                    await self._set_laws_channel_read_only(guild, laws_channel)
                    warning = await self._sync_current_laws(guild)
                    if warning:
                        warnings.append(warning)
                except discord.HTTPException as exc:
                    warnings.append(f"Current-laws channel: {exc}")
        message = "Role reconciliation complete."
        if warnings:
            message += "\n" + "\n".join(f"⚠️ {item}" for item in warnings)
        await self._reply(interaction, message)

    async def red_delete_data_for_user(self, *, requester: str, user_id: int) -> None:
        all_guilds = await self.config.all_guilds()
        for guild_id, settings in all_guilds.items():
            group = self.config.guild_from_id(int(guild_id))
            parties = settings.get("parties") or {}
            changed = False
            for party in parties.values():
                members = [
                    int(uid)
                    for uid in party.get("member_ids", [])
                    if int(uid) != user_id
                ]
                if members != party.get("member_ids", []):
                    party["member_ids"] = members
                    changed = True
                if int(party.get("leader_id") or 0) == user_id:
                    party["leader_id"] = members[0] if members else None
                    changed = True
            if changed:
                await group.parties.set(parties)
            if int(settings.get("president_id") or 0) == user_id:
                await group.president_id.set(None)
            if int(settings.get("vice_president_id") or 0) == user_id:
                await group.vice_president_id.set(None)
            election = settings.get("active_election")
            if election:
                election_changed = False
                for candidate in election.get("candidates", []):
                    if int(candidate.get("leader_id") or 0) == user_id:
                        candidate["leader_id"] = 0
                        election_changed = True
                if election_changed:
                    await group.active_election.set(election)
            history = settings.get("election_history") or []
            history_changed = False
            for record in history:
                if int(record.get("winner_id") or 0) == user_id:
                    record["winner_id"] = None
                    history_changed = True
                for candidate in record.get("candidates", []):
                    if int(candidate.get("leader_id") or 0) == user_id:
                        candidate["leader_id"] = 0
                        history_changed = True
            if history_changed:
                await group.election_history.set(history)
            laws = settings.get("laws") or {}
            laws_changed = False
            for law in laws.values():
                if int(law.get("proposer_id") or 0) == user_id:
                    law["proposer_id"] = 0
                    laws_changed = True
            if laws_changed:
                await group.laws.set(laws)
