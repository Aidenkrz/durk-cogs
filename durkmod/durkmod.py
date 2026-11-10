import logging
import time
from datetime import timedelta
from typing import Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.aidenkrz-cogs.durkmod")

# Discord caps timeouts at 28 days.
MAX_TIMEOUT = timedelta(days=28)
DEFAULT_DURATION = timedelta(minutes=1)

# Mimics Dyno's success embed.
SUCCESS_EMOJI = "\N{WHITE HEAVY CHECK MARK}"
SUCCESS_COLOR = discord.Color(0x43B581)

# Members with this role can use ban regardless of their permissions.
BAN_ROLE_ID = 1569422928586612848


def can_ban():
    async def predicate(ctx: commands.Context) -> bool:
        if not isinstance(ctx.author, discord.Member):
            return False
        if ctx.author.get_role(BAN_ROLE_ID):
            return True
        if ctx.author.guild_permissions.ban_members:
            return True
        return await ctx.bot.is_owner(ctx.author) or await ctx.bot.is_admin(ctx.author)

    return commands.check(predicate)


class DurkMod(commands.Cog):
    """
    Moderation commands.
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xD0C3A0D, force_registration=True)
        # user id -> unix expiry time, or None for a permanent filter
        self.config.register_guild(filtered={})

    async def red_delete_data_for_user(self, *, requester, user_id: int) -> None:
        for guild_id, data in (await self.config.all_guilds()).items():
            if str(user_id) in data.get("filtered", {}):
                async with self.config.guild_from_id(guild_id).filtered() as filtered:
                    filtered.pop(str(user_id), None)

    async def _active_filters(self, guild: discord.Guild) -> dict:
        """The guild's filters with expired ones pruned."""
        now = time.time()
        async with self.config.guild(guild).filtered() as filtered:
            for uid in [uid for uid, expires in filtered.items() if expires is not None and expires <= now]:
                del filtered[uid]
            return dict(filtered)

    async def is_filtered(self, member: discord.Member) -> bool:
        """Whether the member is on the active `filter` list. SentimentFilter calls this."""
        expires = (await self.config.guild(member.guild).filtered()).get(str(member.id), False)
        return expires is None or (expires is not False and expires > time.time())

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        await self._filter_if_targeted(message)

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        # Raw so edits to messages outside the cache are caught too.
        data = payload.data
        author_id = (data.get("author") or {}).get("id")
        # Link embeds loading also fire this event, but only real edits set edited_timestamp.
        if payload.guild_id is None or author_id is None or not data.get("edited_timestamp"):
            return
        guild = self.bot.get_guild(payload.guild_id)
        member = guild and guild.get_member(int(author_id))
        if member is None or not await self.is_filtered(member):
            return
        cached = payload.cached_message
        if cached is not None and cached.content == data.get("content", cached.content):
            return
        channel = guild.get_channel_or_thread(payload.channel_id)
        if channel is None:
            return
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.HTTPException:
            return
        await self._filter_if_targeted(message)

    async def _filter_if_targeted(self, message: discord.Message):
        if message.author.bot or not isinstance(message.author, discord.Member):
            return
        if not await self.is_filtered(message.author):
            return
        sentiment = self.bot.get_cog("SentimentFilter")
        if sentiment is None:
            return
        # SentimentFilter's own listener skips filtered users, so this is the only pass.
        ctx = await self.bot.get_context(message)
        if ctx.valid and ctx.command.cog is self:
            return
        await sentiment.filter_message(message, targeted=True)

    @commands.command()
    @commands.guild_only()
    @commands.bot_has_permissions(moderate_members=True)
    @can_ban()
    async def ban(
        self,
        ctx: commands.Context,
        member: discord.Member,
        duration: Optional[
            commands.TimedeltaConverter(
                minimum=timedelta(seconds=1),
                maximum=MAX_TIMEOUT,
                default_unit="minutes",
            )
        ] = None,
        *,
        reason: Optional[str] = None,
    ):
        """Ban a user for a duration (default 1 minute).

        Examples:
        - `[p]ban @User`
        - `[p]ban @User 1m`
        - `[p]ban @User 2h being annoying`
        - `[p]ban @User 3d`
        """
        if member == ctx.author:
            return await ctx.send("You can't ban yourself.")
        if member == ctx.guild.owner or member.guild_permissions.administrator:
            return await ctx.send("I can't ban that user.")
        if ctx.author != ctx.guild.owner and member.top_role >= ctx.author.top_role:
            return await ctx.send("You can't ban someone with an equal or higher role than you.")
        if member.top_role >= ctx.guild.me.top_role:
            return await ctx.send("I can't ban someone with an equal or higher role than me.")

        duration = duration or DEFAULT_DURATION
        audit_reason = f"{ctx.author} ({ctx.author.id}): {reason or 'No reason given'}"
        try:
            await member.timeout(duration, reason=audit_reason)
        except discord.HTTPException as e:
            log.warning("Failed to timeout %s in %s: %s", member.id, ctx.guild.id, e)
            return await ctx.send("I couldn't ban that user.")

        embed = discord.Embed(
            description=f"{SUCCESS_EMOJI} ***{discord.utils.escape_markdown(member.name)} was banned.***",
            color=SUCCESS_COLOR,
        )
        await ctx.send(embed=embed)

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def filter(
        self,
        ctx: commands.Context,
        member: discord.Member,
        duration: Optional[
            commands.TimedeltaConverter(minimum=timedelta(seconds=1), default_unit="minutes")
        ] = None,
    ):
        """Run a user's messages through the sentiment filter in every channel.

        Without a duration the filter lasts until removed. Running it on a user
        who is already filtered removes their filter.

        Examples:
        - `[p]filter @User`
        - `[p]filter @User 1d`
        """
        name = discord.utils.escape_markdown(member.name)
        key = str(member.id)
        await self._active_filters(ctx.guild)
        async with self.config.guild(ctx.guild).filtered() as filtered:
            if key in filtered:
                del filtered[key]
                text = f"No longer filtering {name}"
            else:
                filtered[key] = time.time() + duration.total_seconds() if duration else None
                text = f"Filtering {name}"
        await ctx.send(embed=discord.Embed(description=f"{SUCCESS_EMOJI} ***{text}***", color=SUCCESS_COLOR))

    @filter.command(name="list")
    async def filter_list(self, ctx: commands.Context):
        """Show who is being filtered."""
        filtered = await self._active_filters(ctx.guild)
        if not filtered:
            return await ctx.send(embed=discord.Embed(description="Nobody is being filtered.", color=SUCCESS_COLOR))
        lines = [
            f"<@{uid}> - " + (f"until <t:{int(expires)}:f> (<t:{int(expires)}:R>)" if expires else "until removed")
            for uid, expires in filtered.items()
        ]
        if self.bot.get_cog("SentimentFilter") is None:
            lines.append("\n**SentimentFilter isn't loaded, so these filters do nothing right now.**")
        await ctx.send(embed=discord.Embed(title="Filtered Users", description="\n".join(lines), color=SUCCESS_COLOR))
