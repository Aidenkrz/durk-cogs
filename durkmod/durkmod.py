import logging
from datetime import timedelta
from typing import Optional

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_timedelta

log = logging.getLogger("red.aidenkrz-cogs.durkmod")

# Discord caps timeouts at 28 days.
MAX_TIMEOUT = timedelta(days=28)

# Members with this role can use ban regardless of their permissions.
BAN_ROLE_ID = 241692808874950657


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

    @commands.command()
    @commands.guild_only()
    @commands.bot_has_permissions(moderate_members=True)
    @can_ban()
    async def ban(
        self,
        ctx: commands.Context,
        member: discord.Member,
        duration: commands.TimedeltaConverter(
            minimum=timedelta(seconds=1),
            maximum=MAX_TIMEOUT,
            default_unit="minutes",
        ),
        *,
        reason: Optional[str] = None,
    ):
        """Ban a user for a duration.

        Examples:
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

        audit_reason = f"{ctx.author} ({ctx.author.id}): {reason or 'No reason given'}"
        try:
            await member.timeout(duration, reason=audit_reason)
        except discord.HTTPException as e:
            log.warning("Failed to timeout %s in %s: %s", member.id, ctx.guild.id, e)
            return await ctx.send("I couldn't ban that user.")

        await ctx.send(f"{member.mention} banned for {humanize_timedelta(timedelta=duration)}.")
