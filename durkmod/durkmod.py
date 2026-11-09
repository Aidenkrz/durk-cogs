import logging
from datetime import timedelta
from typing import Optional

import discord
from redbot.core import commands
from redbot.core.bot import Red

log = logging.getLogger("red.aidenkrz-cogs.durkmod")

# Discord caps timeouts at 28 days.
MAX_TIMEOUT = timedelta(days=28)
DEFAULT_DURATION = timedelta(minutes=1)

# Mimics Dyno's success embed.
SUCCESS_EMOJI = "\N{WHITE HEAVY CHECK MARK}"
SUCCESS_COLOR = discord.Color(0x43B581)

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
