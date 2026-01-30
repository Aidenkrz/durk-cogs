import discord
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from redbot.core import commands, Config
from redbot.core.bot import Red

from .database import SocialCreditDatabase

log = logging.getLogger("red.DurkCogs.SocialCredit")

# Default credit adjustment values
CREDIT_HUG_GIVEN = 5
CREDIT_HUG_RECEIVED = 5
CREDIT_POSITIVE_SENTIMENT = 2
CREDIT_NEGATIVE_SENTIMENT = -10

HUG_GIFS = [
    "https://static.klipy.com/ii/d7aec6f6f171607374b2065c836f92f4/3a/73/47Uxa6Nl.gif,
    "https://static.klipy.com/ii/8ce8357c78ea940b9c2015daf05ce1a5/c0/c8/Gsu4wPlf.gif",
    "https://static.klipy.com/ii/35ccce3d852f7995dd2da910f2abd795/4a/74/bdx8ZIaf.gif",
    "https://static.klipy.com/ii/35ccce3d852f7995dd2da910f2abd795/f9/fd/lLT2zSIO.gif",
]


class SocialCredit(commands.Cog):
    """Track social credit scores based on behavior and interactions."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=8675309424242)
        self.config.register_guild(
            hug_credit_given=CREDIT_HUG_GIVEN,
            hug_credit_received=CREDIT_HUG_RECEIVED,
            positive_sentiment_credit=CREDIT_POSITIVE_SENTIMENT,
            negative_sentiment_credit=CREDIT_NEGATIVE_SENTIMENT,
        )
        self.db: Optional[SocialCreditDatabase] = None

    async def cog_load(self):
        db_path = Path(__file__).parent / "socialcredit.db"
        self.db = SocialCreditDatabase(db_path)
        await self.db.initialize()
        log.info("SocialCredit cog loaded.")

    async def cog_unload(self):
        if self.db:
            await self.db.close()
        log.info("SocialCredit cog unloaded.")

    # ── Public API for cross-cog use ───────────────────────────────────

    async def adjust_user_credit(
        self,
        user_id: int,
        amount: int,
        reason: str,
        target_user_id: int = None,
        guild_id: int = None,
        channel_id: int = None,
    ) -> Optional[int]:
        """Adjust a user's credit. Returns new score, or None if DB unavailable."""
        if not self.db:
            return None
        return await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason=reason,
            target_user_id=target_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
        )

    async def get_user_credit(self, user_id: int) -> Optional[int]:
        """Get a user's credit score. Returns None if DB unavailable."""
        if not self.db:
            return None
        return await self.db.get_score(user_id)

    async def reward_positive_sentiment(
        self, user_id: int, guild_id: int, channel_id: int
    ) -> Optional[int]:
        """Called by messagefilter when a message passes sentiment checks."""
        if not self.db:
            return None
        guild = self.bot.get_guild(guild_id)
        amount = (
            await self.config.guild(guild).positive_sentiment_credit()
            if guild
            else CREDIT_POSITIVE_SENTIMENT
        )
        return await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason="positive_sentiment",
            guild_id=guild_id,
            channel_id=channel_id,
        )

    async def penalize_negative_sentiment(
        self, user_id: int, guild_id: int, channel_id: int
    ) -> Optional[int]:
        """Called by messagefilter when a message triggers a sentiment violation."""
        if not self.db:
            return None
        guild = self.bot.get_guild(guild_id)
        amount = (
            await self.config.guild(guild).negative_sentiment_credit()
            if guild
            else CREDIT_NEGATIVE_SENTIMENT
        )
        return await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason="negative_sentiment",
            guild_id=guild_id,
            channel_id=channel_id,
        )

    # ── Hug command ────────────────────────────────────────────────────

    @commands.command()
    @commands.guild_only()
    async def hug(self, ctx: commands.Context, target: discord.Member):
        """Hug another user! Both of you gain social credit. 24h cooldown per person."""
        if target.id == ctx.author.id:
            return await ctx.send("You can't hug yourself!")
        if target.bot:
            return await ctx.send("You can't hug a bot!")

        cooldown = await self.db.check_hug_cooldown(ctx.author.id, target.id)
        if cooldown is not None:
            last_hug = datetime.fromisoformat(cooldown)
            next_hug = last_hug + timedelta(hours=24)
            remaining = next_hug - datetime.utcnow()
            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes = remainder // 60
            return await ctx.send(
                f"You already hugged {target.display_name} recently! "
                f"Try again in {hours}h {minutes}m."
            )

        await self.db.record_hug(ctx.author.id, target.id)

        hug_given = await self.config.guild(ctx.guild).hug_credit_given()
        hug_received = await self.config.guild(ctx.guild).hug_credit_received()

        new_author = await self.db.adjust_score(
            user_id=ctx.author.id,
            amount=hug_given,
            reason="hug_given",
            target_user_id=target.id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )
        new_target = await self.db.adjust_score(
            user_id=target.id,
            amount=hug_received,
            reason="hug_received",
            target_user_id=ctx.author.id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )

        gif = random.choice(HUG_GIFS)
        embed = discord.Embed(
            title=f"{ctx.author.display_name} hugged {target.display_name}!",
            color=discord.Color.pink(),
        )
        embed.set_image(url=gif)
        embed.add_field(
            name=ctx.author.display_name,
            value=f"Score: {new_author} (+{hug_given})",
            inline=True,
        )
        embed.add_field(
            name=target.display_name,
            value=f"Score: {new_target} (+{hug_received})",
            inline=True,
        )
        embed.set_footer(text="Spread the love! Hugs available once per person every 24 hours.")

        await ctx.send(embed=embed)

    # ── Credit commands ────────────────────────────────────────────────

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def credit(self, ctx: commands.Context, user: discord.Member = None):
        """Check your or another user's social credit score."""
        user = user or ctx.author
        score = await self.db.get_score(user.id)
        rank = await self.db.get_rank(user.id)

        embed = discord.Embed(
            title=f"Social Credit: {user.display_name}",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Score", value=str(score), inline=True)
        embed.add_field(name="Rank", value=f"#{rank}", inline=True)
        embed.set_thumbnail(url=user.display_avatar.url)

        await ctx.send(embed=embed)

    @credit.command(name="log")
    @commands.guild_only()
    async def credit_log(self, ctx: commands.Context, user: discord.Member = None):
        """View recent credit changes for a user."""
        user = user or ctx.author
        entries = await self.db.get_user_log(user.id, limit=10)

        embed = discord.Embed(
            title=f"Credit Log: {user.display_name}",
            color=discord.Color.blue(),
        )

        if not entries:
            embed.description = "No credit history yet."
        else:
            lines = []
            for entry in entries:
                sign = "+" if entry["amount"] > 0 else ""
                reason = entry["reason"].replace("_", " ").title()
                ts = entry["created_at"]
                target_id = entry["target_user_id"]
                if target_id:
                    target_user = self.bot.get_user(target_id)
                    target_name = target_user.display_name if target_user else f"User {target_id}"
                    lines.append(
                        f"`{sign}{entry['amount']}` {reason} (w/ {target_name}) - {ts}"
                    )
                else:
                    lines.append(f"`{sign}{entry['amount']}` {reason} - {ts}")
            embed.description = "\n".join(lines)

        await ctx.send(embed=embed)

    @credit.command(name="top")
    @commands.guild_only()
    async def credit_top(self, ctx: commands.Context):
        """View the social credit leaderboard."""
        entries = await self.db.get_leaderboard(limit=10)

        embed = discord.Embed(
            title="Social Credit Leaderboard",
            color=discord.Color.gold(),
        )

        if not entries:
            embed.description = "No scores recorded yet."
        else:
            lines = []
            for i, entry in enumerate(entries, 1):
                user = self.bot.get_user(entry["user_id"])
                name = user.display_name if user else f"User {entry['user_id']}"
                lines.append(f"**{i}.** {name} - {entry['score']}")
            embed.description = "\n".join(lines)

        await ctx.send(embed=embed)

    @credit.command(name="summary")
    @commands.guild_only()
    async def credit_summary(self, ctx: commands.Context, user: discord.Member = None):
        """View a breakdown of credit changes by category."""
        user = user or ctx.author
        summary = await self.db.get_log_summary(user.id)

        embed = discord.Embed(
            title=f"Credit Summary: {user.display_name}",
            color=discord.Color.blue(),
        )

        if not summary:
            embed.description = "No credit history yet."
        else:
            lines = []
            for reason, total in sorted(summary.items()):
                label = reason.replace("_", " ").title()
                sign = "+" if total > 0 else ""
                lines.append(f"**{label}:** {sign}{total}")
            embed.description = "\n".join(lines)

        await ctx.send(embed=embed)

    # ── Admin commands ─────────────────────────────────────────────────

    @credit.command(name="set")
    @commands.admin_or_permissions(administrator=True)
    async def credit_set(self, ctx: commands.Context, user: discord.Member, score: int):
        """[Admin] Set a user's credit score to an exact value."""
        old_score = await self.db.get_score(user.id)
        await self.db.set_score(user.id, score)
        delta = score - old_score
        await self.db.adjust_score(
            user_id=user.id,
            amount=0,
            reason="admin_set",
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )
        await ctx.send(
            f"Set {user.display_name}'s score to **{score}** (was {old_score}, delta {delta:+d})."
        )

    @credit.command(name="adjust")
    @commands.admin_or_permissions(administrator=True)
    async def credit_adjust(
        self, ctx: commands.Context, user: discord.Member, amount: int
    ):
        """[Admin] Adjust a user's credit score by an amount."""
        new_score = await self.db.adjust_score(
            user_id=user.id,
            amount=amount,
            reason="admin_adjust",
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )
        await ctx.send(
            f"Adjusted {user.display_name}'s score by **{amount:+d}**. New score: **{new_score}**"
        )

    @credit.command(name="config")
    @commands.admin_or_permissions(administrator=True)
    async def credit_config(self, ctx: commands.Context):
        """[Admin] Show current credit configuration for this server."""
        hug_given = await self.config.guild(ctx.guild).hug_credit_given()
        hug_received = await self.config.guild(ctx.guild).hug_credit_received()
        pos = await self.config.guild(ctx.guild).positive_sentiment_credit()
        neg = await self.config.guild(ctx.guild).negative_sentiment_credit()

        embed = discord.Embed(
            title="Social Credit Configuration", color=discord.Color.gold()
        )
        embed.add_field(name="Hug Given", value=f"`+{hug_given}`", inline=True)
        embed.add_field(name="Hug Received", value=f"`+{hug_received}`", inline=True)
        embed.add_field(name="Positive Sentiment", value=f"`+{pos}`", inline=True)
        embed.add_field(name="Negative Sentiment", value=f"`{neg}`", inline=True)
        await ctx.send(embed=embed)

    @credit.command(name="setconfig")
    @commands.admin_or_permissions(administrator=True)
    async def credit_setconfig(
        self,
        ctx: commands.Context,
        key: str,
        value: int,
    ):
        """[Admin] Set a credit config value.

        Keys: hug_given, hug_received, positive_sentiment, negative_sentiment
        """
        key_map = {
            "hug_given": "hug_credit_given",
            "hug_received": "hug_credit_received",
            "positive_sentiment": "positive_sentiment_credit",
            "negative_sentiment": "negative_sentiment_credit",
        }
        config_key = key_map.get(key)
        if not config_key:
            valid = ", ".join(f"`{k}`" for k in key_map)
            return await ctx.send(f"Invalid key. Valid keys: {valid}")

        await self.config.guild(ctx.guild).get_attr(config_key).set(value)
        await ctx.send(f"Set `{key}` to `{value}`.")
