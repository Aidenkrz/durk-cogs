import discord
import logging
import random
import re
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
CREDIT_POSITIVE_BASE = 2
CREDIT_NEGATIVE_BASE = -10

# Default score for timeout scaling (1000 = no multiplier)
DEFAULT_SCORE = 1000

# Placeholder hug GIF list — replace URLs as needed
HUG_GIFS = [
    "https://static.klipy.com/ii/d7aec6f6f171607374b2065c836f92f4/3a/73/47Uxa6Nl.gif",
    "https://static.klipy.com/ii/8ce8357c78ea940b9c2015daf05ce1a5/c0/c8/Gsu4wPlf.gif",
    "https://static.klipy.com/ii/35ccce3d852f7995dd2da910f2abd795/4a/74/bdx8ZIaf.gif",
    "https://static.klipy.com/ii/35ccce3d852f7995dd2da910f2abd795/f9/fd/lLT2zSIO.gif",
    "https://c.tenor.com/SW_VmrncNb0AAAAd/tenor.gif",
    "https://c.tenor.com/SYsRdiK-T7gAAAAd/tenor.gif",
    "https://i.pinimg.com/originals/d9/50/3a/d9503a7d49153bec0910daa5001c2491.gif",
    "https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/d193fc60-75d9-4ab1-81b8-7a3eddefef08/dh8ann7-68f0e8d6-7aa1-455b-915f-ee9f48c9bac4.gif?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1cm46YXBwOjdlMGQxODg5ODIyNjQzNzNhNWYwZDQxNWVhMGQyNmUwIiwiaXNzIjoidXJuOmFwcDo3ZTBkMTg4OTgyMjY0MzczYTVmMGQ0MTVlYTBkMjZlMCIsIm9iaiI6W1t7InBhdGgiOiIvZi9kMTkzZmM2MC03NWQ5LTRhYjEtODFiOC03YTNlZGRlZmVmMDgvZGg4YW5uNy02OGYwZThkNi03YWExLTQ1NWItOTE1Zi1lZTlmNDhjOWJhYzQuZ2lmIn1dXSwiYXVkIjpbInVybjpzZXJ2aWNlOmZpbGUuZG93bmxvYWQiXX0.fNzxobIah_69it-xirUVgx6p00WV0ybzR7XGoe3BcDU",
    "https://d.furaffinity.net/art/s0kz0/1740049376/1740049376.s0kz0_img_1417.gif",
    "https://c.tenor.com/EtBSbdnsA3YAAAAd/tenor.gif",
    "https://c.tenor.com/Y38iX9xrC6oAAAAd/tenor.gif",
    "https://c.tenor.com/HisAEulVSJoAAAAd/tenor.gif",
    "https://cdn.discordapp.com/attachments/1327397242633457747/1466751858835062896/z7rl4enf8czc1.gif?ex=697de29e&is=697c911e&hm=48bea8ecaa0dc865dc3df4d7eb7dcfe30613fa341ee4a1d3569684e2f9005a19&",

]

PILL_GIFS = [
    "https://static.klipy.com/ii/4e7bea9f7a3371424e6c16ebc93252fe/60/61/i4HwAV98a43Y7I6x.gif",
    "https://static.klipy.com/ii/4e7bea9f7a3371424e6c16ebc93252fe/7c/36/c2o3pC8HacN1uIO4Deh.gif",
    "https://static.klipy.com/ii/d7aec6f6f171607374b2065c836f92f4/7f/70/bty9591L.gif",
    "https://static.klipy.com/ii/c3a19a0b747a76e98651f2b9a3cca5ff/72/de/NcUaMaVV.gif",
    "https://static.klipy.com/ii/35ccce3d852f7995dd2da910f2abd795/07/65/gRFZybj7.gif",
    "https://static.klipy.com/ii/4e7bea9f7a3371424e6c16ebc93252fe/2a/cb/hPIxV11Q9kLuHP.gif",
    "https://flipanim.com/gif/w/c/WcBuCNmp.gif",
    "https://media.tenor.com/QHbRuht9SswAAAAM/pills-bilelaca.gif",
]

POSITIVE_REACTIONS = {
    "😀",
    "😃",
    "😄",
    "😁",
    "😊",
    "😇",
    "🙃",
    "😉",
    "😌",
    "😍",
    "🥰",
    "😘",
    "😗",
    "😙",
    "😚",
    "🤗",
    "😻",
    "👍",
    "👍🏻",
    "👍🏼",
    "👍🏽",
    "👍🏾",
    "👍🏿",
    "🫶",
    "🫶🏻",
    "🫶🏼",
    "🫶🏽",
    "🫶🏾",
    "🫶🏿",
    "❤️",
    "🧡",
    "💛",
    "💚",
    "💙",
    "💜",
    "🤎",
    "🖤",
    "🤍",
    "💖",
    "💗",
    "💓",
    "💕",
    "💞",
    "💘",
    "💝",
    "💟",
}


class SocialCredit(commands.Cog):
    """Track social credit scores based on behavior and interactions."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=8675309424242)
        self.config.register_guild(
            hug_credit_given=CREDIT_HUG_GIVEN,
            hug_credit_received=CREDIT_HUG_RECEIVED,
            positive_sentiment_base=CREDIT_POSITIVE_BASE,
            negative_sentiment_base=CREDIT_NEGATIVE_BASE,
            role_thresholds={},  # {"role_id": {"threshold": int, "direction": "above"|"below"}}
            nickname_prefix=False,  # whether to prepend [score] to nicknames
            punishment_rules=[],
        )
        self.db: Optional[SocialCreditDatabase] = None
        self.reaction_cooldowns = {}  # guild_id_user_id: last_process_time
        self.message_cache = {}  # message_id: {'msg': message, 'time': timestamp}

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
        new_score = await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason=reason,
            target_user_id=target_user_id,
            guild_id=guild_id,
            channel_id=channel_id,
        )
        guild = self.bot.get_guild(guild_id) if guild_id else None
        if guild:
            member = guild.get_member(user_id)
            if member:
                await self._sync_member(member, new_score, nick=True, punish=True)
        return new_score

    async def get_user_credit(self, user_id: int) -> Optional[int]:
        """Get a user's credit score. Returns None if DB unavailable."""
        if not self.db:
            return None
        return await self.db.get_score(user_id)

    async def reward_positive_sentiment(
        self,
        user_id: int,
        guild_id: int,
        channel_id: int,
        compound_score: float = 0.0,
    ) -> Optional[int]:
        """Called by messagefilter when a message passes sentiment checks.

        The credit amount scales with how positive the compound score is.
        compound_score ranges 0.0 to 1.0 for positive messages.
        """
        if not self.db:
            return None
        guild = self.bot.get_guild(guild_id)
        base = (
            await self.config.guild(guild).positive_sentiment_base()
            if guild
            else CREDIT_POSITIVE_BASE
        )
        # Scale: compound 0.0 -> 1x base, compound 1.0 -> 3x base
        multiplier = 1.0 + 2.0 * max(0.0, min(1.0, compound_score))
        amount = max(1, int(base * multiplier))
        new_score = await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason="positive_sentiment",
            guild_id=guild_id,
            channel_id=channel_id,
        )
        if guild:
            member = guild.get_member(user_id)
            if member:
                await self._sync_member(member, new_score, nick=True, punish=False)
        return new_score

    async def penalize_negative_sentiment(
        self,
        user_id: int,
        guild_id: int,
        channel_id: int,
        compound_score: float = 0.0,
    ) -> Optional[int]:
        """Called by messagefilter when a message triggers a sentiment violation.

        The credit penalty scales with how negative the compound score is.
        compound_score ranges -1.0 to 0.0 for negative messages.
        """
        if not self.db:
            return None
        guild = self.bot.get_guild(guild_id)
        base = (
            await self.config.guild(guild).negative_sentiment_base()
            if guild
            else CREDIT_NEGATIVE_BASE
        )
        # Scale: compound 0.0 -> 1x base, compound -1.0 -> 3x base
        multiplier = 1.0 + 2.0 * max(0.0, min(1.0, abs(compound_score)))
        amount = min(-1, int(base * multiplier))
        new_score = await self.db.adjust_score(
            user_id=user_id,
            amount=amount,
            reason="negative_sentiment",
            guild_id=guild_id,
            channel_id=channel_id,
        )
        if guild:
            member = guild.get_member(user_id)
            if member:
                await self._sync_member(member, new_score, nick=True, punish=True)
        return new_score

    async def get_timeout_multiplier(self, user_id: int) -> float:
        """Return a timeout multiplier based on user's score.

        1000 (default) = 1.0x, 800 = 2.0x (scaled linearly: each 200 below
        default doubles the timeout).  Minimum multiplier is 0.25x (score >= 1300),
        maximum is 5.0x (score <= 0).
        """
        if not self.db:
            return 1.0
        score = await self.db.get_score(user_id)
        # Every 200 points below default adds 1.0x
        raw = 1.0 + (DEFAULT_SCORE - score) / 200.0
        return max(0.25, min(5.0, raw))

    # ── Role & nickname sync ──────────────────────────────────────────

    async def _sync_member(self, member: discord.Member, score: int, *, nick: bool = True, punish: bool = True):
        """Sync roles, nickname (optional), and punishments (optional) for a member after a score change."""
        await self._sync_roles(member, score)
        if nick:
            await self._sync_nickname(member, score)
        if punish:
            await self._sync_punishments(member, score)

    async def _sync_roles(self, member: discord.Member, score: int):
        """Add/remove roles based on score thresholds for this guild."""
        thresholds = await self.config.guild(member.guild).role_thresholds()
        if not thresholds:
            return

        for role_id_str, cfg in thresholds.items():
            role = member.guild.get_role(int(role_id_str))
            if not role:
                continue

            threshold = cfg["threshold"]
            direction = cfg["direction"]

            should_have = (
                (score >= threshold) if direction == "above"
                else (score <= threshold)
            )

            try:
                if should_have and role not in member.roles:
                    await member.add_roles(role, reason=f"Social credit score {score} meets threshold")
                elif not should_have and role in member.roles:
                    await member.remove_roles(role, reason=f"Social credit score {score} no longer meets threshold")
            except discord.Forbidden:
                pass

    async def _sync_nickname(self, member: discord.Member, score: int):
        """Prepend [rounded score] to the member's nickname if enabled, only on 50-point increments."""
        if not await self.config.guild(member.guild).nickname_prefix():
            return
        # Don't touch the guild owner's nickname (Discord doesn't allow it)
        if member.id == member.guild.owner_id:
            return

        rounded_score = round(score / 50.0) * 50
        current_nick = member.nick or ""
        current_prefix = self._get_current_prefix_score(current_nick)
        if current_prefix == rounded_score:
            return  # No change needed

        base_name = self._strip_score_prefix(member.display_name)
        new_nick = f"[{rounded_score}] {base_name}"
        # Discord nickname limit is 32 chars
        if len(new_nick) > 32:
            new_nick = new_nick[:32]
        # Only update if it actually changed (safety check)
        if member.nick != new_nick:
            try:
                await member.edit(nick=new_nick, reason=f"Social credit score update ({rounded_score})")
            except discord.Forbidden:
                pass

    async def _sync_punishments(self, member: discord.Member, score: int) -> None:
        """Apply punishments if score below thresholds."""
        if member.guild_permissions.administrator or member.id == member.guild.owner_id:
            return

        rules = await self.config.guild(member.guild).punishment_rules()
        if not rules:
            return

        for rule in rules:
            if score > rule["threshold"]:
                continue

            duration_td = self.parse_duration(rule["duration"])
            until = discord.utils.utcnow() + duration_td
            reason = f"Social credit {score} below {rule['threshold']} ({rule['action']} {rule['duration']})"

            try:
                if rule["action"].lower() == "timeout":
                    await member.timeout(until, reason=reason)
                elif rule["action"].lower() == "ban":
                    await member.ban(until=until, reason=reason, delete_message_days=0)
            except (discord.Forbidden, discord.HTTPException):
                pass

    @staticmethod
    def _get_current_prefix_score(nick: str) -> Optional[int]:
        """Extract current score prefix from nick, or None."""
        match = re.match(r"^\[(\d+)\]\s*", nick)
        return int(match.group(1)) if match else None

    @staticmethod
    def _strip_score_prefix(name: str) -> str:
        """Remove a leading [number] prefix from a name."""
        return re.sub(r"^\[\d+\]\s*", "", name)

    @staticmethod
    def parse_duration(dur_str: str) -> timedelta:
        """Parse duration string like '1h30m', '2d' to timedelta."""
        total_secs = 0.0
        for match in re.finditer(r"(\d+(?:\.\d+)?)([smhdwMy])", dur_str, re.I):
            num = float(match.group(1))
            unit = match.group(2).lower()
            mult = {
                "s": 1,
                "m": 60,
                "h": 3600,
                "d": 86400,
                "w": 604800,
                "M": 2629746,  # ~30.44 days
                "y": 31556952,  # ~365.25 days
            }.get(unit, 0)
            total_secs += num * mult
        return timedelta(seconds=total_secs)

    # ── Hug command ────────────────────────────────────────────────────

    @commands.command()
    @commands.guild_only()
    async def hug(self, ctx: commands.Context, target: discord.Member):
        """Hug another user! Both of you gain social credit. 24h cooldown per person."""
        if target.id == ctx.author.id:
            return await ctx.send("You can't hug yourself!")
        if target.bot:
            return await ctx.send("You can't hug a bot!")

        cooldown = await self.db.check_hug_cooldown(ctx.author.id)
        if cooldown is not None:
            last_hug = datetime.fromisoformat(cooldown)
            next_hug = last_hug + timedelta(hours=24)
            remaining = next_hug - datetime.utcnow()
            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes = remainder // 60
            return await ctx.send(
                f"You already hugged someone recently! "
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

        # Sync roles and nickname for both users (no punish on positive hug)
        await self._sync_member(ctx.author, new_author, nick=True, punish=False)
        await self._sync_member(target, new_target, nick=True, punish=False)

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

    # ── Pill command ───────────────────────────────────────────────────

    @commands.command()
    @commands.guild_only()
    async def takepills(self, ctx: commands.Context):
        """Take your happy pills! +50 credit, 4 hour cooldown."""
        cooldown = await self.db.check_pill_cooldown(ctx.author.id)
        if cooldown is not None:
            last_pill = datetime.fromisoformat(cooldown)
            next_pill = last_pill + timedelta(hours=4)
            remaining = next_pill - datetime.utcnow()
            hours, remainder = divmod(int(remaining.total_seconds()), 3600)
            minutes = remainder // 60
            return await ctx.send(
                f"You already took your pills recently! "
                f"Try again in {hours}h {minutes}m."
            )

        await self.db.record_pill(ctx.author.id)

        new_score = await self.db.adjust_score(
            user_id=ctx.author.id,
            amount=50,
            reason="took_pills",
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )

        # Sync roles and nickname (no punish on positive pills)
        await self._sync_member(ctx.author, new_score, nick=True, punish=False)

        gif = random.choice(PILL_GIFS)
        embed = discord.Embed(
            title=f"{ctx.author.display_name} took their happy pills!",
            description="You took your happy pills!",
            color=discord.Color.blue(),
        )
        embed.set_image(url=gif)
        embed.add_field(
            name="Score",
            value=f"{new_score} (+50)",
            inline=True,
        )
        embed.set_footer(text="Stay happy! Pills available once every 4 hours.")

        await ctx.send(embed=embed)

    # ── Credit commands ────────────────────────────────────────────────

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def credit(self, ctx: commands.Context, user: discord.Member = None):
        """Check your or another user's social credit score."""
        user = user or ctx.author
        score = await self.db.get_score(user.id)
        rank = await self.db.get_rank(user.id)
        multiplier = await self.get_timeout_multiplier(user.id)

        embed = discord.Embed(
            title=f"Social Credit: {user.display_name}",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Score", value=str(score), inline=True)
        embed.add_field(name="Rank", value=f"#{rank}", inline=True)
        embed.add_field(name="Timeout Multiplier", value=f"{multiplier:.2f}x", inline=True)
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
        await self.db.adjust_score(
            user_id=user.id,
            amount=0,
            reason="admin_set",
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
        )
        await self._sync_member(user, score, nick=True, punish=True)
        await ctx.send(
            f"Set {user.display_name}'s score to **{score}** (was {old_score}, delta {score - old_score:+d})."
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
        await self._sync_member(user, new_score, nick=True, punish=True)
        await ctx.send(
            f"Adjusted {user.display_name}'s score by **{amount:+d}**. New score: **{new_score}**"
        )

    # ── Role threshold commands ────────────────────────────────────────

    @credit.command(name="addrole")
    @commands.admin_or_permissions(administrator=True)
    async def credit_addrole(
        self,
        ctx: commands.Context,
        role: discord.Role,
        direction: str,
        threshold: int,
    ):
        """[Admin] Assign a role when a user's score is above or below a threshold.

        direction: "above" or "below"
        Example: .credit addrole @GoodCitizen above 1200
        Example: .credit addrole @Shamed below 500
        """
        direction = direction.lower()
        if direction not in ("above", "below"):
            return await ctx.send("Direction must be `above` or `below`.")

        async with self.config.guild(ctx.guild).role_thresholds() as thresholds:
            thresholds[str(role.id)] = {
                "threshold": threshold,
                "direction": direction,
            }

        await ctx.send(
            f"Role {role.mention} will be assigned when score is **{direction}** **{threshold}**."
        )

    @credit.command(name="removerole")
    @commands.admin_or_permissions(administrator=True)
    async def credit_removerole(self, ctx: commands.Context, role: discord.Role):
        """[Admin] Remove a role threshold."""
        async with self.config.guild(ctx.guild).role_thresholds() as thresholds:
            if str(role.id) in thresholds:
                del thresholds[str(role.id)]
                await ctx.send(f"Removed threshold for {role.mention}.")
            else:
                await ctx.send(f"{role.mention} doesn't have a threshold set.")

    @credit.command(name="roles")
    @commands.guild_only()
    async def credit_roles(self, ctx: commands.Context):
        """Show all configured role thresholds."""
        thresholds = await self.config.guild(ctx.guild).role_thresholds()

        embed = discord.Embed(
            title="Social Credit Role Thresholds",
            color=discord.Color.gold(),
        )

        if not thresholds:
            embed.description = "No role thresholds configured."
        else:
            lines = []
            for role_id_str, cfg in thresholds.items():
                role = ctx.guild.get_role(int(role_id_str))
                name = role.mention if role else f"Unknown ({role_id_str})"
                lines.append(
                    f"{name} — **{cfg['direction']}** **{cfg['threshold']}**"
                )
            embed.description = "\n".join(lines)

        await ctx.send(embed=embed)

    @credit.group(name="punish", invoke_without_command=True)
    @commands.guild_only()
    async def punish(self, ctx: commands.Context):
        """Manage punishment rules for low social credit scores."""
        rules = await self.config.guild(ctx.guild).punishment_rules()
        if not rules:
            await ctx.send("No punishment rules set.")
            return

        embed = discord.Embed(title="Social Credit Punishment Rules", color=discord.Color.red())
        lines = []
        for i, rule in enumerate(rules, 1):
            lines.append(f"`{i}.` **{rule['action']}** `{rule['duration']}` **under** `{rule['threshold']}`")
        embed.description = "\n".join(lines)
        await ctx.send(embed=embed)

    @punish.command(name="add")
    @commands.admin_or_permissions(administrator=True)
    async def punish_add(self, ctx: commands.Context, *, args: str):
        """Add a punishment rule. Format: timeout 1h under 800"""
        match = re.match(r"^(\w+)\s+(\S+)\s+under\s+(\d+)$", args.lower().strip())
        if not match:
            await ctx.send("**Usage:** `[p]credit punish add timeout 1h under 800` or `ban 1d under 600`")
            return

        action, duration, thresh_str = match.groups()
        try:
            threshold = int(thresh_str)
        except ValueError:
            await ctx.send("Threshold must be a number.")
            return

        if action not in ("timeout", "ban"):
            await ctx.send("Action must be `timeout` or `ban`.")
            return

        async with self.config.guild(ctx.guild).punishment_rules() as rules:
            rules.append({
                "action": action,
                "duration": duration,
                "threshold": threshold
            })

        await ctx.send(f"✅ Added punishment: `{action}` `{duration}` **under** `{threshold}`")

    @punish.command(name="remove")
    @commands.admin_or_permissions(administrator=True)
    async def punish_remove(self, ctx: commands.Context, index: int):
        """Remove a punishment rule by index."""
        async with self.config.guild(ctx.guild).punishment_rules() as rules:
            if 1 <= index <= len(rules):
                removed = rules.pop(index - 1)
                await ctx.send(f"✅ Removed: `{removed['action']}` `{removed['duration']}` under `{removed['threshold']}`")
            else:
                await ctx.send("❌ Invalid index.")

    @punish.command(name="clear")
    @commands.admin_or_permissions(administrator=True)
    async def punish_clear(self, ctx: commands.Context):
        """Clear all punishment rules."""
        await self.config.guild(ctx.guild).punishment_rules.set([])
        await ctx.send("✅ Cleared all punishment rules.")

    # ── Nickname prefix commands ──────────────────────────────────────

    @credit.command(name="nickname")
    @commands.admin_or_permissions(administrator=True)
    async def credit_nickname(self, ctx: commands.Context):
        """[Admin] Toggle showing [score] in nicknames."""
        current = await self.config.guild(ctx.guild).nickname_prefix()
        new_val = not current
        await self.config.guild(ctx.guild).nickname_prefix.set(new_val)
        state = "enabled" if new_val else "disabled"
        await ctx.send(f"Nickname score prefix **{state}**.")

    @credit.command(name="stripnicks")
    @commands.admin_or_permissions(administrator=True)
    async def credit_stripnicks(self, ctx: commands.Context):
        """[Admin] Remove [score] prefix from all member nicknames in this server."""
        count = 0
        for member in ctx.guild.members:
            if member.bot or member.id == ctx.guild.owner_id:
                continue
            if member.nick and re.match(r"^\[\d+\]\s*", member.nick):
                new_nick = self._strip_score_prefix(member.nick)
                try:
                    await member.edit(
                        nick=new_nick or None,
                        reason="Social credit nickname prefix removal",
                    )
                    count += 1
                except discord.Forbidden:
                    pass
        await ctx.send(f"Stripped score prefix from **{count}** nickname(s).")

    # ── Config commands ────────────────────────────────────────────────

    @credit.command(name="config")
    @commands.admin_or_permissions(administrator=True)
    async def credit_config(self, ctx: commands.Context):
        """[Admin] Show current credit configuration for this server."""
        hug_given = await self.config.guild(ctx.guild).hug_credit_given()
        hug_received = await self.config.guild(ctx.guild).hug_credit_received()
        pos = await self.config.guild(ctx.guild).positive_sentiment_base()
        neg = await self.config.guild(ctx.guild).negative_sentiment_base()
        nick = await self.config.guild(ctx.guild).nickname_prefix()

        embed = discord.Embed(
            title="Social Credit Configuration", color=discord.Color.gold()
        )
        embed.add_field(name="Hug Given", value=f"`+{hug_given}`", inline=True)
        embed.add_field(name="Hug Received", value=f"`+{hug_received}`", inline=True)
        embed.add_field(name="Nickname Prefix", value=f"`{'On' if nick else 'Off'}`", inline=True)
        embed.add_field(
            name="Positive Sentiment Base",
            value=f"`+{pos}` (scales 1x-3x with positivity)",
            inline=False,
        )
        embed.add_field(
            name="Negative Sentiment Base",
            value=f"`{neg}` (scales 1x-3x with negativity)",
            inline=False,
        )
        embed.add_field(
            name="Timeout Scaling",
            value="Score 1000 = 1.0x, 800 = 2.0x, 600 = 3.0x (min 0.25x, max 5.0x)",
            inline=False,
        )

        punish_rules = await self.config.guild(ctx.guild).punishment_rules()
        if punish_rules:
            lines = [f"{r['action']} {r['duration']} under {r['threshold']}" for r in punish_rules]
            embed.add_field(
                name="Punishment Rules",
                value="\n".join(lines),
                inline=False,
            )

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

        Keys: hug_given, hug_received, positive_base, negative_base
        """
        key_map = {
            "hug_given": "hug_credit_given",
            "hug_received": "hug_credit_received",
            "positive_base": "positive_sentiment_base",
            "negative_base": "negative_sentiment_base",
        }
        config_key = key_map.get(key)
        if not config_key:
            valid = ", ".join(f"`{k}`" for k in key_map)
            return await ctx.send(f"Invalid key. Valid keys: {valid}")

        await self.config.guild(ctx.guild).get_attr(config_key).set(value)
        await ctx.send(f"Set `{key}` to `{value}`.")


    async def _handle_reaction_credit(self, payload, amount: int, action: str) -> None:
        """Handle credit adjustment for positive reaction add/remove in filtered channels."""
        import time

        key = f"{payload.guild_id}_{payload.user_id}"
        now = time.time()
        if now - self.reaction_cooldowns.get(key, 0) < 5.0:  # 5s cooldown per user/guild
            return
        self.reaction_cooldowns[key] = now

        channel = self.bot.get_channel(payload.channel_id)
        if not channel or not isinstance(channel, discord.TextChannel):
            return

        emoji_str = str(payload.emoji)
        if emoji_str not in POSITIVE_REACTIONS:
            return  # Early exit: non-positive emoji

        guild = channel.guild
        reactor_member = guild.get_member(payload.user_id)
        if not reactor_member or reactor_member.bot:
            return

        msg_filter = self.bot.get_cog("MessageFilter")
        if not msg_filter or not self.db:
            return

        try:
            channels_cfg = await msg_filter.config.guild(guild).channels()
        except:
            channels_cfg = {}
        try:
            sentiment_cfg = await msg_filter.config.guild(guild).sentiment_channels()
        except:
            sentiment_cfg = {}
        filtered_ids = set(channels_cfg.keys()) | set(sentiment_cfg.keys())
        if str(channel.id) not in filtered_ids:
            return

        # Cache message to avoid rate limits
        cache_key = payload.message_id
        if cache_key in self.message_cache:
            cached = self.message_cache[cache_key]
            if time.time() - cached['time'] < 3600:  # 1 hour cache
                message = cached['msg']
            else:
                del self.message_cache[cache_key]
        if cache_key not in self.message_cache:
            try:
                message = await channel.fetch_message(payload.message_id)
                self.message_cache[cache_key] = {'msg': message, 'time': time.time()}
                if len(self.message_cache) > 500:  # Limit cache size
                    oldest_key = min(self.message_cache, key=lambda k: self.message_cache[k]['time'])
                    del self.message_cache[oldest_key]
            except (discord.NotFound, discord.HTTPException):
                return
        else:
            message = self.message_cache[cache_key]['msg']

        if message.author.id == payload.user_id or message.author.bot:
            return

        new_score = await self.db.adjust_score(
            user_id=payload.user_id,
            amount=amount,
            reason=f"{action}_reaction",
            target_user_id=message.author.id,
            guild_id=guild.id,
            channel_id=channel.id,
        )

        await self._sync_member(reactor_member, new_score, nick=False, punish=amount < 0)
        # Unified sync: no nick on reacts, punish only on retract (-1)


    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload):
        """Award 1 credit for adding a positive reaction to another user's message in a filtered channel."""
        if payload.user_id == self.bot.user.id:
            return
        await self._handle_reaction_credit(payload, 1, "positive")


    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload):
        """Remove 1 credit when a positive reaction is removed from a message in a filtered channel."""
        if payload.user_id == self.bot.user.id:
            return
        await self._handle_reaction_credit(payload, -1, "retracted")
