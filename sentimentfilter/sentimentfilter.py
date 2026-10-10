import logging
import time
from datetime import timedelta
from typing import Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_timedelta

from .jev import DEFAULT_ALLOW, DEFAULT_FLAG, DEFAULT_QUESTION, JevClient, JevError, Judgement

log = logging.getLogger("red.DurkCogs.SentimentFilter")

MAX_TIMEOUT_SECONDS = 28 * 24 * 60 * 60  # Discord's hard limit
LEGACY_IDENTIFIER = 1234567890  # MessageFilter's Config identifier
ERROR_LOG_INTERVAL = 300  # seconds between repeated API error warnings

GREEN, YELLOW, RED = 0x00FF00, 0xFFD700, 0xFF0000


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


class SentimentFilter(commands.Cog):
    """Delete negative or hostile messages, judged by TypeSafe Jev via OpenRouter."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x5E471E47, force_registration=True)
        self.config.register_global(legacy_imported=False)
        self.config.register_guild(
            channels={},  # channel id -> {"filtered_count": int}
            threshold=0.7,
            question=None,  # None means the jev.DEFAULT_* text
            flag=None,
            allow=None,
            timeout=30,
            silent=False,
            log_channel=None,
        )
        self.jev: Optional[JevClient] = None
        self._api_key: Optional[str] = None
        self._last_error_log = 0.0

    async def cog_load(self) -> None:
        await self._import_legacy_settings()
        await self._refresh_client()

    async def cog_unload(self) -> None:
        if self.jev:
            await self.jev.close()

    async def red_delete_data_for_user(self, **kwargs) -> None:
        return

    @commands.Cog.listener()
    async def on_red_api_tokens_update(self, service_name: str, api_tokens: dict):
        if service_name == "openrouter":
            await self._refresh_client()

    async def _refresh_client(self) -> None:
        key = (await self.bot.get_shared_api_tokens("openrouter")).get("api_key")
        if key == self._api_key:
            return
        if self.jev:
            await self.jev.close()
        self._api_key = key
        self.jev = JevClient(key) if key else None

    async def _import_legacy_settings(self) -> None:
        """One-time copy of the sentiment settings that used to live in MessageFilter.

        The old VADER/Detoxify thresholds have no Jev equivalent and are not copied.
        """
        if await self.config.legacy_imported():
            return
        legacy = Config.get_conf(None, identifier=LEGACY_IDENTIFIER, cog_name="MessageFilter")
        key_map = {
            "sentiment_channels": "channels",
            "sentiment_timeout": "timeout",
            "sentiment_silent": "silent",
            "log_channel": "log_channel",
        }
        imported = 0
        for guild_id, old in (await legacy.all_guilds()).items():
            if "sentiment_channels" not in old:
                continue
            guild_conf = self.config.guild_from_id(guild_id)
            for old_key, new_key in key_map.items():
                if old_key in old:
                    await guild_conf.get_attr(new_key).set(old[old_key])
            imported += 1
        if imported:
            log.info("Imported MessageFilter sentiment settings for %d guild(s)", imported)
        await self.config.legacy_imported.set(True)

    async def _judge(self, text: str, settings: dict) -> Judgement:
        return await self.jev.judge(
            text,
            question=settings["question"] or DEFAULT_QUESTION,
            flag=settings["flag"] or DEFAULT_FLAG,
            allow=settings["allow"] or DEFAULT_ALLOW,
        )

    # ── Listener ───────────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if self.jev is None or message.author.bot or not message.guild:
            return
        if not message.content.strip() or not isinstance(message.author, discord.Member):
            return

        settings = await self.config.guild(message.guild).all()
        # Threads inherit their parent channel's setting.
        channel = message.channel
        channel_id = channel.parent_id if isinstance(channel, discord.Thread) else channel.id
        if str(channel_id) not in settings["channels"]:
            return

        if message.author.guild_permissions.administrator:
            return
        if await self.bot.cog_disabled_in_guild(self, message.guild):
            return
        if await self.bot.is_automod_immune(message):
            return
        ctx = await self.bot.get_context(message)
        if ctx.valid and ctx.command.cog is self:
            return

        try:
            judgement = await self._judge(message.content, settings)
        except JevError as exc:
            # Fail open: an API outage shouldn't delete or punish anyone.
            now = time.monotonic()
            if now - self._last_error_log > ERROR_LOG_INTERVAL:
                self._last_error_log = now
                log.warning("Jev request failed, letting messages through: %s", exc)
            return

        social_credit = self.bot.get_cog("SocialCredit")
        if judgement.flagged_p >= settings["threshold"]:
            await self._enforce(message, judgement, settings, str(channel_id), social_credit)
        elif social_credit:
            await social_credit.reward_positive_sentiment(
                message.author.id,
                message.guild.id,
                message.channel.id,
                compound_score=judgement.positive,
            )

    async def _enforce(self, message, judgement: Judgement, settings: dict, channel_key: str, social_credit):
        """Delete, log, DM and time out, then dock social credit.

        Silent mode skips everything except the social credit penalty.
        """
        author = message.author
        if not settings["silent"]:
            try:
                await message.delete()
            except discord.NotFound:
                # Already removed (word filter, AutoMod, a moderator) - don't punish twice.
                return
            except discord.HTTPException as exc:
                log.warning("Could not delete message %s in %s: %s", message.id, message.channel.id, exc)
                return

            async with self.config.guild(message.guild).channels() as channels:
                if channel_key in channels:
                    channels[channel_key]["filtered_count"] = channels[channel_key].get("filtered_count", 0) + 1

            await self._send_log(message, judgement, settings)

            try:
                await author.send(
                    f"Your message in {message.channel.mention} was removed by the sentiment filter "
                    f"(confidence {judgement.flagged_p:.0%}, severity: {judgement.severity_label})",
                    delete_after=120,
                )
            except discord.HTTPException:
                pass

            if settings["timeout"] > 0:
                multiplier = 1.0
                if social_credit:
                    multiplier = await social_credit.get_timeout_multiplier(author.id)
                seconds = min(int(settings["timeout"] * multiplier), MAX_TIMEOUT_SECONDS)
                try:
                    await author.timeout(
                        timedelta(seconds=seconds),
                        reason=(
                            f"Sentiment filter in #{message.channel.name}: "
                            f"{judgement.flagged_p:.2f}, {judgement.severity_label} [x{multiplier:.1f}]"
                        ),
                    )
                except discord.HTTPException:
                    pass

        if social_credit:
            # Severity 0-4 maps onto SocialCredit's -1..0 compound scale.
            await social_credit.penalize_negative_sentiment(
                author.id,
                message.guild.id,
                message.channel.id,
                compound_score=-judgement.severity / 4,
            )

    async def _send_log(self, message: discord.Message, judgement: Judgement, settings: dict):
        log_channel = settings["log_channel"] and message.guild.get_channel(settings["log_channel"])
        if not log_channel:
            return

        embed = discord.Embed(
            color=RED,
            timestamp=message.created_at,
            description=_clip(
                f"**Message sent by {message.author.mention} removed in "
                f"{message.channel.mention}**\n{message.content}",
                4096,
            ),
        )
        embed.add_field(name="Confidence", value=f"{judgement.flagged_p:.2f} (cutoff {settings['threshold']})")
        embed.add_field(name="Severity", value=f"{judgement.severity_label} ({judgement.severity:.2f}/4)")
        embed.set_author(
            name=f"{message.author} ({message.author.id})",
            icon_url=message.author.display_avatar.url,
        )
        embed.set_footer(text=f"Message ID: {message.id}")

        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException:
            pass

    # ── Commands ───────────────────────────────────────────────────────

    @commands.group(aliases=["sentiment"])
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def sentimentfilter(self, ctx: commands.Context):
        """Manage the sentiment filter.

        Messages are judged by TypeSafe Jev through OpenRouter. The bot owner sets the key with
        `[p]set api openrouter api_key,<key>`; without it the filter does nothing.
        """

    @sentimentfilter.command(name="addchannel")
    async def sf_addchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Enable sentiment filtering on a channel (and its threads)."""
        async with self.config.guild(ctx.guild).channels() as channels:
            if str(channel.id) in channels:
                return await self._reply(ctx, "Already Enabled", f"{channel.mention} already has sentiment filtering", YELLOW)
            channels[str(channel.id)] = {"filtered_count": 0}
        await self._reply(ctx, "Channel Added", f"{channel.mention} will now filter negative messages")

    @sentimentfilter.command(name="removechannel")
    async def sf_removechannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Disable sentiment filtering on a channel."""
        async with self.config.guild(ctx.guild).channels() as channels:
            if channels.pop(str(channel.id), None) is None:
                return await self._reply(ctx, "Not Enabled", f"{channel.mention} didn't have sentiment filtering", YELLOW)
        await self._reply(ctx, "Channel Removed", f"Stopped sentiment filtering on {channel.mention}")

    @sentimentfilter.command(name="threshold")
    async def sf_threshold(self, ctx: commands.Context, confidence: float):
        """Set how sure Jev must be before a message is removed (0.0 to 1.0, default 0.7).

        Lower catches more but removes more innocent messages; higher is more lenient.
        """
        if not 0.0 < confidence <= 1.0:
            return await ctx.send("Threshold must be above 0.0 and at most 1.0")
        await self.config.guild(ctx.guild).threshold.set(confidence)
        await self._reply(ctx, "Threshold Updated", f"Messages Jev is at least `{confidence:.0%}` sure about will be removed")

    @sentimentfilter.command(name="question")
    async def sf_question(self, ctx: commands.Context, *, text: Optional[str] = None):
        """Set the yes/no question Jev answers for each message. Leave empty to reset."""
        await self._set_rule_text(ctx, "question", text, DEFAULT_QUESTION)

    @sentimentfilter.command(name="flag")
    async def sf_flag(self, ctx: commands.Context, *, text: Optional[str] = None):
        """Describe what should be removed (the "yes" answer). Leave empty to reset."""
        await self._set_rule_text(ctx, "flag", text, DEFAULT_FLAG)

    @sentimentfilter.command(name="allow")
    async def sf_allow(self, ctx: commands.Context, *, text: Optional[str] = None):
        """Describe what should be kept (the "no" answer). Leave empty to reset."""
        await self._set_rule_text(ctx, "allow", text, DEFAULT_ALLOW)

    async def _set_rule_text(self, ctx: commands.Context, key: str, text: Optional[str], default: str):
        if text and len(text) > 1000:
            return await ctx.send("Keep it under 1000 characters.")
        await self.config.guild(ctx.guild).get_attr(key).set(text or None)
        title = f"{key.title()} {'Updated' if text else 'Reset'}"
        await self._reply(ctx, title, _clip(text or default, 4000))

    @sentimentfilter.command(name="timeout")
    async def sf_timeout(self, ctx: commands.Context, seconds: int):
        """Set the base timeout in seconds for filtered users (0 disables).

        SocialCredit, if loaded, scales this by the user's score.
        """
        if not 0 <= seconds <= MAX_TIMEOUT_SECONDS:
            return await ctx.send(f"Timeout must be between 0 and {MAX_TIMEOUT_SECONDS} seconds (28 days)")
        await self.config.guild(ctx.guild).timeout.set(seconds)
        await self._reply(ctx, "Timeout Updated", f"Users will be timed out for `{seconds}` seconds")

    @sentimentfilter.command(name="silent")
    async def sf_silent(self, ctx: commands.Context):
        """Toggle silent mode: only adjust social credit, don't delete or time out."""
        silent = not await self.config.guild(ctx.guild).silent()
        await self.config.guild(ctx.guild).silent.set(silent)
        if silent:
            desc = "Messages will **not** be deleted or timed out. Social credit will still be adjusted."
        else:
            desc = "Messages will be deleted and users timed out as normal."
        await self._reply(ctx, f"Silent Mode {'Enabled' if silent else 'Disabled'}", desc)

    @sentimentfilter.command(name="logchannel")
    async def sf_logchannel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Set where removed messages are logged. Leave empty to stop logging."""
        await self.config.guild(ctx.guild).log_channel.set(channel.id if channel else None)
        if channel:
            await self._reply(ctx, "Log Channel Set", f"Filtered messages will be logged to {channel.mention}")
        else:
            await self._reply(ctx, "Logging Disabled", "Filtered messages will no longer be logged")

    @sentimentfilter.command(name="settings")
    async def sf_settings(self, ctx: commands.Context):
        """Show the current sentiment filter settings."""
        settings = await self.config.guild(ctx.guild).all()

        channel_lines = []
        for cid, data in settings["channels"].items():
            ch = ctx.guild.get_channel(int(cid))
            if ch:
                channel_lines.append(f"{ch.mention} ({data.get('filtered_count', 0)} filtered)")
        log_channel = settings["log_channel"] and ctx.guild.get_channel(settings["log_channel"])

        embed = discord.Embed(title="Sentiment Filter Settings", color=GREEN if self.jev else RED)
        embed.add_field(
            name="Status",
            value="Active (Jev via OpenRouter)" if self.jev
            else f"**Inactive** - no OpenRouter key. Bot owner: `{ctx.clean_prefix}set api openrouter api_key,<key>`",
            inline=False,
        )
        embed.add_field(name="Threshold", value=f"`{settings['threshold']}`")
        embed.add_field(name="Timeout", value=humanize_timedelta(seconds=settings["timeout"]) or "Disabled")
        embed.add_field(name="Silent Mode", value="On" if settings["silent"] else "Off")
        embed.add_field(name="Log Channel", value=log_channel.mention if log_channel else "None")
        for key, default in (("question", DEFAULT_QUESTION), ("flag", DEFAULT_FLAG), ("allow", DEFAULT_ALLOW)):
            suffix = "" if settings[key] else " (default)"
            embed.add_field(name=f"{key.title()}{suffix}", value=_clip(settings[key] or default, 1024), inline=False)
        embed.add_field(
            name="Channels",
            value=_clip("\n".join(channel_lines), 1024) if channel_lines else "None",
            inline=False,
        )
        await ctx.send(embed=embed)

    @sentimentfilter.command(name="test")
    async def sf_test(self, ctx: commands.Context, *, text: str):
        """Show how Jev would judge a message under this server's settings."""
        if self.jev is None:
            return await ctx.send(f"No OpenRouter key is set. Bot owner: `{ctx.clean_prefix}set api openrouter api_key,<key>`")

        settings = await self.config.guild(ctx.guild).all()
        async with ctx.typing():
            try:
                judgement = await self._judge(text, settings)
            except JevError as exc:
                return await ctx.send(f"Jev request failed: {exc}")

        flagged = judgement.flagged_p >= settings["threshold"]
        embed = discord.Embed(
            title="Sentiment Analysis",
            description=_clip(f"**Text:** {text}", 2000),
            color=RED if flagged else GREEN,
        )
        embed.add_field(name="Confidence", value=f"`{judgement.flagged_p:.2f}` (cutoff `{settings['threshold']}`)")
        embed.add_field(name="Severity", value=f"{judgement.severity_label} (`{judgement.severity:.2f}`/4)")
        embed.add_field(name="Positive Tone", value=f"`{judgement.positive:.2f}`")
        embed.add_field(name="Would Remove", value="Yes" if flagged else "No", inline=False)
        if judgement.cost is not None:
            embed.set_footer(text=f"Cost: ${judgement.cost:.6f}")
        await ctx.send(embed=embed)

    @staticmethod
    async def _reply(ctx: commands.Context, title: str, description: str, color: int = GREEN):
        await ctx.send(embed=discord.Embed(title=title, description=description, color=color))
