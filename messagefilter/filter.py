from redbot.core import commands, Config, checks
import asyncio
from collections import Counter
import discord
from datetime import datetime, timezone, timedelta
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize
from detoxify import Detoxify

nltk.download("vader_lexicon", quiet=True)
nltk.download("punkt_tab", quiet=True)

NEG_PROPORTION_THRESHOLD = 0.45


class MessageFilter(commands.Cog):
    """Automatically delete messages that don't contain required words and filter negative sentiment"""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        self.analyzer = SentimentIntensityAnalyzer()
        self.toxicity_model = Detoxify("original-small")
        default_guild = {
            "channels": {},
            "active": True,
            "sentiment_channels": {},
            "sentiment_threshold": -0.5,
            "sentiment_timeout": 30,
            "toxicity_threshold": 0.7,
            "sentiment_silent": False,
        }
        self.config.register_guild(**default_guild)

    # ── Word-filter channel management ─────────────────────────────────

    @commands.group()
    async def filter(self, ctx):
        """Manage message filtering"""
        pass

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def addchannel(self, ctx, channel: discord.TextChannel):
        async with self.config.guild(ctx.guild).channels() as channels:
            if str(channel.id) not in channels:
                channels[str(channel.id)] = {
                    "words": [],
                    "filtered_count": 0,
                    "word_usage": {},
                }
                embed = discord.Embed(
                    title="✅ Channel Added",
                    description=f"{channel.mention} will now filter messages",
                    color=0x00FF00,
                )
            else:
                embed = discord.Embed(
                    title="⚠️ Already Filtered",
                    description=f"{channel.mention} is already being monitored",
                    color=0xFFD700,
                )
            await ctx.send(embed=embed)

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def removechannel(self, ctx, channel: discord.TextChannel):
        async with self.config.guild(ctx.guild).channels() as channels:
            channel_id = str(channel.id)
            if channel_id in channels:
                del channels[channel_id]
                embed = discord.Embed(
                    title="✅ Channel Removed",
                    description=f"Stopped filtering {channel.mention}",
                    color=0x00FF00,
                )
            else:
                embed = discord.Embed(
                    title="⚠️ Not Filtered",
                    description=f"{channel.mention} wasn't being monitored",
                    color=0xFFD700,
                )
            await ctx.send(embed=embed)

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def addword(self, ctx, *, args: str):
        """Add required words to a channel's filter"""
        try:
            converter = commands.TextChannelConverter()
            channel, _, words_part = args.partition(" ")
            channel = await converter.convert(ctx, channel)
            words = [w.strip().lower() for w in words_part.split(",") if w.strip()]
        except commands.BadArgument:
            channel = ctx.channel
            words = [w.strip().lower() for w in args.split(",") if w.strip()]

        async with self.config.guild(ctx.guild).channels() as channels:
            channel_id = str(channel.id)
            self._migrate_channel(channels, channel_id)

            if channel_id not in channels:
                channels[channel_id] = {
                    "words": [],
                    "filtered_count": 0,
                    "word_usage": {},
                }

            channel_data = channels[channel_id]
            existing_words = channel_data["words"]
            added = [w for w in words if w not in existing_words]
            existing_words.extend(added)

            embed = discord.Embed(color=0x00FF00)
            if added:
                embed.title = f"✅ Added {len(added)} Words"
                embed.description = f"To {channel.mention}'s filter"
                embed.add_field(
                    name="New Words",
                    value=", ".join(f"`{w}`" for w in added) or "None",
                    inline=False,
                )
                embed.add_field(
                    name="Current Filter Words",
                    value=", ".join(f"`{w}`" for w in existing_words) or "None",
                    inline=False,
                )
            else:
                embed.title = "⏩ No Changes"
                embed.description = "All specified words were already in the filter"
                embed.color = 0xFFD700

            await ctx.send(embed=embed)

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def removeword(self, ctx, *, args: str):
        """Remove words from a channel's filter"""
        try:
            converter = commands.TextChannelConverter()
            channel, _, words_part = args.partition(" ")
            channel = await converter.convert(ctx, channel)
            words = [w.strip().lower() for w in words_part.split(",") if w.strip()]
        except commands.BadArgument:
            channel = ctx.channel
            words = [w.strip().lower() for w in args.split(",") if w.strip()]

        async with self.config.guild(ctx.guild).channels() as channels:
            channel_id = str(channel.id)
            self._migrate_channel(channels, channel_id)

            if channel_id not in channels:
                return await ctx.send(f"{channel.mention} is not being filtered")

            channel_data = channels[channel_id]
            required_words = channel_data["words"]
            removed = []

            for word in words:
                if word in required_words:
                    required_words.remove(word)
                    removed.append(word)
                    channel_data["word_usage"].pop(word, None)

            embed = discord.Embed(color=0x00FF00)
            if removed:
                embed.title = f"❌ Removed {len(removed)} Words"
                embed.description = f"From {channel.mention}'s filter"
                embed.add_field(
                    name="Removed Words",
                    value=", ".join(f"`{w}`" for w in removed) or "None",
                    inline=False,
                )
                if required_words:
                    embed.add_field(
                        name="Remaining Words",
                        value=", ".join(f"`{w}`" for w in required_words) or "None",
                        inline=False,
                    )
                else:
                    del channels[channel_id]
                    embed.add_field(
                        name="Channel Removed",
                        value="No words remaining in filter",
                        inline=False,
                    )
            else:
                embed.title = "⏩ No Changes"
                embed.description = "None of these words were in the filter"
                embed.color = 0xFFD700

            await ctx.send(embed=embed)

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def logchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel for logging filtered messages"""
        await self.config.guild(ctx.guild).log_channel.set(channel.id)
        await ctx.send(f"Filter logs will now be sent to {channel.mention}")

    @filter.command()
    async def list(self, ctx):
        """Show currently filtered channels and their required words"""
        channels = await self.config.guild(ctx.guild).channels()
        embed = discord.Embed(title="Filtered Channels", color=0x00FF00)

        for channel_id, channel_data in channels.items():
            if isinstance(channel_data, list):
                channel_data = {
                    "words": channel_data,
                    "filtered_count": 0,
                    "word_usage": {},
                }
            ch = ctx.guild.get_channel(int(channel_id))
            if ch and channel_data.get("words"):
                word_list = ", ".join(f"`{w}`" for w in channel_data["words"]) or "No words set"
                embed.add_field(
                    name=f"#{ch.name}",
                    value=f"Required words: {word_list}",
                    inline=False,
                )

        if not embed.fields:
            embed.description = "No channels being filtered"

        await ctx.send(embed=embed)

    @filter.command()
    async def stats(self, ctx, channel: discord.TextChannel = None):
        """Show filtering statistics for a channel"""
        channel = channel or ctx.channel
        channel_id = str(channel.id)

        channels = await self.config.guild(ctx.guild).channels()

        if channel_id in channels and isinstance(channels[channel_id], list):
            channels[channel_id] = {
                "words": channels[channel_id],
                "filtered_count": 0,
                "word_usage": {},
            }
            await self.config.guild(ctx.guild).channels.set(channels)

        channel_data = channels.get(channel_id, {})

        if not channel_data.get("words"):
            return await ctx.send(f"{channel.mention} is not being filtered")

        embed = discord.Embed(
            title=f"Filter Statistics for #{channel.name}",
            color=0x00FF00,
        )

        filtered_count = channel_data.get("filtered_count", 0)
        embed.add_field(name="🚫 Messages Filtered", value=str(filtered_count), inline=False)

        word_usage = channel_data.get("word_usage", {})
        if word_usage:
            sorted_words = sorted(word_usage.items(), key=lambda x: x[1], reverse=True)
            top_words = "\n".join(f"• `{word}`: {count} uses" for word, count in sorted_words[:5])
            embed.add_field(name="🏆 Top Filter Words", value=top_words, inline=False)
        else:
            embed.add_field(name="📊 Word Usage", value="No usage data collected yet", inline=False)

        await ctx.send(embed=embed)

    # ── Sentiment filter management ────────────────────────────────────

    @filter.group(name="sentiment")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment(self, ctx):
        """Manage sentiment-based filtering"""
        pass

    @sentiment.command(name="addchannel")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_addchannel(self, ctx, channel: discord.TextChannel):
        """Enable sentiment filtering on a channel"""
        async with self.config.guild(ctx.guild).sentiment_channels() as channels:
            channel_id = str(channel.id)
            if channel_id not in channels:
                channels[channel_id] = {"filtered_count": 0}
                embed = discord.Embed(
                    title="Sentiment Channel Added",
                    description=f"{channel.mention} will now filter negative messages",
                    color=0x00FF00,
                )
            else:
                embed = discord.Embed(
                    title="Already Enabled",
                    description=f"{channel.mention} already has sentiment filtering",
                    color=0xFFD700,
                )
            await ctx.send(embed=embed)

    @sentiment.command(name="removechannel")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_removechannel(self, ctx, channel: discord.TextChannel):
        """Disable sentiment filtering on a channel"""
        async with self.config.guild(ctx.guild).sentiment_channels() as channels:
            channel_id = str(channel.id)
            if channel_id in channels:
                del channels[channel_id]
                embed = discord.Embed(
                    title="Sentiment Channel Removed",
                    description=f"Stopped sentiment filtering on {channel.mention}",
                    color=0x00FF00,
                )
            else:
                embed = discord.Embed(
                    title="Not Enabled",
                    description=f"{channel.mention} didn't have sentiment filtering",
                    color=0xFFD700,
                )
            await ctx.send(embed=embed)

    @sentiment.command(name="threshold")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_threshold(self, ctx, score: float):
        """Set the minimum sentiment compound score (range: -1.0 to 0.0)

        Messages scoring below this value are considered negative and will be filtered.
        Default is -0.5. Lower values (e.g. -0.8) are more lenient, higher values (e.g. -0.2) are stricter.
        """
        if score < -1.0 or score > 0.0:
            return await ctx.send("Threshold must be between -1.0 and 0.0")
        await self.config.guild(ctx.guild).sentiment_threshold.set(score)
        embed = discord.Embed(
            title="Sentiment Threshold Updated",
            description=f"Messages with compound score below `{score}` will be filtered",
            color=0x00FF00,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="timeout")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_timeout(self, ctx, seconds: int):
        """Set how long (in seconds) to timeout users who send negative messages"""
        if seconds < 0 or seconds > 2419200:
            return await ctx.send("Timeout must be between 0 and 2419200 seconds (28 days)")
        await self.config.guild(ctx.guild).sentiment_timeout.set(seconds)
        embed = discord.Embed(
            title="Sentiment Timeout Updated",
            description=f"Users will be timed out for `{seconds}` seconds",
            color=0x00FF00,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="toxicitythreshold")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_toxicitythreshold(self, ctx, score: float):
        """Set the Detoxify toxicity threshold (range: -1.0 to 1.0)

        Messages with toxicity/threat/insult scores above this value are filtered.
        Default is 0.7. Lower values are stricter. Negative values mean messages must
        be toxic to stay (inverted mode).
        """
        if score < -1.0 or score > 1.0:
            return await ctx.send("Toxicity threshold must be between -1.0 and 1.0")
        await self.config.guild(ctx.guild).toxicity_threshold.set(score)
        embed = discord.Embed(
            title="Toxicity Threshold Updated",
            description=f"Messages with toxicity scores above `{score}` will be filtered",
            color=0x00FF00,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="silent")
    @commands.admin_or_permissions(administrator=True)
    async def sentiment_silent(self, ctx):
        """Toggle silent mode — only adjust social credit, don't delete messages or timeout users"""
        current = await self.config.guild(ctx.guild).sentiment_silent()
        new_val = not current
        await self.config.guild(ctx.guild).sentiment_silent.set(new_val)
        state = "enabled" if new_val else "disabled"
        desc = (
            "Messages will **not** be deleted or timed out. Social credit will still be adjusted."
            if new_val
            else "Messages will be deleted and users timed out as normal."
        )
        embed = discord.Embed(
            title=f"Silent Mode {state.title()}",
            description=desc,
            color=0x00FF00,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="settings")
    async def sentiment_settings(self, ctx):
        """Show current sentiment filter settings"""
        threshold = await self.config.guild(ctx.guild).sentiment_threshold()
        tox_threshold = await self.config.guild(ctx.guild).toxicity_threshold()
        timeout_secs = await self.config.guild(ctx.guild).sentiment_timeout()
        channels = await self.config.guild(ctx.guild).sentiment_channels()

        channel_list = []
        for cid, data in channels.items():
            ch = ctx.guild.get_channel(int(cid))
            if ch:
                count = data.get("filtered_count", 0)
                channel_list.append(f"{ch.mention} ({count} filtered)")

        embed = discord.Embed(title="Sentiment Filter Settings", color=0x00FF00)
        embed.add_field(name="VADER Threshold", value=f"`{threshold}`", inline=True)
        embed.add_field(name="Toxicity Threshold", value=f"`{tox_threshold}`", inline=True)
        embed.add_field(name="Timeout", value=f"`{timeout_secs}s`", inline=True)
        embed.add_field(
            name="Neg-proportion Threshold",
            value=f"`{NEG_PROPORTION_THRESHOLD}`",
            inline=True,
        )
        embed.add_field(
            name="Channels",
            value="\n".join(channel_list) if channel_list else "None",
            inline=False,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="test")
    async def sentiment_test(self, ctx, *, text: str):
        """Test the sentiment score of a message against all detection layers

        Mirrors the runtime pipeline: VADER (clauses + dedup + neg-proportion)
        runs first. Detoxify only runs if VADER did not trigger.
        """
        threshold = await self.config.guild(ctx.guild).sentiment_threshold()
        tox_threshold = await self.config.guild(ctx.guild).toxicity_threshold()

        deduped = self._deduplicate_text(text)
        clauses = self._split_clauses(text)

        # ── Layer 1: VADER ────────────────────────────────────────────
        vader_triggered = False
        sentence_lines = []

        # Per-clause scoring
        for clause in clauses:
            s = self.analyzer.polarity_scores(clause)
            flag = s["compound"] < threshold
            if flag:
                vader_triggered = True
            marker = "X" if flag else "-"
            sentence_lines.append(f"`[{marker}]` {s['compound']:+.4f} | {clause}")

        # Deduped full-text scoring
        deduped_scores = self.analyzer.polarity_scores(deduped)
        deduped_flag = deduped_scores["compound"] < threshold
        if deduped_flag:
            vader_triggered = True
        deduped_marker = "X" if deduped_flag else "-"
        sentence_lines.append(
            f"`[{deduped_marker}]` {deduped_scores['compound']:+.4f} | (deduped) {deduped}"
        )

        # Neg-proportion check on deduped text
        neg_prop = deduped_scores["neg"]
        neg_flag = neg_prop >= NEG_PROPORTION_THRESHOLD
        if neg_flag:
            vader_triggered = True
        neg_marker = "X" if neg_flag else "-"
        sentence_lines.append(
            f"`[{neg_marker}]` neg={neg_prop:.4f} (threshold {NEG_PROPORTION_THRESHOLD}) | neg-proportion check"
        )

        # ── Layer 2: Detoxify (only if VADER passed) ─────────────────
        detox_triggered = False
        tox = None
        if not vader_triggered:
            loop = asyncio.get_event_loop()
            tox = await loop.run_in_executor(None, self.toxicity_model.predict, text)
            detox_triggered = (
                tox["toxicity"] > tox_threshold
                or tox["threat"] > tox_threshold
                or tox["insult"] > tox_threshold
                or tox["severe_toxicity"] > tox_threshold
            )

        would_filter = vader_triggered or detox_triggered

        embed = discord.Embed(
            title="Sentiment Analysis",
            description=f"**Text:** {text}",
            color=0xFF0000 if would_filter else 0x00FF00,
        )

        embed.add_field(
            name=f"VADER (per-clause + deduped + neg-prop, threshold: {threshold})",
            value="\n".join(sentence_lines) or "No clauses",
            inline=False,
        )

        if tox is not None:
            embed.add_field(
                name=f"Detoxify (threshold: {tox_threshold})",
                value=(
                    f"Toxicity: `{tox['toxicity']:.4f}` | "
                    f"Threat: `{tox['threat']:.4f}` | "
                    f"Insult: `{tox['insult']:.4f}` | "
                    f"Severe: `{tox['severe_toxicity']:.4f}`"
                ),
                inline=False,
            )
        else:
            embed.add_field(
                name=f"Detoxify (threshold: {tox_threshold})",
                value="Skipped — VADER already triggered",
                inline=False,
            )

        triggered_by = []
        if vader_triggered:
            triggered_by.append("VADER")
        if detox_triggered:
            triggered_by.append("Detoxify")
        verdict = "Yes" if would_filter else "No"
        if triggered_by:
            verdict += f" (triggered by: {', '.join(triggered_by)})"
        embed.add_field(name="Would Filter", value=verdict, inline=False)

        await ctx.send(embed=embed)

    # ── Misc commands ──────────────────────────────────────────────────

    @commands.command()
    async def ILOVEWARRIORS(self, ctx):
        """Grants the Warrior role"""
        role_id = 1351752263793774683
        role = ctx.guild.get_role(role_id)

        if not role:
            return await ctx.send("❌ Warrior role not found")

        if role in ctx.author.roles:
            return await ctx.send("Youre already a warrior!")

        try:
            await ctx.author.add_roles(role)
            await ctx.send("You've become a warrior! Welcome to the clan.")
        except discord.Forbidden:
            await ctx.send("❌ I don't have permissions to assign roles")

    # ── Event listener ─────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message):
        await self._check_message(message)

    async def _check_message(self, message):
        if message.author.bot or not message.guild:
            return

        if message.channel.permissions_for(message.author).manage_messages:
            return

        prefixes = await self.bot.get_valid_prefixes(message.guild)
        content = message.content.lower().strip()
        for prefix in prefixes:
            if content.startswith(prefix.lower()):
                cmd = content[len(prefix) :].strip()
                if cmd.startswith("filter") or cmd.startswith("ilovewarriors"):
                    return

        deleted = await self._check_word_filter(message)
        if not deleted:
            await self._check_sentiment(message)

    # ── Word filter runtime ────────────────────────────────────────────

    async def _check_word_filter(self, message):
        """Run the word-based filter. Returns True if the message was deleted."""
        async with self.config.guild(message.guild).channels() as channels:
            channel_id = str(message.channel.id)
            if channel_id not in channels:
                return False

            self._migrate_channel(channels, channel_id)
            channel_data = channels[channel_id]
            required_words = channel_data.get("words", [])

            if not required_words:
                return False

            cleaned = self._strip_markdown(message.content)
            match_found = False

            for word in required_words:
                if self._wildcard_to_regex(word).search(cleaned):
                    channel_data["word_usage"][word] = channel_data["word_usage"].get(word, 0) + 1
                    match_found = True
                    break

            if match_found:
                return False

            try:
                await message.delete()
                await self._log_filtered_message(message)
                channel_data["filtered_count"] = channel_data.get("filtered_count", 0) + 1

                try:
                    word_list = ", ".join(f"`{w}`" for w in required_words)
                    await message.author.send(
                        f"Your message in {message.channel.mention} was filtered because "
                        f"it did not contain one of the following words: {word_list}",
                        delete_after=120,
                    )
                except discord.Forbidden:
                    pass

                try:
                    await message.author.timeout(
                        timedelta(seconds=20),
                        reason=f"Filter violation in #{message.channel.name}",
                    )
                except discord.Forbidden:
                    pass
            except discord.HTTPException:
                pass

            return True

    # ── Sentiment filter runtime ───────────────────────────────────────

    async def _check_sentiment(self, message):
        """Run layered sentiment filtering.

        Order:
        1. VADER — clause-level scoring, deduped full-text scoring, neg-proportion.
        2. Detoxify — only reached when VADER does not trigger.
        """
        channel_id = str(message.channel.id)
        sentiment_channels = await self.config.guild(message.guild).sentiment_channels()

        if channel_id not in sentiment_channels:
            return

        cleaned = self._strip_markdown(message.content)
        if not cleaned:
            return

        threshold = await self.config.guild(message.guild).sentiment_threshold()

        # Layer 1a: clause-level VADER
        clauses = self._split_clauses(cleaned)
        for clause in clauses:
            scores = self.analyzer.polarity_scores(clause)
            if scores["compound"] < threshold:
                await self._handle_sentiment_violation(
                    message, scores, layer="VADER", detail=f"Clause: {clause}"
                )
                return

        # Layer 1b: deduped full-text VADER
        deduped = self._deduplicate_text(cleaned)
        deduped_scores = self.analyzer.polarity_scores(deduped)
        if deduped_scores["compound"] < threshold:
            await self._handle_sentiment_violation(
                message, deduped_scores, layer="VADER", detail=f"Deduped: {deduped}"
            )
            return

        # Layer 1c: neg-proportion check on deduped text
        if deduped_scores["neg"] >= NEG_PROPORTION_THRESHOLD:
            await self._handle_sentiment_violation(
                message,
                deduped_scores,
                layer="VADER",
                detail=f"Neg-proportion {deduped_scores['neg']:.2f} >= {NEG_PROPORTION_THRESHOLD}",
            )
            return

        # Layer 2: Detoxify (only if all VADER checks passed)
        toxicity_threshold = await self.config.guild(message.guild).toxicity_threshold()
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, self.toxicity_model.predict, cleaned)

        if (
            results["toxicity"] > toxicity_threshold
            or results["threat"] > toxicity_threshold
            or results["insult"] > toxicity_threshold
            or results["severe_toxicity"] > toxicity_threshold
        ):
            await self._handle_sentiment_violation(message, results, layer="Detoxify", detail=None)
            return

        # All sentiment checks passed — reward positive behavior
        social_credit = self.bot.get_cog("SocialCredit")
        if social_credit:
            await social_credit.reward_positive_sentiment(
                message.author.id,
                message.guild.id,
                message.channel.id,
                compound_score=deduped_scores["compound"],
            )

    async def _handle_sentiment_violation(self, message, scores, *, layer, detail):
        """Delete message, DM user, timeout, log — shared by both layers. In silent mode, skip delete/DM/timeout but still adjust credit."""
        timeout_secs = await self.config.guild(message.guild).sentiment_timeout()
        channel_id = str(message.channel.id)
        silent = await self.config.guild(message.guild).sentiment_silent()

        try:
            if not silent:
                await message.delete()
                await self._log_sentiment_message(message, scores, layer=layer, detail=detail)

                async with self.config.guild(message.guild).sentiment_channels() as channels:
                    if channel_id in channels:
                        channels[channel_id]["filtered_count"] = (
                            channels[channel_id].get("filtered_count", 0) + 1
                        )

            if layer == "VADER":
                compound = scores["compound"]
                reason_text = (
                    f"Your message in {message.channel.mention} was removed for negative "
                    f"sentiment (score: {compound:.2f})"
                )
                timeout_reason = (
                    f"Negative sentiment in #{message.channel.name} "
                    f"(VADER: {compound:.2f})"
                )
            else:
                compound = -0.5  # default severity for Detoxify triggers
                top_score = max(
                    (scores[k], k) for k in ("toxicity", "threat", "insult", "severe_toxicity")
                )
                reason_text = (
                    f"Your message in {message.channel.mention} was removed for toxic "
                    f"content ({top_score[1]}: {top_score[0]:.2f})"
                )
                timeout_reason = (
                    f"Toxic content in #{message.channel.name} "
                    f"({top_score[1]}: {top_score[0]:.2f})"
                )

            if not silent:
                try:
                    await message.author.send(reason_text, delete_after=120)
                except discord.Forbidden:
                    pass

            # Scale timeout by social credit score
            social_credit = self.bot.get_cog("SocialCredit")
            if timeout_secs > 0 and not silent:
                multiplier = 1.0
                if social_credit:
                    multiplier = await social_credit.get_timeout_multiplier(message.author.id)
                scaled_secs = int(timeout_secs * multiplier)
                try:
                    await message.author.timeout(
                        timedelta(seconds=scaled_secs),
                        reason=f"{timeout_reason} [x{multiplier:.1f}]",
                    )
                except discord.Forbidden:
                    pass

            # Deduct social credit for violation (always, even in silent mode)
            if social_credit:
                await social_credit.penalize_negative_sentiment(
                    message.author.id,
                    message.guild.id,
                    message.channel.id,
                    compound_score=compound,
                )
        except discord.HTTPException:
            pass

    # ── Text processing helpers ────────────────────────────────────────

    def _split_clauses(self, text):
        """Split text into clauses using punctuation, newlines, and conjunctions.

        Discord messages rarely use proper sentence punctuation, so sent_tokenize
        alone won't split "KILL DURK WITH HAMMERS Love Love Love".  This splits on
        commas, semicolons, newlines, pipes, dashes surrounded by spaces, and common
        conjunctions (but, and, or, then, so, yet) when surrounded by spaces.
        The result is merged with sent_tokenize output and deduplicated while
        preserving order.
        """
        sentences = sent_tokenize(text)
        clause_parts = re.split(
            r"[,;\n|]|\s+(?:but|and|or|then|so|yet)\s+|\s-\s", text
        )

        seen = set()
        clauses = []
        for part in sentences + clause_parts:
            stripped = part.strip()
            if stripped and stripped not in seen:
                seen.add(stripped)
                clauses.append(stripped)

        return clauses

    def _deduplicate_text(self, text):
        """Collapse repeated words so padding like "Love Love Love Love" becomes "Love Love".

        Each word is allowed at most 2 occurrences to preserve natural emphasis
        while killing spam-padding that dilutes VADER's compound score.
        """
        words = text.split()
        counts = Counter()
        result = []
        for word in words:
            key = word.lower()
            counts[key] += 1
            if counts[key] <= 2:
                result.append(word)
        return " ".join(result)

    @staticmethod
    def _wildcard_to_regex(word):
        parts = word.split("*")
        escaped = [re.escape(part) for part in parts]
        pattern = ".*".join(escaped)
        if "*" not in word:
            pattern = rf"\b{pattern}\b"
        return re.compile(pattern)

    @staticmethod
    def _strip_markdown(content):
        invisible = r"[\u200B-\u200D\uFEFF\u2060-\u206F\u180E\u00AD\u200E\u200F\u202A-\u202E\u206A-\u206F]"
        content = re.sub(invisible, "", content)

        content = re.sub(r"```.*?```", " ", content, flags=re.DOTALL | re.MULTILINE)
        content = re.sub(r"`[^`]+?`", " ", content)
        content = re.sub(r"\|\|(.*?)\|\|", " ", content, flags=re.DOTALL)
        content = re.sub(r":[a-zA-Z0-9_+-]+:", " ", content)

        content = re.sub(r"~~(.*?)~~", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"\[([^\]\n]+)\]\([^\)]+\)", r"\1", content)

        content = re.sub(r"\*\*\*(.*?)\*\*\*", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"\*\*(.*?)\*\*", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"__(.*?)__", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"\*([^\s\*](?:.*?[^\s\*])?)\*", r"\1", content, flags=re.DOTALL)
        content = re.sub(r"_([^\s_](?:.*?[^\s_])?)_", r"\1", content, flags=re.DOTALL)

        content = re.sub(r"^(>>> ?|>> ?|> ?)(.*)", r"\2", content, flags=re.MULTILINE)
        content = re.sub(r"^#+\s*(.+)", r"\1", content, flags=re.MULTILINE)

        lines = content.split("\n")
        lines = [line for line in lines if "#-" not in line]
        content = "\n".join(lines)

        content = re.sub(r"[~|*_`#-]", " ", content)
        content = re.sub(r"\s+", " ", content).strip()

        return content.lower()

    @staticmethod
    def _migrate_channel(channels, channel_id):
        """Convert legacy list-format channel data to the current dict format."""
        if channel_id in channels and isinstance(channels[channel_id], list):
            channels[channel_id] = {
                "words": channels[channel_id],
                "filtered_count": 0,
                "word_usage": {},
            }

    # ── Logging ────────────────────────────────────────────────────────

    async def _log_filtered_message(self, message):
        log_channel_id = await self.config.guild(message.guild).log_channel()
        if not log_channel_id:
            return
        log_channel = message.guild.get_channel(log_channel_id)
        if not log_channel:
            return

        embed = discord.Embed(
            color=0xFF0000,
            description=(
                f"**Message sent by {message.author.mention} filtered in "
                f"{message.channel.mention}**\n{message.content}"
            ),
        )
        embed.set_author(
            name=f"{message.author.name} ({message.author.id})",
            icon_url=message.author.display_avatar.url,
        )
        embed.set_footer(
            text=(
                f"Author: {message.author.id} | Message ID: {message.id} • "
                f"{datetime.now().strftime('%b %d, %Y %I:%M %p')}"
            )
        )

        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException:
            pass

    async def _log_sentiment_message(self, message, scores, *, layer="VADER", detail=None):
        log_channel_id = await self.config.guild(message.guild).log_channel()
        if not log_channel_id:
            return
        log_channel = message.guild.get_channel(log_channel_id)
        if not log_channel:
            return

        embed = discord.Embed(
            color=0xFF0000,
            description=(
                f"**Message sent by {message.author.mention} removed for negative sentiment "
                f"in {message.channel.mention}** [{layer}]\n{message.content}"
            ),
        )

        if layer == "VADER":
            embed.add_field(name="Compound", value=f"{scores['compound']:.4f}", inline=True)
            embed.add_field(
                name="Pos / Neu / Neg",
                value=f"{scores['pos']:.2f} / {scores['neu']:.2f} / {scores['neg']:.2f}",
                inline=True,
            )
            if detail:
                embed.add_field(name="Flagged", value=detail, inline=False)
        else:
            embed.add_field(name="Toxicity", value=f"{scores['toxicity']:.4f}", inline=True)
            embed.add_field(name="Threat", value=f"{scores['threat']:.4f}", inline=True)
            embed.add_field(name="Insult", value=f"{scores['insult']:.4f}", inline=True)
            embed.add_field(name="Severe", value=f"{scores['severe_toxicity']:.4f}", inline=True)

        embed.set_author(
            name=f"{message.author.name} ({message.author.id})",
            icon_url=message.author.display_avatar.url,
        )
        embed.set_footer(
            text=(
                f"Author: {message.author.id} | Message ID: {message.id} • "
                f"{datetime.now().strftime('%b %d, %Y %I:%M %p')}"
            )
        )

        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException:
            pass
