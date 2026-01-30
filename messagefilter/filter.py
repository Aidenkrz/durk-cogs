from redbot.core import commands, Config, checks
import asyncio
import discord
from datetime import datetime, timezone, timedelta
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import sent_tokenize
from detoxify import Detoxify

nltk.download("vader_lexicon", quiet=True)
nltk.download("punkt_tab", quiet=True)

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
        }
        self.config.register_guild(**default_guild)

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
                    "word_usage": {}
                }
                embed = discord.Embed(
                    title="✅ Channel Added",
                    description=f"{channel.mention} will now filter messages",
                    color=0x00ff00
                )
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(
                    title="⚠️ Already Filtered",
                    description=f"{channel.mention} is already being monitored",
                    color=0xffd700
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
                    color=0x00ff00
                )
            else:
                embed = discord.Embed(
                    title="⚠️ Not Filtered",
                    description=f"{channel.mention} wasn't being monitored",
                    color=0xffd700
                )
            await ctx.send(embed=embed)
                

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def addword(self, ctx, *, args: str):
        """Add required words to a channel's filter"""
        try:
            converter = commands.TextChannelConverter()
            channel, _, words_part = args.partition(' ')
            channel = await converter.convert(ctx, channel)
            words = [w.strip().lower() for w in words_part.split(',') if w.strip()]
        except commands.BadArgument:
            channel = ctx.channel
            words = [w.strip().lower() for w in args.split(',') if w.strip()]
        
        async with self.config.guild(ctx.guild).channels() as channels:
            channel_id = str(channel.id)
            
            if channel_id in channels and isinstance(channels[channel_id], list):
                channels[channel_id] = {
                    "words": channels[channel_id],
                    "filtered_count": 0,
                    "word_usage": {}
                }
            
            if channel_id not in channels:
                channels[channel_id] = {
                    "words": [],
                    "filtered_count": 0,
                    "word_usage": {}
                }
            
            channel_data = channels[channel_id]
            existing_words = channel_data["words"]
            added = []
            
            for word in words:
                if word not in existing_words:
                    existing_words.append(word)
                    added.append(word)
            
            embed = discord.Embed(color=0x00ff00)
            if added:
                embed.title = f"✅ Added {len(added)} Words"
                embed.description = f"To {channel.mention}'s filter"
                embed.add_field(
                    name="New Words",
                    value=', '.join(f'`{word}`' for word in added) or "None",
                    inline=False
                )
                current_words = ', '.join(f'`{w}`' for w in existing_words) or "None"
                embed.add_field(
                    name="Current Filter Words",
                    value=current_words,
                    inline=False
                )
            else:
                embed.title = "⏩ No Changes"
                embed.description = "All specified words were already in the filter"
                embed.color = 0xffd700
            
            await ctx.send(embed=embed)

    @filter.command()
    @commands.admin_or_permissions(administrator=True)
    async def removeword(self, ctx, *, args: str):
        """Remove words from a channel's filter"""
        try:
            converter = commands.TextChannelConverter()
            channel, _, words_part = args.partition(' ')
            channel = await converter.convert(ctx, channel)
            words = [w.strip().lower() for w in words_part.split(',') if w.strip()]
        except commands.BadArgument:
            channel = ctx.channel
            words = [w.strip().lower() for w in args.split(',') if w.strip()]
        
        async with self.config.guild(ctx.guild).channels() as channels:
            channel_id = str(channel.id)
            
            # Migrate legacy format if needed
            if channel_id in channels and isinstance(channels[channel_id], list):
                channels[channel_id] = {
                    "words": channels[channel_id],
                    "filtered_count": 0,
                    "word_usage": {}
                }
                await self.config.guild(ctx.guild).channels.set(channels)
            
            if channel_id not in channels:
                return await ctx.send(f"{channel.mention} is not being filtered")
            
            channel_data = channels[channel_id]
            required_words = channel_data["words"]
            removed = []
            
            for word in words:
                if word in required_words:
                    required_words.remove(word)
                    removed.append(word)
                    # Remove from word usage stats
                    if word in channel_data["word_usage"]:
                        del channel_data["word_usage"][word]
            
            embed = discord.Embed(color=0x00ff00)
            if removed:
                embed.title = f"❌ Removed {len(removed)} Words"
                embed.description = f"From {channel.mention}'s filter"
                embed.add_field(
                    name="Removed Words",
                    value=', '.join(f'`{word}`' for word in removed) or "None",
                    inline=False
                )
                
                if required_words:
                    embed.add_field(
                        name="Remaining Words",
                        value=', '.join(f'`{w}`' for w in required_words) or "None",
                        inline=False
                    )
                    # Update the channel data
                    channels[channel_id] = channel_data
                else:
                    del channels[channel_id]
                    embed.add_field(
                        name="Channel Removed",
                        value="No words remaining in filter",
                        inline=False
                    )
                
                await self.config.guild(ctx.guild).channels.set(channels)
            else:
                embed.title = "⏩ No Changes"
                embed.description = "None of these words were in the filter"
                embed.color = 0xffd700
            
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
        embed = discord.Embed(title="Filtered Channels", color=0x00ff00)
        
        for channel_id, channel_data in channels.items():
            if isinstance(channel_data, list):
                channel_data = {
                    "words": channel_data,
                    "filtered_count": 0,
                    "word_usage": {}
                }
            
            channel = ctx.guild.get_channel(int(channel_id))
            if channel and channel_data.get("words"):
                word_list = ', '.join(f'`{word}`' for word in channel_data["words"]) or "No words set"
                embed.add_field(
                    name=f"#{channel.name}",
                    value=f"Required words: {word_list}",
                    inline=False
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
                "word_usage": {}
            }
            await self.config.guild(ctx.guild).channels.set(channels)
        
        channel_data = channels.get(channel_id, {})
        
        if not channel_data.get("words"):
            return await ctx.send(f"{channel.mention} is not being filtered")
        
        embed = discord.Embed(
            title=f"Filter Statistics for #{channel.name}",
            color=0x00ff00
        )
        
        filtered_count = channel_data.get("filtered_count", 0)
        embed.add_field(name="🚫 Messages Filtered", value=str(filtered_count), inline=False)

        word_usage = channel_data.get("word_usage", {})
        if word_usage:
            sorted_words = sorted(word_usage.items(), key=lambda x: x[1], reverse=True)
            top_words = "\n".join([f"• `{word}`: {count} uses" for word, count in sorted_words[:5]])
            embed.add_field(name="🏆 Top Filter Words", value=top_words, inline=False)
        else:
            embed.add_field(name="📊 Word Usage", value="No usage data collected yet", inline=False)
            
        await ctx.send(embed=embed)
        
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
                    color=0x00ff00,
                )
                await ctx.send(embed=embed)
            else:
                embed = discord.Embed(
                    title="Already Enabled",
                    description=f"{channel.mention} already has sentiment filtering",
                    color=0xffd700,
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
                    color=0x00ff00,
                )
            else:
                embed = discord.Embed(
                    title="Not Enabled",
                    description=f"{channel.mention} didn't have sentiment filtering",
                    color=0xffd700,
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
            color=0x00ff00,
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
            color=0x00ff00,
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
            color=0x00ff00,
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

        embed = discord.Embed(title="Sentiment Filter Settings", color=0x00ff00)
        embed.add_field(name="VADER Threshold", value=f"`{threshold}`", inline=True)
        embed.add_field(name="Toxicity Threshold", value=f"`{tox_threshold}`", inline=True)
        embed.add_field(name="Timeout", value=f"`{timeout_secs}s`", inline=True)
        embed.add_field(
            name="Channels",
            value="\n".join(channel_list) if channel_list else "None",
            inline=False,
        )
        await ctx.send(embed=embed)

    @sentiment.command(name="test")
    async def sentiment_test(self, ctx, *, text: str):
        """Test the sentiment score of a message against both detection layers"""
        threshold = await self.config.guild(ctx.guild).sentiment_threshold()
        tox_threshold = await self.config.guild(ctx.guild).toxicity_threshold()

        # Layer 1: Sentence-level VADER
        sentences = sent_tokenize(text)
        vader_triggered = False
        sentence_lines = []
        for sentence in sentences:
            s = self.analyzer.polarity_scores(sentence)
            flag = s["compound"] < threshold
            if flag:
                vader_triggered = True
            marker = "X" if flag else "-"
            sentence_lines.append(f"`[{marker}]` {s['compound']:+.4f} | {sentence}")

        # Layer 2: Detoxify
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
            color=0xff0000 if would_filter else 0x00ff00,
        )

        embed.add_field(
            name=f"VADER (per-sentence, threshold: {threshold})",
            value="\n".join(sentence_lines) or "No sentences",
            inline=False,
        )
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
        triggered_by = []
        if vader_triggered:
            triggered_by.append("VADER")
        if detox_triggered:
            triggered_by.append("Detoxify")
        embed.add_field(
            name="Would Filter",
            value=f"{'Yes' if would_filter else 'No'}"
            + (f" (triggered by: {', '.join(triggered_by)})" if triggered_by else ""),
            inline=False,
        )
        await ctx.send(embed=embed)

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
                
    @commands.Cog.listener()
    async def on_message(self, message):
        await self.check_message(message)

    async def check_message(self, message):
        if message.author.bot:
            return

        if not message.guild:
            return

        if message.channel.permissions_for(message.author).manage_messages:
            return

        prefixes = await self.bot.get_valid_prefixes(message.guild)
        content = message.content.lower().strip()
        for prefix in prefixes:
            if content.startswith(prefix.lower()):
                cmd = content[len(prefix):].strip()
                if cmd.startswith("filter") or cmd.startswith("ILOVEWARRIORS"):
                    return

        deleted_by_word_filter = await self._check_word_filter(message)

        if not deleted_by_word_filter:
            await self._check_sentiment(message)

    async def _check_word_filter(self, message):
        """Run the word-based filter. Returns True if the message was deleted."""
        async with self.config.guild(message.guild).channels() as channels:
            channel_id = str(message.channel.id)

            if channel_id not in channels:
                return False

            if isinstance(channels[channel_id], list):
                channels[channel_id] = {
                    "words": channels[channel_id],
                    "filtered_count": 0,
                    "word_usage": {}
                }
                await self.config.guild(message.guild).channels.set(channels)

            channel_data = channels[channel_id]
            required_words = channel_data.get("words", [])

            if not required_words:
                return False

            cleaned = self.strip_markdown(message.content)
            regexes = [self.wildcard_to_regex(word) for word in required_words]
            match_found = False

            for word, regex in zip(required_words, regexes):
                if regex.search(cleaned):
                    channel_data["word_usage"][word] = channel_data["word_usage"].get(word, 0) + 1
                    match_found = True
                    break

            if not match_found:
                try:
                    await message.delete()
                    await self.log_filtered_message(message)
                    channel_data["filtered_count"] = channel_data.get("filtered_count", 0) + 1

                    try:
                        word_list = ', '.join(f'`{word}`' for word in required_words)
                        await message.author.send(
                            f"Your message in {message.channel.mention} was filtered because "
                            f"it did not contain one of the following words: {word_list}",
                            delete_after=120
                        )
                    except discord.Forbidden:
                        pass

                    try:
                        await message.author.timeout(
                            timedelta(seconds=20),
                            reason=f"Filter violation in #{message.channel.name}"
                        )
                    except discord.Forbidden:
                        pass

                except discord.HTTPException:
                    pass
                finally:
                    channels[channel_id] = channel_data
                    await self.config.guild(message.guild).channels.set(channels)
                return True
            else:
                channels[channel_id] = channel_data
                await self.config.guild(message.guild).channels.set(channels)
                return False

    async def _check_sentiment(self, message):
        """Run layered sentiment filtering: sentence-level VADER then Detoxify."""
        channel_id = str(message.channel.id)
        sentiment_channels = await self.config.guild(message.guild).sentiment_channels()

        if channel_id not in sentiment_channels:
            return

        cleaned = self.strip_markdown(message.content)
        if not cleaned:
            return

        threshold = await self.config.guild(message.guild).sentiment_threshold()

        # Layer 1: Sentence-level VADER — check each sentence independently
        sentences = sent_tokenize(cleaned)
        for sentence in sentences:
            scores = self.analyzer.polarity_scores(sentence)
            if scores["compound"] < threshold:
                await self._handle_sentiment_violation(
                    message, scores, layer="VADER", detail=f"Sentence: {sentence}"
                )
                return

        # Layer 2: Detoxify — run transformer toxicity check in executor
        toxicity_threshold = await self.config.guild(message.guild).toxicity_threshold()
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, self.toxicity_model.predict, cleaned)

        if (results["toxicity"] > toxicity_threshold
                or results["threat"] > toxicity_threshold
                or results["insult"] > toxicity_threshold
                or results["severe_toxicity"] > toxicity_threshold):
            await self._handle_sentiment_violation(
                message, results, layer="Detoxify", detail=None
            )

    async def _handle_sentiment_violation(self, message, scores, *, layer, detail):
        """Delete message, DM user, timeout, log — shared by both layers."""
        timeout_secs = await self.config.guild(message.guild).sentiment_timeout()
        channel_id = str(message.channel.id)

        try:
            await message.delete()
            await self.log_sentiment_message(message, scores, layer=layer, detail=detail)

            async with self.config.guild(message.guild).sentiment_channels() as channels:
                if channel_id in channels:
                    channels[channel_id]["filtered_count"] = channels[channel_id].get("filtered_count", 0) + 1

            if layer == "VADER":
                reason_text = (
                    f"Your message in {message.channel.mention} was removed for negative "
                    f"sentiment (score: {scores['compound']:.2f})"
                )
                timeout_reason = (
                    f"Negative sentiment in #{message.channel.name} "
                    f"(VADER: {scores['compound']:.2f})"
                )
            else:
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

            try:
                await message.author.send(reason_text, delete_after=120)
            except discord.Forbidden:
                pass

            if timeout_secs > 0:
                try:
                    await message.author.timeout(
                        timedelta(seconds=timeout_secs), reason=timeout_reason
                    )
                except discord.Forbidden:
                    pass

        except discord.HTTPException:
            pass
                        
    def wildcard_to_regex(self, word):
        parts = word.split('*')
        escaped = [re.escape(part) for part in parts]
        pattern = '.*'.join(escaped)
        if '*' not in word:
            pattern = rf'\b{pattern}\b'
    
        return re.compile(pattern)
        
    def strip_markdown(self, content):
        invisible_chars_pattern = r'[\u200B-\u200D\uFEFF\u2060-\u206F\u180E\u00AD\u200E\u200F\u202A-\u202E\u206A-\u206F]'
        content = re.sub(invisible_chars_pattern, '', content)

        content = re.sub(r'```.*?```', ' ', content, flags=re.DOTALL | re.MULTILINE)  # Multi-line code blocks
        content = re.sub(r'`[^`]+?`', ' ', content)  # Inline code
        content = re.sub(r'\|\|(.*?)\|\|', ' ', content, flags=re.DOTALL)  # Spoilers
        content = re.sub(r':[a-zA-Z0-9_+-]+:', ' ', content)  # Emoji tags

        content = re.sub(r'~~(.*?)~~', r'\1', content, flags=re.DOTALL) # Strikethrough
        content = re.sub(r'\[([^\]\n]+)\]\([^\)]+\)', r'\1', content)  # Hyperlinks (keep link text)

        content = re.sub(r'\*\*\*(.*?)\*\*\*', r'\1', content, flags=re.DOTALL)  # Bold Italic
        content = re.sub(r'\*\*(.*?)\*\*', r'\1', content, flags=re.DOTALL)      # Bold
        content = re.sub(r'__(.*?)__', r'\1', content, flags=re.DOTALL)          # Underline (Discord uses this for underline)
        content = re.sub(r'\*([^\s\*](?:.*?[^\s\*])?)\*', r'\1', content, flags=re.DOTALL) # Italic *text* (ensure not empty and not just spaces)
        content = re.sub(r'_([^\s_](?:.*?[^\s_])?)_', r'\1', content, flags=re.DOTALL) # Italic _text_ (ensure not empty and not just spaces)

        content = re.sub(r'^(>>> ?|>> ?|> ?)(.*)', r'\2', content, flags=re.MULTILINE) # Block quotes, keep content
        content = re.sub(r'^#+\s*(.+)', r'\1', content, flags=re.MULTILINE)     # Headers, keep content

        lines = content.split('\n')
        lines = [line for line in lines if '#-' not in line]
        content = '\n'.join(lines)

        content = re.sub(r'[~|*_`#-]', ' ', content)
        content = re.sub(r'\s+', ' ', content).strip()
        
        return content.lower()

    async def log_filtered_message(self, message):
        log_channel_id = await self.config.guild(message.guild).log_channel()
        if not log_channel_id:
            return

        log_channel = message.guild.get_channel(log_channel_id)
        if not log_channel:
            return

        embed = discord.Embed(
            color=0xff0000,
            description=f"**Message sent by {message.author.mention} filtered in {message.channel.mention}**\n"
                       f"{message.content}"
        )
        embed.set_author(
            name=f"{message.author.name} ({message.author.id})",
            icon_url=message.author.display_avatar.url
        )
        embed.set_footer(
            text=f"Author: {message.author.id} | Message ID: {message.id} • {datetime.now().strftime('%b %d, %Y %I:%M %p')}"
        )

        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException:
            pass

    async def log_sentiment_message(self, message, scores, *, layer="VADER", detail=None):
        log_channel_id = await self.config.guild(message.guild).log_channel()
        if not log_channel_id:
            return

        log_channel = message.guild.get_channel(log_channel_id)
        if not log_channel:
            return

        embed = discord.Embed(
            color=0xff0000,
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
                embed.add_field(name="Flagged Sentence", value=detail, inline=False)
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
            text=f"Author: {message.author.id} | Message ID: {message.id} • {datetime.now().strftime('%b %d, %Y %I:%M %p')}"
        )

        try:
            await log_channel.send(embed=embed)
        except discord.HTTPException:
            pass
