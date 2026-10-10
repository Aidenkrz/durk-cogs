from redbot.core import commands, Config
import discord
from datetime import datetime, timedelta
import re


class MessageFilter(commands.Cog):
    """Automatically delete messages that don't contain required words"""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890)
        default_guild = {
            "channels": {},
            "active": True,
            "log_channel": None,
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

        if message.author.guild_permissions.administrator:
            return

        prefixes = await self.bot.get_valid_prefixes(message.guild)
        content = message.content.lower().strip()
        for prefix in prefixes:
            if content.startswith(prefix.lower()):
                cmd = content[len(prefix) :].strip()
                if cmd.startswith("filter") or cmd.startswith("ilovewarriors"):
                    return

        await self._check_word_filter(message)

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

    # ── Text processing helpers ────────────────────────────────────────

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
