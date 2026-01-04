"""Markov chain text generation cog for Red-DiscordBot."""

from __future__ import annotations

import asyncio
import logging
import random
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import discord
from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

from .chain import MarkovChain, TokenInfo, sanitize_message
from .storage import MarkovStorage, MigrationRequiredError

log = logging.getLogger("red.DurkCogs.Markov")


class Markov(commands.Cog):
    """Generate text using Markov chains from Discord messages."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=8675309867530900)
        self.data_path: Path = cog_data_path(self)

        default_guild = {
            "enabled": False,
            "channels": [],  # Whitelisted channel IDs (empty = all)
            "order": 2,  # N-gram order
            "max_order": 3,  # Max order for backoff
            "min_length": 5,  # Min words in output
            "max_length": 50,  # Max words in output (for admins)
            "user_max_length": 10,  # Max words for normal users
            "admin_max_length": 100,  # Max words for Discord admins
            "whitelist": [],  # User IDs who can trigger :chains: reaction
        }

        self.config.register_guild(**default_guild)
        self._storage: Dict[int, MarkovStorage] = {}

        # Cache for active quiz games
        self._quiz_games: Dict[int, dict] = {}

    async def cog_unload(self) -> None:
        """Clean up storage connections."""
        for storage in self._storage.values():
            await storage.close()
        self._storage.clear()

    async def _get_storage(self, guild_id: int) -> MarkovStorage:
        """Get or create storage for a guild."""
        if guild_id not in self._storage:
            storage = MarkovStorage(self.data_path, guild_id)
            await storage.init()
            self._storage[guild_id] = storage
        return self._storage[guild_id]

    async def _train_message(
        self, guild_id: int, user_id: int, text: str, order: int, max_order: int = None
    ) -> None:
        """Train the chain with a message."""
        max_order = max_order or order
        chain = MarkovChain(order=order, max_order=max_order)
        chain.train(text)

        if not chain.chain:
            return

        storage = await self._get_storage(guild_id)

        # Add to guild chain
        await storage.add_transitions(chain.chain)
        # Add to user chain
        await storage.add_transitions(chain.chain, user_id=user_id)

        # Save enhanced chain data
        await storage.add_reverse_transitions(chain.reverse_chain)
        await storage.add_skip_transitions(chain.skip_chain)

        # Save order chains
        for o, order_chain in chain.order_chains.items():
            await storage.add_order_transitions(o, order_chain)

        # Save case memory
        case_data = {k: dict(v.original_forms) for k, v in chain.case_memory.items()}
        await storage.add_case_memory(case_data)

        # Increment message count
        await storage.increment_message_count(user_id)

    async def _load_full_chain(
        self, guild_id: int, order: int, max_order: int = None, user_id: int = None
    ) -> MarkovChain:
        """Load a fully-featured chain from storage.

        For user-specific chains, only the primary chain is loaded to keep
        generation true to the user's speech patterns. Guild-wide auxiliary
        features (reverse chain, skip-grams, etc.) are only used for guild chains.
        """
        max_order = max_order or order
        storage = await self._get_storage(guild_id)

        chain = MarkovChain(order=order, max_order=max_order)

        # Load primary chain
        if user_id:
            chain.chain = await storage.get_user_chain(user_id)
        else:
            chain.chain = await storage.get_guild_chain()

            # Only load enhanced data for guild-wide chain
            # User chains use only their own data for accurate mimicry
            chain.reverse_chain = await storage.get_reverse_chain()
            chain.skip_chain = await storage.get_skip_chain()
            chain.order_chains = await storage.get_all_order_chains()

        # Load case memory (shared - just for proper capitalization)
        case_data = await storage.get_case_memory()
        for word, forms in case_data.items():
            chain.case_memory[word] = TokenInfo(lowercase=word)
            chain.case_memory[word].original_forms = forms

        # Rebuild bloom filter
        for state in chain.chain:
            chain.bloom.add(str(state))

        return chain

    async def _generate_text(
        self,
        guild_id: int,
        order: int,
        min_length: int,
        max_length: int,
        user_id: Optional[int] = None,
        seed_words: Optional[str] = None,
        temperature: float = 1.0,
    ) -> str:
        """Generate text from the chain."""
        settings = await self.config.guild_from_id(guild_id).all()
        max_order = settings.get("max_order", order)

        chain = await self._load_full_chain(guild_id, order, max_order, user_id)

        if not chain.chain:
            return ""

        seed = None
        if seed_words:
            words = seed_words.split()
            seed = chain.find_seed(words)

        return chain.generate(
            min_words=min_length,
            max_words=max_length,
            seed=seed,
            temperature=temperature,
        )

    def _get_max_length(self, member: discord.Member, guild_settings: dict) -> int:
        """Get the maximum length allowed for a user."""
        if member.guild_permissions.administrator:
            return guild_settings["admin_max_length"]
        return guild_settings["user_max_length"]

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        """Listen for messages to train the chain."""
        if not message.guild:
            return
        if message.author.bot:
            return
        if not message.content:
            return

        # Skip messages starting with bot prefix (commands)
        if message.content.startswith("."):
            return

        # Check if this looks like a command
        ctx = await self.bot.get_context(message)
        if ctx.valid:
            return

        settings = await self.config.guild(message.guild).all()
        if not settings["enabled"]:
            return

        # Check channel whitelist
        if settings["channels"] and message.channel.id not in settings["channels"]:
            return

        text = sanitize_message(message.content)
        if len(text.split()) < 3:
            return

        await self._train_message(
            message.guild.id,
            message.author.id,
            text,
            settings["order"],
            settings.get("max_order", settings["order"]),
        )

    @commands.Cog.listener()
    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        """Listen for :chains: reactions from whitelisted users."""
        if not payload.guild_id:
            return

        # Check if it's the chains emoji
        if payload.emoji.name not in ("\u26d3", "\u26d3\ufe0f"):
            return

        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return

        settings = await self.config.guild(guild).all()
        if not settings["enabled"]:
            return

        # Check if user is whitelisted
        if payload.user_id not in settings["whitelist"]:
            return

        channel = guild.get_channel(payload.channel_id)
        if not channel or not isinstance(channel, discord.TextChannel):
            return

        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return

        # Generate a short response
        try:
            text = await self._generate_text(
                guild.id,
                settings["order"],
                min_length=3,
                max_length=15,
            )
        except MigrationRequiredError:
            # Silently fail for reactions - user needs to run migrate command
            return

        if text:
            await message.reply(text, mention_author=True)

    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def markov(self, ctx: commands.Context, length: Optional[int] = None) -> None:
        """Generate random text from the server's Markov chain.

        Optionally specify a length (max 10 for users, 100 for admins).
        """
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)

        if length is None:
            length = settings["min_length"]
        else:
            length = min(length, max_allowed)

        try:
            text = await self._generate_text(
                ctx.guild.id,
                settings["order"],
                min_length=settings["min_length"],
                max_length=length,
            )
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        if text:
            await ctx.send(text)
        else:
            await ctx.send("No chain data yet. Enable training and send some messages!")

    @markov.command(name="help")
    @commands.guild_only()
    async def markov_help(self, ctx: commands.Context) -> None:
        """Show help for Markov commands."""
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)

        embed = discord.Embed(
            title="Markov Chain Commands",
            description="Generate text based on server message history.",
            color=await ctx.embed_color(),
        )

        prefix = ctx.clean_prefix

        embed.add_field(
            name=f"{prefix}markov [length]",
            value=f"Generate random text. Max length: {max_allowed} words.",
            inline=False,
        )
        embed.add_field(
            name=f"{prefix}markov user @user [length]",
            value="Generate text mimicking a specific user.",
            inline=False,
        )
        embed.add_field(
            name=f"{prefix}markov fuse @user1 @user2",
            value="Blend two users' speech patterns together.",
            inline=False,
        )
        embed.add_field(
            name=f"{prefix}markov seed <words>",
            value="Generate text starting from specific words.",
            inline=False,
        )
        embed.add_field(
            name=f"{prefix}markov quiz",
            value="Play 'Who Said It?' - guess if text is real or generated.",
            inline=False,
        )
        embed.add_field(
            name=f"{prefix}markov stats",
            value="Show chain statistics and top contributors.",
            inline=False,
        )

        if ctx.author.guild_permissions.administrator:
            embed.add_field(
                name="Admin Commands",
                value=(
                    f"`{prefix}markovset enable/disable` - Toggle training\n"
                    f"`{prefix}markovset channel add/remove` - Manage channel whitelist\n"
                    f"`{prefix}markovset whitelist add/remove` - Manage reaction whitelist\n"
                    f"`{prefix}markovset train #channel [limit]` - Bulk train from history\n"
                    f"`{prefix}markovset clear [user]` - Clear chain data\n"
                    f"`{prefix}markovset order <1-4>` - Set n-gram order\n"
                    f"`{prefix}markovset length <user> <admin>` - Set max lengths\n"
                    f"`{prefix}markovset status` - Show current settings"
                ),
                inline=False,
            )

        embed.set_footer(text="React with \u26d3 (chains) to trigger a Markov reply (if whitelisted)")

        await ctx.send(embed=embed)

    @markov.command(name="user")
    @commands.guild_only()
    async def markov_user(
        self, ctx: commands.Context, user: discord.Member, length: Optional[int] = None
    ) -> None:
        """Generate text mimicking a specific user."""
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)

        if length is None:
            length = settings["min_length"]
        else:
            length = min(length, max_allowed)

        try:
            text = await self._generate_text(
                ctx.guild.id,
                settings["order"],
                min_length=settings["min_length"],
                max_length=length,
                user_id=user.id,
            )
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        if text:
            await ctx.send(f"**{user.display_name}:** {text}")
        else:
            await ctx.send(f"No chain data for {user.display_name} yet.")

    @markov.command(name="fuse")
    @commands.guild_only()
    async def markov_fuse(
        self,
        ctx: commands.Context,
        user1: discord.Member,
        user2: discord.Member,
        length: Optional[int] = None,
    ) -> None:
        """Fuse two users' speech patterns together."""
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)
        max_order = settings.get("max_order", settings["order"])

        if length is None:
            length = settings["min_length"]
        else:
            length = min(length, max_allowed)

        # Load both user chains
        try:
            chain1 = await self._load_full_chain(
                ctx.guild.id, settings["order"], max_order, user1.id
            )
            chain2 = await self._load_full_chain(
                ctx.guild.id, settings["order"], max_order, user2.id
            )
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        if not chain1.chain:
            await ctx.send(f"No chain data for {user1.display_name}.")
            return
        if not chain2.chain:
            await ctx.send(f"No chain data for {user2.display_name}.")
            return

        # Merge with 50/50 blend
        fused = chain1.merge_weighted(chain2, weight=0.5)

        text = fused.generate(
            min_words=settings["min_length"],
            max_words=length,
        )

        if text:
            await ctx.send(f"**{user1.display_name} + {user2.display_name}:** {text}")
        else:
            await ctx.send("Couldn't generate fused text.")

    @markov.command(name="quiz")
    @commands.guild_only()
    async def markov_quiz(
        self, ctx: commands.Context, order: Optional[int] = None, duration: Optional[int] = None
    ) -> None:
        """Play 'Who Said It?' - guess if the message is real or Markov-generated.

        Admins can optionally specify:
        - order: The n-gram order to use (1-4)
        - duration: How long the quiz runs in seconds (max 30)
        """
        settings = await self.config.guild(ctx.guild).all()
        is_admin = ctx.author.guild_permissions.administrator

        # Handle order parameter (admin only)
        if order is not None:
            if not is_admin:
                await ctx.send("Only admins can change the quiz order.")
                return
            if order < 1 or order > 4:
                await ctx.send("Order must be between 1 and 4.")
                return
        else:
            order = settings["order"]

        # Handle duration parameter (admin only, max 30)
        if duration is not None:
            if not is_admin:
                await ctx.send("Only admins can change the quiz duration.")
                return
            if duration < 5 or duration > 30:
                await ctx.send("Duration must be between 5 and 30 seconds.")
                return
        else:
            duration = 15  # Default duration

        # Check if there's already a game in this channel
        if ctx.channel.id in self._quiz_games:
            await ctx.send("A quiz is already running in this channel!")
            return

        storage = await self._get_storage(ctx.guild.id)
        try:
            stats = await storage.get_stats()
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        if not stats["top_contributors"]:
            await ctx.send("Not enough data to play the quiz. Train the chain first!")
            return

        # Pick a random user with enough messages
        eligible_users = [
            (uid, count) for uid, count in stats["top_contributors"] if count >= 10
        ]
        if not eligible_users:
            await ctx.send("Not enough user data to play the quiz.")
            return

        user_id, _ = random.choice(eligible_users)
        member = ctx.guild.get_member(user_id)
        if not member:
            await ctx.send("Couldn't find a valid user for the quiz.")
            return

        # 50% chance real, 50% generated
        is_real = random.random() < 0.5

        if is_real:
            # Try to find a real message from this user
            real_message = None
            for channel in ctx.guild.text_channels:
                try:
                    async for msg in channel.history(limit=500):
                        if msg.author.id == user_id and len(msg.content.split()) >= 5:
                            if not msg.content.startswith(ctx.prefix or "."):
                                real_message = sanitize_message(msg.content)
                                break
                except discord.Forbidden:
                    continue
                if real_message:
                    break

            if not real_message:
                is_real = False  # Fall back to generated

        if not is_real:
            # Generate fake message using guild chain (more reliable than user chain)
            try:
                text = await self._generate_text(
                    ctx.guild.id,
                    order,
                    min_length=5,
                    max_length=20,
                )
            except MigrationRequiredError:
                await ctx.send("Database needs migration. Run `.markovset migrate` first.")
                return
            if not text:
                await ctx.send("Couldn't generate quiz text. Try training more data first.")
                return
            quiz_text = text
        else:
            quiz_text = real_message

        embed = discord.Embed(
            title="Who Said It?",
            description=f'**"{quiz_text}"**\n\n- {member.display_name}',
            color=await ctx.embed_color(),
        )
        embed.add_field(
            name="Is this real or Markov-generated?",
            value=f"React with \u2705 for REAL or \u274c for FAKE\nYou have {duration} seconds!",
        )

        quiz_msg = await ctx.send(embed=embed)
        await quiz_msg.add_reaction("\u2705")  # checkmark
        await quiz_msg.add_reaction("\u274c")  # X

        # Store game state with message ID to prevent race conditions
        self._quiz_games[ctx.channel.id] = {
            "message_id": quiz_msg.id,
            "is_real": is_real,
            "user": member,
            "text": quiz_text,
        }

        # Wait for reactions
        await asyncio.sleep(duration)

        # Collect results - verify this is still our game
        game = self._quiz_games.get(ctx.channel.id)
        if not game or game.get("message_id") != quiz_msg.id:
            # Another quiz took over or game was cleared
            return

        # Now safe to remove
        self._quiz_games.pop(ctx.channel.id, None)

        try:
            quiz_msg = await ctx.channel.fetch_message(quiz_msg.id)
        except discord.NotFound:
            return

        real_votes = 0
        fake_votes = 0

        for reaction in quiz_msg.reactions:
            if str(reaction.emoji) == "\u2705":
                real_votes = reaction.count - 1  # Subtract bot's reaction
            elif str(reaction.emoji) == "\u274c":
                fake_votes = reaction.count - 1

        answer = "REAL" if game["is_real"] else "MARKOV-GENERATED"
        winners = real_votes if game["is_real"] else fake_votes

        result_embed = discord.Embed(
            title="Quiz Results!",
            description=f'**"{game["text"]}"**\n\nThis was **{answer}**!',
            color=discord.Color.green() if game["is_real"] else discord.Color.red(),
        )
        result_embed.add_field(
            name="Votes",
            value=f"Real: {real_votes} | Fake: {fake_votes}",
        )
        result_embed.add_field(
            name="Winners",
            value=f"{winners} people guessed correctly!",
        )

        await ctx.send(embed=result_embed)

    @markov.command(name="seed")
    @commands.guild_only()
    async def markov_seed(
        self, ctx: commands.Context, *, seed_words: str
    ) -> None:
        """Generate text starting from specific words."""
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)

        try:
            text = await self._generate_text(
                ctx.guild.id,
                settings["order"],
                min_length=settings["min_length"],
                max_length=max_allowed,
                seed_words=seed_words,
            )
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        if text:
            await ctx.send(text)
        else:
            await ctx.send("Couldn't generate text with those seed words.")

    @markov.command(name="stats")
    @commands.guild_only()
    async def markov_stats(self, ctx: commands.Context) -> None:
        """Show Markov chain statistics."""
        storage = await self._get_storage(ctx.guild.id)
        try:
            stats = await storage.get_stats()
        except MigrationRequiredError:
            await ctx.send("Database needs migration. Run `.markovset migrate` first.")
            return

        embed = discord.Embed(
            title="Markov Chain Stats",
            color=await ctx.embed_color(),
        )
        embed.add_field(
            name="Chain Size",
            value=(
                f"{stats['state_count']:,} states\n"
                f"{stats['transition_count']:,} transitions\n"
                f"{stats.get('unique_words', 0):,} unique words\n"
                f"{stats.get('skip_gram_count', 0):,} skip-grams"
            ),
            inline=False,
        )

        if stats["top_contributors"]:
            contributors = []
            for user_id, count in stats["top_contributors"][:5]:
                member = ctx.guild.get_member(user_id)
                name = member.display_name if member else f"Unknown ({user_id})"
                contributors.append(f"{name}: {count:,} messages")
            embed.add_field(
                name="Top Contributors",
                value="\n".join(contributors),
                inline=False,
            )

        await ctx.send(embed=embed)

    @commands.group()
    @commands.guild_only()
    @checks.admin_or_permissions(administrator=True)
    async def markovset(self, ctx: commands.Context) -> None:
        """Configure Markov chain settings."""
        pass

    @markovset.command(name="enable")
    async def markovset_enable(self, ctx: commands.Context) -> None:
        """Enable Markov chain training for this server."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("Markov chain training enabled.")

    @markovset.command(name="disable")
    async def markovset_disable(self, ctx: commands.Context) -> None:
        """Disable Markov chain training for this server."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("Markov chain training disabled.")

    @markovset.command(name="status")
    async def markovset_status(self, ctx: commands.Context) -> None:
        """Show current Markov settings."""
        settings = await self.config.guild(ctx.guild).all()

        embed = discord.Embed(
            title="Markov Settings",
            color=await ctx.embed_color(),
        )
        embed.add_field(name="Enabled", value="Yes" if settings["enabled"] else "No")
        embed.add_field(name="Order", value=str(settings["order"]))
        embed.add_field(name="Max Order (backoff)", value=str(settings.get("max_order", settings["order"])))
        embed.add_field(name="User Max Length", value=str(settings["user_max_length"]))
        embed.add_field(name="Admin Max Length", value=str(settings["admin_max_length"]))

        if settings["channels"]:
            channels = [f"<#{cid}>" for cid in settings["channels"]]
            embed.add_field(
                name="Whitelisted Channels",
                value=", ".join(channels) or "All channels",
                inline=False,
            )
        else:
            embed.add_field(name="Whitelisted Channels", value="All channels", inline=False)

        if settings["whitelist"]:
            users = [f"<@{uid}>" for uid in settings["whitelist"]]
            embed.add_field(
                name="Reaction Whitelist",
                value=", ".join(users),
                inline=False,
            )
        else:
            embed.add_field(name="Reaction Whitelist", value="None", inline=False)

        await ctx.send(embed=embed)

    @markovset.group(name="channel")
    async def markovset_channel(self, ctx: commands.Context) -> None:
        """Manage channel whitelist."""
        pass

    @markovset_channel.command(name="add")
    async def markovset_channel_add(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Add a channel to the whitelist."""
        async with self.config.guild(ctx.guild).channels() as channels:
            if channel.id not in channels:
                channels.append(channel.id)
        await ctx.send(f"Added {channel.mention} to the whitelist.")

    @markovset_channel.command(name="remove")
    async def markovset_channel_remove(
        self, ctx: commands.Context, channel: discord.TextChannel
    ) -> None:
        """Remove a channel from the whitelist."""
        async with self.config.guild(ctx.guild).channels() as channels:
            if channel.id in channels:
                channels.remove(channel.id)
        await ctx.send(f"Removed {channel.mention} from the whitelist.")

    @markovset_channel.command(name="list")
    async def markovset_channel_list(self, ctx: commands.Context) -> None:
        """List whitelisted channels."""
        channels = await self.config.guild(ctx.guild).channels()
        if channels:
            mentions = [f"<#{cid}>" for cid in channels]
            await ctx.send(f"Whitelisted channels: {', '.join(mentions)}")
        else:
            await ctx.send("No channel whitelist set. Training on all channels.")

    @markovset.group(name="whitelist")
    async def markovset_whitelist(self, ctx: commands.Context) -> None:
        """Manage the reaction whitelist."""
        pass

    @markovset_whitelist.command(name="add")
    async def markovset_whitelist_add(
        self, ctx: commands.Context, user: discord.Member
    ) -> None:
        """Add a user to the reaction whitelist."""
        async with self.config.guild(ctx.guild).whitelist() as whitelist:
            if user.id not in whitelist:
                whitelist.append(user.id)
        await ctx.send(f"Added {user.mention} to the reaction whitelist.")

    @markovset_whitelist.command(name="remove")
    async def markovset_whitelist_remove(
        self, ctx: commands.Context, user: discord.Member
    ) -> None:
        """Remove a user from the reaction whitelist."""
        async with self.config.guild(ctx.guild).whitelist() as whitelist:
            if user.id in whitelist:
                whitelist.remove(user.id)
        await ctx.send(f"Removed {user.mention} from the reaction whitelist.")

    @markovset_whitelist.command(name="list")
    async def markovset_whitelist_list(self, ctx: commands.Context) -> None:
        """List users in the reaction whitelist."""
        whitelist = await self.config.guild(ctx.guild).whitelist()
        if whitelist:
            mentions = [f"<@{uid}>" for uid in whitelist]
            await ctx.send(f"Reaction whitelist: {', '.join(mentions)}")
        else:
            await ctx.send("No users in the reaction whitelist.")

    @markovset.command(name="train")
    async def markovset_train(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel,
        limit: int = 1000,
    ) -> None:
        """Bulk train from a channel's message history.

        Limit is capped at 10000 messages.
        """
        limit = min(limit, 10000)
        settings = await self.config.guild(ctx.guild).all()

        msg = await ctx.send(f"Training from {channel.mention}... (0 messages)")
        count = 0

        async for message in channel.history(limit=limit):
            if message.author.bot or not message.content:
                continue

            text = sanitize_message(message.content)
            if len(text.split()) < 3:
                continue

            await self._train_message(
                ctx.guild.id,
                message.author.id,
                text,
                settings["order"],
                settings.get("max_order", settings["order"]),
            )
            count += 1

            if count % 100 == 0:
                await msg.edit(content=f"Training from {channel.mention}... ({count} messages)")

        await msg.edit(content=f"Trained on {count} messages from {channel.mention}.")

    @markovset.command(name="clear")
    async def markovset_clear(
        self, ctx: commands.Context, user: Optional[discord.Member] = None
    ) -> None:
        """Clear chain data. Optionally specify a user to clear only their data."""
        storage = await self._get_storage(ctx.guild.id)

        if user:
            await storage.clear_user(user.id)
            await ctx.send(f"Cleared chain data for {user.mention}.")
        else:
            await storage.clear_all()
            await ctx.send("Cleared all chain data for this server.")

    @markovset.command(name="order")
    async def markovset_order(self, ctx: commands.Context, order: int) -> None:
        """Set the n-gram order (1-4). Higher = more coherent but less creative."""
        if order < 1 or order > 4:
            await ctx.send("Order must be between 1 and 4.")
            return
        await self.config.guild(ctx.guild).order.set(order)
        # Also update max_order if it's lower
        max_order = await self.config.guild(ctx.guild).max_order()
        if max_order < order:
            await self.config.guild(ctx.guild).max_order.set(order)
        await ctx.send(f"Set n-gram order to {order}.")

    @markovset.command(name="maxorder")
    async def markovset_maxorder(self, ctx: commands.Context, max_order: int) -> None:
        """Set the maximum order for backoff (1-4)."""
        if max_order < 1 or max_order > 4:
            await ctx.send("Max order must be between 1 and 4.")
            return
        order = await self.config.guild(ctx.guild).order()
        if max_order < order:
            await ctx.send(f"Max order cannot be less than current order ({order}).")
            return
        await self.config.guild(ctx.guild).max_order.set(max_order)
        await ctx.send(f"Set max order to {max_order}. Chain will train orders 1-{max_order} for backoff.")

    @markovset.command(name="length")
    async def markovset_length(
        self, ctx: commands.Context, user_max: int, admin_max: int
    ) -> None:
        """Set max output lengths for users and admins."""
        if user_max < 1 or admin_max < 1:
            await ctx.send("Lengths must be at least 1.")
            return
        if user_max > admin_max:
            await ctx.send("User max cannot be greater than admin max.")
            return

        await self.config.guild(ctx.guild).user_max_length.set(user_max)
        await self.config.guild(ctx.guild).admin_max_length.set(admin_max)
        await ctx.send(f"Set user max length to {user_max}, admin max length to {admin_max}.")

    @markovset.command(name="migrate")
    async def markovset_migrate(self, ctx: commands.Context) -> None:
        """Migrate old database format to new weighted format.

        Run this once after updating if you have existing chain data.
        This safely handles databases with mixed old/new format data.
        """
        storage = await self._get_storage(ctx.guild.id)

        # Check if migration is actually needed
        needs_migration = await storage.needs_migration()
        if not needs_migration:
            await ctx.send("No migration needed - database already in new format.")
            return

        msg = await ctx.send("Migrating database to new format...")

        try:
            migrated = await storage.migrate_to_counter_format()
            await msg.edit(content=f"Migration complete! Converted {migrated:,} rows to new format.")
        except Exception as e:
            log.error(f"Migration failed: {e}", exc_info=True)
            await msg.edit(content=f"Migration failed: {e}")
