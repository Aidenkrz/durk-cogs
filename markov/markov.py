"""Markov chain text generation cog for Red-DiscordBot."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import discord
from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

from .chain import MarkovChain, sanitize_message
from .storage import MarkovStorage

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
            "min_length": 5,  # Min words in output
            "max_length": 50,  # Max words in output (for admins)
            "user_max_length": 10,  # Max words for normal users
            "admin_max_length": 100,  # Max words for Discord admins
            "whitelist": [],  # User IDs who can trigger :chains: reaction
        }

        self.config.register_guild(**default_guild)
        self._storage: Dict[int, MarkovStorage] = {}

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
        self, guild_id: int, user_id: int, text: str, order: int
    ) -> None:
        """Train the chain with a message."""
        chain = MarkovChain(order=order)
        chain.train(text)

        if not chain.chain:
            return

        storage = await self._get_storage(guild_id)
        # Add to guild chain
        await storage.add_transitions(chain.chain)
        # Add to user chain
        await storage.add_transitions(chain.chain, user_id=user_id)
        # Increment message count
        await storage.increment_message_count(user_id)

    async def _generate_text(
        self,
        guild_id: int,
        order: int,
        min_length: int,
        max_length: int,
        user_id: Optional[int] = None,
        seed_words: Optional[str] = None,
    ) -> str:
        """Generate text from the chain."""
        storage = await self._get_storage(guild_id)

        if user_id:
            chain_data = await storage.get_user_chain(user_id)
        else:
            chain_data = await storage.get_guild_chain()

        if not chain_data:
            return ""

        chain = MarkovChain(order=order)
        chain.chain = chain_data

        seed = None
        if seed_words:
            words = seed_words.split()
            seed = chain.find_seed(words)

        return chain.generate(min_words=min_length, max_words=max_length, seed=seed)

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
            message.guild.id, message.author.id, text, settings["order"]
        )

    @commands.Cog.listener()
    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        """Listen for :chains: reactions from whitelisted users."""
        if not payload.guild_id:
            return

        # Check if it's the chains emoji
        if payload.emoji.name != "\U0001f517":  # :chains: emoji
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
        text = await self._generate_text(
            guild.id,
            settings["order"],
            min_length=3,
            max_length=15,
        )

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

        text = await self._generate_text(
            ctx.guild.id,
            settings["order"],
            min_length=settings["min_length"],
            max_length=length,
        )

        if text:
            await ctx.send(text)
        else:
            await ctx.send("No chain data yet. Enable training and send some messages!")

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

        text = await self._generate_text(
            ctx.guild.id,
            settings["order"],
            min_length=settings["min_length"],
            max_length=length,
            user_id=user.id,
        )

        if text:
            await ctx.send(f"**{user.display_name}:** {text}")
        else:
            await ctx.send(f"No chain data for {user.display_name} yet.")

    @markov.command(name="seed")
    @commands.guild_only()
    async def markov_seed(
        self, ctx: commands.Context, *, seed_words: str
    ) -> None:
        """Generate text starting from specific words."""
        settings = await self.config.guild(ctx.guild).all()
        max_allowed = self._get_max_length(ctx.author, settings)

        text = await self._generate_text(
            ctx.guild.id,
            settings["order"],
            min_length=settings["min_length"],
            max_length=max_allowed,
            seed_words=seed_words,
        )

        if text:
            await ctx.send(text)
        else:
            await ctx.send("Couldn't generate text with those seed words.")

    @markov.command(name="stats")
    @commands.guild_only()
    async def markov_stats(self, ctx: commands.Context) -> None:
        """Show Markov chain statistics."""
        storage = await self._get_storage(ctx.guild.id)
        stats = await storage.get_stats()

        embed = discord.Embed(
            title="Markov Chain Stats",
            color=await ctx.embed_color(),
        )
        embed.add_field(
            name="Chain Size",
            value=f"{stats['state_count']:,} states\n{stats['transition_count']:,} transitions",
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
                ctx.guild.id, message.author.id, text, settings["order"]
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
        await ctx.send(f"Set n-gram order to {order}.")

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
