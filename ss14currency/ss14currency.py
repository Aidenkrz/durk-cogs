import discord
import asyncpg
import logging
import uuid
import asyncio
import aiohttp
import urllib.parse
from discord.ui import Modal, TextInput
from discord import TextStyle
import typing
from typing import Dict, Optional
import random
from discord.ui import View, Button
from dataclasses import dataclass
from pathlib import Path
import aiosqlite

from redbot.core import commands, Config, checks, app_commands
from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.SS14Currency")

async def get_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID) -> Optional[int]:
    """Gets the currency for a given player ID."""
    async with pool.acquire() as conn:
        query = "SELECT server_currency FROM player WHERE user_id = $1;"
        return await conn.fetchval(query, player_id)

async def set_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> bool:
    """Sets the currency for a given player ID to a specific amount."""
    try:
        async with pool.acquire() as conn:
            query = "UPDATE player SET server_currency = $1 WHERE user_id = $2;"
            await conn.execute(query, amount, player_id)
            return True
    except Exception as e:
        log.error(f"Error setting currency for player {player_id}: {e}", exc_info=True)
        return False

async def add_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> bool:
    """Adds an amount of currency to a given player ID. Amount can be negative."""
    try:
        async with pool.acquire() as conn:
            query = "UPDATE player SET server_currency = server_currency + $1 WHERE user_id = $2;"
            await conn.execute(query, amount, player_id)
            return True
    except Exception as e:
        log.error(f"Error adding currency for player {player_id}: {e}", exc_info=True)
        return False

async def get_leaderboard(pool: asyncpg.Pool) -> list:
    """Gets the top 10 players by currency."""
    async with pool.acquire() as conn:
        query = "SELECT last_seen_user_name, server_currency FROM player ORDER BY server_currency DESC LIMIT 10;"
        return await conn.fetch(query)

async def get_player_id_from_discord(pool: asyncpg.Pool, discord_id: int) -> Optional[uuid.UUID]:
    """Gets the player's user_id from their discord ID."""
    async with pool.acquire() as conn:
        query = "SELECT player_id FROM rmc_linked_accounts WHERE discord_id = $1;"
        return await conn.fetchval(query, discord_id)

async def get_user_name_from_id(session: aiohttp.ClientSession, user_id: uuid.UUID) -> Optional[str]:
    """Queries the SS14 auth API for a user's username by their UUID."""
    url = f"https://auth.spacestation14.com/api/query/userid?userid={user_id}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("userName")
            else:
                log.warning(f"API query for {user_id} failed with status {response.status}")
                return None
    except aiohttp.ClientError as e:
        log.error(f"Error querying auth API for {user_id}: {e}", exc_info=True)
        return None

async def transfer_currency(pool: asyncpg.Pool, from_player_id: uuid.UUID, to_player_id: uuid.UUID, amount: int) -> Optional[Dict[str, int]]:
    """Atomically transfers currency from one player to another and returns their old and new balances."""
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                sender_balance = await conn.fetchval("SELECT server_currency FROM player WHERE user_id = $1 FOR UPDATE;", from_player_id)
                if sender_balance is None or sender_balance < amount:
                    return None

                recipient_balance = await conn.fetchval("SELECT server_currency FROM player WHERE user_id = $1 FOR UPDATE;", to_player_id)
                if recipient_balance is None:
                    return None

                await conn.execute("UPDATE player SET server_currency = server_currency - $1 WHERE user_id = $2;", amount, from_player_id)
                await conn.execute("UPDATE player SET server_currency = server_currency + $1 WHERE user_id = $2;", amount, to_player_id)

                return {
                    "sender_old": sender_balance,
                    "sender_new": sender_balance - amount,
                    "recipient_old": recipient_balance,
                    "recipient_new": recipient_balance + amount
                }
    except Exception as e:
        log.error(f"Error during currency transfer from {from_player_id} to {to_player_id}: {e}", exc_info=True)
        return None

@dataclass
class PlayerInfo:
    """Information about a resolved player."""
    player_id: uuid.UUID
    player_name: str
    discord_name: Optional[str] = None


class ConfirmationView(View):
    """View for confirming large transactions."""
    def __init__(self, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.value = None

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: Button):
        self.value = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        self.value = False
        self.stop()
        await interaction.response.defer()


class DbConfigModal(Modal, title="Database Configuration"):

    db_user = TextInput(label="Database Username", style=TextStyle.short, required=True)
    db_pass = TextInput(label="Database Password", style=TextStyle.short, required=True)
    db_host = TextInput(label="Database Host (IP or Domain)", style=TextStyle.short, required=True)
    db_port = TextInput(label="Database Port", style=TextStyle.short, required=True, default="5432")
    db_name = TextInput(label="Database Name", style=TextStyle.short, required=True)

    def __init__(self, cog_instance: 'SS14Currency', guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog_instance
        self.guild_id = guild_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)

        username = self.db_user.value.strip()
        password = self.db_pass.value
        host = self.db_host.value.strip()
        port = self.db_port.value.strip()
        dbname = self.db_name.value.strip()

        if not port.isdigit():
            await interaction.followup.send("Port must be a number.", ephemeral=True)
            return

        encoded_password = urllib.parse.quote(password)
        connection_string = f"postgresql://{username}:{encoded_password}@{host}:{port}/{dbname}"

        await self.cog.config.guild_from_id(self.guild_id).db_connection_string.set(connection_string)

        await self.cog.close_guild_pool(self.guild_id)
        pool = await self.cog.get_pool_for_guild(self.guild_id)

        if pool:
            await interaction.followup.send("Database connection string saved and tested successfully!", ephemeral=True)
        else:
            safe_debug_string = f"postgresql://{username}:********@{host}:{port}/{dbname}"
            await interaction.followup.send(f"Failed to connect using the provided details. Please check them and try again.\n(Attempted connection: `{safe_debug_string}`)", ephemeral=True)
        return False

class SS14Currency(commands.Cog):
    """Cog for managing SS14 server currency."""
    async def close_guild_pool(self, guild_id: int):
        if guild_id in self.guild_pools:
            pool = self.guild_pools.pop(guild_id)
            if pool:
                await pool.close()
                log.info(f"Closed database connection pool for Guild {guild_id}.")
        if guild_id in self.pool_locks:
            del self.pool_locks[guild_id]


    DEFAULT_GUILD = {
        "db_connection_string": None,
        "transfer_rate_limit": 5,  # Max transfers per time window
        "transfer_rate_window": 60,  # Time window in seconds
        "gambling_cooldown": 30,  # Seconds between gambling attempts
        "large_transaction_threshold": 10000,  # Amount requiring confirmation
    }

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier="SS14CurrencyMultiDB", force_registration=True)
        self.config.register_guild(**self.DEFAULT_GUILD)
        self.guild_pools: Dict[int, asyncpg.Pool] = {}
        self.pool_locks: Dict[int, asyncio.Lock] = {}
        self.session = aiohttp.ClientSession()
        
        # Local SQLite database for bot-specific data
        self.local_db_path = Path(__file__).parent / "gambling_stats.db"
        self.local_db: Optional[aiosqlite.Connection] = None
        
        # Rate limiting and cooldown tracking
        self.transfer_timestamps: Dict[int, list] = {}  # user_id -> list of timestamps
        self.gambling_cooldowns: Dict[int, float] = {}  # user_id -> timestamp

    async def get_pool_for_guild(self, guild_id: int) -> Optional[asyncpg.Pool]:
        if guild_id in self.guild_pools:
            return self.guild_pools[guild_id]

        if guild_id not in self.pool_locks:
            self.pool_locks[guild_id] = asyncio.Lock()

        async with self.pool_locks[guild_id]:
            if guild_id in self.guild_pools:
                return self.guild_pools[guild_id]

            conn_string = await self.config.guild_from_id(guild_id).db_connection_string()
            if not conn_string:
                log.warning(f"Database connection string not set for Guild {guild_id}.")
                return None

            try:
                pool = await asyncpg.create_pool(conn_string, min_size=2, max_size=10)
                async with pool.acquire() as conn:
                    await conn.execute("SELECT 1;")
                log.info(f"Database connection pool established for Guild {guild_id}.")
                self.guild_pools[guild_id] = pool
                return pool
            except (asyncpg.PostgresError, OSError) as e:
                log.error(f"Failed to establish database connection pool for Guild {guild_id}: {e}", exc_info=True)
                return None

    async def initialize_local_db(self):
        """Initialize the local SQLite database for gambling stats."""
        if self.local_db is not None:
            return
            
        self.local_db = await aiosqlite.connect(self.local_db_path)
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS gambling_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                player_id TEXT NOT NULL,
                game_type TEXT NOT NULL,
                total_games INTEGER DEFAULT 0,
                total_wins INTEGER DEFAULT 0,
                total_losses INTEGER DEFAULT 0,
                total_wagered INTEGER DEFAULT 0,
                total_won INTEGER DEFAULT 0,
                total_lost INTEGER DEFAULT 0,
                biggest_win INTEGER DEFAULT 0,
                biggest_loss INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(guild_id, player_id, game_type)
            )
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_gambling_stats_player
            ON gambling_stats(guild_id, player_id)
        """)
        await self.local_db.commit()
        log.info("Local gambling stats database initialized.")

    async def resolve_player(
        self,
        user: typing.Union[discord.Member, str],
        pool: asyncpg.Pool
    ) -> Optional[PlayerInfo]:
        """
        Resolves a Discord member or SS14 username to player information.
        
        Returns:
            PlayerInfo object with player_id, player_name, and optionally discord_name
            None if player cannot be found
        """
        player_id = None
        player_name = None
        discord_name = None

        if isinstance(user, discord.Member):
            # Try linked account first
            player_id = await get_player_id_from_discord(pool, user.id)
            if player_id:
                player_name = await get_user_name_from_id(self.session, player_id)
                discord_name = user.display_name
            else:
                # Fall back to username lookup
                player_id = await self.get_user_id_from_name(user.name)
                player_name = user.name
        else:
            # Direct SS14 username lookup
            player_id = await self.get_user_id_from_name(user)
            player_name = user

        if not player_id:
            return None
        
        return PlayerInfo(
            player_id=player_id,
            player_name=player_name,
            discord_name=discord_name
        )

    async def check_rate_limit(self, user_id: int, guild_id: int) -> bool:
        """
        Checks if user has exceeded transfer rate limit.
        Returns True if allowed, False if rate limited.
        """
        limit = await self.config.guild_from_id(guild_id).transfer_rate_limit()
        window = await self.config.guild_from_id(guild_id).transfer_rate_window()
        
        now = asyncio.get_event_loop().time()
        
        if user_id not in self.transfer_timestamps:
            self.transfer_timestamps[user_id] = []
        
        # Remove timestamps outside the window
        self.transfer_timestamps[user_id] = [
            ts for ts in self.transfer_timestamps[user_id]
            if now - ts < window
        ]
        
        if len(self.transfer_timestamps[user_id]) >= limit:
            return False
        
        self.transfer_timestamps[user_id].append(now)
        return True

    async def get_rate_limit_wait_time(self, user_id: int, guild_id: int) -> int:
        """Returns seconds until user can transfer again."""
        if user_id not in self.transfer_timestamps or not self.transfer_timestamps[user_id]:
            return 0
        
        window = await self.config.guild_from_id(guild_id).transfer_rate_window()
        oldest = min(self.transfer_timestamps[user_id])
        now = asyncio.get_event_loop().time()
        
        return max(0, int(window - (now - oldest)))

    async def confirm_large_transaction(
        self,
        ctx: commands.Context,
        amount: int,
        action: str,
        target: str
    ) -> bool:
        """
        Prompts for confirmation if transaction is above threshold.
        Returns True if confirmed or below threshold, False if cancelled.
        """
        threshold = await self.config.guild(ctx.guild).large_transaction_threshold()
        
        if amount < threshold:
            return True
        
        embed = discord.Embed(
            title="⚠️ Large Transaction Confirmation",
            description=(
                f"You are about to {action} **{amount:,}** coins {target}.\n\n"
                f"This is above the threshold of {threshold:,} coins.\n"
                f"Please confirm this action."
            ),
            color=discord.Color.orange()
        )
        
        view = ConfirmationView(timeout=30.0)
        message = await ctx.send(embed=embed, view=view)
        
        await view.wait()
        
        if view.value is None:
            await message.edit(content="❌ Transaction cancelled (timeout).", embed=None, view=None)
            return False
        elif view.value:
            await message.edit(content="✅ Transaction confirmed.", embed=None, view=None)
            return True
        else:
            await message.edit(content="❌ Transaction cancelled.", embed=None, view=None)
            return False

    async def record_gambling_result(
        self,
        guild_id: int,
        player_id: uuid.UUID,
        game_type: str,
        wagered: int,
        won: bool,
        winnings: int  # Net gain/loss
    ) -> bool:
        """Records a gambling game result in LOCAL database."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        try:
            player_id_str = str(player_id)
            
            # Check if record exists
            async with self.local_db.execute(
                "SELECT total_games FROM gambling_stats WHERE guild_id = ? AND player_id = ? AND game_type = ?",
                (guild_id, player_id_str, game_type)
            ) as cursor:
                existing = await cursor.fetchone()
            
            if existing:
                # Update existing record
                await self.local_db.execute("""
                    UPDATE gambling_stats SET
                        total_games = total_games + 1,
                        total_wins = total_wins + ?,
                        total_losses = total_losses + ?,
                        total_wagered = total_wagered + ?,
                        total_won = total_won + ?,
                        total_lost = total_lost + ?,
                        biggest_win = MAX(biggest_win, ?),
                        biggest_loss = MAX(biggest_loss, ?),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE guild_id = ? AND player_id = ? AND game_type = ?
                """, (
                    1 if won else 0,  # wins
                    0 if won else 1,  # losses
                    wagered,
                    max(0, winnings),  # total_won
                    max(0, -winnings), # total_lost
                    max(0, winnings),  # biggest_win
                    max(0, -winnings), # biggest_loss
                    guild_id, player_id_str, game_type
                ))
            else:
                # Insert new record
                await self.local_db.execute("""
                    INSERT INTO gambling_stats (
                        guild_id, player_id, game_type, total_games, total_wins, total_losses,
                        total_wagered, total_won, total_lost, biggest_win, biggest_loss
                    ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    guild_id, player_id_str, game_type,
                    1 if won else 0,  # wins
                    0 if won else 1,  # losses
                    wagered,
                    max(0, winnings),  # total_won
                    max(0, -winnings), # total_lost
                    max(0, winnings),  # biggest_win
                    max(0, -winnings)  # biggest_loss
                ))
            
            await self.local_db.commit()
            return True
        except Exception as e:
            log.error(f"Error recording gambling stats: {e}", exc_info=True)
            return False

    async def get_gambling_stats(
        self,
        guild_id: int,
        player_id: uuid.UUID
    ) -> list:
        """Gets gambling statistics from LOCAL database."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        player_id_str = str(player_id)
        
        async with self.local_db.execute("""
            SELECT
                game_type, total_games, total_wins, total_losses,
                total_wagered, total_won, total_lost, biggest_win, biggest_loss
            FROM gambling_stats
            WHERE guild_id = ? AND player_id = ?
        """, (guild_id, player_id_str)) as cursor:
            rows = await cursor.fetchall()
            
        # Convert to list of dicts
        return [
            {
                'game_type': row[0],
                'total_games': row[1],
                'total_wins': row[2],
                'total_losses': row[3],
                'total_wagered': row[4],
                'total_won': row[5],
                'total_lost': row[6],
                'biggest_win': row[7],
                'biggest_loss': row[8]
            }
            for row in rows
        ]

    @commands.group(name="currency")
    @commands.guild_only()
    async def currency(self, ctx: commands.Context):
        """Manage SS14 server currency."""
        pass

    @currency.command(name="self")
    async def self_coins(self, ctx: commands.Context):
        """Check your own coin balance if your account is linked."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        player_id = await get_player_id_from_discord(pool, ctx.author.id)
        if not player_id:
            await ctx.send("Your Discord account is not linked to an SS14 account. Please link your account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
            return

        balance = await get_player_currency(pool, player_id)
        if balance is not None:
            embed = discord.Embed(title="Your Coin Balance", color=discord.Color.blue())
            embed.add_field(name="Balance", value=f"{balance} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send("Could not retrieve your balance.", ephemeral=True)

    @currency.command(name="get")
    async def get_coins(self, ctx: commands.Context, *, user: typing.Union[discord.Member, str]):
        """Gets the coin balance for a given SS14 username or linked Discord user."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        player_info = await self.resolve_player(user, pool)
        if not player_info:
            if isinstance(user, discord.Member):
                await ctx.send(f"Could not find a linked SS14 account for {user.mention} or an SS14 account with the name `{user.name}`. They can link their account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
            else:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
            return

        balance = await get_player_currency(pool, player_info.player_id)
        if balance is not None:
            embed = discord.Embed(title="Coin Balance", color=discord.Color.blue())
            if player_info.discord_name:
                embed.add_field(name="Discord User", value=discord.utils.escape_markdown(player_info.discord_name), inline=True)
                embed.add_field(name="SS14 Username", value=discord.utils.escape_markdown(player_info.player_name), inline=True)
            else:
                embed.add_field(name="Player", value=discord.utils.escape_markdown(player_info.player_name), inline=False)
            embed.add_field(name="Balance", value=f"{balance} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Could not retrieve the balance for **{player_info.player_name}**.", ephemeral=True)

    @currency.command(name="set")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_coins(self, ctx: commands.Context, user: typing.Union[discord.Member, str], amount: int):
        """Sets the coin balance for a given SS14 username or linked Discord user."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        if amount < 0:
            await ctx.send("You cannot set a negative coin balance.", ephemeral=True)
            return

        player_info = await self.resolve_player(user, pool)
        if not player_info:
            if isinstance(user, discord.Member):
                await ctx.send(f"{user.mention} does not have a linked SS14 account.", ephemeral=True)
            else:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
            return

        # Check for large transaction confirmation
        target_name = player_info.discord_name or player_info.player_name
        if not await self.confirm_large_transaction(ctx, amount, "set balance to", f"for {target_name}"):
            return

        if await set_player_currency(pool, player_info.player_id, amount):
            embed = discord.Embed(title="Balance Set", color=discord.Color.green())
            if player_info.discord_name:
                embed.add_field(name="Discord User", value=discord.utils.escape_markdown(player_info.discord_name), inline=True)
                embed.add_field(name="SS14 Username", value=discord.utils.escape_markdown(player_info.player_name), inline=True)
            else:
                embed.add_field(name="Player", value=discord.utils.escape_markdown(player_info.player_name), inline=False)
            embed.add_field(name="New Balance", value=f"{amount} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Failed to set the balance for **{player_info.player_name}**.", ephemeral=True)

    @currency.command(name="add")
    @checks.admin_or_permissions(manage_guild=True)
    async def add_coins(self, ctx: commands.Context, user: typing.Union[discord.Member, str], amount: int):
        """Adds coins to a given SS14 username or linked Discord user. Can be a negative number."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        player_info = await self.resolve_player(user, pool)
        if not player_info:
            if isinstance(user, discord.Member):
                await ctx.send(f"{user.mention} does not have a linked SS14 account.", ephemeral=True)
            else:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
            return

        # Check for large transaction confirmation (only for positive amounts)
        if amount > 0:
            target_name = player_info.discord_name or player_info.player_name
            if not await self.confirm_large_transaction(ctx, amount, "add", f"to {target_name}"):
                return

        if await add_player_currency(pool, player_info.player_id, amount):
            embed = discord.Embed(title="Balance Updated", color=discord.Color.green())
            if player_info.discord_name:
                embed.add_field(name="Discord User", value=discord.utils.escape_markdown(player_info.discord_name), inline=True)
                embed.add_field(name="SS14 Username", value=discord.utils.escape_markdown(player_info.player_name), inline=True)
            else:
                embed.add_field(name="Player", value=discord.utils.escape_markdown(player_info.player_name), inline=False)
            embed.add_field(name="Amount Added", value=f"{amount} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Failed to add coins for **{player_info.player_name}**.", ephemeral=True)
    @currency.command(name="transfer")
    async def transfer_coins(self, ctx: commands.Context, recipient: typing.Union[discord.Member, str], amount: int):
        """Transfers coins from your linked SS14 account to another player."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        if amount <= 0:
            await ctx.send("You must transfer a positive amount of coins.", ephemeral=True)
            return

        # Check rate limit
        if not await self.check_rate_limit(ctx.author.id, ctx.guild.id):
            wait_time = await self.get_rate_limit_wait_time(ctx.author.id, ctx.guild.id)
            await ctx.send(
                f"⏱️ You're transferring too quickly! Please wait {wait_time} seconds.",
                ephemeral=True
            )
            return

        sender_id = await get_player_id_from_discord(pool, ctx.author.id)
        if not sender_id:
            await ctx.send("Your Discord account is not linked to an SS14 account. Please link your account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
            return

        recipient_info = await self.resolve_player(recipient, pool)
        if not recipient_info:
            if isinstance(recipient, discord.Member):
                await ctx.send(f"{recipient.mention} does not have a linked SS14 account. They can link their account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
            else:
                await ctx.send(f"Could not find a user with the name `{recipient}`.", ephemeral=True)
            return

        if sender_id == recipient_info.player_id:
            await ctx.send("You cannot transfer coins to yourself.", ephemeral=True)
            return

        # Check for large transaction confirmation
        target_name = recipient_info.discord_name or recipient_info.player_name
        if not await self.confirm_large_transaction(ctx, amount, "transfer", f"to {target_name}"):
            return

        transfer_details = await transfer_currency(pool, sender_id, recipient_info.player_id, amount)
        if transfer_details:
            sender_name = await get_user_name_from_id(self.session, sender_id)
            sender_name_escaped = discord.utils.escape_markdown(sender_name)
            sender_discord_name_escaped = discord.utils.escape_markdown(ctx.author.display_name)
            embed = discord.Embed(title="Transfer Successful", color=discord.Color.green())

            sender_field_name = f"Sender: {sender_discord_name_escaped} ({sender_name_escaped})"
            sender_field_value = f"`{transfer_details['sender_old']}` -> `{transfer_details['sender_new']}`"
            embed.add_field(name=sender_field_name, value=sender_field_value, inline=False)

            recipient_name_escaped = discord.utils.escape_markdown(recipient_info.player_name)
            if recipient_info.discord_name:
                recipient_discord_name_escaped = discord.utils.escape_markdown(recipient_info.discord_name)
                recipient_field_name = f"Recipient: {recipient_discord_name_escaped} ({recipient_name_escaped})"
            else:
                recipient_field_name = f"Recipient: {recipient_name_escaped}"
            recipient_field_value = f"`{transfer_details['recipient_old']}` -> `{transfer_details['recipient_new']}`"
            embed.add_field(name=recipient_field_name, value=recipient_field_value, inline=False)

            embed.add_field(name="Amount", value=f"{amount} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send("The transfer failed. This may be due to insufficient funds or an issue with the recipient's account.", ephemeral=True)

    @currency.command(name="leaderboard")
    async def leaderboard(self, ctx: commands.Context):
        """Shows the top 10 players with the most coins."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        leaderboard_data = await get_leaderboard(pool)
        if not leaderboard_data:
            await ctx.send("The leaderboard is currently empty.")
            return

        embed = discord.Embed(title="Top 10 Coin Holders", color=discord.Color.gold())
        for i, record in enumerate(leaderboard_data, 1):
            embed.add_field(name=f"{i}. {discord.utils.escape_markdown(record['last_seen_user_name'])}", value=f"{record['server_currency']} coins", inline=False)
        await ctx.send(embed=embed)

    @currency.command(name="gamblingstats")
    async def gambling_stats(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        """Shows gambling statistics for yourself or another user."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured.", ephemeral=True)
            return

        target = user or ctx.author
        player_id = await get_player_id_from_discord(pool, target.id)
        if not player_id:
            await ctx.send(f"{target.mention} doesn't have a linked account.", ephemeral=True)
            return

        stats = await self.get_gambling_stats(ctx.guild.id, player_id)
        if not stats:
            await ctx.send("No gambling statistics found.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"🎲 Gambling Statistics for {target.display_name}",
            color=discord.Color.purple()
        )
        
        for stat in stats:
            win_rate = (stat['total_wins'] / stat['total_games'] * 100) if stat['total_games'] > 0 else 0
            net_profit = stat['total_won'] - stat['total_lost']
            
            value = (
                f"**Games:** {stat['total_games']} | "
                f"**W/L:** {stat['total_wins']}/{stat['total_losses']}\n"
                f"**Win Rate:** {win_rate:.1f}%\n"
                f"**Wagered:** {stat['total_wagered']} | "
                f"**Net:** {net_profit:+d}\n"
                f"**Biggest Win:** {stat['biggest_win']} | "
                f"**Biggest Loss:** {stat['biggest_loss']}"
            )
            embed.add_field(name=stat['game_type'].title(), value=value, inline=False)
        
        await ctx.send(embed=embed)

    @app_commands.command(name="coinsetdb")
    @app_commands.guild_only()
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe()
    async def coinsetdb_slash(self, interaction: discord.Interaction):
        """Opens a modal to configure the database connection for this server (Admins only)."""
        if not interaction.guild_id:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return
        await interaction.response.send_modal(DbConfigModal(self, interaction.guild_id))

    async def cog_unload(self):
        await self.session.close()
        
        # Close SS14 database pools
        guild_ids = list(self.guild_pools.keys())
        for guild_id in guild_ids:
            pool = self.guild_pools.pop(guild_id)
            if pool:
                await pool.close()
        
        # Close local SQLite database
        if self.local_db:
            await self.local_db.close()
        
        log.info("All database connections closed.")

    async def get_user_id_from_name(self, username: str) -> Optional[uuid.UUID]:
        """Queries the SS14 auth API for a user's UUID by their username."""
        url = f"https://auth.spacestation14.com/api/query/name?name={username}"
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return uuid.UUID(data["userId"])
                else:
                    log.warning(f"API query for {username} failed with status {response.status}")
                    return None
        except aiohttp.ClientError as e:
            log.error(f"Error querying auth API for {username}: {e}", exc_info=True)
            return None

    @currency.command(name="coinflip")
    @commands.cooldown(rate=1, per=30.0, type=commands.BucketType.user)
    async def coinflip(self, ctx: commands.Context, amount: int, opponent: discord.Member = None):
        """Challenges another user to a coinflip for a specified amount.
        
        If no opponent is specified, the challenge will be open for anyone to accept.
        """
        if opponent and opponent.id == ctx.author.id:
            await ctx.send("You cannot challenge yourself to a coinflip.", ephemeral=True)
            return
            
        if opponent and opponent.bot:
            await ctx.send("You cannot challenge a bot to a coinflip.", ephemeral=True)
            return

        if amount <= 0:
            await ctx.send("You must wager a positive amount of coins.", ephemeral=True)
            return

        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return

        challenger_id = await get_player_id_from_discord(pool, ctx.author.id)
        if not challenger_id:
            await ctx.send("You must have a linked SS14 account to start a coinflip.", ephemeral=True)
            return
        
        challenger_balance = await get_player_currency(pool, challenger_id)
        if challenger_balance < amount:
            await ctx.send(f"You do not have enough coins to wager {amount}.", ephemeral=True)
            return

        if opponent:
            opponent_id = await get_player_id_from_discord(pool, opponent.id)
            if not opponent_id:
                await ctx.send(f"{opponent.mention} does not have a linked SS14 account and cannot be challenged.", ephemeral=True)
                return

            opponent_balance = await get_player_currency(pool, opponent_id)
            if opponent_balance < amount:
                await ctx.send(f"{opponent.mention} does not have enough coins to accept this wager.", ephemeral=True)
                return
            
            view = CoinflipView(self, ctx.author, opponent, amount, pool, ctx.guild.id)
            
            embed = discord.Embed(
                title="⚔️ Coinflip Challenge! ⚔️",
                description=f"{ctx.author.mention} has challenged {opponent.mention} to a coinflip for **{amount}** coins!",
                color=discord.Color.orange()
            )
            message = await ctx.send(embed=embed, view=view)
            view.message = message
        else:
            view = OpenCoinflipView(self, ctx.author, amount, pool, ctx.guild.id)
            embed = discord.Embed(
                title="⚔️ Open Coinflip Challenge! ⚔️",
                description=f"{ctx.author.mention} has started an open coinflip challenge for **{amount}** coins! Anyone can accept.",
                color=discord.Color.blue()
            )
            message = await ctx.send(embed=embed, view=view)
            view.message = message

    @coinflip.error
    async def coinflip_error(self, ctx: commands.Context, error):
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send(
                f"🎰 Slow down! You can gamble again in {error.retry_after:.1f} seconds.",
                ephemeral=True
            )
        else:
            raise error

class OpenCoinflipView(View):
    def __init__(self, cog: 'SS14Currency', challenger: discord.Member, amount: int, pool: asyncpg.Pool, guild_id: int):
        super().__init__(timeout=300) # 5 minute timeout for open challenges
        self.cog = cog
        self.challenger = challenger
        self.amount = amount
        self.pool = pool
        self.guild_id = guild_id

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        await self.message.edit(content="This open coinflip challenge has expired.", view=self)

    @discord.ui.button(label="Accept Challenge", style=discord.ButtonStyle.green)
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        opponent = interaction.user
        if opponent.id == self.challenger.id:
            await interaction.response.send_message("You cannot accept your own coinflip challenge.", ephemeral=True)
            return

        await interaction.response.defer()

        challenger_id = await get_player_id_from_discord(self.pool, self.challenger.id)
        opponent_id = await get_player_id_from_discord(self.pool, opponent.id)

        if not opponent_id:
            await interaction.followup.send("You must have a linked SS14 account to accept a coinflip challenge.", ephemeral=True)
            return

        challenger_balance = await get_player_currency(self.pool, challenger_id)
        opponent_balance = await get_player_currency(self.pool, opponent_id)

        if challenger_balance < self.amount:
            await interaction.followup.send(f"{self.challenger.mention} no longer has enough coins for this coinflip.", ephemeral=True)
            self.stop()
            return
        if opponent_balance < self.amount:
            await interaction.followup.send("You do not have enough coins to accept this coinflip.", ephemeral=True)
            self.stop()
            return

        winner = random.choice([self.challenger, opponent])
        loser = opponent if winner.id == self.challenger.id else self.challenger
        
        winner_player_id = challenger_id if winner.id == self.challenger.id else opponent_id
        loser_player_id = opponent_id if winner.id == self.challenger.id else challenger_id

        transfer_details = await transfer_currency(self.pool, loser_player_id, winner_player_id, self.amount)
        
        for item in self.children:
            item.disabled = True

        if transfer_details:
            # Record gambling statistics
            await self.cog.record_gambling_result(
                self.guild_id, winner_player_id, "coinflip",
                self.amount, True, self.amount
            )
            await self.cog.record_gambling_result(
                self.guild_id, loser_player_id, "coinflip",
                self.amount, False, -self.amount
            )
            
            winner_name = await get_user_name_from_id(self.cog.session, winner_player_id)
            loser_name = await get_user_name_from_id(self.cog.session, loser_player_id)

            embed = discord.Embed(title="Coinflip Result!", color=discord.Color.gold())
            embed.description = f"**{discord.utils.escape_markdown(winner.display_name)}** won the coinflip against **{discord.utils.escape_markdown(loser.display_name)}**!"
            
            winner_field_name = f"Winner: {discord.utils.escape_markdown(winner.display_name)} ({discord.utils.escape_markdown(winner_name)})"
            winner_field_value = f"`{transfer_details['recipient_old']}` -> `{transfer_details['recipient_new']}`"
            embed.add_field(name=winner_field_name, value=winner_field_value, inline=False)
            
            loser_field_name = f"Loser: {discord.utils.escape_markdown(loser.display_name)} ({discord.utils.escape_markdown(loser_name)})"
            loser_field_value = f"`{transfer_details['sender_old']}` -> `{transfer_details['sender_new']}`"
            embed.add_field(name=loser_field_name, value=loser_field_value, inline=False)

            embed.add_field(name="Wager", value=f"{self.amount} coins", inline=False)
            
            await self.message.edit(content=None, embed=embed, view=self)
        else:
            await self.message.edit(content="An error occurred during the transfer.", view=self)
        
        self.stop()

class CoinflipView(View):
    def __init__(self, cog: 'SS14Currency', challenger: discord.Member, opponent: discord.Member, amount: int, pool: asyncpg.Pool, guild_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.challenger = challenger
        self.opponent = opponent
        self.amount = amount
        self.pool = pool
        self.guild_id = guild_id
        self.result = None

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        await self.message.edit(content="Coinflip challenge expired.", view=self)

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.green)
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.opponent.id:
            await interaction.response.send_message("You are not the opponent in this coinflip.", ephemeral=True)
            return

        await interaction.response.defer()

        challenger_id = await get_player_id_from_discord(self.pool, self.challenger.id)
        opponent_id = await get_player_id_from_discord(self.pool, self.opponent.id)

        challenger_balance = await get_player_currency(self.pool, challenger_id)
        opponent_balance = await get_player_currency(self.pool, opponent_id)

        if challenger_balance < self.amount:
            await interaction.followup.send(f"{self.challenger.mention} no longer has enough coins for this coinflip.", ephemeral=True)
            self.stop()
            return
        if opponent_balance < self.amount:
            await interaction.followup.send("You no longer have enough coins for this coinflip.", ephemeral=True)
            self.stop()
            return

        winner = random.choice([self.challenger, self.opponent])
        loser = self.opponent if winner.id == self.challenger.id else self.challenger
        
        winner_player_id = challenger_id if winner.id == self.challenger.id else opponent_id
        loser_player_id = opponent_id if winner.id == self.challenger.id else challenger_id

        transfer_details = await transfer_currency(self.pool, loser_player_id, winner_player_id, self.amount)

        for item in self.children:
            item.disabled = True
        
        if transfer_details:
            # Record gambling statistics
            await self.cog.record_gambling_result(
                self.guild_id, winner_player_id, "coinflip",
                self.amount, True, self.amount
            )
            await self.cog.record_gambling_result(
                self.guild_id, loser_player_id, "coinflip",
                self.amount, False, -self.amount
            )
            
            winner_name = await get_user_name_from_id(self.cog.session, winner_player_id)
            loser_name = await get_user_name_from_id(self.cog.session, loser_player_id)

            embed = discord.Embed(title="Coinflip Result!", color=discord.Color.gold())
            embed.description = f"**{discord.utils.escape_markdown(winner.display_name)}** won the coinflip against **{discord.utils.escape_markdown(loser.display_name)}**!"
            
            winner_field_name = f"Winner: {discord.utils.escape_markdown(winner.display_name)} ({discord.utils.escape_markdown(winner_name)})"
            winner_field_value = f"`{transfer_details['recipient_old']}` -> `{transfer_details['recipient_new']}`"
            embed.add_field(name=winner_field_name, value=winner_field_value, inline=False)
            
            loser_field_name = f"Loser: {discord.utils.escape_markdown(loser.display_name)} ({discord.utils.escape_markdown(loser_name)})"
            loser_field_value = f"`{transfer_details['sender_old']}` -> `{transfer_details['sender_new']}`"
            embed.add_field(name=loser_field_name, value=loser_field_value, inline=False)

            embed.add_field(name="Wager", value=f"{self.amount} coins", inline=False)

            await self.message.edit(content=None, embed=embed, view=self)
        else:
            await self.message.edit(content="An error occurred during the transfer.", view=self)
        
        self.stop()


    @discord.ui.button(label="Decline", style=discord.ButtonStyle.red)
    async def decline_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id not in (self.challenger.id, self.opponent.id):
            await interaction.response.send_message("You are not part of this coinflip.", ephemeral=True)
            return

        for item in self.children:
            item.disabled = True

        if interaction.user.id == self.opponent.id:
            await self.message.edit(content=f"{self.opponent.mention} has declined the coinflip.", view=self)
        else: # Challenger cancelled
             await self.message.edit(content=f"{self.challenger.mention} has cancelled the coinflip.", view=self)
        
        self.stop()
