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
import time
import secrets

from redbot.core import commands, Config, checks, app_commands
from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.SS14Currency")

async def get_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID) -> Optional[int]:
    """Gets the currency for a given player ID."""
    async with pool.acquire() as conn:
        query = "SELECT server_currency FROM player WHERE user_id = $1;"
        return await conn.fetchval(query, player_id)

async def set_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> tuple[bool, Optional[int]]:
    """Sets the currency for a given player ID to a specific amount. Returns (success, old_balance)."""
    if amount < 0:
        log.warning(f"Attempted to set negative balance {amount} for player {player_id}")
        return False, None
    
    try:
        async with pool.acquire() as conn:
            # Get old balance
            old_balance = await conn.fetchval("SELECT server_currency FROM player WHERE user_id = $1;", player_id)
            if old_balance is None:
                return False, None
            
            query = "UPDATE player SET server_currency = $1 WHERE user_id = $2;"
            await conn.execute(query, amount, player_id)
            return True, old_balance
    except Exception as e:
        log.error(f"Error setting currency for player {player_id}: {e}", exc_info=True)
        return False, None

async def add_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> tuple[bool, Optional[int], Optional[int]]:
    """Adds an amount of currency to a given player ID. Returns (success, old_balance, new_balance). Prevents negative balances."""
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Get current balance with row lock
                old_balance = await conn.fetchval(
                    "SELECT server_currency FROM player WHERE user_id = $1 FOR UPDATE;",
                    player_id
                )
                if old_balance is None:
                    return False, None, None
                
                new_balance = old_balance + amount
                
                # Negative balance protection
                if new_balance < 0:
                    log.warning(f"Transaction would result in negative balance for {player_id}: {old_balance} + {amount} = {new_balance}")
                    return False, old_balance, None
                
                query = "UPDATE player SET server_currency = $1 WHERE user_id = $2;"
                await conn.execute(query, new_balance, player_id)
                return True, old_balance, new_balance
    except Exception as e:
        log.error(f"Error adding currency for player {player_id}: {e}", exc_info=True)
        return False, None, None

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
    def __init__(self, user_id: int, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.value = None
        self.user_id = user_id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Only the person who initiated this transaction can confirm it.", ephemeral=True)
            return
        self.value = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Only the person who initiated this transaction can cancel it.", ephemeral=True)
            return
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
        "gambling_cooldown": 10,  # Seconds between gambling attempts
        "large_transaction_threshold": 1000,  # Amount requiring confirmation
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
        """Initialize the local SQLite database for gambling stats and transaction history."""
        if self.local_db is not None:
            return
            
        self.local_db = await aiosqlite.connect(self.local_db_path)
        
        # Gambling stats table
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
        
        # Transaction history table
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS transaction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                transaction_type TEXT NOT NULL,
                from_player_id TEXT,
                to_player_id TEXT,
                amount INTEGER NOT NULL,
                balance_before INTEGER,
                balance_after INTEGER,
                notes TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_transaction_guild
            ON transaction_history(guild_id)
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_transaction_from
            ON transaction_history(from_player_id)
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_transaction_to
            ON transaction_history(to_player_id)
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_transaction_timestamp
            ON transaction_history(timestamp)
        """)
        
        # Prediction markets table
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS prediction_markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                market_id TEXT UNIQUE NOT NULL,
                question TEXT NOT NULL,
                created_by_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL DEFAULT 'open',
                winning_option INTEGER,
                resolved_at TIMESTAMP,
                resolved_by_id INTEGER
            )
        """)
        
        # Market options table
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS market_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                option_index INTEGER NOT NULL,
                option_text TEXT NOT NULL,
                FOREIGN KEY (market_id) REFERENCES prediction_markets(market_id),
                UNIQUE(market_id, option_index)
            )
        """)
        
        # Bets table
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS prediction_bets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                player_id TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                option_index INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (market_id) REFERENCES prediction_markets(market_id)
            )
        """)
        
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_markets_guild
            ON prediction_markets(guild_id, status)
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_bets_market
            ON prediction_bets(market_id)
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_bets_player
            ON prediction_bets(player_id, guild_id)
        """)

        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS tax_revenue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                tax_type TEXT NOT NULL,
                amount INTEGER NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await self.local_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_tax_guild
            ON tax_revenue(guild_id)
        """)

        await self.local_db.commit()
        log.info("Local database initialized with gambling stats, transaction history, and prediction markets.")

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
        
        view = ConfirmationView(ctx.author.id, timeout=30.0)
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

    async def log_transaction(
        self,
        guild_id: int,
        transaction_type: str,
        amount: int,
        from_player_id: Optional[uuid.UUID] = None,
        to_player_id: Optional[uuid.UUID] = None,
        balance_before: Optional[int] = None,
        balance_after: Optional[int] = None,
        notes: Optional[str] = None
    ) -> bool:
        """Logs a transaction to the local database."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        try:
            await self.local_db.execute("""
                INSERT INTO transaction_history
                (guild_id, transaction_type, from_player_id, to_player_id, amount, balance_before, balance_after, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                guild_id,
                transaction_type,
                str(from_player_id) if from_player_id else None,
                str(to_player_id) if to_player_id else None,
                amount,
                balance_before,
                balance_after,
                notes
            ))
            await self.local_db.commit()
            return True
        except Exception as e:
            log.error(f"Error logging transaction: {e}", exc_info=True)
            return False

    async def get_transaction_history(
        self,
        guild_id: int,
        player_id: Optional[uuid.UUID] = None,
        limit: int = 10
    ) -> list:
        """Gets transaction history from local database."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        player_id_str = str(player_id) if player_id else None
        
        if player_id_str:
            query = """
                SELECT transaction_type, from_player_id, to_player_id, amount,
                       balance_before, balance_after, notes, timestamp
                FROM transaction_history
                WHERE guild_id = ? AND (from_player_id = ? OR to_player_id = ?)
                ORDER BY timestamp DESC
                LIMIT ?
            """
            params = (guild_id, player_id_str, player_id_str, limit)
        else:
            query = """
                SELECT transaction_type, from_player_id, to_player_id, amount,
                       balance_before, balance_after, notes, timestamp
                FROM transaction_history
                WHERE guild_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            params = (guild_id, limit)
        
        async with self.local_db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        
        return [
            {
                'type': row[0],
                'from_player_id': row[1],
                'to_player_id': row[2],
                'amount': row[3],
                'balance_before': row[4],
                'balance_after': row[5],
                'notes': row[6],
                'timestamp': row[7]
            }
            for row in rows
        ]

    async def get_wealth_distribution(self, pool: asyncpg.Pool) -> dict:
        """Gets wealth distribution statistics."""
        async with pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_players,
                    SUM(server_currency) as total_wealth,
                    AVG(server_currency) as avg_wealth,
                    MIN(server_currency) as min_wealth,
                    MAX(server_currency) as max_wealth,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY server_currency) as median_wealth,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY server_currency) as q1_wealth,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY server_currency) as q3_wealth
                FROM player
                WHERE server_currency > 0
            """)
            
            return dict(stats) if stats else {}

    async def get_transaction_volume(self, guild_id: int, hours: int = 24) -> dict:
        """Gets transaction volume statistics for the specified time period."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        async with self.local_db.execute("""
            SELECT
                COUNT(*) as transaction_count,
                SUM(amount) as total_volume,
                AVG(amount) as avg_transaction,
                MAX(amount) as largest_transaction
            FROM transaction_history
            WHERE guild_id = ?
            AND timestamp >= datetime('now', '-' || ? || ' hours')
        """, (guild_id, hours)) as cursor:
            row = await cursor.fetchone()
            
        if row:
            return {
                'count': row[0] or 0,
                'total': row[1] or 0,
                'average': row[2] or 0,
                'largest': row[3] or 0
            }
        return {'count': 0, 'total': 0, 'average': 0, 'largest': 0}

    async def record_tax(self, guild_id: int, tax_type: str, amount: int) -> bool:
        """Records tax revenue in the local database."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        try:
            await self.local_db.execute("""
                INSERT INTO tax_revenue (guild_id, tax_type, amount)
                VALUES (?, ?, ?)
            """, (guild_id, tax_type, amount))
            await self.local_db.commit()
            return True
        except Exception as e:
            log.error(f"Error recording tax: {e}", exc_info=True)
            return False

    async def get_total_tax_revenue(self, guild_id: int) -> int:
        """Gets total tax revenue collected for a guild."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        async with self.local_db.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM tax_revenue
            WHERE guild_id = ?
        """, (guild_id,)) as cursor:
            result = await cursor.fetchone()
            return result[0] if result else 0

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

        success, old_balance = await set_player_currency(pool, player_info.player_id, amount)
        if success:
            # Log transaction
            await self.log_transaction(
                ctx.guild.id, "admin_set", amount,
                to_player_id=player_info.player_id,
                balance_before=old_balance,
                balance_after=amount,
                notes=f"Set by {ctx.author.name}"
            )
            
            embed = discord.Embed(title="✅ Balance Set", color=discord.Color.green())
            embed.set_footer(text=f"Set by {ctx.author.name}", icon_url=ctx.author.display_avatar.url)
            if player_info.discord_name:
                embed.add_field(name="👤 Discord User", value=discord.utils.escape_markdown(player_info.discord_name), inline=True)
                embed.add_field(name="🎮 SS14 Username", value=discord.utils.escape_markdown(player_info.player_name), inline=True)
            else:
                embed.add_field(name="🎮 Player", value=discord.utils.escape_markdown(player_info.player_name), inline=False)
            embed.add_field(name="💰 Old Balance", value=f"{old_balance:,} coins", inline=True)
            embed.add_field(name="💰 New Balance", value=f"{amount:,} coins", inline=True)
            embed.add_field(name="📊 Change", value=f"{amount - old_balance:+,} coins", inline=True)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"❌ Failed to set the balance for **{player_info.player_name}**.", ephemeral=True)

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

        success, old_balance, new_balance = await add_player_currency(pool, player_info.player_id, amount)
        if success:
            # Log transaction
            await self.log_transaction(
                ctx.guild.id, "admin_add", amount,
                to_player_id=player_info.player_id,
                balance_before=old_balance,
                balance_after=new_balance,
                notes=f"Added by {ctx.author.name}"
            )
            
            embed = discord.Embed(
                title="✅ Balance Updated",
                color=discord.Color.green() if amount > 0 else discord.Color.orange()
            )
            embed.set_footer(text=f"Modified by {ctx.author.name}", icon_url=ctx.author.display_avatar.url)
            if player_info.discord_name:
                embed.add_field(name="👤 Discord User", value=discord.utils.escape_markdown(player_info.discord_name), inline=True)
                embed.add_field(name="🎮 SS14 Username", value=discord.utils.escape_markdown(player_info.player_name), inline=True)
            else:
                embed.add_field(name="🎮 Player", value=discord.utils.escape_markdown(player_info.player_name), inline=False)
            embed.add_field(name="💰 Old Balance", value=f"{old_balance:,} coins", inline=True)
            embed.add_field(name="💰 New Balance", value=f"{new_balance:,} coins", inline=True)
            embed.add_field(name="📊 Amount Added", value=f"{amount:+,} coins", inline=True)
            await ctx.send(embed=embed)
        else:
            if old_balance is not None and old_balance + amount < 0:
                await ctx.send(f"❌ Cannot add {amount} coins - would result in negative balance ({old_balance} + {amount} = {old_balance + amount}).", ephemeral=True)
            else:
                await ctx.send(f"❌ Failed to add coins for **{player_info.player_name}**.", ephemeral=True)
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
            ready_timestamp = int(time.time() + wait_time)
            await ctx.send(
                f"⏱️ You're transferring too quickly! Try again <t:{ready_timestamp}:R>.",
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
            # Log transaction for sender
            await self.log_transaction(
                ctx.guild.id, "transfer", -amount,
                from_player_id=sender_id,
                to_player_id=recipient_info.player_id,
                balance_before=transfer_details['sender_old'],
                balance_after=transfer_details['sender_new'],
                notes=f"Sent to {recipient_info.player_name}"
            )
            
            # Log transaction for recipient
            await self.log_transaction(
                ctx.guild.id, "transfer", amount,
                from_player_id=sender_id,
                to_player_id=recipient_info.player_id,
                balance_before=transfer_details['recipient_old'],
                balance_after=transfer_details['recipient_new'],
                notes=f"Received from {ctx.author.name}"
            )
            
            sender_name = await get_user_name_from_id(self.session, sender_id)
            sender_name_escaped = discord.utils.escape_markdown(sender_name)
            sender_discord_name_escaped = discord.utils.escape_markdown(ctx.author.display_name)
            embed = discord.Embed(title="✅ Transfer Successful", color=discord.Color.green())
            embed.set_footer(text=f"Transfer completed", icon_url=ctx.author.display_avatar.url)

            sender_field_name = f"📤 Sender: {sender_discord_name_escaped} ({sender_name_escaped})"
            sender_field_value = f"`{transfer_details['sender_old']:,}` ➜ `{transfer_details['sender_new']:,}`"
            embed.add_field(name=sender_field_name, value=sender_field_value, inline=False)

            recipient_name_escaped = discord.utils.escape_markdown(recipient_info.player_name)
            if recipient_info.discord_name:
                recipient_discord_name_escaped = discord.utils.escape_markdown(recipient_info.discord_name)
                recipient_field_name = f"📥 Recipient: {recipient_discord_name_escaped} ({recipient_name_escaped})"
            else:
                recipient_field_name = f"📥 Recipient: {recipient_name_escaped}"
            recipient_field_value = f"`{transfer_details['recipient_old']:,}` ➜ `{transfer_details['recipient_new']:,}`"
            embed.add_field(name=recipient_field_name, value=recipient_field_value, inline=False)

            embed.add_field(name="💸 Amount", value=f"{amount:,} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send("❌ The transfer failed. This may be due to insufficient funds or an issue with the recipient's account.", ephemeral=True)

    @currency.command(name="history")
    async def transaction_history(self, ctx: commands.Context, user: Optional[typing.Union[discord.Member, str]] = None, limit: int = 10):
        """Shows transaction history for yourself or another user (admins only for others)."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured.", ephemeral=True)
            return

        # Determine target user
        if user is None:
            # Show own history
            target = ctx.author
            player_id = await get_player_id_from_discord(pool, target.id)
            if not player_id:
                await ctx.send("Your Discord account is not linked to an SS14 account.", ephemeral=True)
                return
            target_name = target.display_name
        else:
            # Check if user has permission to view others' history
            if not ctx.author.guild_permissions.manage_guild:
                await ctx.send("❌ You need Manage Server permissions to view other users' transaction history.", ephemeral=True)
                return
            
            player_info = await self.resolve_player(user, pool)
            if not player_info:
                await ctx.send(f"Could not find user `{user}`.", ephemeral=True)
                return
            player_id = player_info.player_id
            target_name = player_info.discord_name or player_info.player_name

        # Limit to reasonable range
        limit = max(5, min(limit, 50))
        
        history = await self.get_transaction_history(ctx.guild.id, player_id, limit)
        if not history:
            await ctx.send(f"No transaction history found for {target_name}.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"📜 Transaction History for {target_name}",
            description=f"Showing last {len(history)} transaction(s)",
            color=discord.Color.blue()
        )
        
        for i, tx in enumerate(history, 1):
            tx_type = tx['type']
            amount = tx['amount']
            timestamp = tx['timestamp']
            notes = tx['notes'] or "N/A"
            
            # Get other party information
            other_party = ""
            if tx_type == "transfer":
                if str(player_id) == tx['from_player_id']:
                    direction = "📤 Sent"
                    if tx['to_player_id']:
                        other_player_id = uuid.UUID(tx['to_player_id'])
                        other_name = await get_user_name_from_id(self.session, other_player_id)
                        other_party = f"**To:** {discord.utils.escape_markdown(other_name or 'Unknown')}\n"
                else:
                    direction = "📥 Received"
                    if tx['from_player_id']:
                        other_player_id = uuid.UUID(tx['from_player_id'])
                        other_name = await get_user_name_from_id(self.session, other_player_id)
                        other_party = f"**From:** {discord.utils.escape_markdown(other_name or 'Unknown')}\n"
            elif tx_type == "gambling":
                direction = "🎲 Gambling"
                # Amount will be positive for wins, negative for losses
            elif tx_type == "admin_set":
                direction = "⚙️ Balance Set"
            elif tx_type == "admin_add":
                direction = "⚙️ Admin Adjusted"
            elif tx_type == "market_bet":
                direction = "📈 Market Bet"
            elif tx_type == "market_win":
                direction = "💰 Market Win"
            else:
                direction = tx_type
            
            balance_info = ""
            if tx['balance_before'] is not None and tx['balance_after'] is not None:
                balance_info = f"**Balance:** `{tx['balance_before']:,}` ➜ `{tx['balance_after']:,}`\n"
            
            field_value = (
                f"{other_party}"
                f"**Amount:** {amount:+,} coins\n"
                f"{balance_info}"
                f"**Time:** {timestamp[:19]}\n"
                f"**Notes:** {notes}"
            )
            
            embed.add_field(
                name=f"{i}. {direction}",
                value=field_value,
                inline=False
            )
        
        embed.set_footer(text=f"Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="wealth")
    async def wealth_distribution(self, ctx: commands.Context):
        """Shows wealth distribution statistics for the server."""
        pool = await self.get_pool_for_guild(ctx.guild.id)

        if not pool:
            await ctx.send("Database connection is not configured.", ephemeral=True)
            return

        stats = await self.get_wealth_distribution(pool)
        if not stats or not stats.get('total_players'):
            await ctx.send("No wealth data available.", ephemeral=True)
            return

        total = int(stats.get('total_players', 0))
        total_wealth = float(stats.get('total_wealth', 0))
        avg = float(stats.get('avg_wealth', 0))
        median = float(stats.get('median_wealth', 0))
        min_w = float(stats.get('min_wealth', 0))
        max_w = float(stats.get('max_wealth', 0))
        q1 = float(stats.get('q1_wealth', 0))
        q3 = float(stats.get('q3_wealth', 0))

        embed = discord.Embed(
            title="📊 Wealth Distribution Analysis",
            description=f"Statistics for {total:,} players with positive balances",
            color=discord.Color.gold()
        )
        
        embed.add_field(
            name="💰 Total Wealth",
            value=f"{int(total_wealth):,} coins",
            inline=True
        )
        embed.add_field(
            name="📈 Average",
            value=f"{int(avg):,} coins",
            inline=True
        )
        embed.add_field(
            name="📊 Median",
            value=f"{int(median):,} coins",
            inline=True
        )
        embed.add_field(
            name="📉 Minimum",
            value=f"{int(min_w):,} coins",
            inline=True
        )
        embed.add_field(
            name="📈 Maximum",
            value=f"{int(max_w):,} coins",
            inline=True
        )
        embed.add_field(
            name="🎯 Range",
            value=f"{int(max_w - min_w):,} coins",
            inline=True
        )
        embed.add_field(
            name="📊 Q1 (25th percentile)",
            value=f"{int(q1):,} coins",
            inline=True
        )
        embed.add_field(
            name="📊 Q3 (75th percentile)",
            value=f"{int(q3):,} coins",
            inline=True
        )
        embed.add_field(
            name="📏 IQR (Interquartile Range)",
            value=f"{int(q3 - q1):,} coins",
            inline=True
        )
        
        # Calculate wealth inequality (Gini-like metric using quartiles)
        if median > 0:
            inequality = ((avg - median) / median) * 100
            embed.add_field(
                name="⚖️ Inequality Index",
                value=f"{inequality:.1f}% (avg/median deviation)",
                inline=False
            )
        
        embed.set_footer(text=f"Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="volume")
    async def transaction_volume(self, ctx: commands.Context, hours: int = 24):
        """Shows transaction volume statistics for the specified time period (default: 24 hours)."""
        hours = max(1, min(hours, 168))  # Limit between 1 hour and 1 week
        
        stats = await self.get_transaction_volume(ctx.guild.id, hours)
        
        embed = discord.Embed(
            title=f"📊 Transaction Volume ({hours}h)",
            description=f"Statistics for the last {hours} hour(s)",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="🔢 Total Transactions",
            value=f"{stats['count']:,}",
            inline=True
        )
        embed.add_field(
            name="💰 Total Volume",
            value=f"{stats['total']:,} coins",
            inline=True
        )
        embed.add_field(
            name="📊 Average Transaction",
            value=f"{int(stats['average']):,} coins" if stats['average'] else "N/A",
            inline=True
        )
        embed.add_field(
            name="🏆 Largest Transaction",
            value=f"{stats['largest']:,} coins",
            inline=True
        )
        
        if stats['count'] > 0:
            velocity = stats['total'] / hours
            embed.add_field(
                name="⚡ Velocity",
                value=f"{int(velocity):,} coins/hour",
                inline=True
            )
        
        embed.set_footer(text=f"Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="leaderboard")
    async def leaderboard(self, ctx: commands.Context, category: str = "wealth"):
        """Shows various leaderboards. Categories: wealth, gambling, activity"""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured.", ephemeral=True)
            return

        category = category.lower()
        
        if category in ["wealth", "rich", "coins", "balance"]:
            # Existing wealth leaderboard
            leaderboard_data = await get_leaderboard(pool)
            if not leaderboard_data:
                await ctx.send("The leaderboard is currently empty.")
                return

            embed = discord.Embed(
                title="🏆 Wealth Leaderboard",
                description="Top 10 richest players",
                color=discord.Color.gold()
            )
            for i, record in enumerate(leaderboard_data, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                embed.add_field(
                    name=f"{medal} {discord.utils.escape_markdown(record['last_seen_user_name'])}",
                    value=f"{record['server_currency']:,} coins",
                    inline=False
                )
            
        elif category in ["gambling", "gambler", "gamblers", "games"]:
            # Gambling leaderboard (most games played)
            if self.local_db is None:
                await self.initialize_local_db()
            
            async with self.local_db.execute("""
                SELECT player_id, SUM(total_games) as games, SUM(total_won - total_lost) as net_profit
                FROM gambling_stats
                WHERE guild_id = ?
                GROUP BY player_id
                ORDER BY games DESC
                LIMIT 10
            """, (ctx.guild.id,)) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                await ctx.send("No gambling statistics available.")
                return
            
            embed = discord.Embed(
                title="🎰 Gambling Leaderboard",
                description="Top 10 most active gamblers",
                color=discord.Color.purple()
            )
            
            for i, row in enumerate(rows, 1):
                player_id = uuid.UUID(row[0])
                # Try to get username from SS14
                username = await get_user_name_from_id(self.session, player_id)
                if not username:
                    username = str(player_id)[:8]
                
                games = row[1]
                net = row[2]
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                
                embed.add_field(
                    name=f"{medal} {discord.utils.escape_markdown(username)}",
                    value=f"**Games:** {games:,} | **Net:** {net:+,} coins",
                    inline=False
                )
        
        elif category in ["profit", "winners", "lucky"]:
            # Gambling profit leaderboard (biggest winners)
            if self.local_db is None:
                await self.initialize_local_db()
            
            async with self.local_db.execute("""
                SELECT player_id, SUM(total_won - total_lost) as net_profit, SUM(total_games) as games
                FROM gambling_stats
                WHERE guild_id = ?
                GROUP BY player_id
                HAVING net_profit > 0
                ORDER BY net_profit DESC
                LIMIT 10
            """, (ctx.guild.id,)) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                await ctx.send("No gambling profit data available.")
                return
            
            embed = discord.Embed(
                title="💰 Gambling Profit Leaderboard",
                description="Top 10 biggest winners",
                color=discord.Color.green()
            )
            
            for i, row in enumerate(rows, 1):
                player_id = uuid.UUID(row[0])
                username = await get_user_name_from_id(self.session, player_id)
                if not username:
                    username = str(player_id)[:8]
                
                profit = row[1]
                games = row[2]
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                
                embed.add_field(
                    name=f"{medal} {discord.utils.escape_markdown(username)}",
                    value=f"**Profit:** +{profit:,} coins | **Games:** {games:,}",
                    inline=False
                )
        
        elif category in ["losses", "losers", "unlucky"]:
            # Gambling losses leaderboard (biggest losers)
            if self.local_db is None:
                await self.initialize_local_db()
            
            async with self.local_db.execute("""
                SELECT player_id, SUM(total_won - total_lost) as net_profit, SUM(total_games) as games
                FROM gambling_stats
                WHERE guild_id = ?
                GROUP BY player_id
                HAVING net_profit < 0
                ORDER BY net_profit ASC
                LIMIT 10
            """, (ctx.guild.id,)) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                await ctx.send("No gambling loss data available.")
                return
            
            embed = discord.Embed(
                title="📉 Gambling Losses Leaderboard",
                description="Top 10 biggest losers",
                color=discord.Color.red()
            )
            
            for i, row in enumerate(rows, 1):
                player_id = uuid.UUID(row[0])
                username = await get_user_name_from_id(self.session, player_id)
                if not username:
                    username = str(player_id)[:8]
                
                loss = row[1]  # Will be negative
                games = row[2]
                medal = "💸" if i <= 3 else f"{i}."
                
                embed.add_field(
                    name=f"{medal} {discord.utils.escape_markdown(username)}",
                    value=f"**Loss:** {loss:,} coins | **Games:** {games:,}",
                    inline=False
                )
        
        elif category in ["activity", "active", "transactions"]:
            # Most active traders (by transaction count)
            if self.local_db is None:
                await self.initialize_local_db()
            
            async with self.local_db.execute("""
                SELECT 
                    COALESCE(from_player_id, to_player_id) as player_id,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_volume
                FROM transaction_history
                WHERE guild_id = ? AND transaction_type = 'transfer'
                GROUP BY player_id
                ORDER BY tx_count DESC
                LIMIT 10
            """, (ctx.guild.id,)) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                await ctx.send("No transaction activity found.")
                return
            
            embed = discord.Embed(
                title="💸 Activity Leaderboard",
                description="Top 10 most active traders",
                color=discord.Color.green()
            )
            
            for i, row in enumerate(rows, 1):
                if row[0]:
                    player_id = uuid.UUID(row[0])
                    username = await get_user_name_from_id(self.session, player_id)
                    if not username:
                        username = str(player_id)[:8]
                else:
                    continue
                
                tx_count = row[1]
                volume = row[2]
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                
                embed.add_field(
                    name=f"{medal} {discord.utils.escape_markdown(username)}",
                    value=f"**Transactions:** {tx_count:,} | **Volume:** {volume:,} coins",
                    inline=False
                )
        else:
            await ctx.send(
                f"❌ Unknown category `{category}`.\n\n"
                f"**Valid categories:**\n"
                f"• `wealth` - Richest players\n"
                f"• `gambling` - Most active gamblers\n"
                f"• `profit` - Biggest gambling winners\n"
                f"• `losses` - Biggest gambling losers\n"
                f"• `activity` - Most active traders",
                ephemeral=True
            )
            return
        
        # Add available categories to description
        current_desc = embed.description or ""
        embed.description = (
            f"{current_desc}\n\n"
            f"💡 **Categories:** `wealth` • `gambling` • `profit` • `losses` • `activity`"
        )
        embed.set_footer(text=f"Category: {category} | Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="economy")
    async def economy_health(self, ctx: commands.Context):
        """Shows overall economic health indicators for the server."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured.", ephemeral=True)
            return

        # Get tax revenue
        tax_revenue = await self.get_total_tax_revenue(ctx.guild.id)

        # Get wealth distribution
        wealth_stats = await self.get_wealth_distribution(pool)
        
        # Get transaction volume (24h)
        volume_24h = await self.get_transaction_volume(ctx.guild.id, 24)
        
        # Get transaction volume (7d)
        volume_7d = await self.get_transaction_volume(ctx.guild.id, 168)
        
        if not wealth_stats or not wealth_stats.get('total_players'):
            await ctx.send("Not enough data to calculate economic health.", ephemeral=True)
            return

        embed = discord.Embed(
            title="🏦 Economic Health Dashboard",
            description="Overall server economy statistics",
            color=discord.Color.blue()
        )
        
        # Wealth metrics (convert Decimal to float/int)
        total_wealth = float(wealth_stats.get('total_wealth', 0) or 0)
        total_players = int(wealth_stats.get('total_players', 0) or 0)
        avg_wealth = float(wealth_stats.get('avg_wealth', 0) or 0)
        median_wealth = float(wealth_stats.get('median_wealth', 0) or 0)

        embed.add_field(
            name="💰 Total Wealth in Circulation",
            value=f"{int(total_wealth):,} coins",
            inline=False
        )
        embed.add_field(
            name="👥 Active Players",
            value=f"{total_players:,} players",
            inline=True
        )
        embed.add_field(
            name="📊 Wealth per Capita",
            value=f"{int(avg_wealth):,} coins",
            inline=True
        )
        
        # Activity metrics
        embed.add_field(
            name="📈 24h Activity",
            value=f"{volume_24h['count']:,} transactions\n{volume_24h['total']:,} coins moved",
            inline=True
        )
        embed.add_field(
            name="📊 7d Activity",
            value=f"{volume_7d['count']:,} transactions\n{volume_7d['total']:,} coins moved",
            inline=True
        )
        
        # Velocity (economy turnover rate)
        if total_wealth > 0:
            daily_velocity = (volume_24h['total'] / total_wealth) * 100
            weekly_velocity = (volume_7d['total'] / total_wealth) * 100
            
            embed.add_field(
                name="⚡ Money Velocity",
                value=f"**Daily:** {daily_velocity:.2f}% of total wealth\n**Weekly:** {weekly_velocity:.2f}% of total wealth",
                inline=False
            )

        # Tax Revenue
        embed.add_field(
            name="🏦 Total Tax Revenue",
            value=f"{tax_revenue:,} coins collected",
            inline=False
        )

        # Inequality metric
        if median_wealth > 0:
            inequality = ((avg_wealth - median_wealth) / median_wealth) * 100
            status = "🟢 Low" if inequality < 50 else "🟡 Medium" if inequality < 100 else "🔴 High"
            embed.add_field(
                name="⚖️ Wealth Inequality",
                value=f"{status} ({inequality:.1f}%)",
                inline=True
            )
        
        health_score = 0
        health_details = []
        
        # 1. PARTICIPATION RATE (0-25 points)
        # Measures what % of players are economically active
        # Uses logarithmic scaling so it's harder to max out
        if total_players > 0:
            # Log scale: log10(players) * 10, capped at 25
            # 1 player = 0 points, 10 players = 10 points, 100 players = 20 points, 316+ players = 25 points
            participation_score = min(25, math.log10(total_players) * 10)
            health_score += participation_score
            health_details.append(f"👥 Participation: {participation_score:.1f}/25")
        
        # 2. TRANSACTION ACTIVITY (0-25 points)
        # Measures transactions per player per day (activity density)
        if total_players > 0 and volume_24h['count'] > 0:
            tx_per_player = volume_24h['count'] / total_players
            # 0.5 tx/player/day = 12.5 points, 1 tx/player/day = 25 points
            activity_score = min(25, tx_per_player * 25)
            health_score += activity_score
            health_details.append(f"📊 Activity: {activity_score:.1f}/25 ({tx_per_player:.2f} tx/player/day)")
        else:
            health_details.append(f"📊 Activity: 0/25 (no transactions)")
        
        # 3. MONEY VELOCITY (0-20 points)
        # Measures how quickly money moves through the economy
        if total_wealth > 0 and volume_24h['total'] > 0:
            daily_velocity = (volume_24h['total'] / total_wealth) * 100
            # 5% daily velocity = 10 points, 10% = 20 points
            velocity_score = min(20, daily_velocity * 2)
            health_score += velocity_score
            health_details.append(f"⚡ Velocity: {velocity_score:.1f}/20 ({daily_velocity:.2f}% daily)")
        else:
            health_details.append(f"⚡ Velocity: 0/20 (no movement)")
        
        # 4. WEALTH DISTRIBUTION (0-20 points)
        # Lower inequality is better
        if median_wealth > 0:
            inequality = ((avg_wealth - median_wealth) / median_wealth) * 100
            # Inverse scoring: lower inequality = more points
            if inequality < 25:
                distribution_score = 20
            elif inequality < 50:
                distribution_score = 18
            elif inequality < 75:
                distribution_score = 15
            elif inequality < 100:
                distribution_score = 12
            elif inequality < 150:
                distribution_score = 8
            else:
                distribution_score = 5
            health_score += distribution_score
            health_details.append(f"⚖️ Distribution: {distribution_score}/20 ({inequality:.1f}% inequality)")
        else:
            health_details.append(f"⚖️ Distribution: 0/20")
        
        # 5. ECONOMIC DIVERSITY (0-10 points)
        # Measures variety of transaction types (gambling, transfers, markets)
        transaction_types = 0
        if volume_24h['count'] > 0:
            transaction_types += 1  # Has transfers
        
        # Check for gambling activity
        if self.local_db:
            async with self.local_db.execute("""
                SELECT COUNT(*) FROM transaction_history
                WHERE guild_id = ? AND transaction_type = 'gambling'
                AND timestamp >= datetime('now', '-24 hours')
            """, (ctx.guild.id,)) as cursor:
                gambling_count = (await cursor.fetchone())[0]
                if gambling_count > 0:
                    transaction_types += 1
            
            # Check for market activity
            async with self.local_db.execute("""
                SELECT COUNT(*) FROM transaction_history
                WHERE guild_id = ? AND transaction_type IN ('market_bet', 'market_win')
                AND timestamp >= datetime('now', '-24 hours')
            """, (ctx.guild.id,)) as cursor:
                market_count = (await cursor.fetchone())[0]
                if market_count > 0:
                    transaction_types += 1
        
        # 1 type = 3 points, 2 types = 7 points, 3 types = 10 points
        diversity_map = {0: 0, 1: 3, 2: 7, 3: 10}
        diversity_score = diversity_map.get(transaction_types, 0)
        health_score += diversity_score
        
        type_names = []
        if volume_24h['count'] > 0:
            type_names.append("transfers")
        if self.local_db and gambling_count > 0:
            type_names.append("gambling")
        if self.local_db and market_count > 0:
            type_names.append("markets")
        
        types_text = ", ".join(type_names) if type_names else "none"
        health_details.append(f"🎯 Diversity: {diversity_score}/10 ({types_text})")
        
        health_score = min(100, health_score)
        
        # Determine health status and color
        if health_score >= 85:
            health_status = "🟢 Excellent"
            health_color = discord.Color.green()
            health_desc = "Thriving economy with high participation and activity"
        elif health_score >= 70:
            health_status = "🟢 Good"
            health_color = discord.Color.green()
            health_desc = "Healthy economy with solid fundamentals"
        elif health_score >= 55:
            health_status = "🟡 Fair"
            health_color = discord.Color.gold()
            health_desc = "Moderate economy with room for growth"
        elif health_score >= 40:
            health_status = "🟠 Needs Improvement"
            health_color = discord.Color.orange()
            health_desc = "Struggling economy requiring attention"
        else:
            health_status = "🔴 Poor"
            health_color = discord.Color.red()
            health_desc = "Weak economy needing significant intervention"
        
        embed.add_field(
            name="🏥 Economy Health Score",
            value=f"{health_status} ({health_score:.1f}/100)\n*{health_desc}*",
            inline=False
        )
        
        # Add breakdown
        breakdown = "\n".join(health_details)
        embed.add_field(
            name="📋 Score Breakdown",
            value=breakdown,
            inline=False
        )
        
        embed.color = health_color
        embed.set_footer(text=f"Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="markets")
    async def list_markets(self, ctx: commands.Context, status: str = "open"):
        """List prediction markets.
        
        Args:
            status: Filter by status (open/resolved/cancelled/all). Default: open
        """
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Build query based on status filter
        if status.lower() == "all":
            query = """
                SELECT market_id, question, status, created_at
                FROM prediction_markets
                WHERE guild_id = ?
                ORDER BY created_at DESC
                LIMIT 10
            """
            params = (ctx.guild.id,)
        else:
            query = """
                SELECT market_id, question, status, created_at
                FROM prediction_markets
                WHERE guild_id = ? AND status = ?
                ORDER BY created_at DESC
                LIMIT 10
            """
            params = (ctx.guild.id, status.lower())
        
        async with self.local_db.execute(query, params) as cursor:
            markets = await cursor.fetchall()
        
        if not markets:
            await ctx.send(f"📊 No {status} markets found.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title=f"📊 Prediction Markets ({status.title()})",
            description=f"Showing {len(markets)} market(s)",
            color=discord.Color.blue()
        )
        
        for market in markets:
            market_id = market[0]
            question = market[1]
            mkt_status = market[2]
            created = market[3]
            
            # Get total bets
            async with self.local_db.execute("""
                SELECT COUNT(*), SUM(amount)
                FROM prediction_bets
                WHERE market_id = ?
            """, (market_id,)) as cursor:
                bet_stats = await cursor.fetchone()
            
            bet_count = bet_stats[0] or 0
            total_pool = bet_stats[1] or 0
            
            status_emoji = {
                'open': '🟢',
                'resolved': '✅',
                'cancelled': '❌',
                'setup': '⚙️'
            }.get(mkt_status, '⚪')
            
            field_value = (
                f"**ID:** `{market_id}`\n"
                f"**Status:** {status_emoji} {mkt_status.title()}\n"
                f"**Bets:** {bet_count} ({total_pool:,} coins)\n"
                f"**Created:** {created[:10]}"
            )
            
            embed.add_field(
                name=discord.utils.escape_markdown(question[:100]),
                value=field_value,
                inline=False
            )
        
        embed.set_footer(text=f"Use /currency marketinfo <id> for details | Requested by {ctx.author.name}")
        await ctx.send(embed=embed)

    @currency.command(name="marketinfo")
    async def market_info(self, ctx: commands.Context, market_id: str):
        """Get detailed information about a prediction market."""
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Get market details
        async with self.local_db.execute("""
            SELECT question, status, created_at, winning_option, resolved_at
            FROM prediction_markets
            WHERE market_id = ? AND guild_id = ?
        """, (market_id, ctx.guild.id)) as cursor:
            market = await cursor.fetchone()
        
        if not market:
            await ctx.send("❌ Market not found.", ephemeral=True)
            return
        
        question, status, created_at, winning_option, resolved_at = market
        
        # Get options with bet counts
        async with self.local_db.execute("""
            SELECT mo.option_index, mo.option_text,
                   COUNT(pb.id) as bet_count,
                   COALESCE(SUM(pb.amount), 0) as total_wagered
            FROM market_options mo
            LEFT JOIN prediction_bets pb ON mo.market_id = pb.market_id AND mo.option_index = pb.option_index
            WHERE mo.market_id = ?
            GROUP BY mo.option_index, mo.option_text
            ORDER BY mo.option_index
        """, (market_id,)) as cursor:
            options = await cursor.fetchall()
        
        status_emoji = {
            'open': '🟢 Open',
            'resolved': '✅ Resolved',
            'cancelled': '❌ Cancelled',
            'setup': '⚙️ Setup'
        }.get(status, status)
        
        embed = discord.Embed(
            title="📊 Market Information",
            description=f"**{discord.utils.escape_markdown(question)}**",
            color=discord.Color.blue() if status == 'open' else discord.Color.green() if status == 'resolved' else discord.Color.red()
        )
        
        embed.add_field(name="Status", value=status_emoji, inline=True)
        embed.add_field(name="Created", value=created_at[:10], inline=True)
        
        if resolved_at:
            embed.add_field(name="Resolved", value=resolved_at[:10], inline=True)
        
        # Add options
        total_pool = 0
        for opt in options:
            option_idx, option_text, bet_count, wagered = opt
            total_pool += wagered
            
            winner_mark = " 🏆" if winning_option == option_idx else ""
            field_name = f"Option {option_idx}{winner_mark}"
            field_value = f"{discord.utils.escape_markdown(option_text)}\n**Bets:** {bet_count} | **Wagered:** {wagered:,} coins"
            
            embed.add_field(name=field_name, value=field_value, inline=False)
        
        embed.add_field(name="💰 Total Pool", value=f"{total_pool:,} coins", inline=False)
        embed.set_footer(text=f"Market ID: {market_id}")
        
        await ctx.send(embed=embed)

    @currency.command(name="resolvemarket")
    @checks.admin_or_permissions(manage_guild=True)
    async def resolve_market(self, ctx: commands.Context, market_id: str, winning_option: int):
        """Resolve a market and distribute winnings (Admin only).
        
        Args:
            market_id: The market ID
            winning_option: The option number that won
        """
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("❌ Database connection not configured.", ephemeral=True)
            return
        
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Verify market exists and is open
        async with self.local_db.execute("""
            SELECT question, status FROM prediction_markets
            WHERE market_id = ? AND guild_id = ?
        """, (market_id, ctx.guild.id)) as cursor:
            market = await cursor.fetchone()
        
        if not market:
            await ctx.send("❌ Market not found.", ephemeral=True)
            return
        
        if market[1] != 'open':
            await ctx.send(f"❌ Market is {market[1]} and cannot be resolved.", ephemeral=True)
            return
        
        # Verify winning option exists
        async with self.local_db.execute("""
            SELECT option_text FROM market_options
            WHERE market_id = ? AND option_index = ?
        """, (market_id, winning_option)) as cursor:
            option = await cursor.fetchone()
        
        if not option:
            await ctx.send(f"❌ Invalid winning option {winning_option}.", ephemeral=True)
            return
        
        # Get all bets
        async with self.local_db.execute("""
            SELECT option_index, SUM(amount) as total
            FROM prediction_bets
            WHERE market_id = ?
            GROUP BY option_index
        """, (market_id,)) as cursor:
            option_totals = await cursor.fetchall()
        
        # Calculate total pool and winning pool
        total_pool = sum(row[1] for row in option_totals)
        winning_pool = next((row[1] for row in option_totals if row[0] == winning_option), 0)
        
        if winning_pool == 0:
            await ctx.send("⚠️ No one bet on the winning option. Resolving market anyway.", ephemeral=True)
        
        # Get winning bets
        async with self.local_db.execute("""
            SELECT player_id, amount
            FROM prediction_bets
            WHERE market_id = ? AND option_index = ?
        """, (market_id, winning_option)) as cursor:
            winning_bets = await cursor.fetchall()
        
        # Distribute winnings proportionally
        winners_paid = 0
        total_distributed = 0
        
        for player_id_str, bet_amount in winning_bets:
            player_id = uuid.UUID(player_id_str)
            
            # Calculate winnings (proportional share of total pool)
            if winning_pool > 0:
                share = bet_amount / winning_pool
                payout = int(total_pool * share)
            else:
                payout = bet_amount  # Refund if no winners
            
            # Add winnings to player
            success, old_bal, new_bal = await add_player_currency(pool, player_id, payout)
            if success:
                winners_paid += 1
                total_distributed += payout
                
                # Log transaction
                await self.log_transaction(
                    ctx.guild.id, "market_win", payout,
                    to_player_id=player_id,
                    balance_before=old_bal,
                    balance_after=new_bal,
                    notes=f"Won from market {market_id[:16]}..."
                )
        
        # Mark market as resolved
        await self.local_db.execute("""
            UPDATE prediction_markets
            SET status = 'resolved', winning_option = ?, resolved_at = CURRENT_TIMESTAMP, resolved_by_id = ?
            WHERE market_id = ?
        """, (winning_option, ctx.author.id, market_id))
        await self.local_db.commit()
        
        embed = discord.Embed(
            title="✅ Market Resolved",
            description=f"**Question:** {discord.utils.escape_markdown(market[0])}",
            color=discord.Color.green()
        )
        embed.add_field(name="Winning Option", value=f"Option {winning_option}: {discord.utils.escape_markdown(option[0])}", inline=False)
        embed.add_field(name="Total Pool", value=f"{total_pool:,} coins", inline=True)
        embed.add_field(name="Winners Paid", value=f"{winners_paid} player(s)", inline=True)
        embed.add_field(name="Total Distributed", value=f"{total_distributed:,} coins", inline=True)
        embed.set_footer(text=f"Resolved by {ctx.author.name}")
        
        await ctx.send(embed=embed)

    @currency.command(name="cancelmarket")
    @checks.admin_or_permissions(manage_guild=True)
    async def cancel_market(self, ctx: commands.Context, market_id: str):
        """Cancel a market and refund all bets (Admin only)."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("❌ Database connection not configured.", ephemeral=True)
            return
        
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Verify market exists
        async with self.local_db.execute("""
            SELECT question, status FROM prediction_markets
            WHERE market_id = ? AND guild_id = ?
        """, (market_id, ctx.guild.id)) as cursor:
            market = await cursor.fetchone()
        
        if not market:
            await ctx.send("❌ Market not found.", ephemeral=True)
            return
        
        if market[1] == 'cancelled':
            await ctx.send("❌ Market is already cancelled.", ephemeral=True)
            return
        
        if market[1] == 'resolved':
            await ctx.send("❌ Cannot cancel a resolved market.", ephemeral=True)
            return
        
        # Get all bets to refund
        async with self.local_db.execute("""
            SELECT player_id, SUM(amount) as total
            FROM prediction_bets
            WHERE market_id = ?
            GROUP BY player_id
        """, (market_id,)) as cursor:
            refunds = await cursor.fetchall()
        
        # Refund all bets
        refunded_count = 0
        total_refunded = 0
        
        for player_id_str, amount in refunds:
            player_id = uuid.UUID(player_id_str)
            success, old_bal, new_bal = await add_player_currency(pool, player_id, amount)
            if success:
                refunded_count += 1
                total_refunded += amount
                
                # Log transaction
                await self.log_transaction(
                    ctx.guild.id, "market_refund", amount,
                    to_player_id=player_id,
                    balance_before=old_bal,
                    balance_after=new_bal,
                    notes=f"Refund from cancelled market {market_id[:16]}..."
                )
        
        # Mark market as cancelled
        await self.local_db.execute("""
            UPDATE prediction_markets
            SET status = 'cancelled', resolved_at = CURRENT_TIMESTAMP, resolved_by_id = ?
            WHERE market_id = ?
        """, (ctx.author.id, market_id))
        await self.local_db.commit()
        
        embed = discord.Embed(
            title="❌ Market Cancelled",
            description=f"**Question:** {discord.utils.escape_markdown(market[0])}",
            color=discord.Color.red()
        )
        embed.add_field(name="Players Refunded", value=f"{refunded_count} player(s)", inline=True)
        embed.add_field(name="Total Refunded", value=f"{total_refunded:,} coins", inline=True)
        embed.set_footer(text=f"Cancelled by {ctx.author.name}")
        
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
        
    @currency.command(name="createmarket")
    @checks.admin_or_permissions(manage_guild=True)
    async def create_market(self, ctx: commands.Context, *, question: str):
        """Creates a new prediction market (Admin only).
        
        After creating the market, you'll be prompted to add options.
        Example: /currency createmarket Will the server reach 100 players today?
        """
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Generate unique market ID (short 8-character hex)
        market_id = f"{secrets.token_hex(6)}"
        
        # Create the market
        try:
            await self.local_db.execute("""
                INSERT INTO prediction_markets (guild_id, market_id, question, created_by_id, status)
                VALUES (?, ?, ?, ?, 'setup')
            """, (ctx.guild.id, market_id, question, ctx.author.id))
            await self.local_db.commit()
        except Exception as e:
            log.error(f"Error creating market: {e}", exc_info=True)
            await ctx.send("❌ Failed to create market.", ephemeral=True)
            return
        
        # Show option entry view
        view = MarketOptionsView(self, market_id, question, ctx.guild.id, ctx.author.id)
        embed = discord.Embed(
            title="📊 Creating Prediction Market",
            description=f"**Question:** {discord.utils.escape_markdown(question)}\n\nAdd betting options using the buttons below.",
            color=discord.Color.blue()
        )
        embed.add_field(name="Options Added", value="None yet", inline=False)
        embed.set_footer(text="Add at least 2 options, then click 'Finish'")
        
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @currency.command(name="bet")
    async def place_bet(self, ctx: commands.Context, market_id: str, option: int, amount: int):
        """Place a bet on a prediction market.
        
        Args:
            market_id: The ID of the market (from /currency markets)
            option: The option number to bet on
            amount: Amount of coins to wager
        """
        if amount <= 0:
            await ctx.send("❌ You must bet a positive amount.", ephemeral=True)
            return
        
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("❌ Database connection not configured.", ephemeral=True)
            return
        
        if self.local_db is None:
            await self.initialize_local_db()
        
        # Get player ID
        player_id = await get_player_id_from_discord(pool, ctx.author.id)
        if not player_id:
            await ctx.send("❌ Your Discord account is not linked to an SS14 account.", ephemeral=True)
            return
        
        # Check balance
        balance = await get_player_currency(pool, player_id)
        if balance < amount:
            await ctx.send(f"❌ Insufficient funds. You have {balance:,} coins but need {amount:,}.", ephemeral=True)
            return
        
        # Verify market exists and is open
        async with self.local_db.execute("""
            SELECT question, status FROM prediction_markets
            WHERE market_id = ? AND guild_id = ?
        """, (market_id, ctx.guild.id)) as cursor:
            market = await cursor.fetchone()
        
        if not market:
            await ctx.send("❌ Market not found.", ephemeral=True)
            return
        
        if market[1] != 'open':
            await ctx.send(f"❌ This market is {market[1]} and not accepting bets.", ephemeral=True)
            return
        
        # Verify option exists
        async with self.local_db.execute("""
            SELECT option_text FROM market_options
            WHERE market_id = ? AND option_index = ?
        """, (market_id, option)) as cursor:
            option_data = await cursor.fetchone()
        
        if not option_data:
            await ctx.send(f"❌ Invalid option number {option}.", ephemeral=True)
            return
        
        # Deduct coins from player (atomically)
        success, old_balance, new_balance = await add_player_currency(pool, player_id, -amount)
        if not success:
            await ctx.send("❌ Failed to deduct coins. Please try again.", ephemeral=True)
            return
        
        # Record the bet
        try:
            await self.local_db.execute("""
                INSERT INTO prediction_bets (market_id, player_id, guild_id, option_index, amount)
                VALUES (?, ?, ?, ?, ?)
            """, (market_id, str(player_id), ctx.guild.id, option, amount))
            await self.local_db.commit()
            
            # Log transaction
            await self.log_transaction(
                ctx.guild.id, "market_bet", amount,
                from_player_id=player_id,
                balance_before=old_balance,
                balance_after=new_balance,
                notes=f"Bet on market {market_id} option {option}"
            )
            
            embed = discord.Embed(
                title="✅ Bet Placed",
                description=f"**Question:** {discord.utils.escape_markdown(market[0])}",
                color=discord.Color.green()
            )
            embed.add_field(name="Your Bet", value=f"Option {option}: {discord.utils.escape_markdown(option_data[0])}", inline=False)
            embed.add_field(name="Amount Wagered", value=f"{amount:,} coins", inline=True)
            embed.add_field(name="New Balance", value=f"{new_balance:,} coins", inline=True)
            embed.set_footer(text=f"Market ID: {market_id}")
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            # Refund if bet recording fails
            log.error(f"Error recording bet: {e}", exc_info=True)
            await add_player_currency(pool, player_id, amount)
            await ctx.send("❌ Failed to record bet. Your coins have been refunded.", ephemeral=True)

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
    @commands.cooldown(rate=1, per=10.0, type=commands.BucketType.user)
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
            ready_timestamp = int(time.time() + error.retry_after)
            await ctx.send(
                f"🎰 Slow down! You can gamble again <t:{ready_timestamp}:R>.",
                ephemeral=True
            )
        else:
            raise error

# End of SS14Currency class

class MarketOptionsView(View):
    """View for adding options to a prediction market during creation."""
    def __init__(self, cog: 'SS14Currency', market_id: str, question: str, guild_id: int, creator_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.market_id = market_id
        self.question = question
        self.guild_id = guild_id
        self.creator_id = creator_id
        self.options = []
        self.message = None
    
    async def update_embed(self):
        """Update the embed to show current options."""
        embed = discord.Embed(
            title="📊 Creating Prediction Market",
            description=f"**Question:** {discord.utils.escape_markdown(self.question)}\n\nAdd betting options using the buttons below.",
            color=discord.Color.blue()
        )
        
        if self.options:
            options_text = "\n".join([f"{i+1}. {discord.utils.escape_markdown(opt)}" for i, opt in enumerate(self.options)])
            embed.add_field(name="Options Added", value=options_text, inline=False)
        else:
            embed.add_field(name="Options Added", value="None yet", inline=False)
        
        embed.set_footer(text="Add at least 2 options, then click 'Finish'")
        
        if self.message:
            await self.message.edit(embed=embed)
    
    @discord.ui.button(label="Add Option", style=discord.ButtonStyle.green)
    async def add_option(self, interaction: discord.Interaction, button: Button):
        """Add a new option to the market."""
        if interaction.user.id != self.creator_id:
            await interaction.response.send_message("❌ Only the market creator can add options.", ephemeral=True)
            return
            
        modal = AddOptionModal()
        await interaction.response.send_modal(modal)
        await modal.wait()
        
        if modal.submitted_text:
            self.options.append(modal.submitted_text)
            await self.update_embed()
    
    @discord.ui.button(label="Finish", style=discord.ButtonStyle.primary)
    async def finish(self, interaction: discord.Interaction, button: Button):
        """Finish creating the market."""
        if interaction.user.id != self.creator_id:
            await interaction.response.send_message("❌ Only the market creator can finish setup.", ephemeral=True)
            return
            
        if len(self.options) < 2:
            await interaction.response.send_message("❌ You need at least 2 options.", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        try:
            # Add options to database
            for i, option_text in enumerate(self.options):
                await self.cog.local_db.execute("""
                    INSERT INTO market_options (market_id, option_index, option_text)
                    VALUES (?, ?, ?)
                """, (self.market_id, i + 1, option_text))
            
            # Update market status to open
            await self.cog.local_db.execute("""
                UPDATE prediction_markets
                SET status = 'open'
                WHERE market_id = ?
            """, (self.market_id,))
            
            await self.cog.local_db.commit()
            
            # Disable buttons
            for item in self.children:
                item.disabled = True
            
            embed = discord.Embed(
                title="✅ Market Created",
                description=f"**Question:** {discord.utils.escape_markdown(self.question)}",
                color=discord.Color.green()
            )
            
            options_text = "\n".join([f"{i+1}. {discord.utils.escape_markdown(opt)}" for i, opt in enumerate(self.options)])
            embed.add_field(name="Options", value=options_text, inline=False)
            embed.add_field(name="Market ID", value=f"`{self.market_id}`", inline=False)
            embed.add_field(name="Status", value="🟢 Open for betting", inline=False)
            
            await self.message.edit(embed=embed, view=self)
            self.stop()
            
        except Exception as e:
            log.error(f"Error finishing market creation: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to create market.", ephemeral=True)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: Button):
        """Cancel market creation."""
        if interaction.user.id != self.creator_id:
            await interaction.response.send_message("❌ Only the market creator can cancel setup.", ephemeral=True)
            return
            
        await interaction.response.defer()
        
        # Delete the market from database
        await self.cog.local_db.execute("""
            DELETE FROM prediction_markets WHERE market_id = ?
        """, (self.market_id,))
        await self.cog.local_db.commit()
        
        # Disable buttons
        for item in self.children:
            item.disabled = True
        
        await self.message.edit(content="❌ Market creation cancelled.", embed=None, view=self)
        self.stop()


class AddOptionModal(Modal, title="Add Market Option"):
    """Modal for adding an option to a prediction market."""
    
    option_text = TextInput(
        label="Option Description",
        style=TextStyle.short,
        placeholder="e.g., Yes, No, Maybe",
        required=True,
        max_length=200
    )
    
    def __init__(self):
        super().__init__(timeout=None)
        self.submitted_text = None
    
    async def on_submit(self, interaction: discord.Interaction):
        self.submitted_text = self.option_text.value.strip()
        await interaction.response.defer()


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
        
        embed = discord.Embed(
            title="⏱️ Coinflip Challenge Expired",
            description=f"{self.challenger.mention}'s open coinflip challenge for **{self.amount}** coins has expired.",
            color=discord.Color.red()
        )
        await self.message.edit(content=None, embed=embed, view=self)

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
        
        tax_amount = int(self.amount * 0.05)
        winner_receives = self.amount - tax_amount

        transfer_details = await transfer_currency(self.pool, loser_player_id, winner_player_id, winner_receives)
        
        for item in self.children:
            item.disabled = True


        for item in self.children:
            item.disabled = True

        if transfer_details:
            winner_name = await get_user_name_from_id(self.cog.session, winner_player_id)
            loser_name = await get_user_name_from_id(self.cog.session, loser_player_id)
            
            # Record tax
            await self.cog.record_tax(self.guild_id, "coinflip", tax_amount)
            
            # Record gambling statistics (net for winner is reduced by tax)
            await self.cog.record_gambling_result(
                self.guild_id, winner_player_id, "coinflip",
                self.amount, True, winner_receives
            )
            await self.cog.record_gambling_result(
                self.guild_id, loser_player_id, "coinflip",
                self.amount, False, -self.amount
            )
            
            # Log gambling transactions
            await self.cog.log_transaction(
                self.guild_id, "gambling", winner_receives,
                from_player_id=loser_player_id,
                to_player_id=winner_player_id,
                balance_before=transfer_details['recipient_old'],
                balance_after=transfer_details['recipient_new'],
                notes=f"Coinflip win vs {loser_name} (after {tax_amount} tax)"
            )
            await self.cog.log_transaction(
                self.guild_id, "gambling", -self.amount,
                from_player_id=loser_player_id,
                to_player_id=winner_player_id,
                balance_before=transfer_details['sender_old'],
                balance_after=transfer_details['sender_new'],
                notes=f"Coinflip loss vs {winner_name}"
            )

            embed = discord.Embed(title="🪙 Coinflip Result!", color=discord.Color.gold())
            embed.description = f"**{discord.utils.escape_markdown(winner.display_name)}** won the coinflip against **{discord.utils.escape_markdown(loser.display_name)}**!"
            
            winner_field_name = f"🏆 Winner: {discord.utils.escape_markdown(winner.display_name)} ({discord.utils.escape_markdown(winner_name)})"
            winner_field_value = f"`{transfer_details['recipient_old']:,}` ➜ `{transfer_details['recipient_new']:,}`"
            embed.add_field(name=winner_field_name, value=winner_field_value, inline=False)
            
            loser_field_name = f"💸 Loser: {discord.utils.escape_markdown(loser.display_name)} ({discord.utils.escape_markdown(loser_name)})"
            loser_field_value = f"`{transfer_details['sender_old']:,}` ➜ `{transfer_details['sender_new']:,}`"
            embed.add_field(name=loser_field_name, value=loser_field_value, inline=False)

            embed.add_field(name="💰 Total Wager", value=f"{self.amount:,} coins", inline=True)
            embed.add_field(name="🏦 Tax (5%)", value=f"{tax_amount:,} coins", inline=True)
            embed.add_field(name="✨ Winner Receives", value=f"{winner_receives:,} coins", inline=True)
            
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
        
        embed = discord.Embed(
            title="⏱️ Coinflip Challenge Expired",
            description=f"{self.opponent.mention} did not respond to {self.challenger.mention}'s coinflip challenge for **{self.amount}** coins.",
            color=discord.Color.red()
        )
        await self.message.edit(content=None, embed=embed, view=self)

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

        tax_amount = int(self.amount * 0.05)
        winner_receives = self.amount - tax_amount

        transfer_details = await transfer_currency(self.pool, loser_player_id, winner_player_id, winner_receives)
        
        for item in self.children:
            item.disabled = True


        for item in self.children:
            item.disabled = True

        if transfer_details:
            winner_name = await get_user_name_from_id(self.cog.session, winner_player_id)
            loser_name = await get_user_name_from_id(self.cog.session, loser_player_id)
            
            # Record tax
            await self.cog.record_tax(self.guild_id, "coinflip", tax_amount)
            
            # Record gambling statistics (net for winner is reduced by tax)
            await self.cog.record_gambling_result(
                self.guild_id, winner_player_id, "coinflip",
                self.amount, True, winner_receives
            )
            await self.cog.record_gambling_result(
                self.guild_id, loser_player_id, "coinflip",
                self.amount, False, -self.amount
            )
            
            # Log gambling transactions
            await self.cog.log_transaction(
                self.guild_id, "gambling", winner_receives,
                from_player_id=loser_player_id,
                to_player_id=winner_player_id,
                balance_before=transfer_details['recipient_old'],
                balance_after=transfer_details['recipient_new'],
                notes=f"Coinflip win vs {loser_name} (after {tax_amount} tax)"
            )
            await self.cog.log_transaction(
                self.guild_id, "gambling", -self.amount,
                from_player_id=loser_player_id,
                to_player_id=winner_player_id,
                balance_before=transfer_details['sender_old'],
                balance_after=transfer_details['sender_new'],
                notes=f"Coinflip loss vs {winner_name}"
            )

            embed = discord.Embed(title="🪙 Coinflip Result!", color=discord.Color.gold())
            embed.description = f"**{discord.utils.escape_markdown(winner.display_name)}** won the coinflip against **{discord.utils.escape_markdown(loser.display_name)}**!"
            
            winner_field_name = f"🏆 Winner: {discord.utils.escape_markdown(winner.display_name)} ({discord.utils.escape_markdown(winner_name)})"
            winner_field_value = f"`{transfer_details['recipient_old']:,}` ➜ `{transfer_details['recipient_new']:,}`"
            embed.add_field(name=winner_field_name, value=winner_field_value, inline=False)
            
            loser_field_name = f"💸 Loser: {discord.utils.escape_markdown(loser.display_name)} ({discord.utils.escape_markdown(loser_name)})"
            loser_field_value = f"`{transfer_details['sender_old']:,}` ➜ `{transfer_details['sender_new']:,}`"
            embed.add_field(name=loser_field_name, value=loser_field_value, inline=False)

            embed.add_field(name="💰 Total Wager", value=f"{self.amount:,} coins", inline=True)
            embed.add_field(name="🏦 Tax (5%)", value=f"{tax_amount:,} coins", inline=True)
            embed.add_field(name="✨ Winner Receives", value=f"{winner_receives:,} coins", inline=True)
            
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
