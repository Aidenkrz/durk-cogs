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
from discord.ext import tasks
from dataclasses import dataclass
from pathlib import Path
import aiosqlite
import time
import secrets
import math

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

async def get_leaderboardasc(pool: asyncpg.Pool) -> list:
    """Gets the top 10 players by currency."""
    async with pool.acquire() as conn:
        query = "SELECT last_seen_user_name, server_currency FROM player WHERE server_currency != 0 ORDER BY server_currency ASC LIMIT 10;"
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
        "role_payouts": {},  # {role_id (str): amount (int)} - monthly currency per role
    }

    # Seconds in a payout period (30 days)
    PAYOUT_PERIOD = 30 * 24 * 60 * 60

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

        # Start the monthly role payout background loop
        self.payout_loop.start()

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

        # Tracks when each member last received a monthly role payout
        await self.local_db.execute("""
            CREATE TABLE IF NOT EXISTS role_payout_history (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                last_paid_at INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
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

    async def get_last_payout(self, guild_id: int, user_id: int) -> Optional[int]:
        """Gets the unix timestamp of a member's last monthly role payout."""
        if self.local_db is None:
            await self.initialize_local_db()

        async with self.local_db.execute(
            "SELECT last_paid_at FROM role_payout_history WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id)
        ) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else None

    async def set_last_payout(self, guild_id: int, user_id: int, timestamp: int) -> None:
        """Records the unix timestamp of a member's most recent monthly role payout."""
        if self.local_db is None:
            await self.initialize_local_db()

        await self.local_db.execute("""
            INSERT INTO role_payout_history (guild_id, user_id, last_paid_at)
            VALUES (?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET last_paid_at = excluded.last_paid_at
        """, (guild_id, user_id, timestamp))
        await self.local_db.commit()

    async def run_role_payouts(self, guild: discord.Guild) -> int:
        """Pays out monthly currency to eligible members of a guild.

        A member is eligible if they hold at least one configured paying role and
        at least PAYOUT_PERIOD seconds have passed since their own last payout.
        Each eligible member receives the amount of their single highest-value role.
        Returns the number of members paid.
        """
        role_payouts = await self.config.guild(guild).role_payouts()
        if not role_payouts:
            return 0

        # Config keys are stored as strings; normalise to int role ids
        payout_map = {int(role_id): amount for role_id, amount in role_payouts.items()}

        pool = await self.get_pool_for_guild(guild.id)
        if not pool:
            return 0

        now = int(time.time())
        paid_count = 0

        for member in guild.members:
            if member.bot:
                continue

            # Highest-value applicable role only
            amounts = [payout_map[role.id] for role in member.roles if role.id in payout_map]
            if not amounts:
                continue
            amount = max(amounts)
            if amount <= 0:
                continue

            last_paid = await self.get_last_payout(guild.id, member.id)
            if last_paid is not None and now - last_paid < self.PAYOUT_PERIOD:
                continue

            # Currency is credited to a linked SS14 account; skip unlinked members
            # so they start accruing once they link (no back-pay).
            player_id = await get_player_id_from_discord(pool, member.id)
            if not player_id:
                continue

            success, old_balance, new_balance = await add_player_currency(pool, player_id, amount)
            if not success:
                log.warning(f"Failed to pay monthly payout of {amount} to {member.id} in guild {guild.id}.")
                continue

            await self.set_last_payout(guild.id, member.id, now)
            await self.log_transaction(
                guild.id, "role_payout", amount,
                to_player_id=player_id,
                balance_before=old_balance,
                balance_after=new_balance,
                notes="Monthly role payout"
            )
            paid_count += 1

            # Best-effort DM; ignore members with DMs closed or transient errors
            try:
                embed = discord.Embed(
                    title="💰 Monthly Payout",
                    description=f"You received **{amount:,}** coins as your monthly payout in **{guild.name}**!",
                    color=discord.Color.green()
                )
                embed.add_field(name="💰 New Balance", value=f"{new_balance:,} coins", inline=True)
                await member.send(embed=embed)
            except (discord.Forbidden, discord.HTTPException):
                pass

        if paid_count:
            log.info(f"Paid monthly role payout to {paid_count} member(s) in guild {guild.id}.")
        return paid_count

    @tasks.loop(hours=1)
    async def payout_loop(self):
        """Hourly check that pays out any member whose 30-day payout period has elapsed."""
        try:
            await self.initialize_local_db()
            for guild in self.bot.guilds:
                try:
                    await self.run_role_payouts(guild)
                except Exception as e:
                    log.error(f"Error running role payouts for guild {guild.id}: {e}", exc_info=True)
        except Exception as e:
            log.error(f"Error in payout loop: {e}", exc_info=True)

    @payout_loop.before_loop
    async def before_payout_loop(self):
        await self.bot.wait_until_ready()

    @commands.group(name="currency")
    @commands.guild_only()
    async def currency(self, ctx: commands.Context):
        """Manage SS14 server currency."""
        pass

    @currency.group(name="payroll")
    @checks.admin_or_permissions(manage_guild=True)
    async def payroll(self, ctx: commands.Context):
        """Configure monthly currency payouts for roles."""
        pass

    @payroll.command(name="set")
    async def payroll_set(self, ctx: commands.Context, role: discord.Role, amount: int):
        """Give a role a monthly payout of the given amount of coins."""
        if amount <= 0:
            await ctx.send("The payout amount must be a positive number.", ephemeral=True)
            return

        async with self.config.guild(ctx.guild).role_payouts() as payouts:
            payouts[str(role.id)] = amount

        embed = discord.Embed(title="✅ Payroll Updated", color=discord.Color.green())
        embed.add_field(name="🏷️ Role", value=role.mention, inline=True)
        embed.add_field(name="💰 Monthly Payout", value=f"{amount:,} coins", inline=True)
        embed.set_footer(text="Members receive the amount of their highest paying role every 30 days.")
        await ctx.send(embed=embed)

    @payroll.command(name="remove")
    async def payroll_remove(self, ctx: commands.Context, role: discord.Role):
        """Stop paying a monthly payout for a role."""
        async with self.config.guild(ctx.guild).role_payouts() as payouts:
            removed = payouts.pop(str(role.id), None)

        if removed is None:
            await ctx.send(f"{role.mention} does not have a monthly payout configured.", ephemeral=True)
            return

        embed = discord.Embed(title="🗑️ Payroll Removed", color=discord.Color.orange())
        embed.add_field(name="🏷️ Role", value=role.mention, inline=True)
        embed.add_field(name="💰 Was Paying", value=f"{removed:,} coins", inline=True)
        await ctx.send(embed=embed)

    @payroll.command(name="list")
    async def payroll_list(self, ctx: commands.Context):
        """List all roles with a configured monthly payout."""
        payouts = await self.config.guild(ctx.guild).role_payouts()
        if not payouts:
            await ctx.send("No roles have a monthly payout configured. Use `payroll set` to add one.", ephemeral=True)
            return

        # Sort by amount, highest first
        entries = sorted(payouts.items(), key=lambda kv: kv[1], reverse=True)

        embed = discord.Embed(
            title="💵 Monthly Role Payouts",
            description="Members receive the amount of their single highest paying role every 30 days.",
            color=discord.Color.gold()
        )
        for role_id, amount in entries:
            role = ctx.guild.get_role(int(role_id))
            role_display = role.mention if role else f"*deleted role ({role_id})*"
            embed.add_field(name=role_display, value=f"{amount:,} coins / month", inline=False)
        await ctx.send(embed=embed)

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

        elif category in ["poor", "broke", "destitute"]:
            # Existing wealth leaderboard
            leaderboard_data = await get_leaderboardasc(pool)
            if not leaderboard_data:
                await ctx.send("The leaderboard is currently empty.")
                return

            embed = discord.Embed(
                title="🏆 Broke Leaderboard",
                description="Top 10 poorest players",
                color=discord.Color.red()
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
        self.payout_loop.cancel()
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

        loser_details = None
        winner_details = None
        transfer_details = None
        
        try:
            loser_details = await add_player_currency(self.pool, loser_player_id, -self.amount)
            
            if loser_details[0]:
                winner_details = await add_player_currency(self.pool, winner_player_id, winner_receives)
                if winner_details[0]: 
                    transfer_details = {
                        'sender_old': loser_details[1],
                        'sender_new': loser_details[2],
                        'recipient_old': winner_details[1],
                        'recipient_new': winner_details[2],
                    }
                else:
                    print("Coinflip transfer failed: Winner transaction failed. Refunding loser.")
                    await add_player_currency(self.pool, loser_player_id, self.amount)
            
            else:
                print("Coinflip transfer failed: Loser transaction failed (likely insufficient funds).")

        except Exception as e:
            print(f"Coinflip transfer error: {e}")
            transfer_details = None

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

        loser_details = None
        winner_details = None
        transfer_details = None
        
        try:
            loser_details = await add_player_currency(self.pool, loser_player_id, -self.amount)
            
            if loser_details[0]:
                winner_details = await add_player_currency(self.pool, winner_player_id, winner_receives)
                if winner_details[0]: 
                    transfer_details = {
                        'sender_old': loser_details[1],
                        'sender_new': loser_details[2],
                        'recipient_old': winner_details[1],
                        'recipient_new': winner_details[2],
                    }
                else:
                    print("Coinflip transfer failed: Winner transaction failed. Refunding loser.")
                    await add_player_currency(self.pool, loser_player_id, self.amount)
            
            else:
                print("Coinflip transfer failed: Loser transaction failed (likely insufficient funds).")

        except Exception as e:
            print(f"Coinflip transfer error: {e}")
            transfer_details = None

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
