import discord
import asyncpg
import logging
import uuid
import asyncio
import aiohttp
import urllib.parse
from discord.ui import Modal, TextInput
from discord import TextStyle
from typing import Dict, Optional

from redbot.core import commands, Config, checks, app_commands
from redbot.core.bot import Red

log = logging.getLogger("red.DurkCogs.SS14Currency")

async def get_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID) -> Optional[int]:
    """Gets the currency for a given player ID."""
    conn = await pool.acquire()
    try:
        query = "SELECT server_currency FROM player WHERE user_id = $1;"
        result = await conn.fetchval(query, player_id)
        return result
    finally:
        await pool.release(conn)

async def set_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> bool:
    """Sets the currency for a given player ID to a specific amount."""
    conn = await pool.acquire()
    try:
        query = "UPDATE player SET server_currency = $1 WHERE user_id = $2;"
        await conn.execute(query, amount, player_id)
        return True
    except Exception as e:
        log.error(f"Error setting currency for player {player_id}: {e}", exc_info=True)
        return False

async def add_player_currency(pool: asyncpg.Pool, player_id: uuid.UUID, amount: int) -> bool:
    """Adds an amount of currency to a given player ID. Amount can be negative."""
    conn = await pool.acquire()
    try:
        query = "UPDATE player SET server_currency = server_currency + $1 WHERE user_id = $2;"
        await conn.execute(query, amount, player_id)
        return True
    except Exception as e:
        log.error(f"Error adding currency for player {player_id}: {e}", exc_info=True)
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

    DEFAULT_GUILD = {
        "db_connection_string": None,
    }

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier="SS14CurrencyMultiDB", force_registration=True)
        self.config.register_guild(**self.DEFAULT_GUILD)
        self.guild_pools: Dict[int, asyncpg.Pool] = {}
        self.pool_locks: Dict[int, asyncio.Lock] = {}
        self.session = aiohttp.ClientSession()

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

    @commands.group(name="currency")
    @commands.guild_only()
    @checks.admin_or_permissions(manage_guild=True)
    async def currency(self, ctx: commands.Context):
        """Manage SS14 server currency."""
        pass

    @currency.command(name="get")
    async def get_coins(self, ctx: commands.Context, *, username: str):
        """Gets the coin balance for a given SS14 username."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.")
            return

        player_id = await self.get_user_id_from_name(username)
        if not player_id:
            await ctx.send(f"Could not find a user with the name `{username}`.")
            return

        balance = await get_player_currency(pool, player_id)
        if balance is not None:
            await ctx.send(f"**{username}** has **{balance}** coins.")
        else:
            await ctx.send(f"Could not retrieve the balance for **{username}**.")

    @currency.command(name="set")
    async def set_coins(self, ctx: commands.Context, username: str, amount: int):
        """Sets the coin balance for a given SS14 username."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.")
            return
            
        if amount < 0:
            await ctx.send("You cannot set a negative coin balance.")
            return

        player_id = await self.get_user_id_from_name(username)
        if not player_id:
            await ctx.send(f"Could not find a user with the name `{username}`.")
            return

        if await set_player_currency(pool, player_id, amount):
            await ctx.send(f"Successfully set **{username}**'s balance to **{amount}** coins.")
        else:
            await ctx.send(f"Failed to set the balance for **{username}**.")

    @currency.command(name="add")
    async def add_coins(self, ctx: commands.Context, username: str, amount: int):
        """Adds coins to a given SS14 username. Can be a negative number."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.")
            return

        player_id = await self.get_user_id_from_name(username)
        if not player_id:
            await ctx.send(f"Could not find a user with the name `{username}`.")
            return

        if await add_player_currency(pool, player_id, amount):
            await ctx.send(f"Successfully added **{amount}** coins to **{username}**.")
        else:
            await ctx.send(f"Failed to add coins for **{username}**.")

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
        guild_ids = list(self.guild_pools.keys())
        for guild_id in guild_ids:
            pool = self.guild_pools.pop(guild_id)
            if pool:
                await pool.close()
        log.info("All guild database connection pools closed.")

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
