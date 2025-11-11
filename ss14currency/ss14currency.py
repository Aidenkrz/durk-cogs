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
        return False

async def get_leaderboard(pool: asyncpg.Pool) -> list:
    """Gets the top 10 players by currency."""
    conn = await pool.acquire()
    try:
        query = "SELECT last_seen_user_name, server_currency FROM player ORDER BY server_currency DESC LIMIT 10;"
        return await conn.fetch(query)
    finally:
        await pool.release(conn)

async def get_player_id_from_discord(pool: asyncpg.Pool, discord_id: int) -> Optional[uuid.UUID]:
    """Gets the player's user_id from their discord ID."""
    conn = await pool.acquire()
    try:
        query = "SELECT player_id FROM rmc_linked_accounts WHERE discord_id = $1;"
        result = await conn.fetchval(query, discord_id)
        return result
    finally:
        await pool.release(conn)

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
    conn = await pool.acquire()
    try:
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
    finally:
        await pool.release(conn)

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

        player_id = None
        player_name = None
        discord_name = None

        if isinstance(user, discord.Member):
            player_id = await get_player_id_from_discord(pool, user.id)
            if player_id:
                player_name = await get_user_name_from_id(self.session, player_id)
                discord_name = user.display_name
            else:
                player_id = await self.get_user_id_from_name(user.name)
                player_name = user.name
                if not player_id:
                    await ctx.send(f"Could not find a linked SS14 account for {user.mention} or an SS14 account with the name `{user.name}`. They can link their account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
                    return
        else:
            player_id = await self.get_user_id_from_name(user)
            player_name = user
            if not player_id:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
                return

        balance = await get_player_currency(pool, player_id)
        if balance is not None:
            embed = discord.Embed(title="Coin Balance", color=discord.Color.blue())
            if discord_name:
                embed.add_field(name="Discord User", value=discord_name, inline=True)
                embed.add_field(name="SS14 Username", value=player_name, inline=True)
            else:
                embed.add_field(name="Player", value=player_name, inline=False)
            embed.add_field(name="Balance", value=f"{balance} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Could not retrieve the balance for **{player_name}**.", ephemeral=True)

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
            
        player_id = None
        player_name = None
        discord_name = None

        if isinstance(user, discord.Member):
            player_id = await get_player_id_from_discord(pool, user.id)
            if not player_id:
                await ctx.send(f"{user.mention} does not have a linked SS14 account.", ephemeral=True)
                return
            player_name = await get_user_name_from_id(self.session, player_id)
            discord_name = user.display_name
        else:
            player_id = await self.get_user_id_from_name(user)
            player_name = user
            if not player_id:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
                return

        if await set_player_currency(pool, player_id, amount):
            embed = discord.Embed(title="Balance Set", color=discord.Color.green())
            if discord_name:
                embed.add_field(name="Discord User", value=discord_name, inline=True)
                embed.add_field(name="SS14 Username", value=player_name, inline=True)
            else:
                embed.add_field(name="Player", value=player_name, inline=False)
            embed.add_field(name="New Balance", value=f"{amount} coins", inline=False)
            await ctx.send(embed=embed)
        else:
            await ctx.send(f"Failed to set the balance for **{player_name}**.", ephemeral=True)

    @currency.command(name="add")
    @checks.admin_or_permissions(manage_guild=True)
    async def add_coins(self, ctx: commands.Context, user: typing.Union[discord.Member, str], amount: int):
        """Adds coins to a given SS14 username or linked Discord user. Can be a negative number."""
        pool = await self.get_pool_for_guild(ctx.guild.id)
        if not pool:
            await ctx.send("Database connection is not configured for this server.", ephemeral=True)
            return
            
        player_id = None
        player_name = None
        discord_name = None

        if isinstance(user, discord.Member):
            player_id = await get_player_id_from_discord(pool, user.id)
            if not player_id:
                await ctx.send(f"{user.mention} does not have a linked SS14 account.", ephemeral=True)
                return
            player_name = await get_user_name_from_id(self.session, player_id)
            discord_name = user.display_name
        else:
            player_id = await self.get_user_id_from_name(user)
            player_name = user
            if not player_id:
                await ctx.send(f"Could not find a user with the name `{user}`.", ephemeral=True)
                return

        if await add_player_currency(pool, player_id, amount):
            embed = discord.Embed(title="Balance Updated", color=discord.Color.green())
            if discord_name:
                embed.add_field(name="Discord User", value=discord_name, inline=True)
                embed.add_field(name="SS14 Username", value=player_name, inline=True)
            else:
                embed.add_field(name="Player", value=player_name, inline=False)
            embed.add_field(name="Amount Added", value=f"{amount} coins", inline=False)
            await ctx.send(embed=embed)
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

        sender_id = await get_player_id_from_discord(pool, ctx.author.id)
        if not sender_id:
            await ctx.send("Your Discord account is not linked to an SS14 account. Please link your account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
            return

        recipient_id = None
        recipient_name = None
        recipient_discord_name = None

        if isinstance(recipient, discord.Member):
            recipient_id = await get_player_id_from_discord(pool, recipient.id)
            if not recipient_id:
                await ctx.send(f"{recipient.mention} does not have a linked SS14 account. They can link their account in https://discord.com/channels/1202734573247795300/1330738082378551326.", ephemeral=True)
                return
            recipient_name = await get_user_name_from_id(self.session, recipient_id)
            recipient_discord_name = recipient.display_name
        else:
            recipient_id = await self.get_user_id_from_name(recipient)
            recipient_name = recipient
            if not recipient_id:
                await ctx.send(f"Could not find a user with the name `{recipient}`.", ephemeral=True)
                return

        if sender_id == recipient_id:
            await ctx.send("You cannot transfer coins to yourself.", ephemeral=True)
            return

        transfer_details = await transfer_currency(pool, sender_id, recipient_id, amount)
        if transfer_details:
            sender_name = await get_user_name_from_id(self.session, sender_id)
            embed = discord.Embed(title="Transfer Successful", color=discord.Color.green())

            sender_field_name = f"Sender: {ctx.author.display_name} ({sender_name})"
            sender_field_value = f"`{transfer_details['sender_old']}` -> `{transfer_details['sender_new']}`"
            embed.add_field(name=sender_field_name, value=sender_field_value, inline=False)

            if recipient_discord_name:
                recipient_field_name = f"Recipient: {recipient_discord_name} ({recipient_name})"
            else:
                recipient_field_name = f"Recipient: {recipient_name}"
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
            embed.add_field(name=f"{i}. {record['last_seen_user_name']}", value=f"{record['server_currency']} coins", inline=False)
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
