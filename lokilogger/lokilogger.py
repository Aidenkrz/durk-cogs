import asyncio
import logging
import time
from typing import Optional

import aiohttp
import discord
from discord.ext import tasks
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.yourcog.lokilogger")

LOGS_PER_PAGE = 5

class LokiLogger(commands.Cog):
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=1234567890, force_registration=True
        )
        default_guild = {
            "loki_url": None,
            "channel_id": None,
            "role_id": None,
            "query": '{level="error"}',
            "last_timestamp": None,
            "enabled": False,
        }
        self.config.register_guild(**default_guild)
        
        self.interactive_logs = {} 
        self.number_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        self.arrow_emojis = {"left": "⬅️", "right": "➡️"}
        
        self.loki_task.start()

    def cog_unload(self):
        self.loki_task.cancel()
        for message_id in list(self.interactive_logs.keys()):
            if "cleanup_task" in self.interactive_logs[message_id]:
                self.interactive_logs[message_id]["cleanup_task"].cancel()

    async def _generate_log_page_embed(self, message_key_or_id: int, page_number: int) -> Optional[discord.Embed]:
        interactive_session = self.interactive_logs.get(message_key_or_id)
        if not interactive_session:
            return None

        logs = interactive_session["logs"]
        total_pages = interactive_session["total_pages"]
        num_total_logs = interactive_session["num_total_logs"]
        first_ts = interactive_session["first_log_ts_seconds"]
        last_ts = interactive_session["last_log_ts_seconds"]
        
        guild = self.bot.get_guild(interactive_session["guild_id"])
        embed_color = guild.me.color if guild and guild.me else discord.Color.blue()

        start_index = page_number * LOGS_PER_PAGE
        end_index = start_index + LOGS_PER_PAGE
        logs_on_this_page = logs[start_index:end_index]

        embed_title = f"Loki Log Summary (Page {page_number + 1}/{total_pages})"
        embed_description = (
            f"Found **{num_total_logs}** new log entr{'ies' if num_total_logs > 1 else 'y'} "
            f"between <t:{first_ts}:T> and <t:{last_ts}:T>.\n"
            f"React with a number emoji to view full details for the corresponding log."
        )
        
        embed = discord.Embed(title=embed_title, description=embed_description, color=embed_color)

        for i, entry in enumerate(logs_on_this_page):
            server_label = entry['stream'].get('Server', entry['stream'].get('instance', 'Unknown Source'))
            first_line = entry['message'].split('\n', 1)[0]
            max_field_value_len = 1000 
            if len(first_line) > max_field_value_len - 10:
                first_line = first_line[:max_field_value_len - 13] + "..."

            field_name = f"{self.number_emojis[i]} Log from `{server_label}`"
            field_value = f"```{first_line}```"
            embed.add_field(name=field_name, value=field_value, inline=False)
        
        footer_text = f"Page {page_number + 1}/{total_pages} | Reactions active for 1 hour."
        if not logs_on_this_page and total_pages > 0 :
            footer_text = f"Page {page_number + 1}/{total_pages} | No logs on this page. | Reactions active for 1 hour."
            embed.add_field(name="Empty Page", value="No logs to display on this page.", inline=False)

        embed.set_footer(text=footer_text)
        return embed

    async def _cleanup_interactive_log(self, message_id: int, delay: int):
        await asyncio.sleep(delay)
        if message_id in self.interactive_logs:
            del self.interactive_logs[message_id]
            log.debug(f"Cleaned up interactive log for message ID: {message_id}")

    @tasks.loop(minutes=5)
    async def loki_task(self):
        all_guilds = await self.config.all_guilds()
        for guild_id, settings in all_guilds.items():
            if not settings["enabled"] or not settings["loki_url"] or not settings["channel_id"]:
                continue

            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue

            channel = guild.get_channel(settings["channel_id"])
            if not channel:
                log.warning(f"Channel {settings['channel_id']} not found in guild {guild_id}")
                continue
            
            await self._fetch_and_post_logs(guild, channel, settings)

    async def _fetch_and_post_logs(self, guild: discord.Guild, channel: discord.TextChannel, settings: dict):
        loki_url = settings["loki_url"]
        query = settings["query"]
        last_timestamp_ns_str = settings.get("last_timestamp")

        end_ts_ns = int(time.time() * 1_000_000_000)

        if last_timestamp_ns_str:
            start_ts_ns = int(last_timestamp_ns_str)
        else:
            start_ts_ns = end_ts_ns - (5 * 60 * 1_000_000_000)

        max_initial_fetch_duration_ns = 1 * 60 * 60 * 1_000_000_000
        if not last_timestamp_ns_str and (end_ts_ns - start_ts_ns > max_initial_fetch_duration_ns):
            start_ts_ns = end_ts_ns - max_initial_fetch_duration_ns
            log.info(f"[{guild.id}] No last timestamp, capping initial fetch to 1 hour ago.")

        params = {
            "query": query,
            "start": str(start_ts_ns + 1),
            "end": str(end_ts_ns),
            "limit": 1000, 
            "direction": "forward",
        }

        log.info(
            f"[{guild.id}] Fetching logs from Loki: URL='{loki_url}', Query='{query}', "
            f"Start='{params['start']}', End='{params['end']}'"
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(loki_url, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
        except aiohttp.ClientError as e:
            log.error(f"[{guild.id}] Error connecting to Loki API: {e}")
            await channel.send(f"Error connecting to Loki: `{e}`. Please check the URL and Loki server status.")
            return
        except Exception as e:
            log.error(f"[{guild.id}] Error querying Loki or parsing response: {e}")
            await channel.send(f"Error querying Loki: `{e}`.")
            return

        if not data or "data" not in data or "result" not in data["data"] or not data["data"]["result"]:
            log.info(f"[{guild.id}] No new logs found or unexpected response structure from Loki.")
            return

        raw_log_entries = []
        for stream in data["data"]["result"]:
            for value_pair in stream["values"]:
                raw_log_entries.append(
                    {"timestamp": value_pair[0], "message": value_pair[1], "stream": stream.get("stream", {})}
                )
        raw_log_entries.sort(key=lambda x: int(x["timestamp"]))

        if not raw_log_entries:
            log.info(f"[{guild.id}] No new log entries after parsing.")
            return

        log.info(f"[{guild.id}] Found {len(raw_log_entries)} new log entries.")
        
        new_latest_timestamp_ns_str = raw_log_entries[-1]["timestamp"]

        ping_content = ""
        if settings.get("role_id"):
            role = guild.get_role(settings["role_id"])
            if role:
                ping_content = f"{role.mention} "
            else:
                log.warning(f"[{guild.id}] Configured role ID {settings['role_id']} not found.")

        num_logs = len(raw_log_entries)
        first_log_ts_ns = raw_log_entries[0]["timestamp"]
        last_log_ts_ns = new_latest_timestamp_ns_str
        
        first_log_ts_seconds = int(first_log_ts_ns) // 1_000_000_000
        last_log_ts_seconds = int(last_log_ts_ns) // 1_000_000_000
        
        initial_page = 0
        total_pages = (num_logs + LOGS_PER_PAGE - 1) // LOGS_PER_PAGE

        temp_storage_key = f"temp_{guild.id}_{time.time_ns()}"
        self.interactive_logs[temp_storage_key] = {
            "logs": raw_log_entries,
            "current_page": initial_page,
            "message_object": None,
            "guild_id": guild.id,
            "channel_id": channel.id,
            "total_pages": total_pages,
            "first_log_ts_seconds": first_log_ts_seconds,
            "last_log_ts_seconds": last_log_ts_seconds,
            "num_total_logs": num_logs,
            "cleanup_task": None
        }

        try:
            page_embed = await self._generate_log_page_embed(temp_storage_key, initial_page)
            if page_embed is None:
                log.error(f"[{guild.id}] Failed to generate initial page embed for key {temp_storage_key}.")
                if temp_storage_key in self.interactive_logs:
                    del self.interactive_logs[temp_storage_key]
                return

            summary_message = await channel.send(content=ping_content if ping_content else None, embed=page_embed)
            
            session_data = self.interactive_logs.pop(temp_storage_key)
            session_data["message_object"] = summary_message
            cleanup_task = asyncio.create_task(self._cleanup_interactive_log(summary_message.id, delay=3600))
            session_data["cleanup_task"] = cleanup_task
            self.interactive_logs[summary_message.id] = session_data
            
            logs_on_current_page_count = len(raw_log_entries[initial_page*LOGS_PER_PAGE : (initial_page+1)*LOGS_PER_PAGE])
            for i in range(logs_on_current_page_count):
                 if i < LOGS_PER_PAGE:
                    await summary_message.add_reaction(self.number_emojis[i])

            if total_pages > 1:
                await summary_message.add_reaction(self.arrow_emojis["left"])
                await summary_message.add_reaction(self.arrow_emojis["right"])
            
        except discord.HTTPException as e:
            log.error(f"[{guild.id}] Discord API error sending summary log embed or adding reactions: {e}")
            if temp_storage_key in self.interactive_logs:
                del self.interactive_logs[temp_storage_key]
            return
        except Exception as e:
            log.exception(f"[{guild.id}] Unexpected error during initial summary message processing for key {temp_storage_key}: {e}")
            if temp_storage_key in self.interactive_logs:
                del self.interactive_logs[temp_storage_key]
            return

        if new_latest_timestamp_ns_str != last_timestamp_ns_str and new_latest_timestamp_ns_str is not None:
            await self.config.guild(guild).last_timestamp.set(new_latest_timestamp_ns_str)
            log.info(f"[{guild.id}] Updated last_timestamp to {new_latest_timestamp_ns_str}")

    @loki_task.before_loop
    async def before_loki_task(self):
        await self.bot.wait_until_ready()
        log.info("LokiLogger task waiting for bot to be ready...")

    @commands.Cog.listener()
    async def on_reaction_add(self, reaction: discord.Reaction, user: discord.User):
        if user.bot:
            return

        message = reaction.message
        if message.id not in self.interactive_logs:
            return
        
        interactive_session = self.interactive_logs.get(message.id)
        if not interactive_session:
            return

        guild_id = interactive_session["guild_id"]
        current_page = interactive_session["current_page"]
        total_pages = interactive_session["total_pages"]
        logs = interactive_session["logs"]
        message_obj = interactive_session["message_object"]

        new_page_number = current_page
        action_taken = False

        if str(reaction.emoji) == self.arrow_emojis["left"]:
            if current_page > 0:
                new_page_number = current_page - 1
                action_taken = True
        elif str(reaction.emoji) == self.arrow_emojis["right"]:
            if current_page < total_pages - 1:
                new_page_number = current_page + 1
                action_taken = True
        else:
            try:
                emoji_index = self.number_emojis.index(str(reaction.emoji))
                if emoji_index < LOGS_PER_PAGE:
                    actual_log_index = (current_page * LOGS_PER_PAGE) + emoji_index
                    if actual_log_index < len(logs):
                        entry_to_display = logs[actual_log_index]
                        
                        ts_ns = entry_to_display["timestamp"]
                        msg_text = entry_to_display["message"]
                        stream_labels = entry_to_display["stream"]
                        label_str = ", ".join([f"`{k}`=`{v}`" for k, v in stream_labels.items()])
                        
                        max_log_line_len = 1800 - len(label_str)
                        if len(msg_text) > max_log_line_len:
                            msg_text = msg_text[:max_log_line_len] + "... (truncated)"

                        ts_seconds = int(ts_ns) // 1_000_000_000
                        discord_timestamp = f"<t:{ts_seconds}:F> (<t:{ts_seconds}:R>)"
                        
                        formatted_detail_message = (
                            f"**Log Detail ({emoji_index + 1} on page {current_page + 1}):** [{label_str}]\n"
                            f"```\n{msg_text}\n```\n"
                            f"Timestamp: {discord_timestamp}"
                        )
                        try:
                            await message.channel.send(formatted_detail_message)
                        except discord.HTTPException as e:
                            log.error(f"[{guild_id}] Discord API error sending detailed log: {e}")
                        action_taken = True
            except ValueError:
                pass

        if action_taken and new_page_number != current_page :
            interactive_session["current_page"] = new_page_number
            new_embed = await self._generate_log_page_embed(message.id, new_page_number)
            if new_embed and message_obj:
                try:
                    await message_obj.edit(embed=new_embed)
                    await message_obj.clear_reactions()
                    
                    logs_on_new_page_count = len(logs[new_page_number*LOGS_PER_PAGE : (new_page_number+1)*LOGS_PER_PAGE])
                    for i in range(logs_on_new_page_count):
                        if i < LOGS_PER_PAGE:
                           await message_obj.add_reaction(self.number_emojis[i])
                    
                    if total_pages > 1:
                        if new_page_number > 0:
                            await message_obj.add_reaction(self.arrow_emojis["left"])
                        if new_page_number < total_pages - 1:
                            await message_obj.add_reaction(self.arrow_emojis["right"])
                except discord.HTTPException as e:
                    log.error(f"[{guild_id}] Failed to edit message or update reactions for pagination: {e}")
        
        if action_taken:
            try:
                await reaction.remove(user)
            except discord.HTTPException:
                pass
    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def lokiset(self, ctx: commands.Context):
        await ctx.send_help()

    @lokiset.command(name="url")
    async def lokiset_url(self, ctx: commands.Context, url: str):
        if not url.endswith("/loki/api/v1/query_range") and not url.endswith("/loki/api/v1/query"):
            await ctx.send(
                "The URL should typically end with `/loki/api/v1/query_range` or `/loki/api/v1/query`."
            )
        await self.config.guild(ctx.guild).loki_url.set(url)
        await ctx.send(f"Loki API URL set to: `{url}`")

    @lokiset.command(name="channel")
    async def lokiset_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.send(f"Log channel set to: {channel.mention}")

    @lokiset.command(name="role")
    async def lokiset_role(self, ctx: commands.Context, role: Optional[discord.Role] = None):
        if role:
            await self.config.guild(ctx.guild).role_id.set(role.id)
            await ctx.send(f"Role to ping set to: {role.name}")
        else:
            await self.config.guild(ctx.guild).role_id.set(None)
            await ctx.send("Role pinging disabled.")

    @lokiset.command(name="query")
    async def lokiset_query(self, ctx: commands.Context, *, query: str):
        await self.config.guild(ctx.guild).query.set(query)
        await ctx.send(f"Loki query set to: `{query}`")

    @lokiset.command(name="toggle")
    async def lokiset_toggle(self, ctx: commands.Context, on_off: Optional[bool] = None):
        current_status = await self.config.guild(ctx.guild).enabled()
        if on_off is None:
            await ctx.send(f"Log fetching is currently {'enabled' if current_status else 'disabled'}.")
            return

        await self.config.guild(ctx.guild).enabled.set(on_off)
        if on_off:
            await ctx.send("Log fetching enabled.")
            await self.config.guild(ctx.guild).last_timestamp.set(None)
        else:
            await ctx.send("Log fetching disabled.")

    @lokiset.command(name="purge")
    async def lokiset_purge(self, ctx: commands.Context):
        """Purge all active interactive log sessions for this server."""
        guild_id = ctx.guild.id
        purged = 0
        for message_id in list(self.interactive_logs.keys()):
            session = self.interactive_logs[message_id]
            if session.get("guild_id") == guild_id:
                if "cleanup_task" in session and session["cleanup_task"]:
                    session["cleanup_task"].cancel()
                del self.interactive_logs[message_id]
                purged += 1
        await ctx.send(f"Purged {purged} interactive log session{'s' if purged != 1 else ''}.")

    @lokiset.command(name="settings")
    async def lokiset_settings(self, ctx: commands.Context):
        settings = await self.config.guild(ctx.guild).all()
        url = settings.get("loki_url", "Not set")
        channel_id = settings.get("channel_id")
        channel_mention = f"<#{channel_id}>" if channel_id else "Not set"
        role_id = settings.get("role_id")
        role_mention = f"<@&{role_id}>" if role_id else "Not set (no pings)"
        query = settings.get("query", "Not set")
        enabled = "Enabled" if settings.get("enabled") else "Disabled"
        last_ts = settings.get("last_timestamp", "N/A")

        embed = discord.Embed(title="LokiLogger Settings", color=await ctx.embed_color())
        embed.add_field(name="Status", value=enabled, inline=False)
        embed.add_field(name="Loki URL", value=f"`{url}`", inline=False)
        embed.add_field(name="Log Channel", value=channel_mention, inline=False)
        embed.add_field(name="Ping Role", value=role_mention, inline=False)
        embed.add_field(name="Loki Query", value=f"`{query}`", inline=False)
        embed.add_field(name="Last Timestamp Processed", value=f"`{last_ts}`", inline=False)
        await ctx.send(embed=embed)

    @commands.command()
    @commands.guild_only()
    @commands.is_owner()
    async def testloki(self, ctx: commands.Context):
        settings = await self.config.guild(ctx.guild).all()
        if not settings["enabled"]:
            await ctx.send("LokiLogger is not enabled for this server. Use `[p]lokiset toggle true`.")
            return
        if not settings["loki_url"]:
            await ctx.send("Loki URL is not set. Use `[p]lokiset url <your_loki_url>`.")
            return
        if not settings["channel_id"]:
            await ctx.send("Log channel is not set. Use `[p]lokiset channel <#channel>`.")
            return

        channel = ctx.guild.get_channel(settings["channel_id"])
        if not channel:
            await ctx.send(f"Configured log channel (ID: {settings['channel_id']}) not found.")
            return

        await ctx.send(f"Manually fetching logs for {ctx.guild.name}...")
        try:
            await self._fetch_and_post_logs(ctx.guild, channel, settings)
            await ctx.send("Log fetch attempt complete. Check the log channel.")
        except Exception as e:
            await ctx.send(f"An error occurred during manual fetch: ```{e}```")
            log.exception(f"Error during manual loki fetch for guild {ctx.guild.id}", exc_info=e)

async def setup(bot: Red):
    cog = LokiLogger(bot)
    await bot.add_cog(cog)
