import asyncio
import logging
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional, Union

import aiohttp
import discord
from discord.ext import tasks
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.durk-cogs.deadlocktracker")

API_BASE = "https://api.deadlock-api.com"
ASSETS_BASE = "https://assets.deadlock-api.com"

# steamid64 of account_id 0; account_id = steamid64 - this offset
STEAMID64_BASE = 76561197960265728

# How many recent matches the stats command aggregates over.
STATS_WINDOW = 50
# How many recent matches the stats command lists individually.
RECENT_COUNT = 5
# How long (seconds) to cache the heroes/ranks asset maps.
ASSETS_TTL = 86400

MATCH_MODE_NAMES = {
    0: "Invalid",
    1: "Unranked",
    2: "Private Lobby",
    3: "Co-op Bot",
    4: "Ranked",
    5: "Server Test",
    6: "Tutorial",
    7: "Hero Labs",
    8: "Calibration",
}
GAME_MODE_NAMES = {
    0: "Invalid",
    1: "Normal",
    2: "1v1 Test",
    3: "Sandbox",
    4: "Street Brawl",
}
SUBRANK_ROMAN = {0: "", 1: "I", 2: "II", 3: "III", 4: "IV", 5: "V", 6: "VI"}

PROFILE_URL_RE = re.compile(r"steamcommunity\.com/profiles/(\d{17})")
VANITY_URL_RE = re.compile(r"steamcommunity\.com/id/([^/\s]+)")
MENTION_RE = re.compile(r"^<@!?(\d+)>$")


class DeadlockTracker(commands.Cog):
    """
    Look up Deadlock player stats and auto-post watched players' match results.

    Data comes from the public deadlock-api.com API.
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=0xDEAD10C4, force_registration=True
        )
        default_guild = {
            "channel_id": None,
            "enabled": False,
            # account_id (str) -> {account_id, label, discord_id, last_match_id}
            "players": {},
        }
        self.config.register_guild(**default_guild)

        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "durk-cogs/deadlocktracker"},
            timeout=aiohttp.ClientTimeout(total=30),
        )

        # Asset caches (id -> name / tier -> info), refreshed lazily.
        self._heroes: dict[int, str] = {}
        self._ranks: dict[int, dict] = {}
        self._assets_loaded_at: float = 0.0
        self._assets_lock = asyncio.Lock()

        self.feed_task.start()

    def cog_unload(self):
        self.feed_task.cancel()
        asyncio.create_task(self.session.close())

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    async def _api_get(self, path: str, params: Optional[dict] = None):
        """GET from the deadlock API. Returns (data, None) or (None, error_str)."""
        url = f"{API_BASE}{path}"
        try:
            async with self.session.get(url, params=params) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    msg = None
                    if isinstance(data, dict):
                        msg = data.get("error") or data.get("message")
                    return None, msg or f"API returned status {resp.status}"
                return data, None
        except asyncio.TimeoutError:
            return None, "The Deadlock API timed out."
        except aiohttp.ClientError as e:
            return None, f"Could not reach the Deadlock API: {e}"
        except Exception as e:  # noqa: BLE001 - surface anything unexpected
            log.exception("Unexpected error calling %s", url)
            return None, f"Unexpected error: {e}"

    async def _assets_get(self, path: str):
        url = f"{ASSETS_BASE}{path}"
        try:
            async with self.session.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception:  # noqa: BLE001
            log.exception("Error fetching asset %s", url)
            return None

    async def _ensure_assets(self):
        """Load (or refresh) the hero and rank asset maps if stale."""
        if self._heroes and (time.time() - self._assets_loaded_at) < ASSETS_TTL:
            return
        async with self._assets_lock:
            if self._heroes and (time.time() - self._assets_loaded_at) < ASSETS_TTL:
                return
            heroes = await self._assets_get("/v2/heroes")
            ranks = await self._assets_get("/v2/ranks")
            if heroes:
                self._heroes = {
                    h["id"]: h.get("name", f"Hero {h['id']}") for h in heroes
                }
            if ranks:
                self._ranks = {r["tier"]: r for r in ranks}
            if self._heroes or self._ranks:
                self._assets_loaded_at = time.time()

    # ------------------------------------------------------------------
    # Deadlock API convenience wrappers
    # ------------------------------------------------------------------
    async def _get_match_history(self, account_id: int):
        return await self._api_get(f"/v1/players/{account_id}/match-history")

    async def _get_mmr(self, account_id: int):
        data, err = await self._api_get(
            "/v1/players/mmr", params={"account_ids": str(account_id)}
        )
        if err:
            return None, err
        if isinstance(data, list) and data:
            return data[0], None
        return None, None

    async def _get_steam_profile(self, account_id: int):
        data, err = await self._api_get(
            "/v1/players/steam", params={"account_ids": str(account_id)}
        )
        if err:
            return None
        if isinstance(data, list) and data:
            return data[0]
        return None

    async def _steam_search(self, query: str, limit: int = 5):
        data, err = await self._api_get(
            "/v1/players/steam-search",
            params={"search_query": query, "limit": str(limit)},
        )
        if err or not isinstance(data, list):
            return []
        return data

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _to_account_id(value: int) -> int:
        if value > STEAMID64_BASE:
            return value - STEAMID64_BASE
        return value

    def _hero_name(self, hero_id: Optional[int]) -> str:
        if hero_id is None:
            return "Unknown"
        return self._heroes.get(hero_id, f"Hero {hero_id}")

    def _rank_display(self, division: Optional[int], subrank: Optional[int]):
        """Return (label, badge_image_url) for a division/subrank, or (label, None)."""
        if not division:
            return "Unranked", None
        info = self._ranks.get(division)
        if not info:
            return f"Tier {division}", None
        name = info.get("name", f"Tier {division}")
        roman = SUBRANK_ROMAN.get(subrank or 0, "")
        label = f"{name} {roman}".strip()
        images = info.get("images", {}) or {}
        key = f"large_subrank{subrank}" if subrank else "large"
        badge = images.get(key) or images.get("large")
        return label, badge

    @staticmethod
    def _won(row: dict) -> bool:
        return row.get("player_team") == row.get("match_result")

    @staticmethod
    def _fmt_duration(seconds: Optional[int]) -> str:
        seconds = seconds or 0
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"

    @staticmethod
    def _fmt_networth(nw: Optional[int]) -> str:
        nw = nw or 0
        if nw >= 1000:
            return f"{nw / 1000:.0f}k"
        return str(nw)

    def _mode_label(self, row: dict) -> str:
        game_mode = row.get("game_mode")
        if game_mode == 4:
            return "Street Brawl"
        return MATCH_MODE_NAMES.get(row.get("match_mode"), "Match")

    @staticmethod
    def _kda_ratio(kills: int, deaths: int, assists: int) -> float:
        return (kills + assists) / max(deaths, 1)

    # ------------------------------------------------------------------
    # Player resolution
    # ------------------------------------------------------------------
    async def _account_for_member(self, guild: discord.Guild, member_id: int):
        players = await self.config.guild(guild).players()
        for entry in players.values():
            if entry.get("discord_id") == member_id:
                return entry["account_id"]
        return None

    async def _resolve_query(self, ctx: commands.Context, query: str):
        """
        Resolve a free-form player query to an account_id.

        Returns (account_id, None) on success or (None, message) on failure /
        ambiguity, where message is meant to be shown to the user.
        """
        query = query.strip()

        # Discord mention -> linked account in this guild.
        mention = MENTION_RE.match(query)
        if mention:
            member_id = int(mention.group(1))
            account_id = await self._account_for_member(ctx.guild, member_id)
            if account_id is None:
                return None, "That member isn't linked to a Deadlock account here."
            return account_id, None

        # Steam profile URL with a 64-bit id.
        url_match = PROFILE_URL_RE.search(query)
        if url_match:
            return self._to_account_id(int(url_match.group(1))), None

        # Vanity URL -> use the vanity name as a search term.
        vanity = VANITY_URL_RE.search(query)
        if vanity:
            query = vanity.group(1)

        # Plain number -> steamid64 or account_id.
        if query.isdigit():
            return self._to_account_id(int(query)), None

        # Otherwise treat it as a Steam persona name.
        results = await self._steam_search(query, limit=5)
        if not results:
            return None, f"No Steam profiles found for `{query}`."
        if len(results) == 1:
            return results[0]["account_id"], None

        lines = [
            f"• `{r['account_id']}` — {r.get('personaname', 'Unknown')}"
            for r in results[:5]
        ]
        joined = "\n".join(lines)
        return None, (
            f"Multiple profiles matched `{query}`. Re-run with one of these account IDs:\n{joined}"
        )

    # ------------------------------------------------------------------
    # Stats command
    # ------------------------------------------------------------------
    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    async def deadlock(self, ctx: commands.Context):
        """Deadlock player stats."""
        await ctx.send_help()

    @deadlock.command(name="stats")
    async def deadlock_stats(
        self, ctx: commands.Context, *, player: Optional[str] = None
    ):
        """
        Show a Deadlock player's stats.

        `player` may be a @member (if linked), a steamid64, a 32-bit account id,
        a Steam profile URL, or a Steam name. Omit it to use your own linked
        account.
        """
        async with ctx.typing():
            await self._ensure_assets()

            if player is None:
                account_id = await self._account_for_member(ctx.guild, ctx.author.id)
                if account_id is None:
                    return await ctx.send(
                        "You aren't linked to a Deadlock account. "
                        "Provide a player, or have an admin link you with "
                        "`{p}deadlockset addplayer <id> @you`.".format(
                            p=ctx.clean_prefix
                        )
                    )
            else:
                account_id, err = await self._resolve_query(ctx, player)
                if err:
                    return await ctx.send(err)

            history, err = await self._get_match_history(account_id)
            if err:
                return await ctx.send(f"Couldn't fetch match history: {err}")
            if not history:
                return await ctx.send("That player has no recorded matches.")

            mmr, _ = await self._get_mmr(account_id)
            profile = await self._get_steam_profile(account_id)
            embed = self._build_stats_embed(account_id, history, mmr, profile)
            await ctx.send(embed=embed)

    def _build_stats_embed(
        self,
        account_id: int,
        history: list,
        mmr: Optional[dict],
        profile: Optional[dict],
    ) -> discord.Embed:
        window = history[:STATS_WINDOW]
        wins = sum(1 for r in window if self._won(r))
        losses = len(window) - wins
        winrate = (wins / len(window) * 100) if window else 0
        tot_k = sum(r.get("player_kills", 0) for r in window)
        tot_d = sum(r.get("player_deaths", 0) for r in window)
        tot_a = sum(r.get("player_assists", 0) for r in window)
        avg_nw = (
            sum(r.get("net_worth", 0) for r in window) / len(window) if window else 0
        )
        avg_dur = (
            sum(r.get("match_duration_s", 0) for r in window) / len(window)
            if window
            else 0
        )

        name = (profile or {}).get("personaname") or f"Account {account_id}"
        embed = discord.Embed(title=name, color=discord.Color.from_rgb(199, 124, 60))
        if profile:
            embed.url = profile.get("profileurl")
            if profile.get("avatarfull"):
                embed.set_author(name=name, icon_url=profile["avatarfull"])

        rank_label = "Unranked"
        if mmr:
            rank_label, badge = self._rank_display(
                mmr.get("division"), mmr.get("division_tier")
            )
            if badge:
                embed.set_thumbnail(url=badge)
        embed.add_field(name="Rank", value=rank_label, inline=True)
        embed.add_field(
            name=f"Last {len(window)}",
            value=f"{wins}W–{losses}L · {winrate:.0f}%",
            inline=True,
        )
        embed.add_field(
            name="KDA",
            value=f"{self._kda_ratio(tot_k, tot_d, tot_a):.2f}",
            inline=True,
        )
        embed.add_field(
            name="Avg net worth", value=self._fmt_networth(round(avg_nw)), inline=True
        )
        embed.add_field(
            name="Avg length", value=self._fmt_duration(avg_dur), inline=True
        )

        # Top heroes in the window.
        hero_ids = [r.get("hero_id") for r in window if r.get("hero_id") is not None]
        top = Counter(hero_ids).most_common(3)
        if top:
            lines = []
            for hero_id, count in top:
                rows = [r for r in window if r.get("hero_id") == hero_id]
                hw = sum(1 for r in rows if self._won(r))
                k = sum(r.get("player_kills", 0) for r in rows)
                d = sum(r.get("player_deaths", 0) for r in rows)
                a = sum(r.get("player_assists", 0) for r in rows)
                lines.append(
                    f"**{self._hero_name(hero_id)}** — {count} · "
                    f"{hw / count * 100:.0f}% · {self._kda_ratio(k, d, a):.1f} KDA"
                )
            embed.add_field(name="Top heroes", value="\n".join(lines), inline=False)

        # Most recent matches.
        recent_lines = []
        for r in history[:RECENT_COUNT]:
            result = "🟩" if self._won(r) else "🟥"
            recent_lines.append(
                f"{result} {self._hero_name(r.get('hero_id'))} "
                f"{r.get('player_kills', 0)}/{r.get('player_deaths', 0)}/"
                f"{r.get('player_assists', 0)}"
            )
        if recent_lines:
            embed.add_field(
                name="Recent matches", value="\n".join(recent_lines), inline=False
            )

        embed.set_footer(text=f"account id {account_id}")
        return embed

    # ------------------------------------------------------------------
    # Settings group
    # ------------------------------------------------------------------
    @commands.group(invoke_without_command=True)
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def deadlockset(self, ctx: commands.Context):
        """Configure the Deadlock match feed."""
        await ctx.send_help()

    @deadlockset.command(name="channel")
    async def deadlockset_channel(
        self, ctx: commands.Context, channel: discord.TextChannel
    ):
        """Set the channel where match results are posted."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.send(f"Match feed channel set to {channel.mention}.")

    @deadlockset.command(name="toggle")
    async def deadlockset_toggle(
        self, ctx: commands.Context, on_off: Optional[bool] = None
    ):
        """Turn the automatic match feed on or off."""
        current = await self.config.guild(ctx.guild).enabled()
        if on_off is None:
            return await ctx.send(
                f"The match feed is currently {'enabled' if current else 'disabled'}."
            )
        await self.config.guild(ctx.guild).enabled.set(on_off)
        await ctx.send(f"Match feed {'enabled' if on_off else 'disabled'}.")

    @deadlockset.command(name="addplayer")
    async def deadlockset_addplayer(
        self,
        ctx: commands.Context,
        player: str,
        member: Optional[discord.Member] = None,
    ):
        """
        Add a player to the watchlist.

        `player` may be a steamid64, a 32-bit account id, a Steam profile URL,
        or a Steam name. Optionally link a Discord `member` so feed posts ping
        them.
        """
        async with ctx.typing():
            account_id, err = await self._resolve_query(ctx, player)
            if err:
                return await ctx.send(err)

            players = await self.config.guild(ctx.guild).players()
            if str(account_id) in players:
                return await ctx.send(
                    f"Account `{account_id}` is already on the watchlist."
                )

            profile = await self._get_steam_profile(account_id)
            label = (profile or {}).get("personaname") or str(account_id)

            # Seed the watermark with the current newest match so we don't
            # backfill-spam their history.
            history, hist_err = await self._get_match_history(account_id)
            if hist_err:
                return await ctx.send(
                    f"Couldn't verify that account against the Deadlock API: {hist_err}"
                )
            last_match_id = history[0]["match_id"] if history else 0

            players[str(account_id)] = {
                "account_id": account_id,
                "label": label,
                "discord_id": member.id if member else None,
                "last_match_id": last_match_id,
            }
            await self.config.guild(ctx.guild).players.set(players)

        link = f" linked to {member.mention}" if member else ""
        await ctx.send(
            f"Now watching **{label}** (`{account_id}`){link}. "
            f"New matches will be posted to the feed channel.",
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @deadlockset.command(name="removeplayer")
    async def deadlockset_removeplayer(self, ctx: commands.Context, *, player: str):
        """Remove a player from the watchlist (by @member, id, url, or name)."""
        account_id, err = await self._resolve_query(ctx, player)
        if err:
            return await ctx.send(err)
        players = await self.config.guild(ctx.guild).players()
        entry = players.pop(str(account_id), None)
        if entry is None:
            return await ctx.send(f"Account `{account_id}` isn't on the watchlist.")
        await self.config.guild(ctx.guild).players.set(players)
        await ctx.send(f"Removed **{entry.get('label', account_id)}** from the watchlist.")

    @deadlockset.command(name="list")
    async def deadlockset_list(self, ctx: commands.Context):
        """List the watched players."""
        players = await self.config.guild(ctx.guild).players()
        if not players:
            return await ctx.send("No players are being watched yet.")
        lines = []
        for entry in players.values():
            discord_id = entry.get("discord_id")
            link = f" → <@{discord_id}>" if discord_id else ""
            lines.append(
                f"• **{entry.get('label')}** (`{entry['account_id']}`){link}"
            )
        embed = discord.Embed(
            title="Watched Deadlock players",
            description="\n".join(lines),
            color=discord.Color.from_rgb(199, 124, 60),
        )
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    @deadlockset.command(name="settings")
    async def deadlockset_settings(self, ctx: commands.Context):
        """Show the current feed configuration."""
        conf = await self.config.guild(ctx.guild).all()
        channel_id = conf.get("channel_id")
        channel = f"<#{channel_id}>" if channel_id else "Not set"
        embed = discord.Embed(
            title="DeadlockTracker settings",
            color=discord.Color.from_rgb(199, 124, 60),
        )
        embed.add_field(
            name="Status",
            value="Enabled" if conf.get("enabled") else "Disabled",
            inline=True,
        )
        embed.add_field(name="Feed channel", value=channel, inline=True)
        embed.add_field(
            name="Players watched", value=str(len(conf.get("players", {}))), inline=True
        )
        await ctx.send(embed=embed)

    @deadlock.command(name="testfeed")
    @commands.is_owner()
    async def deadlock_testfeed(self, ctx: commands.Context):
        """Manually run one feed poll for this server (bot owner only)."""
        conf = await self.config.guild(ctx.guild).all()
        if not conf.get("channel_id"):
            return await ctx.send("No feed channel is set.")
        if not conf.get("players"):
            return await ctx.send("No players are being watched.")
        channel = ctx.guild.get_channel(conf["channel_id"])
        if channel is None:
            return await ctx.send("The configured feed channel no longer exists.")
        await ctx.send("Running a feed poll for this server…")
        posted = await self._process_guild(ctx.guild, channel)
        await ctx.send(f"Done. Posted {posted} match result(s).")

    # ------------------------------------------------------------------
    # Feed loop
    # ------------------------------------------------------------------
    def _build_match_embed(self, guild: discord.Guild, match_id: int, rows: list):
        """
        Build the embed + ping content for one match.

        `rows` is a list of (entry, match_row) for each watched player in it.
        """
        sample = rows[0][1]
        any_won = any(self._won(r) for _, r in rows)
        all_won = all(self._won(r) for _, r in rows)
        if all_won:
            color = discord.Color.green()
        elif not any_won:
            color = discord.Color.red()
        else:
            color = discord.Color.blurple()

        if len(rows) == 1:
            entry, r = rows[0]
            verb = "won" if self._won(r) else "lost"
            title = f"{entry.get('label')} {verb} as {self._hero_name(r.get('hero_id'))}"
        else:
            title = f"Deadlock match — {len(rows)} watched players"

        embed = discord.Embed(title=title, color=color)
        for entry, r in rows:
            verb = "🟩 Win" if self._won(r) else "🟥 Loss"
            value = (
                f"{r.get('player_kills', 0)}/{r.get('player_deaths', 0)}/"
                f"{r.get('player_assists', 0)} · "
                f"NW {self._fmt_networth(r.get('net_worth'))} · "
                f"{r.get('last_hits', 0)} LH/{r.get('denies', 0)} DN · "
                f"lvl {r.get('hero_level', 0)}"
            )
            embed.add_field(
                name=f"{entry.get('label')} — {verb} · {self._hero_name(r.get('hero_id'))}",
                value=value,
                inline=False,
            )

        embed.set_footer(
            text=(
                f"Deadlock · {self._mode_label(sample)} · "
                f"{self._fmt_duration(sample.get('match_duration_s'))} · match {match_id}"
            )
        )
        start = sample.get("start_time")
        if start:
            embed.timestamp = datetime.fromtimestamp(start, tz=timezone.utc)

        mentions = [
            f"<@{entry['discord_id']}>"
            for entry, _ in rows
            if entry.get("discord_id")
        ]
        content = " ".join(mentions) if mentions else None
        return embed, content

    async def _process_guild(
        self, guild: discord.Guild, channel: discord.abc.Messageable
    ) -> int:
        """Poll all watched players in a guild and post new matches. Returns count."""
        await self._ensure_assets()
        players = await self.config.guild(guild).players()
        if not players:
            return 0

        new_by_match: dict[int, list] = {}
        new_watermarks: dict[str, int] = {}

        for key, entry in players.items():
            account_id = entry["account_id"]
            history, err = await self._get_match_history(account_id)
            if err or not history:
                # Leave the watermark untouched so we retry next cycle.
                continue
            last = entry.get("last_match_id") or 0
            fresh = [r for r in history if r.get("match_id", 0) > last]
            if not fresh:
                continue
            new_watermarks[key] = max(r["match_id"] for r in fresh)
            for r in fresh:
                new_by_match.setdefault(r["match_id"], []).append((entry, r))

        posted = 0
        for match_id in sorted(new_by_match):
            rows = new_by_match[match_id]
            embed, content = self._build_match_embed(guild, match_id, rows)
            try:
                await channel.send(
                    content=content,
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions(
                        users=True, roles=False, everyone=False
                    ),
                )
                posted += 1
            except discord.HTTPException:
                log.exception("Failed to post match %s in guild %s", match_id, guild.id)

        if new_watermarks:
            async with self.config.guild(guild).players() as stored:
                for key, match_id in new_watermarks.items():
                    if key in stored:
                        stored[key]["last_match_id"] = match_id
        return posted

    @tasks.loop(minutes=5)
    async def feed_task(self):
        all_guilds = await self.config.all_guilds()
        for guild_id, conf in all_guilds.items():
            if not conf.get("enabled") or not conf.get("channel_id"):
                continue
            if not conf.get("players"):
                continue
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            channel = guild.get_channel(conf["channel_id"])
            if channel is None:
                log.warning(
                    "Feed channel %s missing in guild %s", conf["channel_id"], guild_id
                )
                continue
            try:
                await self._process_guild(guild, channel)
            except Exception:  # noqa: BLE001 - never let one guild kill the loop
                log.exception("Error processing feed for guild %s", guild_id)

    @feed_task.before_loop
    async def before_feed_task(self):
        await self.bot.wait_until_ready()
        log.info("DeadlockTracker feed task waiting for bot to be ready…")
