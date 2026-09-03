import asyncio
import logging
import re
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

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
            "rankup_channel_id": None,
            "enabled": False,
            "ping_matches": True,
            # account_id (str) -> {account_id, label, discord_id, last_match_id,
            #                      last_badge}
            "players": {},
        }
        self.config.register_guild(**default_guild)

        self.session = aiohttp.ClientSession(
            headers={"User-Agent": "durk-cogs/deadlocktracker"},
            timeout=aiohttp.ClientTimeout(total=30),
        )

        # deadlock-api Patreon key (X-API-KEY); unlocks the official player card
        # and thus real in-game ranks. Loaded from Red's shared API tokens.
        self._api_key: Optional[str] = None

        # Asset caches (id -> name / tier -> info), refreshed lazily.
        self._heroes: dict[int, str] = {}
        self._ranks: dict[int, dict] = {}
        self._assets_loaded_at: float = 0.0
        self._assets_lock = asyncio.Lock()

        self.feed_task.start()

    async def cog_load(self):
        tokens = await self.bot.get_shared_api_tokens("deadlock")
        self._api_key = tokens.get("api_key")

    @commands.Cog.listener()
    async def on_red_api_tokens_update(self, service_name: str, api_tokens: dict):
        if service_name == "deadlock":
            self._api_key = api_tokens.get("api_key")

    def cog_unload(self):
        self.feed_task.cancel()
        asyncio.create_task(self.session.close())

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _auth_headers(self) -> Optional[dict]:
        return {"X-API-KEY": self._api_key} if self._api_key else None

    async def _api_get(self, path: str, params: Optional[dict] = None):
        """GET from the deadlock API. Returns (data, None) or (None, error_str)."""
        url = f"{API_BASE}{path}"
        try:
            async with self.session.get(
                url, params=params, headers=self._auth_headers()
            ) as resp:
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

    async def _get_card(self, account_id: int) -> dict:
        """
        Fetch the official player card (Patreon-only, requires the account to
        have friended a deadlock-api bot).

        Returns a dict: ``badge`` (combined ranked badge, e.g. 102) and
        ``division``/``subrank`` when available; ``needs_friend`` with
        ``invite_links`` when the account must add a bot first; ``no_key`` when
        no API key is configured; otherwise ``error``.
        """
        result = {
            "badge": None,
            "division": None,
            "subrank": None,
            "needs_friend": False,
            "invite_links": [],
            "no_key": False,
            "error": None,
        }
        if not self._api_key:
            result["no_key"] = True
            return result

        url = f"{API_BASE}/v1/players/{account_id}/card"
        try:
            async with self.session.get(url, headers=self._auth_headers()) as resp:
                status = resp.status
                data = await resp.json(content_type=None)
        except Exception as e:  # noqa: BLE001
            result["error"] = str(e)
            return result

        if status == 200 and isinstance(data, dict):
            div = data.get("ranked_rank")
            sub = data.get("ranked_subrank")
            badge = data.get("ranked_badge_level")
            if badge is None and div is not None:
                badge = div * 10 + (sub or 0)
            if badge is not None and div is None:
                div, sub = divmod(int(badge), 10)
            result["badge"] = badge
            result["division"] = div
            result["subrank"] = sub
            return result

        # Error response: detect "friend the bot" (carries Steam invite links).
        msg = ""
        links = []
        if isinstance(data, dict):
            msg = str(data.get("error") or data.get("message") or "")
            links = self._find_invite_links(data)
        low = msg.lower()
        if status == 403 or "patreon" in low:
            result["no_key"] = True
        elif links or "friend" in low or "invite" in low:
            result["needs_friend"] = True
            result["invite_links"] = links
        else:
            result["error"] = msg or f"status {status}"
        return result

    @staticmethod
    def _find_invite_links(obj) -> list:
        """Recursively collect Steam friend/invite URLs from an error payload."""
        found = []

        def walk(o):
            if isinstance(o, str):
                if "steamcommunity.com" in o or o.startswith("steam://") or "s.team" in o:
                    found.append(o)
            elif isinstance(o, dict):
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)

        walk(obj)
        # De-duplicate, preserve order.
        return list(dict.fromkeys(found))

    async def _resolve_rank(self, account_id: int, mmr: Optional[dict] = None) -> dict:
        """
        Resolve a player's rank, preferring the official card badge and falling
        back to the MMR estimate.

        Returns: ``label`` (e.g. "Ascendant II"), ``badge_image`` URL,
        ``sort_value`` (badge number for ranking), ``official`` /``estimated``
        booleans, ``needs_friend``, and ``badge`` (official combined badge or
        None).
        """
        needs_friend = False
        if self._api_key:
            card = await self._get_card(account_id)
            if card["division"] is not None:
                label, image = self._rank_display(card["division"], card["subrank"])
                badge = card["badge"]
                return {
                    "label": label,
                    "badge_image": image,
                    "sort_value": badge or 0,
                    "official": True,
                    "estimated": False,
                    "needs_friend": False,
                    "badge": badge,
                }
            needs_friend = card["needs_friend"]

        # Fall back to the MMR estimate.
        if mmr is None:
            mmr, _ = await self._get_mmr(account_id)
        if mmr:
            div, sub = mmr.get("division"), mmr.get("division_tier")
            label, image = self._rank_display(div, sub)
            sort_value = mmr.get("rank") or 0
        else:
            label, image, sort_value = "Unranked", None, 0
        return {
            "label": label,
            "badge_image": image,
            "sort_value": sort_value,
            "official": False,
            "estimated": True,
            "needs_friend": needs_friend,
            "badge": None,
        }

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
    def _is_brawl(row: dict) -> bool:
        """Street Brawl (game_mode 4) is excluded from aggregated player stats."""
        return row.get("game_mode") == 4

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
            f"• `{r['account_id']}` - {r.get('personaname', 'Unknown')}"
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

            # Street Brawl is a casual mode and is excluded from stats.
            history = [r for r in history if not self._is_brawl(r)]
            if not history:
                return await ctx.send(
                    "That player only has Street Brawl matches, which are "
                    "excluded from stats."
                )

            rank_info = await self._resolve_rank(account_id)
            profile = await self._get_steam_profile(account_id)
            embed = self._build_stats_embed(account_id, history, rank_info, profile)
            await ctx.send(embed=embed)

    @deadlock.command(name="leaderboard", aliases=["lb", "top"])
    async def deadlock_leaderboard(
        self, ctx: commands.Context, sort: str = "rank"
    ):
        """
        Rank this server's watched players against each other.

        `sort` may be `rank` (default), `winrate`, or `kda`. `winrate` and `kda`
        are computed over each player's recent matches and take a little longer.
        """
        sort = sort.lower()
        if sort not in ("rank", "winrate", "kda"):
            return await ctx.send("Sort must be one of: `rank`, `winrate`, `kda`.")

        players = await self.config.guild(ctx.guild).players()
        if not players:
            return await ctx.send("No players are being watched yet.")

        async with ctx.typing():
            await self._ensure_assets()

            rows = []  # (entry, sort_key, display)
            for entry in players.values():
                acc = entry["account_id"]
                if sort == "rank":
                    ri = await self._resolve_rank(acc)
                    label = ri["label"] + (" *(est.)*" if ri["estimated"] else "")
                    rows.append((entry, ri["sort_value"], label))
                else:
                    history, err = await self._get_match_history(acc)
                    # Exclude Street Brawl from win-rate / KDA aggregation.
                    ranked = [r for r in (history or []) if not self._is_brawl(r)]
                    window = ranked[:STATS_WINDOW]
                    if err or not window:
                        rows.append((entry, -1, "no recent matches"))
                        continue
                    wins = sum(1 for r in window if self._won(r))
                    if sort == "winrate":
                        wr = wins / len(window) * 100
                        rows.append(
                            (entry, wr, f"{wr:.0f}% WR ({wins}W·{len(window) - wins}L)")
                        )
                    else:  # kda
                        k = sum(r.get("player_kills", 0) for r in window)
                        d = sum(r.get("player_deaths", 0) for r in window)
                        a = sum(r.get("player_assists", 0) for r in window)
                        kda = self._kda_ratio(k, d, a)
                        rows.append((entry, kda, f"{kda:.2f} KDA"))

        rows.sort(key=lambda x: x[1], reverse=True)
        medals = {0: "🥇", 1: "🥈", 2: "🥉"}
        lines = []
        for i, (entry, _key, display) in enumerate(rows):
            prefix = medals.get(i, f"**{i + 1}.**")
            lines.append(f"{prefix} **{entry.get('label')}** - {display}")

        titles = {
            "rank": "by Rank",
            "winrate": f"by Win Rate (last {STATS_WINDOW})",
            "kda": f"by KDA (last {STATS_WINDOW})",
        }
        embed = discord.Embed(
            title=f"🏆 Deadlock Leaderboard - {titles[sort]}",
            description="\n".join(lines),
            color=discord.Color.from_rgb(199, 124, 60),
        )
        await ctx.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())

    @deadlock.command(name="friendbot")
    @commands.guild_only()
    async def deadlock_friendbot(
        self, ctx: commands.Context, *, player: Optional[str] = None
    ):
        """
        Get the Steam link to friend the deadlock-api bot for official ranks.

        Friending the bot unlocks a player's exact in-game rank (otherwise ranks
        are estimates). `player` defaults to your own linked account.
        """
        if not self._api_key:
            return await ctx.send(
                "No deadlock-api API key is configured, so official ranks aren't "
                "available. The bot owner can set one with "
                f"`{ctx.clean_prefix}set api deadlock api_key <key>` "
                "(requires a deadlock-api Patreon subscription)."
            )

        if player is None:
            account_id = await self._account_for_member(ctx.guild, ctx.author.id)
            if account_id is None:
                return await ctx.send(
                    "Tell me which player - a name, id, profile URL, or @mention "
                    "(if linked)."
                )
        else:
            account_id, err = await self._resolve_query(ctx, player)
            if err:
                return await ctx.send(err)

        async with ctx.typing():
            card = await self._get_card(account_id)

        if card.get("badge"):
            label, _ = self._rank_display(*divmod(int(card["badge"]), 10))
            return await ctx.send(
                f"✅ Account `{account_id}` is already friended - official rank "
                f"is readable (**{label}**)."
            )
        if card.get("no_key"):
            return await ctx.send(
                "The configured API key was rejected (it needs an active "
                "deadlock-api Patreon subscription)."
            )
        if card.get("needs_friend"):
            links = card.get("invite_links") or []
            if links:
                joined = "\n".join(links)
                return await ctx.send(
                    f"To unlock the **official** rank for `{account_id}`, add one of "
                    f"these deadlock-api bots on Steam - it then updates "
                    f"automatically:\n{joined}"
                )
            return await ctx.send(
                f"`{account_id}` needs to friend a deadlock-api bot to unlock "
                "official rank, but the API returned no invite links. See "
                "https://deadlock-api.com for the current bot accounts."
            )
        return await ctx.send(
            f"Couldn't read the player card: {card.get('error') or 'unknown error'}."
        )

    def _build_stats_embed(
        self,
        account_id: int,
        history: list,
        rank_info: dict,
        profile: Optional[dict],
    ) -> discord.Embed:
        window = history[:STATS_WINDOW]
        n = len(window)
        wins = sum(1 for r in window if self._won(r))
        losses = n - wins
        winrate = (wins / n * 100) if n else 0
        tot_k = sum(r.get("player_kills", 0) for r in window)
        tot_d = sum(r.get("player_deaths", 0) for r in window)
        tot_a = sum(r.get("player_assists", 0) for r in window)
        avg = (lambda key: (sum(r.get(key, 0) for r in window) / n) if n else 0)
        avg_nw = avg("net_worth")
        avg_dur = avg("match_duration_s")
        avg_k, avg_d, avg_a = avg("player_kills"), avg("player_deaths"), avg("player_assists")
        avg_lh, avg_dn = avg("last_hits"), avg("denies")

        name = (profile or {}).get("personaname") or f"Account {account_id}"
        embed = discord.Embed(color=discord.Color.from_rgb(199, 124, 60))
        embed.set_author(
            name=name,
            url=(profile or {}).get("profileurl"),
            icon_url=(profile or {}).get("avatarfull"),
        )

        rank_label = rank_info.get("label", "Unranked")
        if rank_info.get("estimated"):
            rank_label += " *(est.)*"
        if rank_info.get("badge_image"):
            embed.set_thumbnail(url=rank_info["badge_image"])

        # Current win/loss streak from the most recent games.
        streak_n, streak_win = self._current_streak(history)
        streak_value = (
            f"{'🟩' if streak_win else '🟥'} {'W' if streak_win else 'L'}{streak_n}"
            if streak_n
            else "-"
        )

        # Row 1: identity / record / momentum
        embed.add_field(name="🏅 Rank", value=rank_label, inline=True)
        embed.add_field(
            name="📊 Win rate",
            value=f"**{winrate:.0f}%**\n{wins}W · {losses}L",
            inline=True,
        )
        embed.add_field(name="🔥 Streak", value=streak_value, inline=True)
        # Row 2: combat
        embed.add_field(
            name="⚔️ KDA",
            value=f"**{self._kda_ratio(tot_k, tot_d, tot_a):.2f}**\n"
            f"{avg_k:.1f} / {avg_d:.1f} / {avg_a:.1f}",
            inline=True,
        )
        embed.add_field(
            name="💰 Avg net worth",
            value=self._fmt_networth(round(avg_nw)),
            inline=True,
        )
        embed.add_field(
            name="⏱️ Avg length", value=self._fmt_duration(avg_dur), inline=True
        )
        # Row 3: farm
        embed.add_field(name="🌾 Avg last hits", value=f"{avg_lh:.0f}", inline=True)
        embed.add_field(name="🛡️ Avg denies", value=f"{avg_dn:.0f}", inline=True)
        embed.add_field(name="🎮 Matches", value=f"{n} of {len(history)}", inline=True)

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
                    f"**{self._hero_name(hero_id)}** · {count} games · "
                    f"{hw / count * 100:.0f}% WR · {self._kda_ratio(k, d, a):.1f} KDA"
                )
            embed.add_field(
                name=f"⭐ Top heroes (last {n})", value="\n".join(lines), inline=False
            )

        # Most recent matches.
        recent_lines = []
        for r in history[:RECENT_COUNT]:
            result = "🟩" if self._won(r) else "🟥"
            start = r.get("start_time")
            when = f" · <t:{start}:R>" if start else ""
            recent_lines.append(
                f"{result} **{self._hero_name(r.get('hero_id'))}** "
                f"{r.get('player_kills', 0)}/{r.get('player_deaths', 0)}/"
                f"{r.get('player_assists', 0)} · "
                f"{self._fmt_networth(r.get('net_worth'))}{when}"
            )
        if recent_lines:
            embed.add_field(
                name="🕑 Recent matches", value="\n".join(recent_lines), inline=False
            )

        footer = f"Deadlock · account {account_id}"
        if rank_info.get("estimated"):
            footer += " · rank estimated - friend the deadlock-api bot for exact rank"
        embed.set_footer(text=footer)
        return embed

    @staticmethod
    def _current_streak(history: list):
        """Return (length, won) of the most-recent unbroken win/loss streak."""
        if not history:
            return 0, False
        first = DeadlockTracker._won(history[0])
        count = 0
        for r in history:
            if DeadlockTracker._won(r) == first:
                count += 1
            else:
                break
        return count, first

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

    @deadlockset.command(name="rankupchannel")
    async def deadlockset_rankupchannel(
        self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None
    ):
        """
        Set the channel for rank-up / derank announcements.

        Omit the channel to clear it; rank-ups then fall back to the match feed
        channel.
        """
        if channel is None:
            await self.config.guild(ctx.guild).rankup_channel_id.set(None)
            return await ctx.send(
                "Rank-up channel cleared. Announcements will use the match feed channel."
            )
        await self.config.guild(ctx.guild).rankup_channel_id.set(channel.id)
        await ctx.send(f"Rank-up announcements will be posted to {channel.mention}.")

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

    @deadlockset.command(name="pingmatches")
    async def deadlockset_pingmatches(
        self, ctx: commands.Context, on_off: Optional[bool] = None
    ):
        """
        Toggle whether match-result posts ping linked members.

        Rank-up announcements always ping (deranks never do); this only affects
        the match feed.
        """
        current = await self.config.guild(ctx.guild).ping_matches()
        if on_off is None:
            return await ctx.send(
                f"Match posts currently {'ping' if current else 'do not ping'} linked members."
            )
        await self.config.guild(ctx.guild).ping_matches.set(on_off)
        await ctx.send(
            f"Match posts will {'now ping' if on_off else 'no longer ping'} linked members."
        )

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

            # Seed the official badge (if available) so the first rank check
            # doesn't fire a spurious announcement. Stays None until the player
            # friends a bot; rank-ups are only tracked from official badges.
            card = await self._get_card(account_id)
            last_badge = card.get("badge")

            players[str(account_id)] = {
                "account_id": account_id,
                "label": label,
                "discord_id": member.id if member else None,
                "last_match_id": last_match_id,
                "last_badge": last_badge,
            }
            await self.config.guild(ctx.guild).players.set(players)

        link = f" linked to {member.mention}" if member else ""
        note = ""
        if not self._api_key:
            note = (
                " Rank shown will be an estimate until a deadlock-api API key is set."
            )
        elif card.get("needs_friend"):
            note = (
                f" Rank will be estimated until **{label}** friends the bot - "
                f"run `{ctx.clean_prefix}deadlock friendbot {account_id}` for the link."
            )
        await ctx.send(
            f"Now watching **{label}** (`{account_id}`){link}. "
            f"New matches will be posted to the feed channel.{note}",
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
        rankup_id = conf.get("rankup_channel_id")
        rankup = f"<#{rankup_id}>" if rankup_id else "Match feed channel"
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
        embed.add_field(name="Rank-up channel", value=rankup, inline=True)
        embed.add_field(
            name="Match pings",
            value="On" if conf.get("ping_matches", True) else "Off",
            inline=True,
        )
        embed.add_field(
            name="Official ranks",
            value="API key set" if self._api_key else "Estimate only (no key)",
            inline=True,
        )
        embed.add_field(
            name="Players watched", value=str(len(conf.get("players", {}))), inline=True
        )
        await ctx.send(embed=embed)

    @deadlock.command(name="testfeed")
    @commands.is_owner()
    async def deadlock_testfeed(self, ctx: commands.Context, count: int = 1):
        """
        Preview the feed by posting each watched player's last `count` matches.

        Unlike the live feed this ignores the per-player watermark (so it always
        has something to show) and does not advance it, so the real feed is
        unaffected. `count` is clamped to 1–10. Bot owner only.
        """
        count = max(1, min(count, 10))
        conf = await self.config.guild(ctx.guild).all()
        if not conf.get("channel_id"):
            return await ctx.send("No feed channel is set.")
        if not conf.get("players"):
            return await ctx.send("No players are being watched.")
        channel = ctx.guild.get_channel(conf["channel_id"])
        if channel is None:
            return await ctx.send("The configured feed channel no longer exists.")
        await ctx.send(
            f"Previewing the last {count} match(es) per watched player…"
        )
        posted = await self._process_guild(
            ctx.guild,
            channel,
            force_recent=count,
            update_watermark=False,
            ping=False,
        )
        await ctx.send(f"Done. Posted {posted} match result(s).")

    # ------------------------------------------------------------------
    # Feed loop
    # ------------------------------------------------------------------
    def _build_match_embed(self, match_id: int, rows: list):
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
            title = f"Deadlock match - {len(rows)} watched players"

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
                name=f"{entry.get('label')} - {verb} · {self._hero_name(r.get('hero_id'))}",
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
        self,
        guild: discord.Guild,
        channel: discord.abc.Messageable,
        force_recent: Optional[int] = None,
        update_watermark: bool = True,
        ping: bool = True,
    ) -> int:
        """
        Poll all watched players in a guild and post matches. Returns the count.

        Normally only matches newer than each player's watermark are posted. If
        `force_recent` is set, the last N matches per player are posted instead
        (a preview), and `update_watermark=False` leaves the live feed untouched.
        `ping=False` suppresses pinging linked members.
        """
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
            if force_recent is not None:
                fresh = history[:force_recent]
            else:
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
            embed, content = self._build_match_embed(match_id, rows)
            if not ping:
                content = None
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

        if update_watermark and new_watermarks:
            async with self.config.guild(guild).players() as stored:
                for key, match_id in new_watermarks.items():
                    if key in stored:
                        stored[key]["last_match_id"] = match_id
        return posted

    def _build_rank_embed(self, entry: dict, old_badge: int, new_badge: int, is_up: bool):
        """Build the embed + ping content for an official rank change."""
        old_label, _ = self._rank_display(*divmod(int(old_badge), 10))
        new_div, new_sub = divmod(int(new_badge), 10)
        new_label, new_image = self._rank_display(new_div, new_sub)
        if is_up:
            title = f"📈 {entry.get('label')} ranked up!"
            color = discord.Color.green()
        else:
            title = f"📉 {entry.get('label')} deranked"
            color = discord.Color.dark_red()

        embed = discord.Embed(
            title=title,
            description=f"{old_label} → **{new_label}**",
            color=color,
        )
        if new_image:
            embed.set_thumbnail(url=new_image)
        embed.set_footer(text=f"Deadlock · account {entry['account_id']}")

        # Rank-ups always ping the linked member; deranks never ping.
        discord_id = entry.get("discord_id")
        content = f"<@{discord_id}>" if (is_up and discord_id) else None
        return embed, content

    async def _check_ranks(self, guild: discord.Guild) -> int:
        """
        Detect and announce official rank changes for watched players.

        Only works with a configured API key, and only for players who have
        friended a deadlock-api bot (so we can read their real badge). Players
        whose official badge can't be read are skipped - no estimate-based
        announcements. Returns the number of changes posted.
        """
        if not self._api_key:
            return 0
        conf = await self.config.guild(guild).all()
        players = conf.get("players", {})
        if not players:
            return 0
        rankup_id = conf.get("rankup_channel_id") or conf.get("channel_id")
        channel = guild.get_channel(rankup_id) if rankup_id else None
        if channel is None:
            return 0
        await self._ensure_assets()

        announcements = []  # (entry, old_badge, new_badge, is_up)
        updates: dict[str, int] = {}
        for key, entry in players.items():
            card = await self._get_card(entry["account_id"])
            new_badge = card.get("badge")
            if not new_badge:
                # Not friended / no card data -> don't track or announce.
                continue
            old_badge = entry.get("last_badge")
            if old_badge is None:
                # Seed silently the first time we can read an official badge.
                updates[key] = new_badge
                continue
            if new_badge != old_badge:
                updates[key] = new_badge
                announcements.append((entry, old_badge, new_badge, new_badge > old_badge))

        posted = 0
        for entry, old_badge, new_badge, is_up in announcements:
            embed, content = self._build_rank_embed(entry, old_badge, new_badge, is_up)
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
                log.exception("Failed to post rank change in guild %s", guild.id)

        if updates:
            async with self.config.guild(guild).players() as stored:
                for key, badge in updates.items():
                    if key in stored:
                        stored[key]["last_badge"] = badge
        return posted

    @tasks.loop(minutes=5)
    async def feed_task(self):
        all_guilds = await self.config.all_guilds()
        for guild_id, conf in all_guilds.items():
            if not conf.get("enabled") or not conf.get("players"):
                continue
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue

            # Match feed (needs the match channel).
            match_channel = None
            if conf.get("channel_id"):
                match_channel = guild.get_channel(conf["channel_id"])
                if match_channel is None:
                    log.warning(
                        "Feed channel %s missing in guild %s",
                        conf["channel_id"],
                        guild_id,
                    )
            if match_channel is not None:
                try:
                    await self._process_guild(
                        guild, match_channel, ping=conf.get("ping_matches", True)
                    )
                except Exception:  # noqa: BLE001 - never let one guild kill the loop
                    log.exception("Error processing match feed for guild %s", guild_id)

            # Rank-up announcements (resolve their own channel).
            try:
                await self._check_ranks(guild)
            except Exception:  # noqa: BLE001
                log.exception("Error processing rank changes for guild %s", guild_id)

    @feed_task.before_loop
    async def before_feed_task(self):
        await self.bot.wait_until_ready()
        log.info("DeadlockTracker feed task waiting for bot to be ready…")
