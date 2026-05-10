from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp
import discord
from bs4 import BeautifulSoup
from discord.ext import tasks
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.durk-cogs.appealfeeder")


POLL_OPTIONS: List[str] = ["Accept", "Reduce", "Deny"]
POLL_DURATION_SECONDS: int = 3 * 86400
REMINDER_AFTER_SECONDS: int = 2 * 86400
DEFAULT_VOTE_THRESHOLD: int = 4
LOOP_MINUTES: int = 5
HTTP_TIMEOUT_SECONDS: int = 20

DEFAULT_TAG_OPEN = "Open"
DEFAULT_TAG_ACCEPTED = "Accepted"
DEFAULT_TAG_REDUCED = "Reduced"
DEFAULT_TAG_DENIED = "Denied"

DEFAULT_INITIAL_TEMPLATE = (
    "Your appeal is being reviewed by our staff. "
    "You can view its current status at {discord_link}, "
    "and a response will be left here when a decision is made."
)
DEFAULT_TEMPLATE_ACCEPTED = (
    "Your appeal has been **accepted** by our review team.\n\n"
    "{votes_breakdown}\n\n"
    "Discussion: {discord_link}"
)
DEFAULT_TEMPLATE_REDUCED = (
    "After review, your ban will be **reduced**. A staff member will "
    "follow up shortly with the new terms.\n\n"
    "{votes_breakdown}\n\n"
    "Discussion: {discord_link}"
)
DEFAULT_TEMPLATE_DENIED = (
    "Your appeal has been **denied** by our review team.\n\n"
    "{votes_breakdown}\n\n"
    "Discussion: {discord_link}"
)
DEFAULT_TEMPLATE_TIE = (
    "The review concluded without a clear decision; a staff member will "
    "follow up directly.\n\n"
    "{votes_breakdown}\n\n"
    "Discussion: {discord_link}"
)

DEFAULT_EXTRACT_FIELDS: Dict[str, str] = {
    "In-game username": r"(?im)^\s*Username\s*:\s*(.+?)\s*$",
    "Ban reason": r"(?ims)^\s*Ban reason\s*:\s*(.+?)(?:\n\s*\n|\Z)",
}

EMBED_FIELD_VALUE_LIMIT = 1024


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def html_to_text(cooked_html: str) -> str:
    """Convert Discourse 'cooked' HTML to plain text, preserving paragraph breaks."""
    soup = BeautifulSoup(cooked_html or "", "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for block in soup.find_all(["p", "div", "li"]):
        block.append("\n")
    text = soup.get_text()
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_fields(text: str, patterns: Dict[str, str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for label, pattern in patterns.items():
        try:
            m = re.search(pattern, text)
        except re.error:
            log.warning("Invalid extract pattern for %s: %s", label, pattern)
            continue
        if m and m.lastindex:
            value = m.group(1).strip()
            if value:
                out[label] = value
    return out


def discord_thread_link(guild_id: int, thread_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{thread_id}"


def parse_category_input(raw: str) -> Tuple[Optional[int], Optional[str]]:
    """Parse user-provided category text into (category_id, slug_path).

    Accepts any of:
      - Numeric ID:                            '6'
      - Full URL with trailing ID:             'https://.../c/ban-appeals/game-server-appeals/6'
      - Path with ID:                          'c/ban-appeals/game-server-appeals/6'
      - Slug path only (needs auto-resolve):   'ban-appeals/game-server-appeals'

    Strips trailing `.json`, `/latest`, `/l/<filter>` decoration.
    """
    s = (raw or "").strip()
    if not s:
        return None, None
    if "://" in s:
        s = urlparse(s).path
    s = s.strip("/")
    if s.startswith("c/"):
        s = s[2:]
    s = re.sub(r"\.json$", "", s)
    s = re.sub(r"/l/(?:latest|new|top|hot|unread|categories)$", "", s)
    s = re.sub(r"/(?:latest|new|top|hot)$", "", s)
    parts = [p for p in s.split("/") if p]
    if not parts:
        return None, None
    if parts[-1].isdigit():
        return int(parts[-1]), "/".join(parts[:-1]) or None
    return None, "/".join(parts)


def votes_breakdown(options: List[str], counts: List[int]) -> str:
    total = sum(counts)
    parts: List[str] = []
    for opt, c in zip(options, counts):
        pct = (c / total * 100) if total else 0.0
        parts.append(f"- {opt}: {c} vote(s) ({pct:.0f}%)")
    return "\n".join(parts)


def winning_outcome(options: List[str], counts: List[int]) -> Optional[str]:
    """Return the option string with the most votes, or None on tie / no votes."""
    if not counts or sum(counts) == 0:
        return None
    top = max(counts)
    winners = [opt for opt, c in zip(options, counts) if c == top]
    if len(winners) != 1:
        return None
    return winners[0]


# ---------- Discourse client ----------


class DiscourseError(Exception):
    pass


class DiscourseClient:
    """Lightweight async Discourse JSON client."""

    def __init__(self, base_url: str, *, api_key: Optional[str] = None,
                 api_username: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.api_username = api_username

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_username)

    def _auth_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": "AppealFeeder/1.0"}
        if self.has_credentials:
            headers["Api-Key"] = self.api_key  # type: ignore[assignment]
            headers["Api-Username"] = self.api_username  # type: ignore[assignment]
        return headers

    async def _request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = path if path.startswith("http") else urljoin(self.base_url + "/", path.lstrip("/"))
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS)
        async with session.request(
            method, url, params=params, json=json_body,
            headers=self._auth_headers(), timeout=timeout,
        ) as resp:
            if resp.status >= 400:
                body_preview = (await resp.text())[:300]
                raise DiscourseError(
                    f"{method} {url} -> HTTP {resp.status}: {body_preview}"
                )
            try:
                return await resp.json()
            except aiohttp.ContentTypeError as e:
                raise DiscourseError(f"Non-JSON response from {url}: {e}") from e

    async def list_category_topics(
        self, session: aiohttp.ClientSession, category_id: int,
    ) -> List[Dict[str, Any]]:
        """Hit `/c/<id>.json` and return its topic list (defaults to the
        'latest' filter on the Discourse side)."""
        data = await self._request(session, "GET", f"c/{int(category_id)}.json")
        topics = (data.get("topic_list") or {}).get("topics") or []
        return list(topics)

    async def fetch_categories(
        self, session: aiohttp.ClientSession,
    ) -> List[Dict[str, Any]]:
        """Return a flat list of all categories (parents + sub-categories).

        Tries `/site.json` first (which exposes a flat `categories` array),
        and falls back to `/categories.json` (which may nest sub-categories).
        """
        try:
            data = await self._request(session, "GET", "site.json")
            cats = data.get("categories")
            if isinstance(cats, list) and cats:
                return list(cats)
        except DiscourseError as e:
            log.debug("site.json fetch failed (%s); falling back to categories.json", e)

        data = await self._request(session, "GET", "categories.json")
        raw = ((data.get("category_list") or {}).get("categories")) or []
        flat: List[Dict[str, Any]] = []

        def _visit(c: Dict[str, Any]) -> None:
            flat.append(c)
            for sc in (c.get("subcategory_list") or []):
                _visit(sc)

        for c in raw:
            _visit(c)
        return flat

    async def resolve_category(
        self, session: aiohttp.ClientSession, slug_path: str,
    ) -> Optional[Dict[str, Any]]:
        """Resolve a slug path like 'ban-appeals/game-server-appeals' to its
        category record. Returns None if not found or ambiguous."""
        parts = [p.lower() for p in slug_path.strip("/").split("/") if p]
        if not parts:
            return None
        cats = await self.fetch_categories(session)
        if not cats:
            return None
        by_id: Dict[int, Dict[str, Any]] = {
            int(c["id"]): c for c in cats if "id" in c
        }
        last = parts[-1]
        candidates = [c for c in cats if (c.get("slug") or "").lower() == last]
        if len(parts) == 1:
            return candidates[0] if len(candidates) == 1 else None
        for cand in candidates:
            chain: List[str] = []
            cur: Optional[Dict[str, Any]] = cand
            depth = 0
            while cur is not None and depth < 8:
                chain.append((cur.get("slug") or "").lower())
                pid = cur.get("parent_category_id")
                cur = by_id.get(int(pid)) if pid else None
                depth += 1
            chain.reverse()
            if len(chain) >= len(parts) and chain[-len(parts):] == parts:
                return cand
        return None

    async def get_topic(
        self, session: aiohttp.ClientSession, topic_id: int
    ) -> Dict[str, Any]:
        return await self._request(session, "GET", f"t/{int(topic_id)}.json")

    async def post_reply(
        self, session: aiohttp.ClientSession, topic_id: int, raw: str
    ) -> Dict[str, Any]:
        if not self.has_credentials:
            raise DiscourseError("Discourse API key not configured.")
        return await self._request(
            session, "POST", "posts.json",
            json_body={"topic_id": int(topic_id), "raw": raw},
        )


# ---------- Cog ----------


class AppealFeeder(commands.Cog):
    """Mirror Discourse ban appeals into a Discord forum and run role-restricted votes."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=0xA99EA1F33DE2, force_registration=True,
        )
        self.config.register_guild(
            enabled=False,
            discourse_base_url=None,
            discourse_category_id=None,
            discourse_category_label=None,
            discourse_api_key=None,
            discourse_api_username=None,
            forum_channel_id=None,
            vote_role_id=None,
            vote_threshold=DEFAULT_VOTE_THRESHOLD,
            tag_open=DEFAULT_TAG_OPEN,
            tag_accepted=DEFAULT_TAG_ACCEPTED,
            tag_reduced=DEFAULT_TAG_REDUCED,
            tag_denied=DEFAULT_TAG_DENIED,
            initial_comment_template=DEFAULT_INITIAL_TEMPLATE,
            template_accepted=DEFAULT_TEMPLATE_ACCEPTED,
            template_reduced=DEFAULT_TEMPLATE_REDUCED,
            template_denied=DEFAULT_TEMPLATE_DENIED,
            template_tie=DEFAULT_TEMPLATE_TIE,
            extract_fields=DEFAULT_EXTRACT_FIELDS,
            last_topic_id=None,
            tracked_appeals={},
            archived_appeals={},
        )

        self._reminder_tasks: Dict[str, asyncio.Task] = {}
        self._loop_task: Optional[tasks.Loop] = None
        self._session: Optional[aiohttp.ClientSession] = None

    # ----- lifecycle -----

    async def cog_load(self) -> None:
        self._session = aiohttp.ClientSession()
        # Reschedule reminders for any open tracked appeals that haven't been
        # reminded yet.
        all_guilds = await self.config.all_guilds()
        for guild_id, settings in all_guilds.items():
            for topic_id, info in (settings.get("tracked_appeals") or {}).items():
                if info.get("reminded"):
                    continue
                created_ts = info.get("created_ts")
                if not created_ts:
                    continue
                remind_at = datetime.fromtimestamp(int(created_ts), tz=timezone.utc) \
                    + timedelta(seconds=REMINDER_AFTER_SECONDS)
                self._schedule_reminder(int(guild_id), str(topic_id), remind_at)
        self.appeal_loop.start()
        log.info("AppealFeeder loaded; %d reminder(s) scheduled.", len(self._reminder_tasks))

    async def cog_unload(self) -> None:
        try:
            self.appeal_loop.cancel()
        except Exception:
            pass
        for task in list(self._reminder_tasks.values()):
            task.cancel()
        self._reminder_tasks.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ----- main loop -----

    @tasks.loop(minutes=LOOP_MINUTES)
    async def appeal_loop(self) -> None:
        all_guilds = await self.config.all_guilds()
        for guild_id, settings in all_guilds.items():
            if not settings.get("enabled"):
                continue
            try:
                await self._poll_guild(guild_id, settings)
            except Exception:
                log.exception("AppealFeeder poll failed for guild %s", guild_id)

    @appeal_loop.before_loop
    async def _before_loop(self) -> None:
        await self.bot.wait_until_ready()

    async def _poll_guild(self, guild_id: int, settings: Dict[str, Any]) -> None:
        base = settings.get("discourse_base_url")
        category_id = settings.get("discourse_category_id")
        forum_id = settings.get("forum_channel_id")
        role_id = settings.get("vote_role_id")
        if not (base and category_id and forum_id and role_id):
            return

        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return
        forum = guild.get_channel(forum_id)
        if not isinstance(forum, discord.ForumChannel):
            log.warning("Configured forum channel %s in guild %s is not a forum.",
                        forum_id, guild_id)
            return
        role = guild.get_role(role_id)
        if role is None:
            log.warning("Configured vote role %s missing in guild %s.", role_id, guild_id)
            return

        client = DiscourseClient(
            base,
            api_key=settings.get("discourse_api_key"),
            api_username=settings.get("discourse_api_username"),
        )
        if self._session is None:
            return
        try:
            topics = await client.list_category_topics(self._session, int(category_id))
        except DiscourseError as e:
            log.warning("Discourse listing failed for guild %s: %s", guild_id, e)
            return

        last_topic_id = settings.get("last_topic_id")
        if last_topic_id is None:
            # First run for this guild: initialize the cursor to the newest
            # topic we can see and process nothing. To backfill explicitly,
            # an admin can run `appealset resetcursor 0`.
            if topics:
                max_id = max(int(t.get("id", 0)) for t in topics)
                await self.config.guild_from_id(guild_id).last_topic_id.set(max_id)
                log.info(
                    "AppealFeeder first run for guild %s: cursor initialized to %s, "
                    "no backfill.", guild_id, max_id,
                )
            return

        last_topic_id = int(last_topic_id)
        # Filter out topics we've already seen, sort oldest -> newest so we
        # process them in chronological order.
        new_topics = sorted(
            (t for t in topics if int(t.get("id", 0)) > last_topic_id),
            key=lambda t: int(t.get("id", 0)),
        )

        if not new_topics:
            return

        new_last_id = last_topic_id
        for topic in new_topics:
            topic_id = int(topic["id"])
            try:
                await self._process_new_topic(
                    guild=guild, forum=forum, role=role,
                    client=client, topic_summary=topic, settings=settings,
                )
            except Exception:
                log.exception(
                    "Failed to process Discourse topic %s in guild %s",
                    topic_id, guild_id,
                )
                # Don't advance last_topic_id past a topic we failed on; we'll
                # retry it next loop iteration.
                break
            new_last_id = topic_id

        if new_last_id != last_topic_id:
            await self.config.guild_from_id(guild_id).last_topic_id.set(new_last_id)

    # ----- topic processing -----

    async def _process_new_topic(
        self,
        *,
        guild: discord.Guild,
        forum: discord.ForumChannel,
        role: discord.Role,
        client: DiscourseClient,
        topic_summary: Dict[str, Any],
        settings: Dict[str, Any],
    ) -> None:
        topic_id = int(topic_summary["id"])
        if self._session is None:
            return
        topic_data = await client.get_topic(self._session, topic_id)

        title = (topic_data.get("title") or topic_summary.get("title") or "").strip() \
            or f"Appeal #{topic_id}"
        slug = topic_data.get("slug") or topic_summary.get("slug") or ""
        topic_url = urljoin(client.base_url + "/", f"t/{slug}/{topic_id}" if slug else f"t/{topic_id}")

        posts = ((topic_data.get("post_stream") or {}).get("posts") or [])
        op = posts[0] if posts else {}
        op_username = op.get("username") or topic_data.get("created_by", {}).get("username") or "unknown"
        op_avatar_template = op.get("avatar_template") or ""
        avatar_url = self._avatar_url(client.base_url, op_avatar_template)
        cooked = op.get("cooked") or ""
        body_text = html_to_text(cooked)

        extract_patterns = settings.get("extract_fields") or DEFAULT_EXTRACT_FIELDS
        fields = extract_fields(body_text, extract_patterns)

        # Discourse OP timestamp drives only the embed's relative-time display.
        discourse_created_at_iso = (
            op.get("created_at") or topic_data.get("created_at") or ""
        )
        try:
            discourse_created_at = datetime.fromisoformat(
                discourse_created_at_iso.replace("Z", "+00:00")
            )
        except ValueError:
            discourse_created_at = utcnow()

        # Reminder timing anchors to when *we* picked up the appeal, so a
        # late-arriving or backfilled topic doesn't fire a reminder immediately.
        processed_at = utcnow()

        # Build embed
        embed = discord.Embed(
            title=truncate(title, 256),
            url=topic_url,
            color=discord.Color.orange(),
            timestamp=discourse_created_at,
        )
        author_kwargs: Dict[str, Any] = {
            "name": f"@{op_username}",
            "url": urljoin(client.base_url + "/", f"u/{op_username}"),
        }
        if avatar_url:
            author_kwargs["icon_url"] = avatar_url
        embed.set_author(**author_kwargs)
        for label, value in fields.items():
            embed.add_field(
                name=label,
                value=truncate(value, EMBED_FIELD_VALUE_LIMIT),
                inline=False,
            )
        embed.add_field(name="Forum thread", value=topic_url, inline=False)
        embed.set_footer(text=f"Discourse topic #{topic_id}")

        # Build forum thread name (Discord caps at 100 chars)
        thread_name = truncate(title, 100)

        applied_tags: List[discord.ForumTag] = []
        open_tag = self._find_tag(forum, settings.get("tag_open") or DEFAULT_TAG_OPEN)
        if open_tag is not None:
            applied_tags.append(open_tag)

        create_kwargs: Dict[str, Any] = {"name": thread_name, "embed": embed}
        if applied_tags:
            create_kwargs["applied_tags"] = applied_tags
        try:
            created = await forum.create_thread(**create_kwargs)
        except discord.Forbidden:
            log.error(
                "Missing permissions to create thread in forum %s for guild %s",
                forum.id, guild.id,
            )
            raise
        except discord.HTTPException:
            log.exception(
                "Failed to create forum thread for Discourse topic %s", topic_id,
            )
            raise

        thread = created.thread

        # Kick off poll via Polls cog API
        polls_cog = self.bot.get_cog("Polls")
        if polls_cog is None or not hasattr(polls_cog, "api"):
            log.error(
                "Polls cog not available; deleting thread %s for topic %s",
                thread.id, topic_id,
            )
            try:
                await thread.delete()
            except discord.HTTPException:
                pass
            raise RuntimeError("Polls cog not loaded")

        poll_question = truncate(f"Appeal: {title}", 256)
        try:
            poll = await polls_cog.api.create_poll(
                guild=guild,
                channel=thread,
                author_id=self.bot.user.id if self.bot.user else 0,
                question=poll_question,
                options=POLL_OPTIONS,
                duration_seconds=POLL_DURATION_SECONDS,
                allowed_role_ids=[role.id],
                hide_voters=True,
            )
        except Exception:
            log.exception("Polls.create_poll failed for topic %s", topic_id)
            try:
                await thread.delete()
            except discord.HTTPException:
                pass
            raise

        # Persist tracking BEFORE the optional Discourse comment so a comment
        # failure doesn't lose state.
        appeal_record: Dict[str, Any] = {
            "discord_thread_id": thread.id,
            "discord_message_id": created.message.id,
            "poll_id": poll.id,
            "topic_url": topic_url,
            "title": title,
            "created_ts": int(processed_at.timestamp()),
            "discourse_created_ts": int(discourse_created_at.timestamp()),
            "reminded": False,
        }
        async with self.config.guild(guild).tracked_appeals() as tracked:
            tracked[str(topic_id)] = appeal_record

        # Schedule reminder
        remind_at = processed_at + timedelta(seconds=REMINDER_AFTER_SECONDS)
        self._schedule_reminder(guild.id, str(topic_id), remind_at)

        # Optional: post initial comment back to Discourse
        initial_template = (settings.get("initial_comment_template") or "").strip()
        if client.has_credentials and initial_template:
            link = discord_thread_link(guild.id, thread.id)
            try:
                rendered = initial_template.format(discord_link=link)
            except (KeyError, IndexError) as e:
                log.warning("Initial comment template formatting failed: %s", e)
                rendered = initial_template
            try:
                await client.post_reply(self._session, topic_id, rendered)
            except DiscourseError as e:
                log.warning("Failed to post initial Discourse comment for topic %s: %s",
                            topic_id, e)

        log.info(
            "Mirrored Discourse topic %s -> thread %s (poll %s) in guild %s",
            topic_id, thread.id, poll.id, guild.id,
        )

    @staticmethod
    def _avatar_url(base_url: str, template: str) -> Optional[str]:
        if not template:
            return None
        sized = template.replace("{size}", "96")
        if sized.startswith("http"):
            return sized
        return urljoin(base_url.rstrip("/") + "/", sized.lstrip("/"))

    @staticmethod
    def _find_tag(forum: discord.ForumChannel, name: str) -> Optional[discord.ForumTag]:
        if not name:
            return None
        needle = name.casefold()
        for tag in forum.available_tags:
            if tag.name.casefold() == needle:
                return tag
        return None

    # ----- reminders -----

    def _reminder_key(self, guild_id: int, topic_id: str) -> str:
        return f"{guild_id}:{topic_id}"

    def _schedule_reminder(
        self, guild_id: int, topic_id: str, remind_at: datetime,
    ) -> None:
        key = self._reminder_key(guild_id, topic_id)
        existing = self._reminder_tasks.get(key)
        if existing is not None and not existing.done():
            existing.cancel()
        delay = max(0.0, (remind_at - utcnow()).total_seconds())
        self._reminder_tasks[key] = asyncio.create_task(
            self._reminder_after(guild_id, topic_id, delay)
        )

    def _cancel_reminder(self, guild_id: int, topic_id: str) -> None:
        key = self._reminder_key(guild_id, topic_id)
        task = self._reminder_tasks.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    async def _reminder_after(
        self, guild_id: int, topic_id: str, delay: float,
    ) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self._do_reminder(guild_id, topic_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Reminder task failed for guild=%s topic=%s",
                          guild_id, topic_id)
        finally:
            self._reminder_tasks.pop(self._reminder_key(guild_id, topic_id), None)

    async def _do_reminder(self, guild_id: int, topic_id: str) -> None:
        settings = await self.config.guild_from_id(guild_id).all()
        tracked = settings.get("tracked_appeals") or {}
        info = tracked.get(topic_id)
        if not info or info.get("reminded"):
            return

        threshold = int(settings.get("vote_threshold") or DEFAULT_VOTE_THRESHOLD)
        poll_id = info.get("poll_id")
        polls_cog = self.bot.get_cog("Polls")
        if polls_cog is None or not hasattr(polls_cog, "api"):
            log.warning("Polls cog gone, skipping reminder for topic %s", topic_id)
            return

        # Don't ping if the poll has been closed, cancelled, or deleted between
        # the time this reminder was scheduled and now.
        try:
            current_poll = await polls_cog.api.get_poll(poll_id)
        except Exception:
            log.exception("Failed to fetch poll %s for reminder check", poll_id)
            return
        if current_poll is None or getattr(current_poll, "status", None) != "open":
            await self._mark_reminded(guild_id, topic_id)
            return

        try:
            votes_map = await polls_cog.api.get_votes(poll_id)
        except Exception:
            log.exception("Failed to fetch votes for poll %s", poll_id)
            return

        unique_voters = len(votes_map)
        if unique_voters >= threshold:
            # Threshold met, record as reminded so we don't keep checking.
            await self._mark_reminded(guild_id, topic_id)
            return

        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return
        thread_id = int(info["discord_thread_id"])
        thread: Optional[discord.abc.Messageable] = (
            guild.get_thread(thread_id) or self.bot.get_channel(thread_id)
        )
        if thread is None:
            try:
                thread = await self.bot.fetch_channel(thread_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                log.warning("Reminder thread %s not accessible", thread_id)
                await self._mark_reminded(guild_id, topic_id)
                return

        role_id = settings.get("vote_role_id")
        role_mention = f"<@&{role_id}>" if role_id else ""
        msg = (
            f"{role_mention} only **{unique_voters}**/{threshold} votes have "
            f"been cast on this appeal, please review."
        ).strip()
        try:
            await thread.send(
                msg,
                allowed_mentions=discord.AllowedMentions(roles=True),
            )
        except (discord.Forbidden, discord.HTTPException):
            log.exception("Failed to send reminder in thread %s", thread.id)

        await self._mark_reminded(guild_id, topic_id)

    async def _mark_reminded(self, guild_id: int, topic_id: str) -> None:
        async with self.config.guild_from_id(guild_id).tracked_appeals() as tracked:
            if topic_id in tracked:
                tracked[topic_id]["reminded"] = True

    # ----- poll closed listener -----

    @commands.Cog.listener()
    async def on_poll_closed(self, poll: Any) -> None:
        await self._handle_poll_terminal(poll)

    @commands.Cog.listener()
    async def on_poll_cancelled(self, poll: Any) -> None:
        await self._handle_poll_terminal(poll)

    @commands.Cog.listener()
    async def on_poll_deleted(self, poll: Any) -> None:
        """Silent cleanup: a moderator deleted the poll outright. We drop our
        tracking and reminder, but don't archive the thread, swap tags, or
        post a closing Discourse comment, staff is presumably handling
        whatever follow-up is needed manually."""
        guild_id = getattr(poll, "guild_id", None)
        poll_id = getattr(poll, "id", None)
        if guild_id is None or poll_id is None:
            return
        tracked = await self.config.guild_from_id(guild_id).tracked_appeals()
        topic_id: Optional[str] = None
        for tid, rec in (tracked or {}).items():
            if rec.get("poll_id") == poll_id:
                topic_id = tid
                break
        if topic_id is None:
            return
        self._cancel_reminder(int(guild_id), topic_id)
        async with self.config.guild_from_id(guild_id).tracked_appeals() as tracked_:
            tracked_.pop(topic_id, None)
        log.info(
            "AppealFeeder: poll %s deleted, dropped tracking for topic %s",
            poll_id, topic_id,
        )

    async def _handle_poll_terminal(self, poll: Any) -> None:
        guild_id = getattr(poll, "guild_id", None)
        poll_id = getattr(poll, "id", None)
        if guild_id is None or poll_id is None:
            return
        settings = await self.config.guild_from_id(guild_id).all()
        tracked = settings.get("tracked_appeals") or {}

        topic_id: Optional[str] = None
        info: Optional[Dict[str, Any]] = None
        for tid, rec in tracked.items():
            if rec.get("poll_id") == poll_id:
                topic_id = tid
                info = rec
                break
        if topic_id is None or info is None:
            return

        # Cancel the reminder if it's still pending
        self._cancel_reminder(int(guild_id), topic_id)

        polls_cog = self.bot.get_cog("Polls")
        counts: List[int] = []
        if polls_cog is not None and hasattr(polls_cog, "api"):
            try:
                counts = await polls_cog.api.get_vote_counts(poll_id)
            except Exception:
                log.exception("Could not fetch final vote counts for %s", poll_id)

        outcome = winning_outcome(POLL_OPTIONS, counts) if counts else None
        breakdown = votes_breakdown(POLL_OPTIONS, counts) if counts else "_No votes were cast._"

        guild = self.bot.get_guild(int(guild_id))
        forum_id = settings.get("forum_channel_id")
        forum = guild.get_channel(forum_id) if guild and forum_id else None
        thread_id = int(info.get("discord_thread_id", 0))

        # Resolve the thread once for reuse across tag swap + close.
        thread: Optional[discord.Thread] = None
        if guild and thread_id:
            cached = guild.get_thread(thread_id)
            if isinstance(cached, discord.Thread):
                thread = cached
            else:
                try:
                    fetched = await self.bot.fetch_channel(thread_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    fetched = None
                if isinstance(fetched, discord.Thread):
                    thread = fetched

        # Swap forum tag based on outcome
        if isinstance(forum, discord.ForumChannel) and thread is not None:
            tag_name = {
                "Accept": settings.get("tag_accepted") or DEFAULT_TAG_ACCEPTED,
                "Reduce": settings.get("tag_reduced") or DEFAULT_TAG_REDUCED,
                "Deny": settings.get("tag_denied") or DEFAULT_TAG_DENIED,
            }.get(outcome or "")
            new_tag = self._find_tag(forum, tag_name) if tag_name else None
            if new_tag is not None:
                open_tag = self._find_tag(forum, settings.get("tag_open") or DEFAULT_TAG_OPEN)
                new_tags = [
                    t for t in thread.applied_tags
                    if not (open_tag and t.id == open_tag.id)
                ]
                if all(t.id != new_tag.id for t in new_tags):
                    new_tags.append(new_tag)
                try:
                    await thread.edit(applied_tags=new_tags)
                except (discord.Forbidden, discord.HTTPException):
                    log.exception("Failed to update forum tags on thread %s", thread_id)

        # Post closing comment back to Discourse if creds are set
        base = settings.get("discourse_base_url")
        client = DiscourseClient(
            base or "",
            api_key=settings.get("discourse_api_key"),
            api_username=settings.get("discourse_api_username"),
        )
        if base and client.has_credentials:
            template = {
                "Accept": settings.get("template_accepted") or DEFAULT_TEMPLATE_ACCEPTED,
                "Reduce": settings.get("template_reduced") or DEFAULT_TEMPLATE_REDUCED,
                "Deny": settings.get("template_denied") or DEFAULT_TEMPLATE_DENIED,
            }.get(outcome or "")
            if template is None:
                template = settings.get("template_tie") or DEFAULT_TEMPLATE_TIE
            link = discord_thread_link(int(guild_id), thread_id) if thread_id else ""
            try:
                rendered = template.format(
                    discord_link=link, votes_breakdown=breakdown,
                )
            except (KeyError, IndexError) as e:
                log.warning("Closing template formatting failed: %s", e)
                rendered = template
            if self._session is not None:
                try:
                    await client.post_reply(self._session, int(topic_id), rendered)
                except DiscourseError as e:
                    log.warning(
                        "Failed to post closing Discourse comment for topic %s: %s",
                        topic_id, e,
                    )

        # Close (archive + lock) the Discord forum thread so it's no longer
        # an active discussion. Lock first, then archive — archived threads
        # can be edited by their owner without the lock.
        if thread is not None:
            try:
                await thread.edit(
                    locked=True, archived=True,
                    reason="Appeal poll closed",
                )
            except (discord.Forbidden, discord.HTTPException):
                log.exception("Failed to close forum thread %s", thread_id)

        # Archive
        async with self.config.guild_from_id(guild_id).tracked_appeals() as tracked_:
            if topic_id in tracked_:
                tracked_.pop(topic_id, None)
        async with self.config.guild_from_id(guild_id).archived_appeals() as arch:
            arch[topic_id] = {
                **info,
                "closed_ts": int(utcnow().timestamp()),
                "outcome": outcome or "tie",
            }

    # ----- commands -----

    @commands.group(name="appealset", invoke_without_command=True)
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def appealset(self, ctx: commands.Context) -> None:
        """Configure the AppealFeeder cog."""
        await ctx.send_help()

    @appealset.command(name="enable")
    async def appealset_enable(self, ctx: commands.Context) -> None:
        """Enable the appeal feeder for this server."""
        settings = await self.config.guild(ctx.guild).all()
        missing = []
        if not settings.get("discourse_base_url"):
            missing.append("`instance`")
        if not settings.get("discourse_category_id"):
            missing.append("`category`")
        if not settings.get("forum_channel_id"):
            missing.append("`forum`")
        if not settings.get("vote_role_id"):
            missing.append("`role`")
        if missing:
            await ctx.send(
                "Cannot enable, missing required settings: " + ", ".join(missing)
                + f". Use `{ctx.clean_prefix}appealset settings` to review."
            )
            return
        if self.bot.get_cog("Polls") is None:
            await ctx.send(
                "The `Polls` cog must be loaded first. "
                f"Try `{ctx.clean_prefix}load polls`."
            )
            return
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("AppealFeeder enabled.")

    @appealset.command(name="disable")
    async def appealset_disable(self, ctx: commands.Context) -> None:
        """Disable the appeal feeder for this server."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("AppealFeeder disabled.")

    @appealset.command(name="instance")
    async def appealset_instance(self, ctx: commands.Context, url: str) -> None:
        """Set the Discourse instance URL (e.g. `https://forums.example.com`)."""
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            await ctx.send("Please provide a full URL including `https://`.")
            return
        clean = f"{parsed.scheme}://{parsed.netloc}"
        await self.config.guild(ctx.guild).discourse_base_url.set(clean)
        await ctx.send(f"Discourse instance set to `{clean}`.")

    @appealset.command(name="category")
    async def appealset_category(
        self, ctx: commands.Context, *, category: str,
    ) -> None:
        """Set the Discourse category to monitor.

        Accepts any of:
          • A category URL: `https://forums.example.com/c/ban-appeals/game-server-appeals/6`
          • A slug path with ID: `c/ban-appeals/game-server-appeals/6`
          • A slug path without ID (auto-resolved): `ban-appeals/game-server-appeals`
          • A bare numeric ID: `6`
        """
        base = await self.config.guild(ctx.guild).discourse_base_url()
        if not base:
            await ctx.send(
                f"Set the Discourse instance first with "
                f"`{ctx.clean_prefix}appealset instance <url>`."
            )
            return
        if self._session is None:
            await ctx.send("HTTP session unavailable; please reload the cog.")
            return

        cat_id, slug_path = parse_category_input(category)
        if cat_id is None and not slug_path:
            await ctx.send(
                "Couldn't parse that. Provide a category URL, a slug path, or a numeric ID."
            )
            return

        client = DiscourseClient(
            base,
            api_key=await self.config.guild(ctx.guild).discourse_api_key(),
            api_username=await self.config.guild(ctx.guild).discourse_api_username(),
        )

        if cat_id is None:
            assert slug_path is not None
            try:
                cat = await client.resolve_category(self._session, slug_path)
            except DiscourseError as e:
                await ctx.send(f"Discourse lookup failed: `{e}`")
                return
            if cat is None:
                await ctx.send(
                    f"No category matching `{slug_path}` found on `{base}`. "
                    "If the slug is correct, try passing the numeric category ID directly."
                )
                return
            cat_id = int(cat["id"])
            slug_path = (cat.get("slug") or slug_path).strip("/")

        # Validate by hitting /c/<id>.json
        try:
            topics = await client.list_category_topics(self._session, cat_id)
        except DiscourseError as e:
            await ctx.send(f"Could not load category `{cat_id}`: `{e}`")
            return

        await self.config.guild(ctx.guild).discourse_category_id.set(cat_id)
        await self.config.guild(ctx.guild).discourse_category_label.set(slug_path or str(cat_id))
        label = slug_path or str(cat_id)
        await ctx.send(
            f"Category set to `{label}` (id `{cat_id}`). "
            f"Endpoint validated, found {len(topics)} recent topic(s)."
        )

    @appealset.command(name="forum")
    async def appealset_forum(
        self, ctx: commands.Context, channel: discord.ForumChannel,
    ) -> None:
        """Set the Discord forum channel where appeal threads will be posted."""
        await self.config.guild(ctx.guild).forum_channel_id.set(channel.id)
        await ctx.send(f"Forum channel set to {channel.mention}.")

    @appealset.command(name="role")
    async def appealset_role(self, ctx: commands.Context, role: discord.Role) -> None:
        """Set the role allowed to vote (and pinged on low-turnout reminders)."""
        await self.config.guild(ctx.guild).vote_role_id.set(role.id)
        await ctx.send(f"Vote/reminder role set to {role.mention}.",
                       allowed_mentions=discord.AllowedMentions.none())

    @appealset.command(name="threshold")
    async def appealset_threshold(self, ctx: commands.Context, count: int) -> None:
        """Set the minimum unique-voter count below which the reminder fires at day 5."""
        if count < 1:
            await ctx.send("Threshold must be at least 1.")
            return
        await self.config.guild(ctx.guild).vote_threshold.set(int(count))
        await ctx.send(f"Vote threshold set to **{count}**.")

    @appealset.command(name="apikey")
    async def appealset_apikey(
        self, ctx: commands.Context, *, api_key: Optional[str] = None,
    ) -> None:
        """Set (or clear) the Discourse API key. Run with no argument to clear.

        Tip: run this in DM to avoid exposing the key in chat, and delete the
        invocation message afterwards if you must run it in a channel.
        """
        if api_key is None or not api_key.strip():
            await self.config.guild(ctx.guild).discourse_api_key.set(None)
            await ctx.send("Discourse API key cleared.")
            return
        await self.config.guild(ctx.guild).discourse_api_key.set(api_key.strip())
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            pass
        await ctx.send("Discourse API key saved (your message was deleted if possible).")

    @appealset.command(name="apiuser")
    async def appealset_apiuser(
        self, ctx: commands.Context, *, username: Optional[str] = None,
    ) -> None:
        """Set the Discourse username associated with the API key."""
        if username is None or not username.strip():
            await self.config.guild(ctx.guild).discourse_api_username.set(None)
            await ctx.send("Discourse API username cleared.")
            return
        await self.config.guild(ctx.guild).discourse_api_username.set(username.strip())
        await ctx.send(f"Discourse API username set to `{username.strip()}`.")

    @appealset.group(name="tag", invoke_without_command=True)
    async def appealset_tag(self, ctx: commands.Context) -> None:
        """Configure forum tag names used by the cog."""
        await ctx.send_help()

    @appealset_tag.command(name="open")
    async def appealset_tag_open(self, ctx: commands.Context, *, name: str) -> None:
        await self.config.guild(ctx.guild).tag_open.set(name)
        await ctx.send(f"Open-state tag name set to `{name}`.")

    @appealset_tag.command(name="accepted")
    async def appealset_tag_accepted(self, ctx: commands.Context, *, name: str) -> None:
        await self.config.guild(ctx.guild).tag_accepted.set(name)
        await ctx.send(f"Accepted-state tag name set to `{name}`.")

    @appealset_tag.command(name="reduced")
    async def appealset_tag_reduced(self, ctx: commands.Context, *, name: str) -> None:
        await self.config.guild(ctx.guild).tag_reduced.set(name)
        await ctx.send(f"Reduced-state tag name set to `{name}`.")

    @appealset_tag.command(name="denied")
    async def appealset_tag_denied(self, ctx: commands.Context, *, name: str) -> None:
        await self.config.guild(ctx.guild).tag_denied.set(name)
        await ctx.send(f"Denied-state tag name set to `{name}`.")

    @appealset.group(name="template", invoke_without_command=True)
    async def appealset_template(self, ctx: commands.Context) -> None:
        """Configure Discourse comment templates.

        Available placeholders:
          • `{discord_link}`, link to the Discord forum thread
          • `{votes_breakdown}`, tally text (closing comments only)
        """
        await ctx.send_help()

    async def _set_template(
        self, ctx: commands.Context, key: str, label: str, text: Optional[str],
    ) -> None:
        if text is None or not text.strip():
            # For initial: empty disables. For close templates: revert to default.
            if key == "initial_comment_template":
                await self.config.guild(ctx.guild).set_raw(key, value=None)
                await ctx.send(f"{label} cleared (no comment will be posted).")
            else:
                default = {
                    "template_accepted": DEFAULT_TEMPLATE_ACCEPTED,
                    "template_reduced": DEFAULT_TEMPLATE_REDUCED,
                    "template_denied": DEFAULT_TEMPLATE_DENIED,
                    "template_tie": DEFAULT_TEMPLATE_TIE,
                }[key]
                await self.config.guild(ctx.guild).set_raw(key, value=default)
                await ctx.send(f"{label} reset to default.")
            return
        await self.config.guild(ctx.guild).set_raw(key, value=text)
        await ctx.send(f"{label} updated.")

    @appealset_template.command(name="initial")
    async def appealset_template_initial(
        self, ctx: commands.Context, *, text: Optional[str] = None,
    ) -> None:
        """Initial Discourse comment posted when an appeal is picked up."""
        await self._set_template(ctx, "initial_comment_template", "Initial comment", text)

    @appealset_template.command(name="accepted")
    async def appealset_template_accepted(
        self, ctx: commands.Context, *, text: Optional[str] = None,
    ) -> None:
        """Closing comment when `Accept` wins."""
        await self._set_template(ctx, "template_accepted", "Accepted template", text)

    @appealset_template.command(name="reduced")
    async def appealset_template_reduced(
        self, ctx: commands.Context, *, text: Optional[str] = None,
    ) -> None:
        """Closing comment when `Reduce` wins."""
        await self._set_template(ctx, "template_reduced", "Reduced template", text)

    @appealset_template.command(name="denied")
    async def appealset_template_denied(
        self, ctx: commands.Context, *, text: Optional[str] = None,
    ) -> None:
        """Closing comment when `Deny` wins."""
        await self._set_template(ctx, "template_denied", "Denied template", text)

    @appealset_template.command(name="tie")
    async def appealset_template_tie(
        self, ctx: commands.Context, *, text: Optional[str] = None,
    ) -> None:
        """Fallback closing comment for ties or no-vote outcomes."""
        await self._set_template(ctx, "template_tie", "Tie/no-vote template", text)

    @appealset.group(name="field", invoke_without_command=True)
    async def appealset_field(self, ctx: commands.Context) -> None:
        """Configure the labelled fields extracted from each appeal's body."""
        await ctx.send_help()

    @appealset_field.command(name="add")
    async def appealset_field_add(
        self, ctx: commands.Context, label: str, *, pattern: str,
    ) -> None:
        """Add (or overwrite) an extraction field.

        `label` is the embed-field name. `pattern` is a Python regex with one
        capturing group; flags can be embedded with `(?im)` etc.
        """
        try:
            re.compile(pattern)
        except re.error as e:
            await ctx.send(f"Invalid regex: `{e}`")
            return
        async with self.config.guild(ctx.guild).extract_fields() as fields:
            fields[label] = pattern
        await ctx.send(f"Field `{label}` set.")

    @appealset_field.command(name="remove")
    async def appealset_field_remove(
        self, ctx: commands.Context, *, label: str,
    ) -> None:
        async with self.config.guild(ctx.guild).extract_fields() as fields:
            if label not in fields:
                await ctx.send(f"No field named `{label}`.")
                return
            fields.pop(label, None)
        await ctx.send(f"Field `{label}` removed.")

    @appealset_field.command(name="list")
    async def appealset_field_list(self, ctx: commands.Context) -> None:
        fields = await self.config.guild(ctx.guild).extract_fields()
        if not fields:
            await ctx.send("No extraction fields configured.")
            return
        lines = [f"• **{label}**, `{pat}`" for label, pat in fields.items()]
        await ctx.send("\n".join(lines)[:1900])

    @appealset_field.command(name="reset")
    async def appealset_field_reset(self, ctx: commands.Context) -> None:
        await self.config.guild(ctx.guild).extract_fields.set(dict(DEFAULT_EXTRACT_FIELDS))
        await ctx.send("Extraction fields reset to defaults.")

    @appealset.command(name="settings")
    async def appealset_settings(self, ctx: commands.Context) -> None:
        """Show the current configuration."""
        s = await self.config.guild(ctx.guild).all()

        def _yn(v: Any) -> str:
            return "✅ set" if v else "❌ not set"

        forum_id = s.get("forum_channel_id")
        forum_mention = f"<#{forum_id}>" if forum_id else "Not set"
        role_id = s.get("vote_role_id")
        role_mention = f"<@&{role_id}>" if role_id else "Not set"

        embed = discord.Embed(
            title="AppealFeeder settings",
            color=await ctx.embed_color(),
        )
        embed.add_field(
            name="Status",
            value="Enabled" if s.get("enabled") else "Disabled",
            inline=True,
        )
        embed.add_field(
            name="Discourse instance",
            value=f"`{s.get('discourse_base_url') or 'Not set'}`",
            inline=False,
        )
        cat_id = s.get("discourse_category_id")
        cat_label = s.get("discourse_category_label")
        if cat_id:
            cat_value = f"`{cat_label or cat_id}` (id `{cat_id}`)"
        else:
            cat_value = "Not set"
        embed.add_field(name="Category", value=cat_value, inline=False)
        embed.add_field(name="Forum channel", value=forum_mention, inline=True)
        embed.add_field(name="Vote role", value=role_mention, inline=True)
        embed.add_field(
            name="Vote threshold",
            value=str(s.get("vote_threshold") or DEFAULT_VOTE_THRESHOLD),
            inline=True,
        )
        embed.add_field(
            name="API key", value=_yn(s.get("discourse_api_key")), inline=True,
        )
        embed.add_field(
            name="API username",
            value=f"`{s.get('discourse_api_username')}`" if s.get("discourse_api_username") else _yn(None),
            inline=True,
        )
        embed.add_field(
            name="Last topic ID",
            value=f"`{s.get('last_topic_id')}`" if s.get("last_topic_id") else ",",
            inline=True,
        )
        embed.add_field(
            name="Tracked / archived",
            value=f"{len(s.get('tracked_appeals') or {})} / {len(s.get('archived_appeals') or {})}",
            inline=True,
        )
        embed.add_field(
            name="Tags (open/accept/reduce/deny)",
            value=f"`{s.get('tag_open')}` / `{s.get('tag_accepted')}` / "
                  f"`{s.get('tag_reduced')}` / `{s.get('tag_denied')}`",
            inline=False,
        )
        await ctx.send(
            embed=embed,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    @appealset.command(name="resetcursor")
    async def appealset_resetcursor(
        self, ctx: commands.Context, topic_id: Optional[int] = None,
    ) -> None:
        """Reset the last-seen Discourse topic cursor.

        With no argument, clears the cursor (next run will set it to the latest
        without backfilling). With a topic ID, sets the cursor explicitly.
        """
        await self.config.guild(ctx.guild).last_topic_id.set(topic_id)
        if topic_id is None:
            await ctx.send(
                "Cursor cleared. The next poll will set it to the newest topic "
                "without backfilling."
            )
        else:
            await ctx.send(f"Cursor set to `{topic_id}`.")

    @appealset.command(name="purge")
    async def appealset_purge(
        self, ctx: commands.Context, confirm: Optional[str] = None,
    ) -> None:
        """Wipe the cog's tracking state for this guild. **Destructive.**

        Without an argument, prints a dry-run summary. Pass `confirm` to
        actually run it. Best-effort:
          • cancels pending reminder tasks
          • deletes the matching polls via the Polls cog
          • deletes the Discord forum threads the cog created
          • clears tracked + archived appeal records
          • resets the Discourse cursor (next poll re-initializes it)
        """
        s = await self.config.guild(ctx.guild).all()
        tracked: Dict[str, Any] = s.get("tracked_appeals") or {}
        archived: Dict[str, Any] = s.get("archived_appeals") or {}
        cursor = s.get("last_topic_id")

        if not tracked and not archived and cursor is None:
            await ctx.send("Nothing to purge.")
            return

        if confirm != "confirm":
            await ctx.send(
                f"Would purge **{len(tracked)}** active and **{len(archived)}** "
                f"archived appeal record(s), and reset the cursor "
                f"(currently `{cursor}`).\n"
                f"Run `{ctx.clean_prefix}appealset purge confirm` to proceed."
            )
            return

        polls_cog = self.bot.get_cog("Polls")
        deleted_polls = 0
        deleted_threads = 0

        for topic_id, info in list(tracked.items()):
            self._cancel_reminder(ctx.guild.id, topic_id)
            poll_id = info.get("poll_id")
            if polls_cog is not None and hasattr(polls_cog, "api") and poll_id:
                try:
                    if await polls_cog.api.delete_poll(poll_id):
                        deleted_polls += 1
                except Exception:
                    log.exception("Failed to delete poll %s during purge", poll_id)
            thread_id = info.get("discord_thread_id")
            if thread_id:
                thread = ctx.guild.get_thread(int(thread_id))
                if thread is None:
                    try:
                        thread = await self.bot.fetch_channel(int(thread_id))
                    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                        thread = None
                if isinstance(thread, discord.Thread):
                    try:
                        await thread.delete()
                        deleted_threads += 1
                    except (discord.Forbidden, discord.HTTPException):
                        log.exception("Failed to delete thread %s during purge", thread_id)

        await self.config.guild(ctx.guild).tracked_appeals.set({})
        await self.config.guild(ctx.guild).archived_appeals.set({})
        await self.config.guild(ctx.guild).last_topic_id.set(None)

        await ctx.send(
            f"Purge complete. Cleared **{len(tracked)}** tracked + "
            f"**{len(archived)}** archived record(s); deleted "
            f"**{deleted_polls}** poll(s) and **{deleted_threads}** thread(s). "
            f"Cursor cleared, next poll initializes it without backfilling."
        )

    @appealset.command(name="testpoll")
    @commands.is_owner()
    async def appealset_testpoll(self, ctx: commands.Context) -> None:
        """Bot owner: trigger one polling pass immediately."""
        settings = await self.config.guild(ctx.guild).all()
        if not settings.get("enabled"):
            await ctx.send("AppealFeeder is disabled for this guild.")
            return
        await ctx.send("Polling Discourse…")
        try:
            await self._poll_guild(ctx.guild.id, settings)
        except Exception as e:
            log.exception("Manual poll failed")
            await ctx.send(f"Manual poll failed: ```{e}```")
            return
        await ctx.send("Manual poll done. Check the forum channel and logs.")

    @appealset.command(name="status")
    async def appealset_status(self, ctx: commands.Context) -> None:
        """Show currently-tracked appeals and their state."""
        s = await self.config.guild(ctx.guild).all()
        tracked = s.get("tracked_appeals") or {}
        if not tracked:
            await ctx.send("No appeals are currently being tracked.")
            return
        lines: List[str] = []
        for topic_id, info in sorted(tracked.items(), key=lambda kv: int(kv[0])):
            age_h = (utcnow() - datetime.fromtimestamp(
                int(info.get("created_ts", 0)), tz=timezone.utc,
            )).total_seconds() / 3600
            reminded = "🔔" if info.get("reminded") else "⏳"
            lines.append(
                f"{reminded} `#{topic_id}`, <#{info.get('discord_thread_id')}> "
                f"poll `{info.get('poll_id')}` (age {age_h:.1f}h)"
            )
        await ctx.send(
            truncate("\n".join(lines), 1900),
            allowed_mentions=discord.AllowedMentions.none(),
        )
