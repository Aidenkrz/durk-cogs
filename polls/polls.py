from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import discord
from discord import app_commands, ui
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.durk-cogs.polls")


SLUG_ALPHABET = (
    "♥♦♣♠★◆●■▲▼"
    "☀☁☂☃☘☯♪♫☮"
    "☢☣⚓⚔⚖⚙⚛⚜⚡"
    "❄❉❖✿✦✪✯❤"
    "☆◇○□△▽☄☕"
    "✈✉✏✒⚘⚐⚑"
    "♔♕♖♗♘♙"
)
SLUG_LENGTH = 6

MIN_DURATION_S = 30
MAX_DURATION_S = 30 * 86400
TALLY_DEBOUNCE_S = 2.0

MAX_OPTIONS = 25
MIN_OPTIONS = 2
MAX_OPTION_LENGTH = 80
MAX_QUESTION_LENGTH = 256
MAX_VOTERS_DISPLAYED_PER_OPTION = 20

DURATION_RE = re.compile(
    r"^(?:(\d+)\s*d)?\s*(?:(\d+)\s*h)?\s*(?:(\d+)\s*m)?\s*(?:(\d+)\s*s)?$"
)
ROLE_MENTION_RE = re.compile(r"<@&(\d+)>")

VOTE_CUSTOM_ID_PREFIX = "polls:vote:"

SCHEMA_VERSION = 1

class PollsError(Exception):
    """Base exception for the Polls cog."""


class PollNotFoundError(PollsError):
    pass


class PollClosedError(PollsError):
    pass


class InvalidPollInputError(PollsError):
    pass

@dataclass
class Poll:
    id: str
    guild_id: int
    channel_id: int
    message_id: Optional[int]
    author_id: int
    question: str
    options: List[str]
    settings: Dict[str, Any]
    created_at: datetime
    closes_at: datetime
    status: str
    closed_at: Optional[datetime]

    @property
    def hide_voters(self) -> bool:
        return bool(self.settings.get("hide_voters", False))

    @property
    def hide_tally_until_close(self) -> bool:
        return bool(self.settings.get("hide_tally_until_close", False))

    @property
    def max_choices(self) -> int:
        return int(self.settings.get("max_choices", 1))

    @property
    def allowed_role_ids(self) -> List[int]:
        return [int(r) for r in self.settings.get("allowed_role_ids", [])]


# ---------- Helpers ----------


def utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def to_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def from_ts(ts: Optional[int]) -> Optional[datetime]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def generate_slug() -> str:
    return "".join(secrets.choice(SLUG_ALPHABET) for _ in range(SLUG_LENGTH))


def parse_duration(text: str) -> int:
    """Parse a duration string like '1h30m' into seconds. Raises InvalidPollInputError."""
    raw = (text or "").strip().lower()
    if not raw:
        raise InvalidPollInputError("Duration is required.")
    m = DURATION_RE.match(raw)
    if not m or not any(m.groups()):
        raise InvalidPollInputError(
            f"Invalid duration `{text}`. Use forms like `30s`, `15m`, `2h`, `1d12h`, `7d`."
        )
    d, h, mi, s = (int(x) if x else 0 for x in m.groups())
    total = d * 86400 + h * 3600 + mi * 60 + s
    if total < MIN_DURATION_S:
        raise InvalidPollInputError(
            f"Duration too short: minimum is {MIN_DURATION_S} seconds."
        )
    if total > MAX_DURATION_S:
        raise InvalidPollInputError("Duration too long: maximum is 30 days.")
    return total


def format_duration(seconds: int) -> str:
    seconds = int(seconds)
    parts: List[str] = []
    for label, size in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
        n, seconds = divmod(seconds, size)
        if n:
            parts.append(f"{n}{label}")
    return "".join(parts) or "0s"


def parse_settings(text: str) -> Dict[str, Any]:
    """Parse settings flags string like 'hide_voters, hide_tally, choices=3'."""
    out: Dict[str, Any] = {
        "hide_voters": False,
        "hide_tally_until_close": False,
        "max_choices": 1,
    }
    raw = (text or "").strip()
    if not raw:
        return out
    flags = [f.strip().lower() for f in raw.split(",") if f.strip()]
    unknown: List[str] = []
    for flag in flags:
        if flag in ("hide_voters", "hide-voters", "hidevoters", "anonymous"):
            out["hide_voters"] = True
        elif flag in ("hide_tally", "hide-tally", "hidetally", "secret"):
            out["hide_tally_until_close"] = True
        elif flag.startswith("choices="):
            value = flag.split("=", 1)[1].strip()
            try:
                n = int(value)
            except ValueError:
                raise InvalidPollInputError(f"Invalid value for `choices`: `{value}`.")
            if n < 1:
                raise InvalidPollInputError("`choices` must be at least 1.")
            out["max_choices"] = n
        else:
            unknown.append(flag)
    if unknown:
        raise InvalidPollInputError(
            "Unknown settings flag(s): " + ", ".join(f"`{u}`" for u in unknown)
            + ". Valid flags: `hide_voters`, `hide_tally`, `choices=N`."
        )
    return out


def parse_options(text: str) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        raise InvalidPollInputError("Options are required.")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(lines) < MIN_OPTIONS:
        raise InvalidPollInputError(
            f"Need at least {MIN_OPTIONS} options (one per line)."
        )
    if len(lines) > MAX_OPTIONS:
        raise InvalidPollInputError(
            f"Too many options: maximum is {MAX_OPTIONS}."
        )
    too_long = [l for l in lines if len(l) > MAX_OPTION_LENGTH]
    if too_long:
        raise InvalidPollInputError(
            f"Option(s) exceed {MAX_OPTION_LENGTH} characters: "
            + ", ".join(f"`{l[:30]}…`" for l in too_long[:3])
        )
    if len(set(lines)) != len(lines):
        raise InvalidPollInputError("Duplicate options are not allowed.")
    return lines


def parse_allowed_roles(
    text: str, guild: discord.Guild
) -> Tuple[List[int], List[str]]:
    """Returns (resolved_role_ids, unresolved_tokens)."""
    raw = (text or "").strip()
    if not raw:
        return [], []
    tokens = [t.strip() for t in raw.split(",") if t.strip()]
    resolved: List[int] = []
    unresolved: List[str] = []
    for token in tokens:
        m = ROLE_MENTION_RE.match(token)
        if m:
            role = guild.get_role(int(m.group(1)))
            if role:
                resolved.append(role.id)
                continue
        if token.isdigit():
            role = guild.get_role(int(token))
            if role:
                resolved.append(role.id)
                continue
        role = discord.utils.get(guild.roles, name=token)
        if role:
            resolved.append(role.id)
            continue
        unresolved.append(token)
    seen: set = set()
    deduped = [r for r in resolved if not (r in seen or seen.add(r))]
    return deduped, unresolved


# ---------- DB layer ----------


class PollsDB:
    def __init__(self, path: Path):
        self._path = path
        self._conn: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._init_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("PollsDB is not open")
        return self._conn

    async def _init_schema(self) -> None:
        async with self._lock:
            await self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS polls (
                    id            TEXT PRIMARY KEY,
                    guild_id      INTEGER NOT NULL,
                    channel_id    INTEGER NOT NULL,
                    message_id    INTEGER,
                    author_id     INTEGER NOT NULL,
                    question      TEXT NOT NULL,
                    options_json  TEXT NOT NULL,
                    settings_json TEXT NOT NULL,
                    created_at    INTEGER NOT NULL,
                    closes_at     INTEGER NOT NULL,
                    status        TEXT NOT NULL DEFAULT 'open',
                    closed_at     INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_polls_status_closes
                    ON polls(status, closes_at);
                CREATE INDEX IF NOT EXISTS idx_polls_guild_status
                    ON polls(guild_id, status);
                CREATE INDEX IF NOT EXISTS idx_polls_author
                    ON polls(author_id);

                CREATE TABLE IF NOT EXISTS votes (
                    poll_id       TEXT NOT NULL,
                    user_id       INTEGER NOT NULL,
                    option_index  INTEGER NOT NULL,
                    voted_at      INTEGER NOT NULL,
                    PRIMARY KEY (poll_id, user_id, option_index),
                    FOREIGN KEY (poll_id) REFERENCES polls(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_votes_poll
                    ON votes(poll_id);
                CREATE INDEX IF NOT EXISTS idx_votes_user
                    ON votes(poll_id, user_id);
                """
            )
            await self.conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            await self.conn.commit()

    @staticmethod
    def _row_to_poll(row: aiosqlite.Row) -> Poll:
        return Poll(
            id=row["id"],
            guild_id=row["guild_id"],
            channel_id=row["channel_id"],
            message_id=row["message_id"],
            author_id=row["author_id"],
            question=row["question"],
            options=json.loads(row["options_json"]),
            settings=json.loads(row["settings_json"]),
            created_at=from_ts(row["created_at"]),
            closes_at=from_ts(row["closes_at"]),
            status=row["status"],
            closed_at=from_ts(row["closed_at"]),
        )

    async def insert_poll(self, poll: Poll) -> None:
        async with self._lock:
            await self.conn.execute(
                """
                INSERT INTO polls (
                    id, guild_id, channel_id, message_id, author_id,
                    question, options_json, settings_json,
                    created_at, closes_at, status, closed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    poll.id,
                    poll.guild_id,
                    poll.channel_id,
                    poll.message_id,
                    poll.author_id,
                    poll.question,
                    json.dumps(poll.options),
                    json.dumps(poll.settings),
                    to_ts(poll.created_at),
                    to_ts(poll.closes_at),
                    poll.status,
                    to_ts(poll.closed_at) if poll.closed_at else None,
                ),
            )
            await self.conn.commit()

    async def slug_exists(self, slug: str) -> bool:
        async with self.conn.execute(
            "SELECT 1 FROM polls WHERE id = ?", (slug,)
        ) as cur:
            return await cur.fetchone() is not None

    async def get_poll(self, poll_id: str) -> Optional[Poll]:
        async with self.conn.execute(
            "SELECT * FROM polls WHERE id = ?", (poll_id,)
        ) as cur:
            row = await cur.fetchone()
            return self._row_to_poll(row) if row else None

    async def list_polls(
        self,
        *,
        guild_id: Optional[int] = None,
        status: Optional[str] = None,
        author_id: Optional[int] = None,
    ) -> List[Poll]:
        clauses: List[str] = []
        params: List[Any] = []
        if guild_id is not None:
            clauses.append("guild_id = ?")
            params.append(guild_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if author_id is not None:
            clauses.append("author_id = ?")
            params.append(author_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT * FROM polls {where} ORDER BY created_at DESC"
        async with self.conn.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [self._row_to_poll(r) for r in rows]

    async def list_due_polls(self, now_ts: int) -> List[Poll]:
        async with self.conn.execute(
            "SELECT * FROM polls WHERE status = 'open' AND closes_at <= ?",
            (now_ts,),
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_poll(r) for r in rows]

    async def list_open_polls(self) -> List[Poll]:
        async with self.conn.execute(
            "SELECT * FROM polls WHERE status = 'open'"
        ) as cur:
            rows = await cur.fetchall()
            return [self._row_to_poll(r) for r in rows]

    async def set_message_id(self, poll_id: str, message_id: int) -> None:
        async with self._lock:
            await self.conn.execute(
                "UPDATE polls SET message_id = ? WHERE id = ?",
                (message_id, poll_id),
            )
            await self.conn.commit()

    async def update_closes_at(self, poll_id: str, closes_at: datetime) -> None:
        async with self._lock:
            await self.conn.execute(
                "UPDATE polls SET closes_at = ? WHERE id = ?",
                (to_ts(closes_at), poll_id),
            )
            await self.conn.commit()

    async def mark_closed(
        self, poll_id: str, *, status: str, closed_at: datetime
    ) -> None:
        async with self._lock:
            await self.conn.execute(
                "UPDATE polls SET status = ?, closed_at = ? WHERE id = ?",
                (status, to_ts(closed_at), poll_id),
            )
            await self.conn.commit()

    async def delete_poll(self, poll_id: str) -> None:
        async with self._lock:
            await self.conn.execute("DELETE FROM polls WHERE id = ?", (poll_id,))
            await self.conn.commit()

    # --- votes ---

    async def get_user_votes(self, poll_id: str, user_id: int) -> List[int]:
        async with self.conn.execute(
            "SELECT option_index FROM votes WHERE poll_id = ? AND user_id = ? ORDER BY option_index",
            (poll_id, user_id),
        ) as cur:
            rows = await cur.fetchall()
            return [int(r["option_index"]) for r in rows]

    async def insert_vote(
        self, poll_id: str, user_id: int, option_index: int
    ) -> None:
        async with self._lock:
            await self.conn.execute(
                """
                INSERT OR IGNORE INTO votes (poll_id, user_id, option_index, voted_at)
                VALUES (?, ?, ?, ?)
                """,
                (poll_id, user_id, option_index, to_ts(utcnow())),
            )
            await self.conn.commit()

    async def delete_vote(
        self, poll_id: str, user_id: int, option_index: int
    ) -> None:
        async with self._lock:
            await self.conn.execute(
                "DELETE FROM votes WHERE poll_id = ? AND user_id = ? AND option_index = ?",
                (poll_id, user_id, option_index),
            )
            await self.conn.commit()

    async def delete_user_votes(self, poll_id: str, user_id: int) -> None:
        async with self._lock:
            await self.conn.execute(
                "DELETE FROM votes WHERE poll_id = ? AND user_id = ?",
                (poll_id, user_id),
            )
            await self.conn.commit()

    async def get_vote_counts(
        self, poll_id: str, num_options: int
    ) -> List[int]:
        counts = [0] * num_options
        async with self.conn.execute(
            "SELECT option_index, COUNT(*) AS c FROM votes WHERE poll_id = ? GROUP BY option_index",
            (poll_id,),
        ) as cur:
            async for row in cur:
                idx = int(row["option_index"])
                if 0 <= idx < num_options:
                    counts[idx] = int(row["c"])
        return counts

    async def get_voters_by_option(
        self, poll_id: str, num_options: int
    ) -> List[List[int]]:
        out: List[List[int]] = [[] for _ in range(num_options)]
        async with self.conn.execute(
            "SELECT user_id, option_index FROM votes WHERE poll_id = ? ORDER BY voted_at",
            (poll_id,),
        ) as cur:
            async for row in cur:
                idx = int(row["option_index"])
                if 0 <= idx < num_options:
                    out[idx].append(int(row["user_id"]))
        return out

    async def get_all_user_votes_map(
        self, poll_id: str
    ) -> Dict[int, List[int]]:
        out: Dict[int, List[int]] = {}
        async with self.conn.execute(
            "SELECT user_id, option_index FROM votes WHERE poll_id = ?",
            (poll_id,),
        ) as cur:
            async for row in cur:
                out.setdefault(int(row["user_id"]), []).append(int(row["option_index"]))
        for uid in out:
            out[uid].sort()
        return out


# ---------- Embed rendering ----------


def _bar(pct: float, width: int = 12) -> str:
    filled = int(round((pct / 100.0) * width))
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def render_poll_embed(
    poll: Poll,
    counts: List[int],
    voters_by_option: Optional[List[List[int]]] = None,
    *,
    member_namer=None,
    total_unique_voters: Optional[int] = None,
) -> discord.Embed:
    if poll.status == "open":
        color = discord.Color.blurple()
        state_text = f"Ends {discord.utils.format_dt(poll.closes_at, 'R')}"
    elif poll.status == "closed":
        color = discord.Color.dark_gray()
        when = poll.closed_at or poll.closes_at
        state_text = f"Closed {discord.utils.format_dt(when, 'R')}"
    else:  # cancelled
        color = discord.Color.dark_red()
        when = poll.closed_at or utcnow()
        state_text = f"Cancelled {discord.utils.format_dt(when, 'R')}"

    embed = discord.Embed(title=poll.question, color=color)

    show_tally = poll.status != "open" or not poll.hide_tally_until_close
    total_votes = sum(counts)

    desc_lines: List[str] = [f"_{state_text}_", ""]
    for i, option in enumerate(poll.options):
        if show_tally:
            count = counts[i]
            pct = (count / total_votes * 100) if total_votes else 0.0
            desc_lines.append(f"**{i + 1}.** {option}")
            desc_lines.append(f"`{_bar(pct)}` `{count}` ({pct:.1f}%)")
        else:
            desc_lines.append(f"**{i + 1}.** {option}")

    if not show_tally and total_unique_voters is not None:
        desc_lines.append("")
        desc_lines.append(
            f"*{total_unique_voters} vote(s) so far — results hidden until close.*"
        )

    embed.description = "\n".join(desc_lines)

    if (
        poll.status != "open"
        and not poll.hide_voters
        and voters_by_option is not None
        and member_namer is not None
    ):
        for i, voter_ids in enumerate(voters_by_option):
            if not voter_ids:
                continue
            shown = voter_ids[:MAX_VOTERS_DISPLAYED_PER_OPTION]
            extra = len(voter_ids) - len(shown)
            names = ", ".join(member_namer(uid) for uid in shown)
            if extra > 0:
                names += f" *(+{extra} more)*"
            embed.add_field(
                name=f"Voted {i + 1}. {poll.options[i]}",
                value=names,
                inline=False,
            )

    flags: List[str] = []
    if poll.max_choices > 1:
        flags.append(f"choose up to {poll.max_choices}")
    if poll.hide_voters:
        flags.append("anonymous")
    if poll.hide_tally_until_close and poll.status == "open":
        flags.append("results hidden")
    if poll.allowed_role_ids:
        flags.append("role-restricted")

    footer = f"ID: {poll.id}"
    if flags:
        footer += "  •  " + " · ".join(flags)
    embed.set_footer(text=footer)

    return embed


def render_results_message(poll: Poll, counts: List[int]) -> str:
    total = sum(counts)
    if total == 0:
        return f"📊 Poll **{poll.id}** closed with no votes."
    pairs = sorted(
        ((i, c) for i, c in enumerate(counts)),
        key=lambda p: p[1],
        reverse=True,
    )
    top_count = pairs[0][1]
    winners = [i for i, c in pairs if c == top_count]
    if len(winners) == 1:
        winner_text = f"**{poll.options[winners[0]]}** wins with {top_count} vote(s)"
    else:
        names = ", ".join(f"**{poll.options[i]}**" for i in winners)
        winner_text = f"Tie between {names} ({top_count} vote(s) each)"
    return (
        f"📊 Poll **{poll.id}** has closed — {winner_text}. "
        f"Total votes cast: {total}."
    )


# ---------- View ----------


class PollView(ui.View):
    """Persistent View with one button per poll option."""

    def __init__(self, cog: "Polls", poll: Poll):
        super().__init__(timeout=None)
        self._cog = cog
        self.poll_id = poll.id
        for i, option in enumerate(poll.options):
            label = option if len(option) <= 80 else option[:77] + "…"
            btn = ui.Button(
                label=label,
                style=discord.ButtonStyle.primary,
                custom_id=f"{VOTE_CUSTOM_ID_PREFIX}{poll.id}:{i}",
                row=i // 5,
            )
            btn.callback = self._make_callback(i)
            self.add_item(btn)

    def _make_callback(self, option_index: int):
        async def callback(interaction: discord.Interaction) -> None:
            await self._cog._handle_vote(interaction, self.poll_id, option_index)

        return callback


def make_disabled_view(poll: Poll) -> ui.View:
    view = ui.View(timeout=None)
    for i, option in enumerate(poll.options):
        label = option if len(option) <= 80 else option[:77] + "…"
        view.add_item(
            ui.Button(
                label=label,
                style=discord.ButtonStyle.secondary,
                custom_id=f"{VOTE_CUSTOM_ID_PREFIX}closed:{poll.id}:{i}",
                row=i // 5,
                disabled=True,
            )
        )
    return view


# ---------- Modal ----------


class PollCreateModal(ui.Modal, title="Create Poll"):
    question = ui.TextInput(
        label="Question",
        style=discord.TextStyle.short,
        max_length=MAX_QUESTION_LENGTH,
        required=True,
        placeholder="What should we have for dinner?",
    )
    options_input = ui.TextInput(
        label="Options (one per line, 2–25)",
        style=discord.TextStyle.paragraph,
        max_length=2000,
        required=True,
        placeholder="Pizza\nSushi\nTacos",
    )
    duration_input = ui.TextInput(
        label="Duration",
        style=discord.TextStyle.short,
        max_length=32,
        required=True,
        placeholder="1h, 30m, 2d, 1d12h",
    )
    settings_input = ui.TextInput(
        label="Settings (optional)",
        style=discord.TextStyle.short,
        max_length=200,
        required=False,
        placeholder="hide_voters, hide_tally, choices=3",
    )
    allowed_roles_input = ui.TextInput(
        label="Allowed Roles (optional, mods only)",
        style=discord.TextStyle.short,
        max_length=500,
        required=False,
        placeholder="Members, 123456789012345678, @SomeRole",
    )

    def __init__(self, cog: "Polls"):
        super().__init__(timeout=600)
        self._cog = cog

    async def on_submit(self, interaction: discord.Interaction) -> None:  # type: ignore[override]
        try:
            await self._cog._handle_modal_submit(interaction, self)
        except InvalidPollInputError as e:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Could not create poll",
                    description=str(e),
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )
        except Exception:
            log.exception("Unexpected error creating poll from modal")
            err_embed = discord.Embed(
                title="Error",
                description="An unexpected error occurred while creating the poll.",
                color=discord.Color.red(),
            )
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        embed=err_embed, ephemeral=True
                    )
                else:
                    await interaction.followup.send(embed=err_embed, ephemeral=True)
            except Exception:
                pass


# ---------- Public API ----------


class PollsAPI:
    """Public API for other cogs.

    Usage::

        polls = bot.get_cog("Polls").api
        poll = await polls.create_poll(...)
    """

    def __init__(self, cog: "Polls"):
        self._cog = cog

    async def create_poll(
        self,
        *,
        guild: discord.Guild,
        channel: discord.abc.Messageable,
        author_id: int,
        question: str,
        options: List[str],
        duration_seconds: int,
        hide_voters: bool = False,
        hide_tally_until_close: bool = False,
        max_choices: int = 1,
        allowed_role_ids: Optional[List[int]] = None,
    ) -> Poll:
        """Create a new poll. Bypasses user-facing permission checks."""
        if not isinstance(question, str) or not question.strip():
            raise InvalidPollInputError("Question is required.")
        if len(question) > MAX_QUESTION_LENGTH:
            raise InvalidPollInputError(
                f"Question exceeds {MAX_QUESTION_LENGTH} characters."
            )
        if not options or len(options) < MIN_OPTIONS:
            raise InvalidPollInputError(
                f"At least {MIN_OPTIONS} options are required."
            )
        if len(options) > MAX_OPTIONS:
            raise InvalidPollInputError(
                f"Too many options (max {MAX_OPTIONS})."
            )
        if any(len(o) > MAX_OPTION_LENGTH for o in options):
            raise InvalidPollInputError(
                f"Each option must be <= {MAX_OPTION_LENGTH} characters."
            )
        if len(set(options)) != len(options):
            raise InvalidPollInputError("Duplicate options are not allowed.")
        if duration_seconds < MIN_DURATION_S or duration_seconds > MAX_DURATION_S:
            raise InvalidPollInputError(
                f"Duration must be between {MIN_DURATION_S}s and {MAX_DURATION_S}s."
            )
        if max_choices < 1 or max_choices > len(options):
            raise InvalidPollInputError(
                "max_choices must be between 1 and the number of options."
            )

        return await self._cog._create_poll(
            guild=guild,
            channel=channel,
            author_id=author_id,
            question=question.strip(),
            options=list(options),
            duration_seconds=duration_seconds,
            hide_voters=hide_voters,
            hide_tally_until_close=hide_tally_until_close,
            max_choices=max_choices,
            allowed_role_ids=list(allowed_role_ids or []),
        )

    async def get_poll(self, poll_id: str) -> Optional[Poll]:
        return await self._cog.db.get_poll(poll_id)

    async def list_polls(
        self,
        *,
        guild_id: Optional[int] = None,
        status: Optional[str] = None,
        author_id: Optional[int] = None,
    ) -> List[Poll]:
        return await self._cog.db.list_polls(
            guild_id=guild_id, status=status, author_id=author_id
        )

    async def edit_poll(self, poll_id: str, *, duration_seconds: int) -> Poll:
        if duration_seconds < MIN_DURATION_S or duration_seconds > MAX_DURATION_S:
            raise InvalidPollInputError(
                f"Duration must be between {MIN_DURATION_S}s and {MAX_DURATION_S}s."
            )
        return await self._cog._edit_poll_duration(poll_id, duration_seconds)

    async def close_poll(self, poll_id: str) -> Poll:
        """Close a poll early (cancelled status)."""
        return await self._cog._close_poll(poll_id, reason="cancelled")

    async def delete_poll(self, poll_id: str) -> bool:
        return await self._cog._delete_poll(poll_id)

    async def get_votes(self, poll_id: str) -> Dict[int, List[int]]:
        """Returns user_id -> sorted list of option indexes voted for."""
        return await self._cog.db.get_all_user_votes_map(poll_id)

    async def get_vote_counts(self, poll_id: str) -> List[int]:
        poll = await self._cog.db.get_poll(poll_id)
        if not poll:
            raise PollNotFoundError(f"Poll {poll_id} not found.")
        return await self._cog.db.get_vote_counts(poll_id, len(poll.options))


# ---------- Cog ----------


class Polls(commands.Cog):
    """Create polls with configurable duration, hidden voters/tallies,
    multi-choice, and per-poll role restrictions. Slash commands plus
    a public API and lifecycle events for other cogs."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.db = PollsDB(cog_data_path(self) / "polls.db")
        self.api = PollsAPI(self)
        self._close_tasks: Dict[str, asyncio.Task] = {}
        self._tally_tasks: Dict[str, asyncio.Task] = {}

    # ----- lifecycle -----

    async def cog_load(self) -> None:
        await self.db.open()
        # Re-attach persistent views and reschedule close tasks for any
        # polls that were open across the restart.
        open_polls = await self.db.list_open_polls()
        for poll in open_polls:
            if poll.message_id is not None:
                try:
                    self.bot.add_view(PollView(self, poll), message_id=poll.message_id)
                except Exception:
                    log.exception("Failed to register persistent view for %s", poll.id)
            self._schedule_close(poll)
        log.info("Polls cog loaded with %d open poll(s).", len(open_polls))

    async def cog_unload(self) -> None:
        for task in list(self._close_tasks.values()):
            task.cancel()
        for task in list(self._tally_tasks.values()):
            task.cancel()
        self._close_tasks.clear()
        self._tally_tasks.clear()
        await self.db.close()

    # ----- creation -----

    async def _generate_unique_slug(self) -> str:
        for _ in range(50):
            slug = generate_slug()
            if not await self.db.slug_exists(slug):
                return slug
        raise RuntimeError("Could not generate a unique poll slug after 50 attempts.")

    async def _create_poll(
        self,
        *,
        guild: discord.Guild,
        channel: discord.abc.Messageable,
        author_id: int,
        question: str,
        options: List[str],
        duration_seconds: int,
        hide_voters: bool,
        hide_tally_until_close: bool,
        max_choices: int,
        allowed_role_ids: List[int],
    ) -> Poll:
        slug = await self._generate_unique_slug()
        now = utcnow()
        closes_at = now + timedelta(seconds=duration_seconds)
        settings: Dict[str, Any] = {
            "hide_voters": bool(hide_voters),
            "hide_tally_until_close": bool(hide_tally_until_close),
            "max_choices": int(max_choices),
            "allowed_role_ids": [int(r) for r in allowed_role_ids],
        }
        poll = Poll(
            id=slug,
            guild_id=guild.id,
            channel_id=getattr(channel, "id", 0),
            message_id=None,
            author_id=author_id,
            question=question,
            options=options,
            settings=settings,
            created_at=now,
            closes_at=closes_at,
            status="open",
            closed_at=None,
        )
        await self.db.insert_poll(poll)

        embed = render_poll_embed(poll, counts=[0] * len(poll.options))
        view = PollView(self, poll)
        try:
            message = await channel.send(embed=embed, view=view)
        except Exception:
            log.exception("Failed to send poll message for %s", slug)
            await self.db.delete_poll(slug)
            raise

        await self.db.set_message_id(slug, message.id)
        poll = replace(poll, message_id=message.id)

        self._schedule_close(poll)
        self.bot.dispatch("poll_created", poll)
        return poll

    async def _handle_modal_submit(
        self, interaction: discord.Interaction, modal: PollCreateModal
    ) -> None:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.channel, discord.abc.Messageable):
            raise InvalidPollInputError("Polls can only be created in a guild channel.")

        question = str(modal.question.value).strip()
        if not question:
            raise InvalidPollInputError("Question is required.")
        if len(question) > MAX_QUESTION_LENGTH:
            raise InvalidPollInputError(
                f"Question exceeds {MAX_QUESTION_LENGTH} characters."
            )

        options = parse_options(str(modal.options_input.value))
        duration_s = parse_duration(str(modal.duration_input.value))
        settings = parse_settings(str(modal.settings_input.value))

        if settings["max_choices"] > len(options):
            raise InvalidPollInputError(
                f"`choices={settings['max_choices']}` exceeds the number of options ({len(options)})."
            )

        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else guild.get_member(interaction.user.id)
        )
        warning_lines: List[str] = []

        allowed_role_ids: List[int] = []
        roles_text = str(modal.allowed_roles_input.value or "").strip()
        if roles_text:
            if member is None or not member.guild_permissions.moderate_members:
                raise InvalidPollInputError(
                    "Only members with the **Moderate Members** permission can set "
                    "`Allowed Roles` on a poll."
                )
            resolved, unresolved = parse_allowed_roles(roles_text, guild)
            allowed_role_ids = resolved
            if unresolved:
                warning_lines.append(
                    "Could not resolve role(s): "
                    + ", ".join(f"`{u}`" for u in unresolved)
                )

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            poll = await self._create_poll(
                guild=guild,
                channel=interaction.channel,
                author_id=interaction.user.id,
                question=question,
                options=options,
                duration_seconds=duration_s,
                hide_voters=settings["hide_voters"],
                hide_tally_until_close=settings["hide_tally_until_close"],
                max_choices=settings["max_choices"],
                allowed_role_ids=allowed_role_ids,
            )
        except discord.Forbidden:
            await interaction.followup.send(
                embed=discord.Embed(
                    title="Could not post poll",
                    description="I don't have permission to send messages in this channel.",
                    color=discord.Color.red(),
                ),
                ephemeral=True,
            )
            return

        confirm = discord.Embed(
            title="Poll created",
            description=(
                f"**ID:** `{poll.id}`\n"
                f"**Closes:** {discord.utils.format_dt(poll.closes_at, 'R')}"
            ),
            color=discord.Color.green(),
        )
        if warning_lines:
            confirm.add_field(
                name="Warnings",
                value="\n".join(warning_lines),
                inline=False,
            )
        await interaction.followup.send(embed=confirm, ephemeral=True)

    # ----- voting -----

    async def _handle_vote(
        self,
        interaction: discord.Interaction,
        poll_id: str,
        option_index: int,
    ) -> None:
        poll = await self.db.get_poll(poll_id)
        if poll is None:
            await interaction.response.send_message(
                "This poll no longer exists.", ephemeral=True
            )
            return
        if poll.status != "open":
            await interaction.response.send_message(
                "This poll is closed.", ephemeral=True
            )
            return
        if not (0 <= option_index < len(poll.options)):
            await interaction.response.send_message(
                "Invalid option.", ephemeral=True
            )
            return

        member = interaction.user
        if not isinstance(member, discord.Member) and interaction.guild is not None:
            member = interaction.guild.get_member(member.id) or member

        if poll.allowed_role_ids and isinstance(member, discord.Member):
            member_role_ids = {r.id for r in member.roles}
            if not member_role_ids.intersection(poll.allowed_role_ids):
                await interaction.response.send_message(
                    "You don't have a role allowed to vote in this poll.",
                    ephemeral=True,
                )
                return

        current = await self.db.get_user_votes(poll_id, member.id)

        max_choices = poll.max_choices
        if max_choices == 1:
            old = list(current)
            if option_index in current:
                # Toggle off — they're un-voting their single choice.
                await self.db.delete_user_votes(poll_id, member.id)
                feedback = "🗳️ Vote removed."
                self.bot.dispatch("poll_vote_removed", poll, member, option_index)
            else:
                await self.db.delete_user_votes(poll_id, member.id)
                await self.db.insert_vote(poll_id, member.id, option_index)
                feedback = f"✅ You voted for: **{poll.options[option_index]}**"
                if old:
                    self.bot.dispatch(
                        "poll_vote_changed", poll, member, old, [option_index]
                    )
                else:
                    self.bot.dispatch(
                        "poll_vote_cast", poll, member, option_index
                    )
        else:
            if option_index in current:
                await self.db.delete_vote(poll_id, member.id, option_index)
                new = [i for i in current if i != option_index]
                feedback = (
                    f"➖ Removed vote for **{poll.options[option_index]}** "
                    f"({len(new)}/{max_choices} used)"
                )
                self.bot.dispatch("poll_vote_removed", poll, member, option_index)
            else:
                if len(current) >= max_choices:
                    await interaction.response.send_message(
                        f"You've already used all {max_choices} of your votes. "
                        f"Click one of your existing choices to free a slot.",
                        ephemeral=True,
                    )
                    return
                await self.db.insert_vote(poll_id, member.id, option_index)
                new_count = len(current) + 1
                feedback = (
                    f"✅ Voted for **{poll.options[option_index]}** "
                    f"({new_count}/{max_choices} used)"
                )
                self.bot.dispatch("poll_vote_cast", poll, member, option_index)

        await interaction.response.send_message(feedback, ephemeral=True)
        self._schedule_tally_update(poll_id)

    # ----- tally update (debounced) -----

    def _schedule_tally_update(self, poll_id: str) -> None:
        if poll_id in self._tally_tasks:
            return
        self._tally_tasks[poll_id] = asyncio.create_task(
            self._do_tally_update(poll_id)
        )

    async def _do_tally_update(self, poll_id: str) -> None:
        try:
            await asyncio.sleep(TALLY_DEBOUNCE_S)
            poll = await self.db.get_poll(poll_id)
            if poll is None or poll.status != "open" or poll.message_id is None:
                return
            counts = await self.db.get_vote_counts(poll_id, len(poll.options))
            total_unique = len(await self.db.get_all_user_votes_map(poll_id))
            channel = self.bot.get_channel(poll.channel_id)
            if channel is None:
                return
            try:
                message = channel.get_partial_message(poll.message_id)
                embed = render_poll_embed(
                    poll, counts, total_unique_voters=total_unique
                )
                await message.edit(embed=embed)
            except discord.NotFound:
                log.warning("Poll message %s gone, skipping tally update", poll.message_id)
            except discord.HTTPException:
                log.exception("Failed editing poll message %s", poll.message_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Unexpected error in tally update for %s", poll_id)
        finally:
            self._tally_tasks.pop(poll_id, None)

    # ----- closing -----

    def _schedule_close(self, poll: Poll) -> None:
        if poll.id in self._close_tasks:
            self._close_tasks[poll.id].cancel()
        delay = (poll.closes_at - utcnow()).total_seconds()
        self._close_tasks[poll.id] = asyncio.create_task(
            self._close_after_delay(poll.id, max(0.0, delay))
        )

    async def _close_after_delay(self, poll_id: str, delay: float) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            poll = await self.db.get_poll(poll_id)
            if poll is None or poll.status != "open":
                return
            await self._close_poll(poll_id, reason="closed")
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Error in scheduled close for %s", poll_id)
        finally:
            self._close_tasks.pop(poll_id, None)

    async def _close_poll(self, poll_id: str, *, reason: str) -> Poll:
        """reason: 'closed' (natural) or 'cancelled' (early)."""
        poll = await self.db.get_poll(poll_id)
        if poll is None:
            raise PollNotFoundError(f"Poll {poll_id} not found.")
        if poll.status != "open":
            return poll

        status = "closed" if reason == "closed" else "cancelled"
        closed_at = utcnow()
        await self.db.mark_closed(poll_id, status=status, closed_at=closed_at)
        poll = replace(poll, status=status, closed_at=closed_at)

        # Cancel scheduled tasks (but never self-cancel — that would abort
        # the rest of this method when _close_poll is invoked from the
        # scheduled close task itself).
        current = asyncio.current_task()
        ct = self._close_tasks.pop(poll_id, None)
        if ct is not None and not ct.done() and ct is not current:
            ct.cancel()
        tt = self._tally_tasks.pop(poll_id, None)
        if tt is not None and not tt.done() and tt is not current:
            tt.cancel()

        # Render final state on the original message + post results message
        counts = await self.db.get_vote_counts(poll_id, len(poll.options))
        voters = (
            await self.db.get_voters_by_option(poll_id, len(poll.options))
            if not poll.hide_voters
            else None
        )

        channel = self.bot.get_channel(poll.channel_id)
        guild = self.bot.get_guild(poll.guild_id)

        def _name(uid: int) -> str:
            if guild is not None:
                m = guild.get_member(uid)
                if m is not None:
                    return m.display_name
            return f"<@{uid}>"

        if channel is not None and poll.message_id is not None:
            try:
                message = channel.get_partial_message(poll.message_id)
                final_embed = render_poll_embed(
                    poll,
                    counts,
                    voters_by_option=voters,
                    member_namer=_name,
                )
                await message.edit(embed=final_embed, view=make_disabled_view(poll))
            except discord.NotFound:
                log.warning("Original poll message %s missing on close", poll.message_id)
            except discord.HTTPException:
                log.exception("Failed updating poll message on close: %s", poll.message_id)

            try:
                await channel.send(
                    render_results_message(poll, counts),
                    reference=(
                        discord.MessageReference(
                            message_id=poll.message_id,
                            channel_id=poll.channel_id,
                            guild_id=poll.guild_id,
                            fail_if_not_exists=False,
                        )
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            except discord.HTTPException:
                log.exception("Failed sending results message for %s", poll_id)

        if reason == "closed":
            self.bot.dispatch("poll_closed", poll)
        else:
            self.bot.dispatch("poll_cancelled", poll)

        return poll

    async def _edit_poll_duration(
        self, poll_id: str, duration_seconds: int
    ) -> Poll:
        poll = await self.db.get_poll(poll_id)
        if poll is None:
            raise PollNotFoundError(f"Poll {poll_id} not found.")
        if poll.status != "open":
            raise PollClosedError(f"Poll {poll_id} is not open.")

        new_closes_at = poll.created_at + timedelta(seconds=duration_seconds)
        if new_closes_at <= utcnow():
            raise InvalidPollInputError(
                "New duration would put the close time in the past."
            )
        await self.db.update_closes_at(poll_id, new_closes_at)
        poll = replace(poll, closes_at=new_closes_at)
        self._schedule_close(poll)

        # Refresh embed to show new close time
        if poll.message_id is not None:
            channel = self.bot.get_channel(poll.channel_id)
            if channel is not None:
                try:
                    message = channel.get_partial_message(poll.message_id)
                    counts = await self.db.get_vote_counts(poll_id, len(poll.options))
                    total_unique = len(await self.db.get_all_user_votes_map(poll_id))
                    await message.edit(
                        embed=render_poll_embed(
                            poll, counts, total_unique_voters=total_unique
                        )
                    )
                except discord.HTTPException:
                    log.exception("Failed editing poll message on duration change")

        self.bot.dispatch("poll_edited", poll, ["duration"])
        return poll

    async def _delete_poll(self, poll_id: str) -> bool:
        poll = await self.db.get_poll(poll_id)
        if poll is None:
            return False

        ct = self._close_tasks.pop(poll_id, None)
        if ct is not None and not ct.done():
            ct.cancel()
        tt = self._tally_tasks.pop(poll_id, None)
        if tt is not None and not tt.done():
            tt.cancel()

        if poll.message_id is not None:
            channel = self.bot.get_channel(poll.channel_id)
            if channel is not None:
                try:
                    message = channel.get_partial_message(poll.message_id)
                    await message.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
                except discord.HTTPException:
                    log.exception("Failed deleting poll message %s", poll.message_id)

        await self.db.delete_poll(poll_id)
        self.bot.dispatch("poll_deleted", poll)
        return True

    # ----- permission helper -----

    @staticmethod
    def _can_manage(member: discord.Member, poll: Poll) -> bool:
        if member.id == poll.author_id:
            return True
        return bool(member.guild_permissions.moderate_members)

    # ----- slash commands -----

    poll_group = app_commands.Group(name="poll", description="Create and manage polls")

    @poll_group.command(name="create", description="Create a new poll")
    async def slash_poll_create(self, interaction: discord.Interaction) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Polls can only be created in a server.", ephemeral=True
            )
            return
        await interaction.response.send_modal(PollCreateModal(self))

    async def _autocomplete_polls(
        self,
        interaction: discord.Interaction,
        current: str,
        *,
        manage_only: bool,
        status: Optional[str] = "open",
    ) -> List[app_commands.Choice[str]]:
        if interaction.guild is None:
            return []
        polls = await self.db.list_polls(
            guild_id=interaction.guild.id, status=status
        )
        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else interaction.guild.get_member(interaction.user.id)
        )
        is_mod = bool(
            member is not None and member.guild_permissions.moderate_members
        )
        out: List[app_commands.Choice[str]] = []
        needle = current.lower()
        for poll in polls:
            if manage_only and not is_mod and poll.author_id != interaction.user.id:
                continue
            label = f"{poll.id}  •  {poll.question}"
            if len(label) > 100:
                label = label[:97] + "…"
            if needle and needle not in label.lower():
                continue
            out.append(app_commands.Choice(name=label, value=poll.id))
            if len(out) >= 25:
                break
        return out

    @poll_group.command(name="close", description="Close a poll early")
    @app_commands.describe(poll_id="The poll to close")
    async def slash_poll_close(
        self, interaction: discord.Interaction, poll_id: str
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Server only.", ephemeral=True
            )
            return
        poll = await self.db.get_poll(poll_id)
        if poll is None or poll.guild_id != interaction.guild.id:
            await interaction.response.send_message(
                "Poll not found.", ephemeral=True
            )
            return
        if poll.status != "open":
            await interaction.response.send_message(
                "Poll is already closed.", ephemeral=True
            )
            return
        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else interaction.guild.get_member(interaction.user.id)
        )
        if member is None or not self._can_manage(member, poll):
            await interaction.response.send_message(
                "You don't have permission to close this poll.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._close_poll(poll_id, reason="cancelled")
        await interaction.followup.send(
            f"Poll `{poll_id}` closed.", ephemeral=True
        )

    @slash_poll_close.autocomplete("poll_id")
    async def _ac_close(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[str]]:
        return await self._autocomplete_polls(
            interaction, current, manage_only=True, status="open"
        )

    @poll_group.command(name="edit", description="Change a poll's duration")
    @app_commands.describe(
        poll_id="The poll to edit",
        duration="New total duration from creation time, e.g. '1h', '2d', '1d12h'",
    )
    async def slash_poll_edit(
        self,
        interaction: discord.Interaction,
        poll_id: str,
        duration: str,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Server only.", ephemeral=True
            )
            return
        poll = await self.db.get_poll(poll_id)
        if poll is None or poll.guild_id != interaction.guild.id:
            await interaction.response.send_message(
                "Poll not found.", ephemeral=True
            )
            return
        if poll.status != "open":
            await interaction.response.send_message(
                "Poll is not open.", ephemeral=True
            )
            return
        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else interaction.guild.get_member(interaction.user.id)
        )
        if member is None or not self._can_manage(member, poll):
            await interaction.response.send_message(
                "You don't have permission to edit this poll.",
                ephemeral=True,
            )
            return
        try:
            seconds = parse_duration(duration)
        except InvalidPollInputError as e:
            await interaction.response.send_message(
                f"Invalid duration: {e}", ephemeral=True
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            updated = await self._edit_poll_duration(poll_id, seconds)
        except InvalidPollInputError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return
        await interaction.followup.send(
            f"Poll `{poll_id}` will now close "
            f"{discord.utils.format_dt(updated.closes_at, 'R')}.",
            ephemeral=True,
        )

    @slash_poll_edit.autocomplete("poll_id")
    async def _ac_edit(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[str]]:
        return await self._autocomplete_polls(
            interaction, current, manage_only=True, status="open"
        )

    @poll_group.command(name="list", description="List polls in this server")
    @app_commands.describe(status="Filter by status (default: open)")
    @app_commands.choices(
        status=[
            app_commands.Choice(name="open", value="open"),
            app_commands.Choice(name="closed", value="closed"),
            app_commands.Choice(name="cancelled", value="cancelled"),
            app_commands.Choice(name="all", value="all"),
        ]
    )
    async def slash_poll_list(
        self,
        interaction: discord.Interaction,
        status: Optional[app_commands.Choice[str]] = None,
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Server only.", ephemeral=True
            )
            return
        status_value = (status.value if status else "open")
        filter_status = None if status_value == "all" else status_value
        polls = await self.db.list_polls(
            guild_id=interaction.guild.id, status=filter_status
        )
        if not polls:
            await interaction.response.send_message(
                f"No {status_value} polls in this server.", ephemeral=True
            )
            return
        embed = discord.Embed(
            title=f"Polls in {interaction.guild.name} ({status_value})",
            color=discord.Color.blurple(),
        )
        for poll in polls[:25]:
            if poll.status == "open":
                state = f"ends {discord.utils.format_dt(poll.closes_at, 'R')}"
            else:
                when = poll.closed_at or poll.closes_at
                state = f"{poll.status} {discord.utils.format_dt(when, 'R')}"
            embed.add_field(
                name=f"`{poll.id}` — {poll.question[:60]}",
                value=f"by <@{poll.author_id}> · {state}",
                inline=False,
            )
        if len(polls) > 25:
            embed.set_footer(text=f"Showing 25 of {len(polls)} polls.")
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @poll_group.command(name="info", description="Show details for a poll")
    @app_commands.describe(poll_id="The poll to inspect")
    async def slash_poll_info(
        self, interaction: discord.Interaction, poll_id: str
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Server only.", ephemeral=True
            )
            return
        poll = await self.db.get_poll(poll_id)
        if poll is None or poll.guild_id != interaction.guild.id:
            await interaction.response.send_message(
                "Poll not found.", ephemeral=True
            )
            return
        counts = await self.db.get_vote_counts(poll_id, len(poll.options))
        voters = (
            await self.db.get_voters_by_option(poll_id, len(poll.options))
            if (poll.status != "open" or not poll.hide_tally_until_close)
            and not poll.hide_voters
            else None
        )
        guild = interaction.guild

        def _name(uid: int) -> str:
            m = guild.get_member(uid)
            return m.display_name if m else f"<@{uid}>"

        embed = render_poll_embed(
            poll,
            counts,
            voters_by_option=voters if voters and poll.status != "open" else None,
            member_namer=_name,
            total_unique_voters=len(
                await self.db.get_all_user_votes_map(poll_id)
            ),
        )
        embed.add_field(
            name="Created by",
            value=f"<@{poll.author_id}>",
            inline=True,
        )
        embed.add_field(
            name="Channel",
            value=f"<#{poll.channel_id}>",
            inline=True,
        )
        if poll.allowed_role_ids:
            embed.add_field(
                name="Allowed roles",
                value=", ".join(f"<@&{r}>" for r in poll.allowed_role_ids),
                inline=False,
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @slash_poll_info.autocomplete("poll_id")
    async def _ac_info(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[str]]:
        return await self._autocomplete_polls(
            interaction, current, manage_only=False, status=None
        )

    @poll_group.command(name="delete", description="Delete a poll permanently")
    @app_commands.describe(poll_id="The poll to delete")
    async def slash_poll_delete(
        self, interaction: discord.Interaction, poll_id: str
    ) -> None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "Server only.", ephemeral=True
            )
            return
        poll = await self.db.get_poll(poll_id)
        if poll is None or poll.guild_id != interaction.guild.id:
            await interaction.response.send_message(
                "Poll not found.", ephemeral=True
            )
            return
        member = (
            interaction.user
            if isinstance(interaction.user, discord.Member)
            else interaction.guild.get_member(interaction.user.id)
        )
        if member is None or not self._can_manage(member, poll):
            await interaction.response.send_message(
                "You don't have permission to delete this poll.",
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._delete_poll(poll_id)
        await interaction.followup.send(
            f"Poll `{poll_id}` deleted.", ephemeral=True
        )

    @slash_poll_delete.autocomplete("poll_id")
    async def _ac_delete(
        self, interaction: discord.Interaction, current: str
    ) -> List[app_commands.Choice[str]]:
        return await self._autocomplete_polls(
            interaction, current, manage_only=True, status=None
        )
