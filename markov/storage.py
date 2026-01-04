"""SQLite storage handler for Markov chain data."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiosqlite

log = logging.getLogger("red.DurkCogs.Markov")


class MarkovStorage:
    """Async SQLite storage for Markov chain data."""

    def __init__(self, data_path: Path, guild_id: int):
        """Initialize storage for a guild.

        Args:
            data_path: Base path for cog data.
            guild_id: The guild ID.
        """
        self.guild_id = guild_id
        self.db_path = data_path / f"{guild_id}.db"
        self._connection: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """Initialize the database and create tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = await aiosqlite.connect(self.db_path)
        await self._create_tables()

    async def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        await self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS guild_chain (
                state TEXT PRIMARY KEY,
                transitions TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_chains (
                user_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                transitions TEXT NOT NULL,
                PRIMARY KEY (user_id, state)
            );

            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER PRIMARY KEY,
                message_count INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_user_chains_user
            ON user_chains(user_id);
            """
        )
        await self._connection.commit()

    async def add_transitions(
        self,
        chain_data: Dict[Tuple[str, ...], List[str]],
        user_id: Optional[int] = None,
    ) -> None:
        """Add transitions to the chain.

        Args:
            chain_data: Dictionary mapping states to transitions.
            user_id: If provided, add to user's chain. Otherwise, add to guild chain.
        """
        if not chain_data:
            return

        if user_id:
            await self._add_user_transitions(user_id, chain_data)
        else:
            await self._add_guild_transitions(chain_data)

    async def _add_guild_transitions(
        self, chain_data: Dict[Tuple[str, ...], List[str]]
    ) -> None:
        """Add transitions to the guild chain."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            # Get existing transitions
            cursor = await self._connection.execute(
                "SELECT transitions FROM guild_chain WHERE state = ?", (state_key,)
            )
            row = await cursor.fetchone()

            if row:
                existing = json.loads(row[0])
                existing.extend(new_transitions)
                await self._connection.execute(
                    "UPDATE guild_chain SET transitions = ? WHERE state = ?",
                    (json.dumps(existing), state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO guild_chain (state, transitions) VALUES (?, ?)",
                    (state_key, json.dumps(new_transitions)),
                )

        await self._connection.commit()

    async def _add_user_transitions(
        self, user_id: int, chain_data: Dict[Tuple[str, ...], List[str]]
    ) -> None:
        """Add transitions to a user's chain."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            cursor = await self._connection.execute(
                "SELECT transitions FROM user_chains WHERE user_id = ? AND state = ?",
                (user_id, state_key),
            )
            row = await cursor.fetchone()

            if row:
                existing = json.loads(row[0])
                existing.extend(new_transitions)
                await self._connection.execute(
                    "UPDATE user_chains SET transitions = ? WHERE user_id = ? AND state = ?",
                    (json.dumps(existing), user_id, state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO user_chains (user_id, state, transitions) VALUES (?, ?, ?)",
                    (user_id, state_key, json.dumps(new_transitions)),
                )

        await self._connection.commit()

    async def get_guild_chain(self) -> Dict[Tuple[str, ...], List[str]]:
        """Get the full guild chain.

        Returns:
            Dictionary mapping states to transitions.
        """
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM guild_chain"
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): json.loads(row[1]) for row in rows}

    async def get_user_chain(self, user_id: int) -> Dict[Tuple[str, ...], List[str]]:
        """Get a user's chain.

        Args:
            user_id: The user ID.

        Returns:
            Dictionary mapping states to transitions.
        """
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM user_chains WHERE user_id = ?", (user_id,)
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): json.loads(row[1]) for row in rows}

    async def increment_message_count(self, user_id: int) -> None:
        """Increment a user's message count.

        Args:
            user_id: The user ID.
        """
        await self._connection.execute(
            """
            INSERT INTO stats (user_id, message_count) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET message_count = message_count + 1
            """,
            (user_id,),
        )
        await self._connection.commit()

    async def get_stats(self) -> Dict:
        """Get chain statistics.

        Returns:
            Dictionary with guild stats and top contributors.
        """
        # Guild chain stats
        cursor = await self._connection.execute(
            "SELECT COUNT(*), SUM(json_array_length(transitions)) FROM guild_chain"
        )
        row = await cursor.fetchone()
        state_count = row[0] or 0
        transition_count = row[1] or 0

        # Top contributors
        cursor = await self._connection.execute(
            "SELECT user_id, message_count FROM stats ORDER BY message_count DESC LIMIT 10"
        )
        top_contributors = await cursor.fetchall()

        return {
            "state_count": state_count,
            "transition_count": transition_count,
            "top_contributors": [(row[0], row[1]) for row in top_contributors],
        }

    async def clear_all(self) -> None:
        """Clear all chain data for this guild."""
        await self._connection.executescript(
            """
            DELETE FROM guild_chain;
            DELETE FROM user_chains;
            DELETE FROM stats;
            """
        )
        await self._connection.commit()

    async def clear_user(self, user_id: int) -> None:
        """Clear a specific user's chain data.

        Args:
            user_id: The user ID to clear.
        """
        await self._connection.execute(
            "DELETE FROM user_chains WHERE user_id = ?", (user_id,)
        )
        await self._connection.execute("DELETE FROM stats WHERE user_id = ?", (user_id,))
        await self._connection.commit()
