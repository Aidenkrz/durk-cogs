"""SQLite storage handler for Markov chain data."""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

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

            CREATE TABLE IF NOT EXISTS case_memory (
                word TEXT PRIMARY KEY,
                forms TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reverse_chain (
                state TEXT PRIMARY KEY,
                transitions TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS skip_chain (
                state TEXT PRIMARY KEY,
                transitions TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_chains (
                chain_order INTEGER NOT NULL,
                state TEXT NOT NULL,
                transitions TEXT NOT NULL,
                PRIMARY KEY (chain_order, state)
            );

            CREATE INDEX IF NOT EXISTS idx_user_chains_user
            ON user_chains(user_id);

            CREATE INDEX IF NOT EXISTS idx_order_chains_order
            ON order_chains(chain_order);
            """
        )
        await self._connection.commit()

    async def add_transitions(
        self,
        chain_data: Dict[Tuple[str, ...], Counter],
        user_id: Optional[int] = None,
    ) -> None:
        """Add transitions to the chain.

        Args:
            chain_data: Dictionary mapping states to Counter of transitions.
            user_id: If provided, add to user's chain. Otherwise, add to guild chain.
        """
        if not chain_data:
            return

        if user_id:
            await self._add_user_transitions(user_id, chain_data)
        else:
            await self._add_guild_transitions(chain_data)

    async def _add_guild_transitions(
        self, chain_data: Dict[Tuple[str, ...], Counter]
    ) -> None:
        """Add transitions to the guild chain."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            cursor = await self._connection.execute(
                "SELECT transitions FROM guild_chain WHERE state = ?", (state_key,)
            )
            row = await cursor.fetchone()

            if row:
                existing = Counter(json.loads(row[0]))
                existing.update(new_transitions)
                await self._connection.execute(
                    "UPDATE guild_chain SET transitions = ? WHERE state = ?",
                    (json.dumps(dict(existing)), state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO guild_chain (state, transitions) VALUES (?, ?)",
                    (state_key, json.dumps(dict(new_transitions))),
                )

        await self._connection.commit()

    async def _add_user_transitions(
        self, user_id: int, chain_data: Dict[Tuple[str, ...], Counter]
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
                existing = Counter(json.loads(row[0]))
                existing.update(new_transitions)
                await self._connection.execute(
                    "UPDATE user_chains SET transitions = ? WHERE user_id = ? AND state = ?",
                    (json.dumps(dict(existing)), user_id, state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO user_chains (user_id, state, transitions) VALUES (?, ?, ?)",
                    (user_id, state_key, json.dumps(dict(new_transitions))),
                )

        await self._connection.commit()

    async def add_reverse_transitions(
        self, chain_data: Dict[Tuple[str, ...], Counter]
    ) -> None:
        """Add reverse chain transitions."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            cursor = await self._connection.execute(
                "SELECT transitions FROM reverse_chain WHERE state = ?", (state_key,)
            )
            row = await cursor.fetchone()

            if row:
                existing = Counter(json.loads(row[0]))
                existing.update(new_transitions)
                await self._connection.execute(
                    "UPDATE reverse_chain SET transitions = ? WHERE state = ?",
                    (json.dumps(dict(existing)), state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO reverse_chain (state, transitions) VALUES (?, ?)",
                    (state_key, json.dumps(dict(new_transitions))),
                )

        await self._connection.commit()

    async def add_skip_transitions(
        self, chain_data: Dict[Tuple[str, str], Counter]
    ) -> None:
        """Add skip-gram transitions."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            cursor = await self._connection.execute(
                "SELECT transitions FROM skip_chain WHERE state = ?", (state_key,)
            )
            row = await cursor.fetchone()

            if row:
                existing = Counter(json.loads(row[0]))
                existing.update(new_transitions)
                await self._connection.execute(
                    "UPDATE skip_chain SET transitions = ? WHERE state = ?",
                    (json.dumps(dict(existing)), state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO skip_chain (state, transitions) VALUES (?, ?)",
                    (state_key, json.dumps(dict(new_transitions))),
                )

        await self._connection.commit()

    async def add_order_transitions(
        self, order: int, chain_data: Dict[Tuple[str, ...], Counter]
    ) -> None:
        """Add transitions for a specific order chain."""
        for state, new_transitions in chain_data.items():
            state_key = json.dumps(list(state))

            cursor = await self._connection.execute(
                "SELECT transitions FROM order_chains WHERE chain_order = ? AND state = ?",
                (order, state_key),
            )
            row = await cursor.fetchone()

            if row:
                existing = Counter(json.loads(row[0]))
                existing.update(new_transitions)
                await self._connection.execute(
                    "UPDATE order_chains SET transitions = ? WHERE chain_order = ? AND state = ?",
                    (json.dumps(dict(existing)), order, state_key),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO order_chains (chain_order, state, transitions) VALUES (?, ?, ?)",
                    (order, state_key, json.dumps(dict(new_transitions))),
                )

        await self._connection.commit()

    async def add_case_memory(self, case_data: Dict[str, Dict[str, int]]) -> None:
        """Add case memory data."""
        for word, forms in case_data.items():
            cursor = await self._connection.execute(
                "SELECT forms FROM case_memory WHERE word = ?", (word,)
            )
            row = await cursor.fetchone()

            if row:
                existing = Counter(json.loads(row[0]))
                existing.update(forms)
                await self._connection.execute(
                    "UPDATE case_memory SET forms = ? WHERE word = ?",
                    (json.dumps(dict(existing)), word),
                )
            else:
                await self._connection.execute(
                    "INSERT INTO case_memory (word, forms) VALUES (?, ?)",
                    (word, json.dumps(forms)),
                )

        await self._connection.commit()

    async def get_guild_chain(self) -> Dict[Tuple[str, ...], Counter]:
        """Get the full guild chain.

        Returns:
            Dictionary mapping states to Counter of transitions.
        """
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM guild_chain"
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): Counter(json.loads(row[1])) for row in rows}

    async def get_user_chain(self, user_id: int) -> Dict[Tuple[str, ...], Counter]:
        """Get a user's chain.

        Args:
            user_id: The user ID.

        Returns:
            Dictionary mapping states to Counter of transitions.
        """
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM user_chains WHERE user_id = ?", (user_id,)
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): Counter(json.loads(row[1])) for row in rows}

    async def get_reverse_chain(self) -> Dict[Tuple[str, ...], Counter]:
        """Get the reverse chain."""
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM reverse_chain"
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): Counter(json.loads(row[1])) for row in rows}

    async def get_skip_chain(self) -> Dict[Tuple[str, str], Counter]:
        """Get the skip-gram chain."""
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM skip_chain"
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): Counter(json.loads(row[1])) for row in rows}

    async def get_order_chain(self, order: int) -> Dict[Tuple[str, ...], Counter]:
        """Get a specific order chain."""
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM order_chains WHERE chain_order = ?", (order,)
        )
        rows = await cursor.fetchall()

        return {tuple(json.loads(row[0])): Counter(json.loads(row[1])) for row in rows}

    async def get_all_order_chains(self) -> Dict[int, Dict[Tuple[str, ...], Counter]]:
        """Get all order chains."""
        cursor = await self._connection.execute(
            "SELECT DISTINCT chain_order FROM order_chains"
        )
        orders = [row[0] for row in await cursor.fetchall()]

        result = {}
        for order in orders:
            result[order] = await self.get_order_chain(order)
        return result

    async def get_case_memory(self) -> Dict[str, Counter]:
        """Get case memory."""
        cursor = await self._connection.execute(
            "SELECT word, forms FROM case_memory"
        )
        rows = await cursor.fetchall()

        return {row[0]: Counter(json.loads(row[1])) for row in rows}

    async def increment_message_count(self, user_id: int) -> None:
        """Increment a user's message count."""
        await self._connection.execute(
            """
            INSERT INTO stats (user_id, message_count) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET message_count = message_count + 1
            """,
            (user_id,),
        )
        await self._connection.commit()

    async def get_stats(self) -> Dict:
        """Get chain statistics."""
        # Guild chain stats - count total transitions from Counter dicts
        cursor = await self._connection.execute(
            "SELECT state, transitions FROM guild_chain"
        )
        rows = await cursor.fetchall()
        state_count = len(rows)
        transition_count = sum(
            sum(json.loads(row[1]).values()) for row in rows
        )

        # Top contributors
        cursor = await self._connection.execute(
            "SELECT user_id, message_count FROM stats ORDER BY message_count DESC LIMIT 10"
        )
        top_contributors = await cursor.fetchall()

        # Unique words
        cursor = await self._connection.execute("SELECT COUNT(*) FROM case_memory")
        unique_words = (await cursor.fetchone())[0] or 0

        # Skip-gram count
        cursor = await self._connection.execute("SELECT COUNT(*) FROM skip_chain")
        skip_count = (await cursor.fetchone())[0] or 0

        return {
            "state_count": state_count,
            "transition_count": transition_count,
            "unique_words": unique_words,
            "skip_gram_count": skip_count,
            "top_contributors": [(row[0], row[1]) for row in top_contributors],
        }

    async def clear_all(self) -> None:
        """Clear all chain data for this guild."""
        await self._connection.executescript(
            """
            DELETE FROM guild_chain;
            DELETE FROM user_chains;
            DELETE FROM stats;
            DELETE FROM case_memory;
            DELETE FROM reverse_chain;
            DELETE FROM skip_chain;
            DELETE FROM order_chains;
            """
        )
        await self._connection.commit()

    async def clear_user(self, user_id: int) -> None:
        """Clear a specific user's chain data."""
        await self._connection.execute(
            "DELETE FROM user_chains WHERE user_id = ?", (user_id,)
        )
        await self._connection.execute("DELETE FROM stats WHERE user_id = ?", (user_id,))
        await self._connection.commit()
