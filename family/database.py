import aiosqlite
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

log = logging.getLogger("red.DurkCogs.Family.database")


class FamilyDatabase:
    """SQLite database handler for the Family cog."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        """Initialize the database and create tables."""
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self._create_tables()
        log.info(f"Family database initialized at {self.db_path}")

    async def close(self):
        """Close the database connection."""
        if self.db:
            await self.db.close()
            log.info("Family database connection closed")

    async def _create_tables(self):
        """Create all required tables."""
        await self.db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                display_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS marriages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user1_id INTEGER NOT NULL,
                user2_id INTEGER NOT NULL,
                married_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user1_id) REFERENCES users(user_id),
                FOREIGN KEY (user2_id) REFERENCES users(user_id),
                UNIQUE(user1_id, user2_id),
                CHECK(user1_id < user2_id)
            );

            CREATE TABLE IF NOT EXISTS parent_child (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id INTEGER NOT NULL,
                child_id INTEGER NOT NULL,
                relationship_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES users(user_id),
                FOREIGN KEY (child_id) REFERENCES users(user_id),
                UNIQUE(parent_id, child_id)
            );

            CREATE TABLE IF NOT EXISTS pending_proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proposal_type TEXT NOT NULL,
                proposer_id INTEGER NOT NULL,
                target_id INTEGER NOT NULL,
                child_id INTEGER,
                message_id INTEGER,
                channel_id INTEGER,
                guild_id INTEGER,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (proposer_id) REFERENCES users(user_id),
                FOREIGN KEY (target_id) REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_marriages_user1 ON marriages(user1_id);
            CREATE INDEX IF NOT EXISTS idx_marriages_user2 ON marriages(user2_id);
            CREATE INDEX IF NOT EXISTS idx_parent_child_parent ON parent_child(parent_id);
            CREATE INDEX IF NOT EXISTS idx_parent_child_child ON parent_child(child_id);
            CREATE INDEX IF NOT EXISTS idx_proposals_target ON pending_proposals(target_id);
            CREATE INDEX IF NOT EXISTS idx_proposals_expires ON pending_proposals(expires_at);
        """)
        await self.db.commit()

    # === User Operations ===

    async def ensure_user(self, user_id: int, display_name: str = None):
        """Ensure a user exists in the database."""
        await self.db.execute(
            """INSERT INTO users (user_id, display_name) VALUES (?, ?)
               ON CONFLICT(user_id) DO UPDATE SET
               display_name = COALESCE(excluded.display_name, display_name),
               updated_at = CURRENT_TIMESTAMP""",
            (user_id, display_name)
        )
        await self.db.commit()

    # === Marriage Operations ===

    async def get_spouses(self, user_id: int) -> List[int]:
        """Get all spouse IDs for a user."""
        async with self.db.execute("""
            SELECT CASE WHEN user1_id = ? THEN user2_id ELSE user1_id END as spouse_id
            FROM marriages
            WHERE user1_id = ? OR user2_id = ?
        """, (user_id, user_id, user_id)) as cursor:
            rows = await cursor.fetchall()
            return [row["spouse_id"] for row in rows]

    async def are_married(self, user1_id: int, user2_id: int) -> bool:
        """Check if two users are married."""
        low, high = min(user1_id, user2_id), max(user1_id, user2_id)
        async with self.db.execute(
            "SELECT 1 FROM marriages WHERE user1_id = ? AND user2_id = ?",
            (low, high)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def create_marriage(self, user1_id: int, user2_id: int):
        """Create a marriage between two users."""
        low, high = min(user1_id, user2_id), max(user1_id, user2_id)
        await self.ensure_user(low)
        await self.ensure_user(high)
        await self.db.execute(
            "INSERT INTO marriages (user1_id, user2_id) VALUES (?, ?)",
            (low, high)
        )
        await self.db.commit()

    async def delete_marriage(self, user1_id: int, user2_id: int) -> bool:
        """Delete a marriage. Returns True if a marriage was deleted."""
        low, high = min(user1_id, user2_id), max(user1_id, user2_id)
        cursor = await self.db.execute(
            "DELETE FROM marriages WHERE user1_id = ? AND user2_id = ?",
            (low, high)
        )
        await self.db.commit()
        return cursor.rowcount > 0

    async def get_marriage_count(self, user_id: int) -> int:
        """Get the number of marriages for a user."""
        async with self.db.execute("""
            SELECT COUNT(*) as count FROM marriages
            WHERE user1_id = ? OR user2_id = ?
        """, (user_id, user_id)) as cursor:
            row = await cursor.fetchone()
            return row["count"] if row else 0

    # === Parent-Child Operations ===

    async def get_parents(self, child_id: int) -> List[int]:
        """Get all parent IDs for a child."""
        async with self.db.execute(
            "SELECT parent_id FROM parent_child WHERE child_id = ?",
            (child_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [row["parent_id"] for row in rows]

    async def get_children(self, parent_id: int) -> List[int]:
        """Get all child IDs for a parent."""
        async with self.db.execute(
            "SELECT child_id FROM parent_child WHERE parent_id = ?",
            (parent_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [row["child_id"] for row in rows]

    async def get_siblings(self, user_id: int) -> List[int]:
        """Get all sibling IDs (share at least one parent)."""
        async with self.db.execute("""
            SELECT DISTINCT pc2.child_id as sibling_id
            FROM parent_child pc1
            JOIN parent_child pc2 ON pc1.parent_id = pc2.parent_id
            WHERE pc1.child_id = ? AND pc2.child_id != ?
        """, (user_id, user_id)) as cursor:
            rows = await cursor.fetchall()
            return [row["sibling_id"] for row in rows]

    async def get_parent_count(self, child_id: int) -> int:
        """Get the number of parents for a child."""
        async with self.db.execute(
            "SELECT COUNT(*) as count FROM parent_child WHERE child_id = ?",
            (child_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row["count"] if row else 0

    async def is_parent_of(self, parent_id: int, child_id: int) -> bool:
        """Check if user is a parent of the child."""
        async with self.db.execute(
            "SELECT 1 FROM parent_child WHERE parent_id = ? AND child_id = ?",
            (parent_id, child_id)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def create_parent_child(self, parent_id: int, child_id: int, relationship_type: str):
        """Create a parent-child relationship."""
        await self.ensure_user(parent_id)
        await self.ensure_user(child_id)
        await self.db.execute(
            "INSERT INTO parent_child (parent_id, child_id, relationship_type) VALUES (?, ?, ?)",
            (parent_id, child_id, relationship_type)
        )
        await self.db.commit()

    async def delete_parent_child(self, parent_id: int, child_id: int) -> bool:
        """Delete a parent-child relationship. Returns True if deleted."""
        cursor = await self.db.execute(
            "DELETE FROM parent_child WHERE parent_id = ? AND child_id = ?",
            (parent_id, child_id)
        )
        await self.db.commit()
        return cursor.rowcount > 0

    # === Relationship Traversal ===

    async def get_all_relatives(self, user_id: int, visited: set = None) -> set:
        """
        Get all user IDs that are related to the given user.
        Traverses parents, children, and siblings recursively.
        """
        if visited is None:
            visited = set()

        if user_id in visited:
            return visited

        visited.add(user_id)

        # Get parents
        parents = await self.get_parents(user_id)
        for parent_id in parents:
            await self.get_all_relatives(parent_id, visited)

        # Get children
        children = await self.get_children(user_id)
        for child_id in children:
            await self.get_all_relatives(child_id, visited)

        # Get siblings
        siblings = await self.get_siblings(user_id)
        for sibling_id in siblings:
            visited.add(sibling_id)

        return visited

    async def are_related(self, user1_id: int, user2_id: int) -> bool:
        """Check if two users are related (share family tree)."""
        relatives = await self.get_all_relatives(user1_id)
        return user2_id in relatives

    async def get_relationship_type(self, user1_id: int, user2_id: int) -> Optional[str]:
        """
        Get the relationship type between two users.
        Returns: 'spouse', 'parent', 'child', 'sibling', or None
        """
        # Check spouse
        if await self.are_married(user1_id, user2_id):
            return "spouse"

        # Check if user2 is parent of user1
        if user2_id in await self.get_parents(user1_id):
            return "parent"

        # Check if user2 is child of user1
        if user2_id in await self.get_children(user1_id):
            return "child"

        # Check siblings
        if user2_id in await self.get_siblings(user1_id):
            return "sibling"

        return None

    # === Proposal Operations ===

    async def create_proposal(
        self,
        proposal_type: str,
        proposer_id: int,
        target_id: int,
        message_id: int,
        channel_id: int,
        guild_id: int,
        expires_at: float,
        child_id: int = None
    ) -> int:
        """Create a pending proposal and return its ID."""
        await self.ensure_user(proposer_id)
        await self.ensure_user(target_id)
        if child_id:
            await self.ensure_user(child_id)

        cursor = await self.db.execute("""
            INSERT INTO pending_proposals
            (proposal_type, proposer_id, target_id, child_id,
             message_id, channel_id, guild_id, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'))
        """, (proposal_type, proposer_id, target_id, child_id,
              message_id, channel_id, guild_id, expires_at))
        await self.db.commit()
        return cursor.lastrowid

    async def get_proposal(self, proposal_id: int) -> Optional[Dict[str, Any]]:
        """Get a proposal by ID."""
        async with self.db.execute(
            "SELECT * FROM pending_proposals WHERE id = ?",
            (proposal_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def get_proposal_by_message(self, message_id: int) -> Optional[Dict[str, Any]]:
        """Get a proposal by message ID."""
        async with self.db.execute(
            "SELECT * FROM pending_proposals WHERE message_id = ?",
            (message_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def delete_proposal(self, proposal_id: int):
        """Delete a proposal."""
        await self.db.execute("DELETE FROM pending_proposals WHERE id = ?", (proposal_id,))
        await self.db.commit()

    async def get_expired_proposals(self) -> List[Dict[str, Any]]:
        """Get all expired proposals."""
        async with self.db.execute("""
            SELECT * FROM pending_proposals
            WHERE expires_at < datetime('now')
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_pending_proposals_for_user(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all pending proposals where user is the target."""
        async with self.db.execute("""
            SELECT * FROM pending_proposals
            WHERE target_id = ? AND expires_at > datetime('now')
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def has_pending_proposal(self, proposer_id: int, target_id: int, proposal_type: str) -> bool:
        """Check if there's already a pending proposal of this type."""
        async with self.db.execute("""
            SELECT 1 FROM pending_proposals
            WHERE proposer_id = ? AND target_id = ? AND proposal_type = ?
            AND expires_at > datetime('now')
        """, (proposer_id, target_id, proposal_type)) as cursor:
            return await cursor.fetchone() is not None

    # === Statistics ===

    async def get_total_marriages(self) -> int:
        """Get total number of marriages."""
        async with self.db.execute("SELECT COUNT(*) as count FROM marriages") as cursor:
            row = await cursor.fetchone()
            return row["count"] if row else 0

    async def get_total_parent_child(self) -> int:
        """Get total number of parent-child relationships."""
        async with self.db.execute("SELECT COUNT(*) as count FROM parent_child") as cursor:
            row = await cursor.fetchone()
            return row["count"] if row else 0

    async def get_family_size(self, user_id: int) -> int:
        """Get the size of a user's family tree."""
        relatives = await self.get_all_relatives(user_id)
        # Include spouses
        spouses = await self.get_spouses(user_id)
        relatives.update(spouses)
        return len(relatives)

    async def reset_all(self):
        """Delete all family data (marriages, parent-child relationships, proposals)."""
        await self.db.execute("DELETE FROM marriages")
        await self.db.execute("DELETE FROM parent_child")
        await self.db.execute("DELETE FROM pending_proposals")
        await self.db.execute("DELETE FROM users")
        await self.db.commit()
