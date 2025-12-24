import aiosqlite
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Set
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

            CREATE TABLE IF NOT EXISTS family_profiles (
                user_id INTEGER PRIMARY KEY,
                family_title TEXT,
                family_motto TEXT,
                family_crest_url TEXT,
                family_owner_id INTEGER,
                looking_for_match INTEGER DEFAULT 0,
                match_bio TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (family_owner_id) REFERENCES users(user_id)
            );

            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                banned_by INTEGER NOT NULL,
                reason TEXT,
                banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (banned_by) REFERENCES users(user_id)
            );
        """)
        await self.db.commit()

        # Migration: Add family_owner_id column if it doesn't exist
        try:
            await self.db.execute("ALTER TABLE family_profiles ADD COLUMN family_owner_id INTEGER")
            await self.db.commit()
        except Exception:
            pass  # Column already exists

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

    async def get_all_users_with_relations(self) -> set:
        """Get all user IDs that have at least one family relation."""
        users = set()

        # Users in marriages
        async with self.db.execute("SELECT user1_id, user2_id FROM marriages") as cursor:
            async for row in cursor:
                users.add(row[0])
                users.add(row[1])

        # Users in parent-child relationships
        async with self.db.execute("SELECT parent_id, child_id FROM parent_child") as cursor:
            async for row in cursor:
                users.add(row[0])
                users.add(row[1])

        return users

    async def reset_all(self):
        """Delete all family data (marriages, parent-child relationships, proposals)."""
        await self.db.execute("DELETE FROM marriages")
        await self.db.execute("DELETE FROM parent_child")
        await self.db.execute("DELETE FROM pending_proposals")
        await self.db.execute("DELETE FROM family_profiles")
        await self.db.execute("DELETE FROM users")
        await self.db.commit()

    # === Family Profile Operations ===

    async def get_family_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get a user's family profile."""
        async with self.db.execute(
            "SELECT * FROM family_profiles WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def set_family_title(self, user_id: int, title: Optional[str]):
        """Set a user's family title (surname, dynasty name, etc.)."""
        # When setting a title, user becomes owner if they don't have one already
        await self.db.execute("""
            INSERT INTO family_profiles (user_id, family_title, family_owner_id, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                family_title = excluded.family_title,
                family_owner_id = COALESCE(family_profiles.family_owner_id, excluded.family_owner_id),
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, title, user_id))
        await self.db.commit()

    async def set_family_motto(self, user_id: int, motto: Optional[str]):
        """Set a user's family motto."""
        # When setting a motto, user becomes owner if they don't have one already
        await self.db.execute("""
            INSERT INTO family_profiles (user_id, family_motto, family_owner_id, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                family_motto = excluded.family_motto,
                family_owner_id = COALESCE(family_profiles.family_owner_id, excluded.family_owner_id),
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, motto, user_id))
        await self.db.commit()

    async def set_family_crest(self, user_id: int, crest_url: Optional[str]):
        """Set a user's family crest URL."""
        # When setting a crest, user becomes owner if they don't have one already
        await self.db.execute("""
            INSERT INTO family_profiles (user_id, family_crest_url, family_owner_id, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                family_crest_url = excluded.family_crest_url,
                family_owner_id = COALESCE(family_profiles.family_owner_id, excluded.family_owner_id),
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, crest_url, user_id))
        await self.db.commit()

    async def set_looking_for_match(self, user_id: int, looking: bool, bio: Optional[str] = None):
        """Set whether a user is looking for a match and their bio."""
        await self.db.execute("""
            INSERT INTO family_profiles (user_id, looking_for_match, match_bio, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                looking_for_match = excluded.looking_for_match,
                match_bio = COALESCE(excluded.match_bio, family_profiles.match_bio),
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, 1 if looking else 0, bio))
        await self.db.commit()

    async def get_singles_looking(self) -> List[Dict[str, Any]]:
        """Get all users who are looking for a match and have no spouses."""
        async with self.db.execute("""
            SELECT fp.user_id, fp.match_bio, fp.family_title
            FROM family_profiles fp
            WHERE fp.looking_for_match = 1
            AND NOT EXISTS (
                SELECT 1 FROM marriages m
                WHERE m.user1_id = fp.user_id OR m.user2_id = fp.user_id
            )
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def inherit_family_profile(self, child_id: int, parent_id: int):
        """Have a child inherit the family title and crest from a parent (if they don't have their own)."""
        parent_profile = await self.get_family_profile(parent_id)
        if not parent_profile:
            return

        child_profile = await self.get_family_profile(child_id)

        # Only inherit if parent has values and child doesn't
        title_to_set = None
        crest_to_set = None
        motto_to_set = None

        if parent_profile.get("family_title"):
            if not child_profile or not child_profile.get("family_title"):
                title_to_set = parent_profile["family_title"]

        if parent_profile.get("family_crest_url"):
            if not child_profile or not child_profile.get("family_crest_url"):
                crest_to_set = parent_profile["family_crest_url"]

        if parent_profile.get("family_motto"):
            if not child_profile or not child_profile.get("family_motto"):
                motto_to_set = parent_profile["family_motto"]

        # Apply inheritance
        if title_to_set or crest_to_set or motto_to_set:
            await self.db.execute("""
                INSERT INTO family_profiles (user_id, family_title, family_crest_url, family_motto, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    family_title = COALESCE(family_profiles.family_title, excluded.family_title),
                    family_crest_url = COALESCE(family_profiles.family_crest_url, excluded.family_crest_url),
                    family_motto = COALESCE(family_profiles.family_motto, excluded.family_motto),
                    updated_at = CURRENT_TIMESTAMP
            """, (child_id, title_to_set, crest_to_set, motto_to_set))
            await self.db.commit()

    async def get_all_descendants(self, user_id: int) -> List[int]:
        """Get all descendants (children, grandchildren, etc.) of a user recursively."""
        descendants = []
        to_process = [user_id]
        processed = set()

        while to_process:
            current = to_process.pop(0)
            if current in processed:
                continue
            processed.add(current)

            children = await self.get_children(current)
            for child_id in children:
                if child_id not in processed and child_id not in descendants:
                    descendants.append(child_id)
                    to_process.append(child_id)

        return descendants

    async def propagate_family_profile(self, user_id: int) -> int:
        """Propagate user's family profile to all descendants who don't have their own.
        Returns the number of descendants updated."""
        profile = await self.get_family_profile(user_id)
        if not profile:
            return 0

        # User is the owner (either explicitly or because they set the profile)
        owner_id = profile.get("family_owner_id") or user_id

        descendants = await self.get_all_descendants(user_id)
        updated_count = 0

        for descendant_id in descendants:
            descendant_profile = await self.get_family_profile(descendant_id)

            # Check what needs to be inherited (including owner)
            needs_update = False
            if not descendant_profile or not descendant_profile.get("family_owner_id"):
                needs_update = True
            if profile.get("family_title") and (not descendant_profile or not descendant_profile.get("family_title")):
                needs_update = True
            if profile.get("family_crest_url") and (not descendant_profile or not descendant_profile.get("family_crest_url")):
                needs_update = True
            if profile.get("family_motto") and (not descendant_profile or not descendant_profile.get("family_motto")):
                needs_update = True

            if needs_update:
                await self.inherit_family_profile_with_owner(descendant_id, user_id)
                updated_count += 1

        return updated_count

    async def find_relationship_path(self, user1_id: int, user2_id: int) -> Optional[List[dict]]:
        """
        Find the relationship path between two users using BFS.
        Returns a list of steps like [{'user_id': X, 'relation': 'parent'}, ...] or None if not connected.
        """
        if user1_id == user2_id:
            return [{'user_id': user1_id, 'relation': 'self'}]

        # BFS to find path
        from collections import deque

        visited = {user1_id}
        queue = deque([(user1_id, [{'user_id': user1_id, 'relation': 'start'}])])

        while queue:
            current_id, path = queue.popleft()

            # Get all connections
            connections = []

            # Spouses
            spouses = await self.get_spouses(current_id)
            for spouse_id in spouses:
                connections.append((spouse_id, 'spouse'))

            # Parents
            parents = await self.get_parents(current_id)
            for parent_id in parents:
                connections.append((parent_id, 'parent'))

            # Children
            children = await self.get_children(current_id)
            for child_id in children:
                connections.append((child_id, 'child'))

            # Siblings
            siblings = await self.get_siblings(current_id)
            for sibling_id in siblings:
                connections.append((sibling_id, 'sibling'))

            for next_id, relation in connections:
                if next_id == user2_id:
                    # Found the target
                    return path + [{'user_id': next_id, 'relation': relation}]

                if next_id not in visited:
                    visited.add(next_id)
                    queue.append((next_id, path + [{'user_id': next_id, 'relation': relation}]))

        return None  # Not connected

    async def get_all_connected_users(self, user_id: int) -> Set[int]:
        """Get all users connected to this user through any family relationship."""
        from collections import deque

        connected = set()
        queue = deque([user_id])

        while queue:
            current_id = queue.popleft()
            if current_id in connected:
                continue
            connected.add(current_id)

            # Get all connections
            spouses = await self.get_spouses(current_id)
            parents = await self.get_parents(current_id)
            children = await self.get_children(current_id)
            siblings = await self.get_siblings(current_id)

            for next_id in spouses + parents + children + siblings:
                if next_id not in connected:
                    queue.append(next_id)

        return connected

    # === Family Ownership Methods ===

    async def get_family_owner(self, user_id: int) -> Optional[int]:
        """Get the owner ID of a user's family profile."""
        async with self.db.execute(
            "SELECT family_owner_id FROM family_profiles WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                return row[0]
        return None

    async def set_family_owner(self, user_id: int, owner_id: int):
        """Set the owner of a user's family profile."""
        await self.db.execute("""
            UPDATE family_profiles SET family_owner_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (owner_id, user_id))
        await self.db.commit()

    async def get_family_members(self, owner_id: int) -> List[int]:
        """Get all users who belong to a family owned by owner_id."""
        async with self.db.execute(
            "SELECT user_id FROM family_profiles WHERE family_owner_id = ?",
            (owner_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def remove_from_family(self, user_id: int):
        """Remove a user from their family (clear family profile ownership, keep matchmaking data)."""
        await self.db.execute("""
            UPDATE family_profiles
            SET family_title = NULL, family_motto = NULL, family_crest_url = NULL,
                family_owner_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
        """, (user_id,))
        await self.db.commit()

    async def cleanup_disconnected_family_members(self, owner_id: int) -> List[int]:
        """
        Remove family membership from anyone not connected to the owner.
        Returns list of user IDs that were removed.
        """
        # Get all users connected to the owner
        connected = await self.get_all_connected_users(owner_id)

        # Get all current family members
        members = await self.get_family_members(owner_id)

        # Find members who are no longer connected
        disconnected = [m for m in members if m not in connected]

        # Remove disconnected members
        for user_id in disconnected:
            await self.remove_from_family(user_id)

        return disconnected

    async def inherit_family_profile_with_owner(self, child_id: int, parent_id: int):
        """
        Have a child inherit family profile from a parent, including the owner reference.
        Only inherits if child doesn't already have a family owner.
        """
        parent_profile = await self.get_family_profile(parent_id)
        if not parent_profile:
            return False

        child_profile = await self.get_family_profile(child_id)

        # Don't override if child already has a family owner
        if child_profile and child_profile.get("family_owner_id"):
            return False

        parent_owner = parent_profile.get("family_owner_id") or parent_id

        # Only inherit if parent has profile content
        if not any([parent_profile.get("family_title"), parent_profile.get("family_crest_url"), parent_profile.get("family_motto")]):
            return False

        title = parent_profile.get("family_title")
        crest = parent_profile.get("family_crest_url")
        motto = parent_profile.get("family_motto")

        await self.db.execute("""
            INSERT INTO family_profiles (user_id, family_title, family_crest_url, family_motto, family_owner_id, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                family_title = COALESCE(family_profiles.family_title, excluded.family_title),
                family_crest_url = COALESCE(family_profiles.family_crest_url, excluded.family_crest_url),
                family_motto = COALESCE(family_profiles.family_motto, excluded.family_motto),
                family_owner_id = COALESCE(family_profiles.family_owner_id, excluded.family_owner_id),
                updated_at = CURRENT_TIMESTAMP
        """, (child_id, title, crest, motto, parent_owner))
        await self.db.commit()
        return True

    # === Ban Operations ===

    async def ban_user(self, user_id: int, banned_by: int, reason: str = None):
        """Ban a user from using the family system."""
        await self.db.execute("""
            INSERT INTO banned_users (user_id, banned_by, reason)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                banned_by = excluded.banned_by,
                reason = excluded.reason,
                banned_at = CURRENT_TIMESTAMP
        """, (user_id, banned_by, reason))
        await self.db.commit()

    async def unban_user(self, user_id: int) -> bool:
        """Unban a user. Returns True if user was banned."""
        cursor = await self.db.execute(
            "DELETE FROM banned_users WHERE user_id = ?",
            (user_id,)
        )
        await self.db.commit()
        return cursor.rowcount > 0

    async def is_banned(self, user_id: int) -> bool:
        """Check if a user is banned."""
        async with self.db.execute(
            "SELECT 1 FROM banned_users WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def get_ban_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get ban information for a user."""
        async with self.db.execute(
            "SELECT * FROM banned_users WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def get_all_bans(self) -> List[Dict[str, Any]]:
        """Get all banned users."""
        async with self.db.execute("SELECT * FROM banned_users ORDER BY banned_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # === User Data Deletion ===

    async def delete_all_user_connections(self, user_id: int) -> Dict[str, int]:
        """Delete all connections for a user. Returns counts of deleted items."""
        counts = {}

        # Delete marriages
        cursor = await self.db.execute(
            "DELETE FROM marriages WHERE user1_id = ? OR user2_id = ?",
            (user_id, user_id)
        )
        counts["marriages"] = cursor.rowcount

        # Delete parent-child where user is parent
        cursor = await self.db.execute(
            "DELETE FROM parent_child WHERE parent_id = ?",
            (user_id,)
        )
        counts["children_removed"] = cursor.rowcount

        # Delete parent-child where user is child
        cursor = await self.db.execute(
            "DELETE FROM parent_child WHERE child_id = ?",
            (user_id,)
        )
        counts["parents_removed"] = cursor.rowcount

        # Delete pending proposals involving user
        cursor = await self.db.execute(
            "DELETE FROM pending_proposals WHERE proposer_id = ? OR target_id = ?",
            (user_id, user_id)
        )
        counts["proposals"] = cursor.rowcount

        # Clear family profile
        cursor = await self.db.execute(
            "DELETE FROM family_profiles WHERE user_id = ?",
            (user_id,)
        )
        counts["profile"] = cursor.rowcount

        await self.db.commit()
        return counts

    # === Global Cleanup ===

    async def cleanup_all_orphaned_profiles(self) -> int:
        """
        Remove family profiles for users who have no family connections.
        Returns count of profiles removed.
        """
        # Get all users with family connections
        connected_users = await self.get_all_users_with_relations()

        # Delete profiles for users not in connected_users
        if connected_users:
            placeholders = ",".join("?" * len(connected_users))
            cursor = await self.db.execute(f"""
                DELETE FROM family_profiles
                WHERE user_id NOT IN ({placeholders})
                AND (family_title IS NOT NULL OR family_motto IS NOT NULL OR family_crest_url IS NOT NULL)
            """, tuple(connected_users))
        else:
            # No connected users, delete all profiles with content
            cursor = await self.db.execute("""
                DELETE FROM family_profiles
                WHERE family_title IS NOT NULL OR family_motto IS NOT NULL OR family_crest_url IS NOT NULL
            """)

        await self.db.commit()
        return cursor.rowcount

    async def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the family system."""
        stats = {}

        # Total marriages
        async with self.db.execute("SELECT COUNT(*) FROM marriages") as cursor:
            stats["total_marriages"] = (await cursor.fetchone())[0]

        # Total parent-child relationships
        async with self.db.execute("SELECT COUNT(*) FROM parent_child") as cursor:
            stats["total_parent_child"] = (await cursor.fetchone())[0]

        # Total unique users with relations
        users = await self.get_all_users_with_relations()
        stats["unique_users"] = len(users)

        # Total family profiles
        async with self.db.execute("SELECT COUNT(*) FROM family_profiles WHERE family_title IS NOT NULL OR family_motto IS NOT NULL OR family_crest_url IS NOT NULL") as cursor:
            stats["total_profiles"] = (await cursor.fetchone())[0]

        # Total banned users
        async with self.db.execute("SELECT COUNT(*) FROM banned_users") as cursor:
            stats["total_banned"] = (await cursor.fetchone())[0]

        # Pending proposals
        async with self.db.execute("SELECT COUNT(*) FROM pending_proposals WHERE expires_at > datetime('now')") as cursor:
            stats["pending_proposals"] = (await cursor.fetchone())[0]

        # Users looking for match
        async with self.db.execute("SELECT COUNT(*) FROM family_profiles WHERE looking_for_match = 1") as cursor:
            stats["looking_for_match"] = (await cursor.fetchone())[0]

        return stats

    async def find_disconnected_family_trees(self) -> List[Set[int]]:
        """
        Find all separate/disconnected family trees in the database.
        Returns a list of sets, each set containing user IDs in a connected tree.
        """
        all_users = await self.get_all_users_with_relations()

        if not all_users:
            return []

        # Find connected components
        visited = set()
        components = []

        for user_id in all_users:
            if user_id not in visited:
                # BFS to find all connected users
                connected = await self.get_all_connected_users(user_id)
                visited.update(connected)
                components.append(connected)

        return components

    async def get_users_not_connected_to(self, root_user_id: int) -> Set[int]:
        """
        Find all users who have relationships but are NOT connected to the specified user.
        Useful for finding 'orphaned' family trees.
        """
        all_users = await self.get_all_users_with_relations()
        connected_to_root = await self.get_all_connected_users(root_user_id)

        return all_users - connected_to_root

    async def delete_users_relationships(self, user_ids: Set[int]) -> Dict[str, int]:
        """
        Delete all relationships for a set of users.
        Returns counts of deleted items.
        """
        if not user_ids:
            return {"marriages": 0, "parent_child": 0, "profiles": 0}

        counts = {"marriages": 0, "parent_child": 0, "profiles": 0}

        for user_id in user_ids:
            # Delete marriages
            cursor = await self.db.execute(
                "DELETE FROM marriages WHERE user1_id = ? OR user2_id = ?",
                (user_id, user_id)
            )
            counts["marriages"] += cursor.rowcount

            # Delete parent-child relationships
            cursor = await self.db.execute(
                "DELETE FROM parent_child WHERE parent_id = ? OR child_id = ?",
                (user_id, user_id)
            )
            counts["parent_child"] += cursor.rowcount

            # Clear family profile
            cursor = await self.db.execute(
                "DELETE FROM family_profiles WHERE user_id = ?",
                (user_id,)
            )
            counts["profiles"] += cursor.rowcount

        await self.db.commit()
        return counts
