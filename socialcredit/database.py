import aiosqlite
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger("red.DurkCogs.SocialCredit.database")

DEFAULT_SCORE = 1000


class SocialCreditDatabase:
    """SQLite database handler for the SocialCredit cog."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        """Open connection, create tables, set row_factory."""
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self._create_tables()
        log.info(f"SocialCredit database initialized at {self.db_path}")

    async def close(self):
        """Close the database connection."""
        if self.db:
            await self.db.close()
            log.info("SocialCredit database connection closed")

    async def _create_tables(self):
        """Create all required tables."""
        await self.db.executescript("""
            CREATE TABLE IF NOT EXISTS user_credits (
                user_id INTEGER PRIMARY KEY,
                score INTEGER NOT NULL DEFAULT 1000,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS credit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                target_user_id INTEGER,
                amount INTEGER NOT NULL,
                reason TEXT NOT NULL,
                guild_id INTEGER,
                channel_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES user_credits(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_credit_log_user
                ON credit_log(user_id);
            CREATE INDEX IF NOT EXISTS idx_credit_log_created
                ON credit_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_credit_log_reason
                ON credit_log(reason);

            CREATE TABLE IF NOT EXISTS hug_cooldowns (
                user_id INTEGER NOT NULL,
                target_user_id INTEGER NOT NULL,
                last_hug_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, target_user_id)
            );

            CREATE TABLE IF NOT EXISTS pill_cooldowns (
                user_id INTEGER PRIMARY KEY,
                last_pill_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await self.db.commit()

    # ── Score operations ───────────────────────────────────────────────

    async def ensure_user(self, user_id: int) -> int:
        """Ensure user exists with default score. Returns current score."""
        await self.db.execute(
            "INSERT OR IGNORE INTO user_credits (user_id, score) VALUES (?, ?)",
            (user_id, DEFAULT_SCORE),
        )
        await self.db.commit()
        return await self.get_score(user_id)

    async def get_score(self, user_id: int) -> int:
        """Get user's current score. Creates user if not exists."""
        async with self.db.execute(
            "SELECT score FROM user_credits WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["score"]
        return await self.ensure_user(user_id)

    async def adjust_score(
        self,
        user_id: int,
        amount: int,
        reason: str,
        target_user_id: int = None,
        guild_id: int = None,
        channel_id: int = None,
    ) -> int:
        """Adjust score by amount (positive or negative). Logs the change.
        Returns the new score."""
        await self.ensure_user(user_id)

        await self.db.execute(
            """UPDATE user_credits
               SET score = score + ?, updated_at = CURRENT_TIMESTAMP
               WHERE user_id = ?""",
            (amount, user_id),
        )
        await self.db.execute(
            """INSERT INTO credit_log
               (user_id, target_user_id, amount, reason, guild_id, channel_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, target_user_id, amount, reason, guild_id, channel_id),
        )
        await self.db.commit()
        return await self.get_score(user_id)

    async def set_score(self, user_id: int, score: int) -> None:
        """Admin override: set score to an exact value."""
        await self.ensure_user(user_id)
        await self.db.execute(
            """UPDATE user_credits
               SET score = ?, updated_at = CURRENT_TIMESTAMP
               WHERE user_id = ?""",
            (score, user_id),
        )
        await self.db.commit()

    # ── Leaderboard ────────────────────────────────────────────────────

    async def get_leaderboard(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Top N users by score."""
        async with self.db.execute(
            "SELECT user_id, score FROM user_credits ORDER BY score DESC LIMIT ?",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_rank(self, user_id: int) -> int:
        """Return 1-based rank of user."""
        score = await self.get_score(user_id)
        async with self.db.execute(
            "SELECT COUNT(*) AS rank FROM user_credits WHERE score > ?",
            (score,),
        ) as cursor:
            row = await cursor.fetchone()
            return (row["rank"] if row else 0) + 1

    # ── Credit log queries ─────────────────────────────────────────────

    async def get_user_log(
        self, user_id: int, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Recent credit changes for a user, newest first."""
        async with self.db.execute(
            """SELECT * FROM credit_log
               WHERE user_id = ?
               ORDER BY created_at DESC
               LIMIT ?""",
            (user_id, limit),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_log_summary(self, user_id: int) -> Dict[str, int]:
        """Aggregate credit changes by reason for a user."""
        async with self.db.execute(
            """SELECT reason, SUM(amount) AS total
               FROM credit_log
               WHERE user_id = ?
               GROUP BY reason""",
            (user_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row["reason"]: row["total"] for row in rows}

    async def get_reason_counts(self, user_id: int) -> Dict[str, int]:
        """Count of credit log entries per reason for a user."""
        async with self.db.execute(
            """SELECT reason, COUNT(*) AS cnt
               FROM credit_log
               WHERE user_id = ?
               GROUP BY reason""",
            (user_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row["reason"]: row["cnt"] for row in rows}

    # ── Hug cooldowns ──────────────────────────────────────────────────

    async def check_hug_cooldown(self, user_id: int) -> Optional[str]:
        """Returns None if hug is allowed.
        Returns ISO timestamp string of last_hug_at if still on cooldown."""
        async with self.db.execute(
            """SELECT last_hug_at FROM hug_cooldowns
            WHERE user_id = ?
            AND last_hug_at > datetime('now', '-24 hours')
            ORDER BY last_hug_at DESC
            LIMIT 1""",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["last_hug_at"]
        return None

    async def record_hug(self, user_id: int, target_user_id: int) -> None:
        """Upsert the cooldown record."""
        await self.db.execute(
            """INSERT INTO hug_cooldowns (user_id, target_user_id, last_hug_at)
               VALUES (?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(user_id, target_user_id)
               DO UPDATE SET last_hug_at = CURRENT_TIMESTAMP""",
            (user_id, target_user_id),
        )
        await self.db.commit()

    # ── Pill cooldowns ─────────────────────────────────────────────────

    async def check_pill_cooldown(self, user_id: int) -> Optional[str]:
        """Returns None if pill is allowed.
        Returns ISO timestamp string of last_pill_at if still on cooldown."""
        async with self.db.execute(
            """SELECT last_pill_at FROM pill_cooldowns
            WHERE user_id = ?
            AND last_pill_at > datetime('now', '-4 hours')
            LIMIT 1""",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["last_pill_at"]
        return None

    async def record_pill(self, user_id: int) -> None:
        """Upsert the pill cooldown record."""
        await self.db.execute(
            """INSERT INTO pill_cooldowns (user_id, last_pill_at)
               VALUES (?, CURRENT_TIMESTAMP)
               ON CONFLICT(user_id)
               DO UPDATE SET last_pill_at = CURRENT_TIMESTAMP""",
            (user_id,),
        )
        await self.db.commit()

    # ── Cleanup ────────────────────────────────────────────────────────

    async def delete_user_data(self, user_id: int) -> Dict[str, int]:
        """Delete all data for a user. Returns counts of deleted rows."""
        counts = {}

        cursor = await self.db.execute(
            "DELETE FROM credit_log WHERE user_id = ? OR target_user_id = ?",
            (user_id, user_id),
        )
        counts["log_entries"] = cursor.rowcount

        cursor = await self.db.execute(
            "DELETE FROM hug_cooldowns WHERE user_id = ? OR target_user_id = ?",
            (user_id, user_id),
        )
        counts["cooldowns"] = cursor.rowcount

        cursor = await self.db.execute(
            "DELETE FROM pill_cooldowns WHERE user_id = ?",
            (user_id,),
        )
        counts["pill_cooldowns"] = cursor.rowcount

        cursor = await self.db.execute(
            "DELETE FROM user_credits WHERE user_id = ?", (user_id,)
        )
        counts["credit_record"] = cursor.rowcount

        await self.db.commit()
        return counts
