"""
database — async sqlite3 wrapper for all persistent data
tables: accounts, proxies, report_attempts, report_waves, campaigns, bans
"""

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class Database:
    """
    async sqlite3 database manager

    usage:
        db = Database("output/tiktok_obliterator.db")
        await db.initialize()
        await db.save_account(burner_account)
        await db.save_report_attempt(report_attempt)
    """

    SCHEMA = """
    -- burner accounts
    CREATE TABLE IF NOT EXISTS accounts (
        account_id TEXT PRIMARY KEY,
        email TEXT NOT NULL,
        tiktok_uid TEXT,
        tiktok_username TEXT,
        session_cookie TEXT,
        session_token TEXT,
        proxy_used TEXT,
        fingerprint_id TEXT,
        is_active INTEGER DEFAULT 1,
        report_count INTEGER DEFAULT 0,
        created_at TEXT,
        last_used_at TEXT,
        retired_at TEXT
    );

    -- proxy pool tracking
    CREATE TABLE IF NOT EXISTS proxies (
        address TEXT PRIMARY KEY,
        protocol TEXT DEFAULT 'socks5',
        status TEXT DEFAULT 'fresh',
        fail_count INTEGER DEFAULT 0,
        success_count INTEGER DEFAULT 0,
        last_used_at REAL DEFAULT 0,
        added_at REAL,
        region TEXT
    );

    -- individual report attempts
    CREATE TABLE IF NOT EXISTS report_attempts (
        attempt_id TEXT PRIMARY KEY,
        wave_id TEXT,
        target_uid TEXT,
        burner_account_id TEXT,
        category TEXT,
        report_text TEXT,
        status TEXT,
        http_code INTEGER,
        error_detail TEXT,
        proxy_used TEXT,
        response_time_ms REAL,
        fired_at TEXT
    );

    -- report waves (batches)
    CREATE TABLE IF NOT EXISTS report_waves (
        wave_id TEXT PRIMARY KEY,
        target_username TEXT,
        target_uid TEXT,
        total_requested INTEGER,
        total_delivered INTEGER,
        total_rate_limited INTEGER,
        total_blocked INTEGER,
        total_failed INTEGER,
        accounts_used INTEGER,
        accounts_burned INTEGER,
        escalation_level TEXT,
        delivery_rate REAL,
        started_at TEXT,
        completed_at TEXT
    );

    -- campaigns (one per target)
    CREATE TABLE IF NOT EXISTS campaigns (
        campaign_id TEXT PRIMARY KEY,
        target_username TEXT,
        target_uid TEXT,
        status TEXT DEFAULT 'active',
        total_reports_fired INTEGER DEFAULT 0,
        total_reports_delivered INTEGER DEFAULT 0,
        total_waves INTEGER DEFAULT 0,
        max_escalation_level TEXT,
        ban_confirmed INTEGER DEFAULT 0,
        time_to_ban_seconds REAL,
        started_at TEXT,
        completed_at TEXT
    );

    -- confirmed bans (trophy case)
    CREATE TABLE IF NOT EXISTS bans (
        ban_id TEXT PRIMARY KEY,
        target_username TEXT,
        target_uid TEXT,
        campaign_id TEXT,
        total_reports_fired INTEGER,
        time_to_ban_seconds REAL,
        escalation_level_reached TEXT,
        banned_at TEXT,
        note TEXT
    );

    -- indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active);
    CREATE INDEX IF NOT EXISTS idx_accounts_report_count ON accounts(report_count);
    CREATE INDEX IF NOT EXISTS idx_report_attempts_wave ON report_attempts(wave_id);
    CREATE INDEX IF NOT EXISTS idx_report_attempts_target ON report_attempts(target_uid);
    CREATE INDEX IF NOT EXISTS idx_report_waves_target ON report_waves(target_uid);
    CREATE INDEX IF NOT EXISTS idx_campaigns_target ON campaigns(target_username);
    CREATE INDEX IF NOT EXISTS idx_bans_target ON bans(target_username);
    """

    def __init__(self, db_path: str = "output/tiktok_obliterator.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    async def initialize(self):
        """create database and run schema"""
        async with self._lock:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.executescript(self.SCHEMA)
            self._conn.commit()
            logger.info("[db] initialized at %s", self.db_path)

    async def close(self):
        """close database connection"""
        async with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None
                logger.debug("[db] connection closed")

    # ─── accounts ───────────────────────────────────────────────────

    async def save_account(self, account) -> None:
        """insert or update a burner account"""
        async with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO accounts
                   (account_id, email, tiktok_uid, tiktok_username,
                    session_cookie, session_token, proxy_used, fingerprint_id,
                    is_active, report_count, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    account.account_id,
                    account.email,
                    account.tiktok_uid,
                    account.tiktok_username,
                    account.session_cookie,
                    account.session_token,
                    account.proxy_used,
                    account.fingerprint_id,
                    1 if account.is_active else 0,
                    account.report_count,
                    account.created_at,
                ),
            )
            self._conn.commit()

    async def update_account_status(self, account_id: str, is_active: bool, report_count: int = 0):
        """update account active status and report count"""
        async with self._lock:
            self._conn.execute(
                """UPDATE accounts
                   SET is_active = ?, report_count = ?, last_used_at = ?
                   WHERE account_id = ?""",
                (1 if is_active else 0, report_count, datetime.utcnow().isoformat(), account_id),
            )
            self._conn.commit()

    async def get_active_accounts_count(self) -> int:
        """count available burner accounts"""
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT COUNT(*) FROM accounts WHERE is_active = 1 AND report_count < 4"
            )
            return cursor.fetchone()[0]

    async def get_accounts_ready_for_reporting(self, limit: int = 50) -> list[dict]:
        """get active accounts with low report counts"""
        async with self._lock:
            cursor = self._conn.execute(
                """SELECT * FROM accounts
                   WHERE is_active = 1 AND report_count < 4
                   ORDER BY report_count ASC, created_at DESC
                   LIMIT ?""",
                (limit,),
            )
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # ─── report attempts ────────────────────────────────────────────

    async def save_report_attempt(self, attempt) -> None:
        """save a single report attempt"""
        async with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO report_attempts
                   (attempt_id, wave_id, target_uid, burner_account_id,
                    category, report_text, status, http_code, error_detail,
                    proxy_used, response_time_ms, fired_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    attempt.attempt_id,
                    getattr(attempt, "wave_id", ""),
                    attempt.target_uid,
                    attempt.burner_account_id,
                    attempt.category,
                    attempt.report_text,
                    attempt.status.value,
                    attempt.http_code,
                    attempt.error_detail,
                    attempt.proxy_used,
                    attempt.response_time_ms,
                    attempt.fired_at,
                ),
            )
            self._conn.commit()

    # ─── report waves ───────────────────────────────────────────────

    async def save_report_wave(self, wave) -> None:
        """save a report wave summary"""
        async with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO report_waves
                   (wave_id, target_username, target_uid, total_requested,
                    total_delivered, total_rate_limited, total_blocked,
                    total_failed, accounts_used, accounts_burned,
                    escalation_level, delivery_rate, started_at, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    wave.wave_id,
                    wave.target_username,
                    wave.target_uid,
                    wave.total_requested,
                    wave.total_delivered,
                    wave.total_rate_limited,
                    wave.total_blocked,
                    wave.total_failed,
                    wave.accounts_used,
                    wave.accounts_burned,
                    wave.escalation_level,
                    wave.delivery_rate,
                    wave.started_at,
                    wave.completed_at,
                ),
            )
            self._conn.commit()

    # ─── campaigns ──────────────────────────────────────────────────

    async def create_campaign(self, target_username: str, target_uid: str) -> str:
        """create a new campaign record, return campaign_id"""
        campaign_id = f"camp_{int(time.time()*1000)}"
        async with self._lock:
            self._conn.execute(
                """INSERT INTO campaigns
                   (campaign_id, target_username, target_uid, started_at)
                   VALUES (?, ?, ?, ?)""",
                (campaign_id, target_username, target_uid, datetime.utcnow().isoformat()),
            )
            self._conn.commit()
        logger.info("[db] campaign created: %s for @%s", campaign_id, target_username)
        return campaign_id

    async def update_campaign(self, campaign_id: str, **kwargs):
        """update campaign fields"""
        if not kwargs:
            return
        set_clause = ", ".join(f"{k} = ?" for k in kwargs)
        values = list(kwargs.values()) + [campaign_id]
        async with self._lock:
            self._conn.execute(
                f"UPDATE campaigns SET {set_clause} WHERE campaign_id = ?",
                values,
            )
            self._conn.commit()

    async def complete_campaign(self, campaign_id: str, ban_confirmed: bool = False):
        """mark campaign as completed"""
        async with self._lock:
            self._conn.execute(
                """UPDATE campaigns
                   SET status = ?, ban_confirmed = ?, completed_at = ?
                   WHERE campaign_id = ?""",
                (
                    "completed",
                    1 if ban_confirmed else 0,
                    datetime.utcnow().isoformat(),
                    campaign_id,
                ),
            )
            self._conn.commit()

    # ─── bans ───────────────────────────────────────────────────────

    async def save_ban(self, ban_data: dict):
        """record a confirmed ban"""
        ban_id = f"ban_{int(time.time()*1000)}"
        async with self._lock:
            self._conn.execute(
                """INSERT INTO bans
                   (ban_id, target_username, target_uid, campaign_id,
                    total_reports_fired, time_to_ban_seconds,
                    escalation_level_reached, banned_at, note)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ban_id,
                    ban_data.get("target_username", ""),
                    ban_data.get("target_uid", ""),
                    ban_data.get("campaign_id", ""),
                    ban_data.get("total_reports_fired", 0),
                    ban_data.get("time_to_ban_seconds", 0),
                    ban_data.get("escalation_level", ""),
                    datetime.utcnow().isoformat(),
                    ban_data.get("note", ""),
                ),
            )
            self._conn.commit()
        logger.info("[db] ★ ban recorded: @%s (#%s)", ban_data.get("target_username"), ban_id)
        return ban_id

    async def get_ban_count(self) -> int:
        """total confirmed bans"""
        async with self._lock:
            cursor = self._conn.execute("SELECT COUNT(*) FROM bans")
            return cursor.fetchone()[0]

    async def get_recent_bans(self, limit: int = 10) -> list[dict]:
        """get most recent bans"""
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM bans ORDER BY banned_at DESC LIMIT ?",
                (limit,),
            )
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # ─── stats ──────────────────────────────────────────────────────

    async def get_total_stats(self) -> dict:
        """get overall tool statistics"""
        async with self._lock:
            total_reports = self._conn.execute(
                "SELECT COUNT(*) FROM report_attempts"
            ).fetchone()[0]

            total_delivered = self._conn.execute(
                "SELECT COUNT(*) FROM report_attempts WHERE status = 'delivered'"
            ).fetchone()[0]

            total_bans = self._conn.execute(
                "SELECT COUNT(*) FROM bans"
            ).fetchone()[0]

            total_accounts = self._conn.execute(
                "SELECT COUNT(*) FROM accounts"
            ).fetchone()[0]

            active_accounts = self._conn.execute(
                "SELECT COUNT(*) FROM accounts WHERE is_active = 1"
            ).fetchone()[0]

            return {
                "total_reports_fired": total_reports,
                "total_reports_delivered": total_delivered,
                "total_bans": total_bans,
                "total_accounts_created": total_accounts,
                "active_accounts": active_accounts,
            }