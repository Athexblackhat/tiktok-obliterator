"""
account pool — in-memory burner account management
handles: get available account, release back, retire, stats,
         auto-refill triggers when pool runs low
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PoolStats:
    """current account pool statistics"""
    total_accounts: int = 0
    available: int = 0
    in_use: int = 0
    retired: int = 0
    max_report_threshold: int = 4
    needs_refill: bool = False
    refill_trigger_at: int = 20


class AccountPool:
    """
    in-memory pool of burner accounts ready for reporting

    usage:
        pool = AccountPool(
            db=db_instance,
            factory=account_factory_instance,
            min_pool_size=50,
            refill_batch_size=20,
        )
        await pool.initialize()

        account = await pool.get_available()
        # use account for reporting...
        await pool.release(account)  # if still usable
        # or
        await pool.retire(account)   # if done/burned
    """

    def __init__(
        self,
        db=None,                # Database instance
        factory=None,           # AccountFactory instance
        min_pool_size: int = 50,
        refill_batch_size: int = 20,
        max_retired_memory: int = 500,
        max_reports_per_account: int = 4,
        refill_cooldown: int = 60,  # seconds between refill attempts
    ):
        self.db = db
        self.factory = factory
        self.min_pool_size = min_pool_size
        self.refill_batch_size = refill_batch_size
        self.max_retired_memory = max_retired_memory
        self.max_reports_per_account = max_reports_per_account
        self.refill_cooldown = refill_cooldown

        # pools
        self._available: list = []        # ready to use
        self._in_use: dict[str, any] = {}  # account_id → BurnerAccount
        self._retired: list = []          # recently retired (limited memory)

        self._lock = asyncio.Lock()
        self._refill_task: Optional[asyncio.Task] = None
        self._last_refill_attempt: float = 0.0
        self._refill_in_progress: bool = False

    async def initialize(self):
        """load existing accounts from DB and start refill monitor"""
        if self.db:
            accounts_data = await self.db.get_accounts_ready_for_reporting(limit=200)
            for acct_data in accounts_data:
                account = self._dict_to_account(acct_data)
                if account:
                    self._available.append(account)

            logger.info("[pool] initialized — %d accounts loaded from DB", len(self._available))

        # check if we need more
        await self._check_and_refill()

    async def add(self, account):
        """add a freshly created account to the pool"""
        async with self._lock:
            if account.is_active and account.report_count < self.max_reports_per_account:
                self._available.append(account)
                logger.debug("[pool] + account %s added — available: %d", account.account_id, len(self._available))
            else:
                self._retired.append(account)

    async def get_available(self) -> Optional[any]:
        """
        get an available burner account for reporting
        returns None if pool is empty
        """
        async with self._lock:
            if not self._available:
                logger.warning("[pool] exhausted — no available accounts")
                # trigger emergency refill
                asyncio.create_task(self._emergency_refill())
                return None

            account = self._available.pop(0)
            self._in_use[account.account_id] = account
            logger.debug("[pool] - account %s checked out — available: %d", account.account_id, len(self._available))

            # check if pool is getting low
            if len(self._available) < self.min_pool_size // 2:
                asyncio.create_task(self._check_and_refill())

            return account

    async def release(self, account):
        """return account to pool if still usable"""
        async with self._lock:
            self._in_use.pop(account.account_id, None)

            if account.is_active and account.report_count < self.max_reports_per_account:
                self._available.append(account)
                logger.debug("[pool] account %s released — reports: %d", account.account_id, account.report_count)
            else:
                await self.retire(account)

    async def retire(self, account):
        """permanently retire an account"""
        async with self._lock:
            self._in_use.pop(account.account_id, None)
            if account in self._available:
                self._available.remove(account)

            account.is_active = False
            self._retired.append(account)

            # trim retired memory
            while len(self._retired) > self.max_retired_memory:
                self._retired.pop(0)

            # update DB
            if self.db:
                await self.db.update_account_status(
                    account.account_id,
                    is_active=False,
                    report_count=account.report_count,
                )

            logger.debug("[pool] account %s retired — total retired: %d", account.account_id, len(self._retired))

    async def get_stats(self) -> PoolStats:
        """get current pool statistics"""
        async with self._lock:
            return PoolStats(
                total_accounts=len(self._available) + len(self._in_use) + len(self._retired),
                available=len(self._available),
                in_use=len(self._in_use),
                retired=len(self._retired),
                max_report_threshold=self.max_reports_per_account,
                needs_refill=len(self._available) < self.min_pool_size,
                refill_trigger_at=self.min_pool_size // 2,
            )

    # ─── internal ───────────────────────────────────────────────────

    async def _check_and_refill(self):
        """check if pool needs refilling and start if needed"""
        now = time.time()
        if now - self._last_refill_attempt < self.refill_cooldown:
            return
        if self._refill_in_progress:
            return
        if not self.factory:
            logger.debug("[pool] no factory configured — skipping refill")
            return

        async with self._lock:
            available_count = len(self._available)

        if available_count < self.min_pool_size:
            self._refill_in_progress = True
            self._last_refill_attempt = now
            needed = self.min_pool_size - available_count + self.refill_batch_size
            logger.info("[pool] refill triggered — need %d accounts", needed)

            try:
                new_accounts = await self.factory.create_batch(
                    count=needed,
                    max_parallel=5,
                )
                async with self._lock:
                    for acct in new_accounts:
                        self._available.append(acct)
                logger.info("[pool] refill complete — %d new accounts added", len(new_accounts))
            except Exception as e:
                logger.error("[pool] refill failed: %s", e)
            finally:
                self._refill_in_progress = False

    async def _emergency_refill(self):
        """emergency refill when pool hits zero during a campaign"""
        logger.warning("[pool] EMERGENCY REFILL — pool is empty!")
        await self._check_and_refill()

    def _dict_to_account(self, data: dict) -> Optional[any]:
        """convert DB row dict back to BurnerAccount"""
        try:
            from core.account_factory import BurnerAccount, AccountCreateStage

            return BurnerAccount(
                account_id=data.get("account_id", ""),
                email=data.get("email", ""),
                tiktok_uid=data.get("tiktok_uid", ""),
                tiktok_username=data.get("tiktok_username", ""),
                session_cookie=data.get("session_cookie", ""),
                session_token=data.get("session_token", ""),
                proxy_used=data.get("proxy_used", ""),
                fingerprint_id=data.get("fingerprint_id", "default"),
                created_at=data.get("created_at", ""),
                report_count=data.get("report_count", 0),
                is_active=bool(data.get("is_active", 1)),
                stage_reached=AccountCreateStage.COMPLETE,
            )
        except Exception as e:
            logger.error("[pool] failed to reconstruct account: %s", e)
            return None