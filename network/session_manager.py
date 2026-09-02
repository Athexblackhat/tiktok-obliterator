"""
session manager — manages aiohttp sessions & cookie jars per burner account
handles: session creation, cookie persistence, session reuse, expiry detection,
         concurrent session limits, cleanup of dead sessions
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class ManagedSession:
    """a managed HTTP session tied to a burner account"""
    account_id: str
    session: aiohttp.ClientSession
    cookie_jar: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)
    request_count: int = 0
    is_active: bool = True
    proxy_bound: Optional[str] = None  # optional — bind session to specific proxy
    fingerprint_id: Optional[str] = None


class SessionManager:
    """
    manages aiohttp sessions for burner accounts

    usage:
        manager = SessionManager(max_sessions=100, session_ttl=1800)
        session = await manager.get_session(account_id="burner_001")
        # use session for requests...
        await manager.release_session("burner_001")
    """

    def __init__(
        self,
        max_sessions: int = 100,
        session_ttl: int = 1800,  # 30 min — rotate sessions periodically
        max_requests_per_session: int = 500,
        cleanup_interval: int = 300,
    ):
        self.max_sessions = max_sessions
        self.session_ttl = session_ttl
        self.max_requests_per_session = max_requests_per_session
        self.cleanup_interval = cleanup_interval

        self._sessions: dict[str, ManagedSession] = {}  # account_id → ManagedSession
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self):
        """start background cleanup task"""
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self):
        """stop background cleanup and close all sessions"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            self._cleanup_task = None

        async with self._lock:
            for managed in self._sessions.values():
                await self._close_session(managed)
            self._sessions.clear()
            logger.info("[session] all sessions closed")

    async def get_session(
        self,
        account_id: str,
        cookie_string: Optional[str] = None,
        proxy: Optional[str] = None,
        fingerprint_id: Optional[str] = None,
    ) -> aiohttp.ClientSession:
        """
        get or create a managed session for a burner account

        account_id: unique burner account identifier
        cookie_string: session cookie string from account creation
        proxy: optional — bind this session to a specific proxy
        fingerprint_id: optional — track which fingerprint was used
        """
        async with self._lock:
            # check if session exists and is still valid
            if account_id in self._sessions:
                managed = self._sessions[account_id]
                if self._is_session_valid(managed):
                    managed.last_used_at = time.time()
                    managed.request_count += 1
                    return managed.session
                else:
                    # expired — close and recreate
                    await self._close_session(managed)
                    del self._sessions[account_id]

            # enforce max sessions
            if len(self._sessions) >= self.max_sessions:
                await self._evict_oldest()

            # create new session
            cookie_jar = self._parse_cookies(cookie_string) if cookie_string else {}
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.6478.122 Mobile Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }

            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers=headers,
                cookies=cookie_jar,
            )

            managed = ManagedSession(
                account_id=account_id,
                session=session,
                cookie_jar=cookie_jar,
                proxy_bound=proxy,
                fingerprint_id=fingerprint_id,
            )
            self._sessions[account_id] = managed

            logger.debug("[session] created session for %s (total: %d)", account_id, len(self._sessions))
            return session

    async def release_session(self, account_id: str):
        """release a session without closing it — mark as available"""
        async with self._lock:
            if account_id in self._sessions:
                self._sessions[account_id].last_used_at = time.time()

    async def invalidate_session(self, account_id: str):
        """immediately close and remove a session (account banned/expired)"""
        async with self._lock:
            if account_id in self._sessions:
                managed = self._sessions[account_id]
                await self._close_session(managed)
                del self._sessions[account_id]
                logger.debug("[session] invalidated session for %s", account_id)

    async def register(self, account):  # BurnerAccount
        """register a burner account's session — convenience method"""
        await self.get_session(
            account_id=account.account_id,
            cookie_string=account.session_cookie,
            fingerprint_id=account.fingerprint_id,
        )

    async def get_cookie_jar(self, account_id: str) -> Optional[dict]:
        """get the cookie jar for an account"""
        async with self._lock:
            managed = self._sessions.get(account_id)
            if managed:
                return dict(managed.session.cookie_jar.filter_cookies(
                    aiohttp.ClientSession()._base_url or "https://www.tiktok.com"
                ))
        return None

    # ─── internal ───────────────────────────────────────────────────

    def _is_session_valid(self, managed: ManagedSession) -> bool:
        """check if a session is still usable"""
        if not managed.is_active:
            return False
        if managed.session.closed:
            return False
        # TTL check
        age = time.time() - managed.created_at
        if age > self.session_ttl:
            return False
        # request count check
        if managed.request_count >= self.max_requests_per_session:
            return False
        return True

    async def _close_session(self, managed: ManagedSession):
        """safely close a session"""
        managed.is_active = False
        try:
            if not managed.session.closed:
                await managed.session.close()
        except Exception as e:
            logger.debug("[session] error closing session: %s", e)

    async def _evict_oldest(self):
        """evict the oldest session to make room"""
        if not self._sessions:
            return

        oldest_id = min(
            self._sessions.keys(),
            key=lambda k: self._sessions[k].last_used_at,
        )
        managed = self._sessions[oldest_id]
        await self._close_session(managed)
        del self._sessions[oldest_id]
        logger.debug("[session] evicted oldest session: %s", oldest_id)

    async def _cleanup_loop(self):
        """background task — periodically clean expired sessions"""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            async with self._lock:
                to_remove = [
                    aid for aid, m in self._sessions.items()
                    if not self._is_session_valid(m) or m.session.closed
                ]
                for aid in to_remove:
                    await self._close_session(self._sessions[aid])
                    del self._sessions[aid]
                if to_remove:
                    logger.debug("[session] cleaned %d expired sessions", len(to_remove))

    @staticmethod
    def _parse_cookies(cookie_string: str) -> dict:
        """parse raw cookie string into dict"""
        cookies = {}
        for item in cookie_string.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key.strip()] = value.strip()
        return cookies

    @property
    def active_sessions(self) -> int:
        return len(self._sessions)