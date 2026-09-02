"""
proxy rotator — manages socks5 proxy pool with health checking
handles: loading, rotation (round-robin / random / weighted), dead proxy detection,
         auto-removal of failed proxies, cooldown for rate-limited proxies,
         proxy validation on load
"""

import asyncio
import logging
import random
import socket
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class ProxyStatus(Enum):
    """current state of a proxy"""
    FRESH = "fresh"          # just loaded, not yet tested
    ACTIVE = "active"        # working, available
    COOLDOWN = "cooldown"    # rate limited, retry after cooldown
    DEAD = "dead"            # failed too many times, removed
    TESTING = "testing"      # currently being health checked


@dataclass
class ProxyInfo:
    """metadata for a single proxy"""
    address: str              # ip:port or user:pass@ip:port
    protocol: str = "socks5"  # socks5, socks4, http
    status: ProxyStatus = ProxyStatus.FRESH
    fail_count: int = 0
    success_count: int = 0
    last_used_at: float = 0.0
    last_checked_at: float = 0.0
    cooldown_until: float = 0.0
    response_time_ms: float = 0.0
    region: Optional[str] = None  # geo region if known
    added_at: float = field(default_factory=time.time)

    @property
    def is_available(self) -> bool:
        return self.status in (ProxyStatus.FRESH, ProxyStatus.ACTIVE)

    @property
    def formatted(self) -> str:
        """return proxy in url format: socks5://user:pass@ip:port"""
        addr = self.address
        if "://" in addr:
            return addr
        return f"{self.protocol}://{addr}"


class ProxyRotator:
    """
    async proxy pool manager with health checking and rotation

    usage:
        rotator = ProxyRotator(
            proxy_file="proxies/socks5_list.txt",
            rotation_mode="random",
            max_failures=3,
            cooldown_seconds=300,
        )
        await rotator.load_proxies()
        proxy = await rotator.get_proxy()
        # use proxy...
        await rotator.report_success(proxy)
        # or
        await rotator.report_failure(proxy)
    """

    def __init__(
        self,
        proxy_file: Optional[str] = None,
        proxy_list: Optional[list[str]] = None,
        rotation_mode: str = "random",
        max_failures: int = 3,
        cooldown_seconds: int = 300,
        health_check_timeout: float = 5.0,
        min_proxies_required: int = 5,
        auto_health_check: bool = True,
        health_check_interval: int = 600,
    ):
        """
        proxy_file: path to socks5_list.txt
        proxy_list: alternative — pass list of proxy strings directly
        rotation_mode: "random", "round_robin", "weighted" (by success rate)
        max_failures: how many consecutive failures before marking DEAD
        cooldown_seconds: how long to cooldown after rate limit
        health_check_timeout: seconds for TCP connectivity test
        min_proxies_required: warn if pool drops below this
        auto_health_check: periodically test dead/cooldown proxies
        health_check_interval: seconds between health check cycles
        """
        self.proxy_file = proxy_file
        self.proxy_list = proxy_list or []
        self.rotation_mode = rotation_mode
        self.max_failures = max_failures
        self.cooldown_seconds = cooldown_seconds
        self.health_check_timeout = health_check_timeout
        self.min_proxies_required = min_proxies_required
        self.auto_health_check = auto_health_check
        self.health_check_interval = health_check_interval

        self._proxies: dict[str, ProxyInfo] = {}  # address → ProxyInfo
        self._available: list[str] = []  # addresses currently available
        self._round_robin_index: int = 0
        self._lock = asyncio.Lock()
        self._health_check_task: Optional[asyncio.Task] = None

    async def load_proxies(self) -> int:
        """load proxies from file or list, parse and validate"""
        raw_proxies = []

        if self.proxy_file:
            try:
                with open(self.proxy_file, "r") as f:
                    raw_proxies = [
                        line.strip()
                        for line in f
                        if line.strip() and not line.strip().startswith("#")
                    ]
                logger.info("[proxy] loaded %d proxies from %s", len(raw_proxies), self.proxy_file)
            except FileNotFoundError:
                logger.error("[proxy] proxy file not found: %s", self.proxy_file)
                return 0

        if self.proxy_list:
            raw_proxies.extend(self.proxy_list)

        # parse and create ProxyInfo objects
        async with self._lock:
            for raw in raw_proxies:
                parsed = self._parse_proxy_string(raw)
                if parsed:
                    self._proxies[parsed.address] = parsed

            self._rebuild_available_list()

            logger.info(
                "[proxy] pool ready — %d total, %d available",
                len(self._proxies),
                len(self._available),
            )

            # start background health checker
            if self.auto_health_check:
                self._health_check_task = asyncio.create_task(self._health_check_loop())

        return len(self._proxies)

    async def get_proxy(self) -> Optional[str]:
        """
        get next available proxy based on rotation mode
        returns: formatted proxy string or None if pool exhausted
        """
        async with self._lock:
            if not self._available:
                logger.warning("[proxy] pool exhausted — no available proxies")
                return None

            if self.rotation_mode == "round_robin":
                selected = self._get_round_robin()
            elif self.rotation_mode == "weighted":
                selected = self._get_weighted()
            else:  # random
                selected = random.choice(self._available)

            if selected:
                proxy_info = self._proxies[selected]
                proxy_info.last_used_at = time.time()
                return proxy_info.formatted

            return None

    async def get_proxy_batch(self, count: int) -> list[str]:
        """get multiple proxies at once, all unique"""
        proxies = []
        for _ in range(count):
            proxy = await self.get_proxy()
            if proxy:
                proxies.append(proxy)
            else:
                break
        return proxies

    async def report_success(self, proxy_str: str):
        """mark proxy as successful"""
        async with self._lock:
            info = self._find_proxy(proxy_str)
            if info:
                info.success_count += 1
                info.fail_count = 0  # reset consecutive failures
                info.status = ProxyStatus.ACTIVE
                if info.address not in self._available:
                    self._available.append(info.address)

    async def report_failure(self, proxy_str: str, rate_limited: bool = False):
        """
        mark proxy as failed
        rate_limited=True → put in cooldown instead of counting as failure
        """
        async with self._lock:
            info = self._find_proxy(proxy_str)
            if not info:
                return

            if rate_limited:
                info.status = ProxyStatus.COOLDOWN
                info.cooldown_until = time.time() + self.cooldown_seconds
                if info.address in self._available:
                    self._available.remove(info.address)
                logger.debug("[proxy] %s rate limited — cooldown for %ds", _mask_addr(info.address), self.cooldown_seconds)
            else:
                info.fail_count += 1
                if info.fail_count >= self.max_failures:
                    info.status = ProxyStatus.DEAD
                    if info.address in self._available:
                        self._available.remove(info.address)
                    logger.warning("[proxy] %s marked DEAD after %d failures", _mask_addr(info.address), info.fail_count)
                else:
                    # temporary remove from available but keep testing
                    if info.address in self._available:
                        self._available.remove(info.address)
                    logger.debug("[proxy] %s failure %d/%d", _mask_addr(info.address), info.fail_count, self.max_failures)

    async def get_pool_stats(self) -> dict:
        """return current pool statistics"""
        async with self._lock:
            total = len(self._proxies)
            active = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.ACTIVE)
            fresh = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.FRESH)
            cooldown = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.COOLDOWN)
            dead = sum(1 for p in self._proxies.values() if p.status == ProxyStatus.DEAD)
            available = len(self._available)

            return {
                "total": total,
                "available": available,
                "active": active,
                "fresh": fresh,
                "cooldown": cooldown,
                "dead": dead,
                "rotation_mode": self.rotation_mode,
                "min_required": self.min_proxies_required,
                "healthy": available >= self.min_proxies_required,
            }

    async def refresh_cooldowns(self):
        """move proxies out of cooldown if cooldown period expired"""
        now = time.time()
        async with self._lock:
            for addr, info in self._proxies.items():
                if info.status == ProxyStatus.COOLDOWN and info.cooldown_until <= now:
                    info.status = ProxyStatus.ACTIVE
                    info.fail_count = 0
                    if addr not in self._available:
                        self._available.append(addr)
                    logger.debug("[proxy] %s cooldown expired — back in pool", _mask_addr(addr))

    # ─── internal ───────────────────────────────────────────────────

    def _get_round_robin(self) -> Optional[str]:
        if not self._available:
            return None
        self._round_robin_index = self._round_robin_index % len(self._available)
        selected = self._available[self._round_robin_index]
        self._round_robin_index += 1
        return selected

    def _get_weighted(self) -> Optional[str]:
        if not self._available:
            return None
        # weight by success count — more successful proxies used more often
        weights = []
        for addr in self._available:
            info = self._proxies.get(addr)
            if info:
                weights.append(max(1, info.success_count + 1))
            else:
                weights.append(1)

        total = sum(weights)
        if total == 0:
            return random.choice(self._available)

        return random.choices(self._available, weights=weights, k=1)[0]

    def _find_proxy(self, proxy_str: str) -> Optional[ProxyInfo]:
        """find ProxyInfo by formatted proxy string"""
        # normalize — strip protocol prefix
        clean = proxy_str
        if "://" in clean:
            clean = clean.split("://", 1)[1]

        # direct match
        if clean in self._proxies:
            return self._proxies[clean]

        # try matching by address
        for addr, info in self._proxies.items():
            if clean in addr or addr in clean:
                return info

        return None

    def _parse_proxy_string(self, raw: str) -> Optional[ProxyInfo]:
        """parse proxy string into ProxyInfo"""
        raw = raw.strip()
        if not raw:
            return None

        protocol = "socks5"
        address = raw

        # detect protocol
        if "://" in raw:
            protocol, address = raw.split("://", 1)

        # basic validation
        if ":" not in address:
            return None

        return ProxyInfo(
            address=address,
            protocol=protocol,
        )

    def _rebuild_available_list(self):
        """rebuild the available proxies list"""
        self._available = [
            addr
            for addr, info in self._proxies.items()
            if info.is_available
        ]
        if self._available:
            random.shuffle(self._available)

    async def _health_check_loop(self):
        """background task — periodically test dead/cooldown proxies"""
        while True:
            await asyncio.sleep(self.health_check_interval)
            await self.refresh_cooldowns()
            await self._test_dead_proxies()

    async def _test_dead_proxies(self):
        """test dead proxies to see if they've recovered"""
        dead_proxies = [
            (addr, info) for addr, info in self._proxies.items()
            if info.status == ProxyStatus.DEAD
        ]

        if not dead_proxies:
            return

        logger.debug("[proxy] testing %d dead proxies...", len(dead_proxies))

        for addr, info in dead_proxies:
            is_alive = await self._tcp_ping(info)
            if is_alive:
                async with self._lock:
                    info.status = ProxyStatus.ACTIVE
                    info.fail_count = 0
                    if addr not in self._available:
                        self._available.append(addr)
                logger.info("[proxy] %s resurrected — back in pool", _mask_addr(addr))

    async def _tcp_ping(self, info: ProxyInfo) -> bool:
        """basic TCP connectivity test for a proxy"""
        try:
            host, port_str = info.address.rsplit(":", 1)
            port = int(port_str)

            # strip auth for host resolution
            if "@" in host:
                host = host.split("@", 1)[1]

            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=self.health_check_timeout,
            )
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    @property
    def available_count(self) -> int:
        return len(self._available)

    @property
    def total_count(self) -> int:
        return len(self._proxies)


def _mask_addr(address: str) -> str:
    """mask proxy address for logging"""
    if "@" in address:
        return address.split("@")[-1]
    return address