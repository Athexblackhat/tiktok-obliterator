"""
captcha solver — 2captcha & capsolver API integration
handles arkose labs funcaptcha (tiktok's primary), dice, capy, hcaptcha as fallback
async-first, proxy-aware, automatic retry with exponential backoff
"""

import asyncio
import logging
from enum import Enum
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


class CaptchaType(Enum):
    """captcha types tiktok throws during signup/login/reporting"""

    FUNCAPTCHA = "funcaptcha"  # arkose labs — primary on tiktok web
    DICE = "dice"  # dice captcha — rare fallback
    CAPY = "capy"  # capy puzzle — even rarer
    HCAPTCHA = "hcaptcha"  # occasional on certain endpoints
    RECAPTCHA_V2 = "recaptcha_v2"  # edge case, old tiktok pages


class CaptchaSolveError(Exception):
    """raised when captcha solving fails after all retries"""

    def __init__(self, captcha_type: CaptchaType, reason: str, attempts: int = 0):
        self.captcha_type = captcha_type
        self.reason = reason
        self.attempts = attempts
        super().__init__(
            f"[{captcha_type.value}] solve failed after {attempts} attempts: {reason}"
        )


class CaptchaSolver:
    """
    async captcha solver with dual-provider fallback (2captcha → capsolver)

    usage:
        solver = CaptchaSolver(api_key="your_2captcha_key", fallback_key="capsolver_key")
        token = await solver.solve_funcaptcha(
            public_key="arkose-public-key",
            page_url="https://www.tiktok.com/signup",
            proxy="user:pass@ip:port"  # optional, but recommended
        )
    """

    # tiktok's known arkose labs configuration
    TIKTOK_ARKOSE_PUBLIC_KEY = "7C4C8A9D-3F3D-4A6E-B22E-2E2E2E2E2E2E"
    TIKTOK_ARKOSE_SUBDOMAIN = "tiktok-api"

    def __init__(
        self,
        api_key: str,
        fallback_key: Optional[str] = None,
        poll_interval: float = 3.0,
        max_poll_time: int = 120,
        max_retries: int = 3,
        proxy_type: str = "socks5",
    ):
        """
        api_key: 2captcha.com API key (primary)
        fallback_key: capsolver.com API key (secondary, optional)
        poll_interval: kitni der mein status check karna hai (seconds)
        max_poll_time: maximum kitni der wait karna hai solve ke liye
        max_retries: kitni baar retry karna hai agar solve fail ho
        proxy_type: "http", "socks5", "socks4" — captcha solver ke liye proxy type
        """
        self.api_key = api_key
        self.fallback_key = fallback_key
        self.poll_interval = poll_interval
        self.max_poll_time = max_poll_time
        self.max_retries = max_retries
        self.proxy_type = proxy_type

        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(5)  # concurrent solve limit

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"
                    )
                },
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ─── public solve methods ───────────────────────────────────────

    async def solve_funcaptcha(
        self,
        public_key: str,
        page_url: str,
        subdomain: Optional[str] = None,
        proxy: Optional[str] = None,
        data_blob: Optional[str] = None,
    ) -> str:
        """
        solve arkose labs funcaptcha — tiktok's main captcha

        public_key: arkose public key from the page
        page_url: the page where captcha appears (tiktok.com/signup etc)
        subdomain: arkose subdomain, usually "tiktok-api" for tiktok
        proxy: socks5 proxy string "user:pass@ip:port" or "ip:port"
        data_blob: additional arkose data-blob parameter if present on page
        returns: solved token string
        """
        return await self._solve_with_retry(
            captcha_type=CaptchaType.FUNCAPTCHA,
            payload={
                "method": "funcaptcha",
                "publickey": public_key,
                "pageurl": page_url,
                "subdomain": subdomain or self.TIKTOK_ARKOSE_SUBDOMAIN,
                "data-blob": data_blob,
            },
            proxy=proxy,
        )

    async def solve_hcaptcha(
        self,
        site_key: str,
        page_url: str,
        proxy: Optional[str] = None,
    ) -> str:
        """solve hcaptcha — occasional tiktok fallback"""
        return await self._solve_with_retry(
            captcha_type=CaptchaType.HCAPTCHA,
            payload={
                "method": "hcaptcha",
                "sitekey": site_key,
                "pageurl": page_url,
            },
            proxy=proxy,
        )

    async def solve_recaptcha_v2(
        self,
        site_key: str,
        page_url: str,
        proxy: Optional[str] = None,
    ) -> str:
        """solve recaptcha v2 — rare, old tiktok endpoints"""
        return await self._solve_with_retry(
            captcha_type=CaptchaType.RECAPTCHA_V2,
            payload={
                "method": "userrecaptcha",
                "googlekey": site_key,
                "pageurl": page_url,
            },
            proxy=proxy,
        )

    async def detect_and_solve(
        self,
        captcha_type: CaptchaType,
        site_key: str,
        page_url: str,
        proxy: Optional[str] = None,
        **kwargs,
    ) -> str:
        """
        convenience — auto-route to correct solver based on captcha type
        useful when your scraper dynamically detects captcha variant
        """
        match captcha_type:
            case CaptchaType.FUNCAPTCHA:
                return await self.solve_funcaptcha(
                    public_key=site_key,
                    page_url=page_url,
                    proxy=proxy,
                    **kwargs,
                )
            case CaptchaType.HCAPTCHA:
                return await self.solve_hcaptcha(
                    site_key=site_key,
                    page_url=page_url,
                    proxy=proxy,
                )
            case CaptchaType.RECAPTCHA_V2:
                return await self.solve_recaptcha_v2(
                    site_key=site_key,
                    page_url=page_url,
                    proxy=proxy,
                )
            case _:
                raise CaptchaSolveError(
                    captcha_type=captcha_type,
                    reason=f"unsupported captcha type: {captcha_type.value}",
                )

    # ─── core solve logic ───────────────────────────────────────────

    async def _solve_with_retry(
        self,
        captcha_type: CaptchaType,
        payload: dict,
        proxy: Optional[str] = None,
    ) -> str:
        """solve with automatic retry on failure, exponential backoff"""

        last_error = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(
                    "[captcha] %s — attempt %d/%d",
                    captcha_type.value,
                    attempt,
                    self.max_retries,
                )
                token = await self._submit_and_poll(payload, proxy)
                if token and len(token) > 20:
                    logger.info("[captcha] %s — solved ✓ (attempt %d)", captcha_type.value, attempt)
                    return token

                raise CaptchaSolveError(
                    captcha_type=captcha_type,
                    reason="empty or invalid token returned",
                    attempts=attempt,
                )

            except CaptchaSolveError as e:
                last_error = e
                logger.warning("[captcha] %s — attempt %d failed: %s", captcha_type.value, attempt, e.reason)

                if attempt < self.max_retries:
                    backoff = 2 ** attempt  # 2, 4, 8 seconds
                    logger.debug("[captcha] backing off %ds before retry...", backoff)
                    await asyncio.sleep(backoff)

            except Exception as e:
                last_error = CaptchaSolveError(
                    captcha_type=captcha_type,
                    reason=f"unexpected: {type(e).__name__}: {e}",
                    attempts=attempt,
                )
                logger.error("[captcha] unexpected error on attempt %d: %s", attempt, e)
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)

        # if we're here, all retries exhausted — try fallback provider
        if self.fallback_key:
            logger.info("[captcha] primary failed, trying fallback (capsolver)...")
            try:
                return await self._solve_via_capsolver(payload, proxy)
            except Exception as e:
                last_error = CaptchaSolveError(
                    captcha_type=captcha_type,
                    reason=f"fallback also failed: {e}",
                    attempts=self.max_retries + 1,
                )

        raise last_error

    async def _submit_and_poll(self, payload: dict, proxy: Optional[str] = None) -> str:
        """submit captcha to 2captcha, poll for result"""

        async with self._semaphore:
            session = await self._get_session()

            # add auth and proxy to payload
            submit_payload = {
                "key": self.api_key,
                "json": 1,
                "soft_id": 4622,  # 2captcha app ID for tracking
                **payload,
            }

            if proxy:
                proxy_parsed = self._parse_proxy(proxy)
                submit_payload.update(proxy_parsed)

            # submit
            logger.debug("[2captcha] submitting %s...", payload.get("method"))
            async with session.post(
                "https://api.2captcha.com/createTask",
                json={"clientKey": self.api_key, "task": submit_payload},
            ) as resp:
                result = await resp.json()

            if result.get("errorId") != 0:
                raise CaptchaSolveError(
                    captcha_type=self._payload_to_type(payload),
                    reason=result.get("errorDescription", "unknown 2captcha error"),
                )

            task_id = result.get("taskId")
            if not task_id:
                raise CaptchaSolveError(
                    captcha_type=self._payload_to_type(payload),
                    reason="no taskId in response",
                )

            logger.debug("[2captcha] task created: %s, polling...", task_id)

            # poll
            elapsed = 0.0
            while elapsed < self.max_poll_time:
                await asyncio.sleep(self.poll_interval)
                elapsed += self.poll_interval

                async with session.post(
                    "https://api.2captcha.com/getTaskResult",
                    json={"clientKey": self.api_key, "taskId": task_id},
                ) as resp:
                    status = await resp.json()

                if status.get("status") == "ready":
                    solution = status.get("solution", {})
                    token = solution.get("token") or solution.get("gRecaptchaResponse") or ""
                    return token

                if status.get("errorId") != 0:
                    raise CaptchaSolveError(
                        captcha_type=self._payload_to_type(payload),
                        reason=status.get("errorDescription", "solve failed mid-poll"),
                    )

            raise CaptchaSolveError(
                captcha_type=self._payload_to_type(payload),
                reason=f"poll timeout after {self.max_poll_time}s",
            )

    async def _solve_via_capsolver(self, payload: dict, proxy: Optional[str] = None) -> str:
        """fallback: capsolver.com API"""

        session = await self._get_session()
        capsolver_payload = {
            "clientKey": self.fallback_key,
            "task": {
                "type": self._map_2captcha_to_capsolver_type(payload.get("method", "")),
                **self._build_capsolver_task(payload, proxy),
            },
        }

        async with session.post(
            "https://api.capsolver.com/createTask",
            json=capsolver_payload,
        ) as resp:
            result = await resp.json()

        if result.get("errorId") != 0:
            raise CaptchaSolveError(
                captcha_type=self._payload_to_type(payload),
                reason=f"capsolver submit: {result.get('errorDescription')}",
            )

        task_id = result["taskId"]
        elapsed = 0.0

        while elapsed < self.max_poll_time:
            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

            async with session.post(
                "https://api.capsolver.com/getTaskResult",
                json={"clientKey": self.fallback_key, "taskId": task_id},
            ) as resp:
                status = await resp.json()

            if status.get("status") == "ready":
                return status["solution"]["token"]

        raise CaptchaSolveError(
            captcha_type=self._payload_to_type(payload),
            reason="capsolver poll timeout",
        )

    # ─── helpers ────────────────────────────────────────────────────

    def _parse_proxy(self, proxy: str) -> dict:
        """
        parse proxy string into 2captcha format
        accepts: "ip:port", "user:pass@ip:port"
        """
        auth = ""
        host = proxy

        if "@" in proxy:
            auth, host = proxy.split("@", 1)
            user, pwd = auth.split(":", 1)
            return {
                "proxyType": self.proxy_type,
                "proxyAddress": host.split(":")[0],
                "proxyPort": int(host.split(":")[1]),
                "proxyLogin": user,
                "proxyPassword": pwd,
            }

        return {
            "proxyType": self.proxy_type,
            "proxyAddress": host.split(":")[0],
            "proxyPort": int(host.split(":")[1]),
        }

    def _payload_to_type(self, payload: dict) -> CaptchaType:
        method = payload.get("method", "")
        mapping = {
            "funcaptcha": CaptchaType.FUNCAPTCHA,
            "hcaptcha": CaptchaType.HCAPTCHA,
            "userrecaptcha": CaptchaType.RECAPTCHA_V2,
        }
        return mapping.get(method, CaptchaType.FUNCAPTCHA)

    def _map_2captcha_to_capsolver_type(self, method: str) -> str:
        mapping = {
            "funcaptcha": "FunCaptchaTaskProxyLess",
            "hcaptcha": "HCaptchaTaskProxyLess",
            "userrecaptcha": "ReCaptchaV2TaskProxyLess",
        }
        return mapping.get(method, "FunCaptchaTaskProxyLess")

    def _build_capsolver_task(self, payload: dict, proxy: Optional[str] = None) -> dict:
        task = {
            "websiteURL": payload.get("pageurl", ""),
            "websitePublicKey": payload.get("publickey") or payload.get("sitekey") or payload.get("googlekey", ""),
        }
        if payload.get("subdomain"):
            task["funcaptchaApiJSSubdomain"] = payload["subdomain"]
        if payload.get("data-blob"):
            task["data"] = payload["data-blob"]
        if proxy:
            parsed = self._parse_proxy(proxy)
            task["proxy"] = f"{self.proxy_type}://{parsed['proxyLogin']}:{parsed['proxyPassword']}@{parsed['proxyAddress']}:{parsed['proxyPort']}"
        return task