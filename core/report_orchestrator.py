"""
report orchestrator — mass reporting engine
takes burner accounts, fires coordinated reports at target
handles: account rotation, proxy rotation, category diversification,
         report text variation, rate limit avoidance, retry logic
"""

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


class ReportDeliveryStatus(Enum):
    """outcome of a single report attempt"""
    DELIVERED = "delivered"          # report accepted by tiktok
    RATE_LIMITED = "rate_limited"    # 429 — slow down
    BLOCKED = "blocked"              # 403 — account or IP flagged
    INVALID_SESSION = "invalid_session"  # 401 — burner account expired
    FAILED = "failed"                # network error or unknown
    CAPTCHA_REQUIRED = "captcha_required"  # captcha wall on report endpoint


@dataclass
class ReportAttempt:
    """a single report firing attempt"""
    attempt_id: str = field(default_factory=lambda: f"rpt_{int(time.time()*1000)}")
    target_uid: str = ""
    burner_account_id: str = ""
    category: str = ""
    report_text: str = ""
    status: ReportDeliveryStatus = ReportDeliveryStatus.FAILED
    http_code: int = 0
    error_detail: str = ""
    proxy_used: str = ""
    fired_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    response_time_ms: float = 0.0


@dataclass
class ReportWaveResult:
    """summary of a report wave"""
    wave_id: str = field(default_factory=lambda: f"wave_{int(time.time())}")
    target_username: str = ""
    target_uid: str = ""
    total_requested: int = 0
    total_delivered: int = 0
    total_rate_limited: int = 0
    total_blocked: int = 0
    total_failed: int = 0
    accounts_used: int = 0
    accounts_burned: int = 0
    attempts: list[ReportAttempt] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    escalation_level: Optional[str] = None

    @property
    def delivery_rate(self) -> float:
        if self.total_requested == 0:
            return 0.0
        return self.total_delivered / self.total_requested

    @property
    def is_effective(self) -> bool:
        """was this wave likely to trigger moderation?"""
        return self.total_delivered >= 10 and self.delivery_rate >= 0.5


class ReportOrchestrator:
    """
    coordinates mass reporting campaigns against target accounts

    usage:
        orchestrator = ReportOrchestrator(
            account_pool=pool_instance,
            proxy_rotator=proxy_instance,
            session_manager=session_instance,
            db=db_instance,
        )
        result = await orchestrator.fire_wave(
            target_info=target,
            report_count=50,
            category_weights={"harassment": 30, "spam": 30, ...}
        )
    """

    # known tiktok report endpoints
    REPORT_ENDPOINTS = [
        "https://www.tiktok.com/aweme/v2/aweme/feedback/",
        "https://www.tiktok.com/api/commit/item/feedback/",
        "https://m.tiktok.com/api/commit/item/feedback/",
    ]

    # report categories tiktok accepts
    REPORT_CATEGORIES = {
        "harassment": {
            "reason_id": 1,
            "sub_reasons": [101, 102, 103],  # bullying, threats, targeted harassment
        },
        "spam": {
            "reason_id": 2,
            "sub_reasons": [201, 202],  # fake engagement, misleading
        },
        "impersonation": {
            "reason_id": 3,
            "sub_reasons": [301, 302],  # pretending to be someone, fake account
        },
        "underage": {
            "reason_id": 4,
            "sub_reasons": [401, 402],  # under 13, minor safety
        },
        "self_harm": {
            "reason_id": 5,
            "sub_reasons": [501, 502],  # suicide/self-harm content
        },
        "illegal": {
            "reason_id": 6,
            "sub_reasons": [601, 602, 603],  # drugs, weapons, illegal activities
        },
        "terrorism": {
            "reason_id": 7,
            "sub_reasons": [701, 702],  # violent extremism, terrorist content
        },
    }

    # diverse report text templates per category — tiktok's NLP looks for patterns
    REPORT_TEXT_TEMPLATES = {
        "harassment": [
            "This account is repeatedly harassing and bullying other users in comments and videos.",
            "Targeted harassment campaign against multiple users. Check their comment history.",
            "This user sends threatening messages and creates videos mocking specific individuals.",
            "Coordinated bullying behavior. Multiple victims have reported this account.",
            "The user is engaging in persistent online harassment targeting minors.",
        ],
        "spam": [
            "This account is posting spam content and fake engagement bait repeatedly.",
            "Bot account posting misleading links and scam content across multiple videos.",
            "Mass spam posting identical comments promoting fraudulent services.",
            "Fake engagement farming — buying views and likes through bot networks.",
            "This account exists solely to spam comment sections with promotional garbage.",
        ],
        "impersonation": [
            "This account is impersonating a known creator and scamming their followers.",
            "Fake account pretending to be someone else, stealing their content and identity.",
            "Impersonating a minor for inappropriate purposes. Needs immediate review.",
            "This user created a copycat account using another creator's name and content.",
            "Identity theft — using stolen photos and personal information of another person.",
        ],
        "underage": [
            "This account appears to belong to someone under 13 years old, violating TikTok's minimum age.",
            "Minor posting inappropriate content. Account holder appears to be underage.",
            "Underage user being exploited in comments. Child safety concern.",
            "Account holder is clearly under 13 based on content and appearance.",
            "Child safety violation — minor engaging with adult content and users.",
        ],
        "self_harm": [
            "This account is posting content that glorifies self-harm and suicide.",
            "Dangerous content promoting self-harm behaviors to vulnerable viewers.",
            "Suicide encouragement and self-harm instructions found on this profile.",
            "Content depicting self-harm with instructional elements. Extremely dangerous.",
            "This user is spreading self-harm challenges targeting teenage audience.",
        ],
        "illegal": [
            "This account is promoting illegal activities and prohibited goods.",
            "Content shows illegal drug use and distribution. Violates community guidelines.",
            "Weapons trading and illegal goods promotion found on this account.",
            "Criminal activity being broadcast. Immediate moderator review required.",
            "Illegal gambling and unlicensed financial services being promoted.",
        ],
        "terrorism": [
            "Violent extremist content found on this account. National security concern.",
            "Terrorism glorification and recruitment content targeting vulnerable users.",
            "Violent extremist propaganda being shared across this user's content.",
            "Content promotes terrorist organizations and violent attacks.",
            "Extremist recruitment material targeting young users on this profile.",
        ],
    }

    def __init__(
        self,
        account_pool=None,      # AccountPool instance
        proxy_rotator=None,     # ProxyRotator instance
        session_manager=None,   # SessionManager instance
        db=None,                # Database instance
        report_logger=None,     # ReportLogger instance
        config: Optional[dict] = None,
    ):
        self.account_pool = account_pool
        self.proxy_rotator = proxy_rotator
        self.session_manager = session_manager
        self.db = db
        self.report_logger = report_logger
        self.config = config or {}

        # concurrency
        self._max_concurrent_reports = self.config.get("report_max_concurrent", 8)
        self._report_semaphore = asyncio.Semaphore(self._max_concurrent_reports)

        # timing
        self._min_delay = self.config.get("report_min_delay_ms", 300)   # milliseconds
        self._max_delay = self.config.get("report_max_delay_ms", 8000)  # milliseconds

        # retry
        self._max_retries_per_report = self.config.get("report_max_retries", 2)

        # stats
        self._total_reports_fired = 0
        self._total_reports_delivered = 0
        self._active_waves: dict[str, ReportWaveResult] = {}

        self._http_session: Optional[aiohttp.ClientSession] = None

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.6478.122 Mobile Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
        return self._http_session

    async def close(self):
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    # ─── main API ───────────────────────────────────────────────────

    async def fire_wave(
        self,
        target_info,           # TargetInfo
        report_count: int,
        category_weights: Optional[dict] = None,
        escalation_level=None,  # EscalationLevel enum or None
    ) -> ReportWaveResult:
        """
        fire a coordinated wave of reports at the target

        target_info: TargetInfo dataclass
        report_count: how many reports to fire in this wave
        category_weights: dict of {category_name: weight} — defaults to balanced
        escalation_level: EscalationLevel for logging/reporting
        returns: ReportWaveResult with full delivery stats
        """
        if category_weights is None:
            category_weights = {
                "harassment": 25, "spam": 25, "impersonation": 20,
                "underage": 10, "self_harm": 10, "illegal": 5, "terrorism": 5,
            }

        result = ReportWaveResult(
            target_username=target_info.username,
            target_uid=target_info.uid,
            total_requested=report_count,
            escalation_level=escalation_level.name if escalation_level else None,
        )
        wave_id = result.wave_id
        self._active_waves[wave_id] = result

        logger.info(
            "[orchestrator] wave %s — @%s — firing %d reports with concurrency %d",
            wave_id,
            target_info.username,
            report_count,
            self._max_concurrent_reports,
        )

        # build category distribution
        categories = self._weighted_category_pick(category_weights, report_count)

        # build report tasks
        tasks = []
        for idx, category in enumerate(categories):
            tasks.append(
                self._fire_single_report(
                    wave_id=wave_id,
                    target_info=target_info,
                    category=category,
                    report_index=idx,
                )
            )

        # fire all with concurrency control, staggered starts
        sem_tasks = []
        for i, task in enumerate(tasks):
            sem_tasks.append(self._report_with_semaphore(task, i))

        await asyncio.gather(*sem_tasks, return_exceptions=True)

        result.completed_at = datetime.utcnow().isoformat()

        # update stats
        self._total_reports_fired += result.total_requested
        self._total_reports_delivered += result.total_delivered

        # log to storage
        if self.report_logger:
            await self.report_logger.log_wave(result)
        if self.db:
            await self.db.save_report_wave(result)

        logger.info(
            "[orchestrator] wave %s complete — %d/%d delivered (%.0f%%), "
            "%d rate-limited, %d blocked, %d accounts used, %d burned",
            wave_id,
            result.total_delivered,
            result.total_requested,
            result.delivery_rate * 100,
            result.total_rate_limited,
            result.total_blocked,
            result.accounts_used,
            result.accounts_burned,
        )

        self._active_waves.pop(wave_id, None)
        return result

    async def fire_single(
        self,
        target_info,
        category: str = "harassment",
        burner_account=None,  # BurnerAccount — if None, pulled from pool
        proxy: Optional[str] = None,
    ) -> ReportAttempt:
        """
        fire a single report — useful for testing or precise targeting
        """
        wave_id = f"single_{int(time.time())}"

        if burner_account is None and self.account_pool:
            burner_account = await self.account_pool.get_available()
            if burner_account is None:
                return ReportAttempt(
                    target_uid=target_info.uid,
                    status=ReportDeliveryStatus.FAILED,
                    error_detail="no burner accounts available",
                )

        if burner_account is None:
            return ReportAttempt(
                target_uid=target_info.uid,
                status=ReportDeliveryStatus.FAILED,
                error_detail="no burner account provided and no pool configured",
            )

        return await self._execute_report(
            target_info=target_info,
            category=category,
            burner=burner_account,
            proxy=proxy,
        )

    # ─── internal report execution ──────────────────────────────────

    async def _report_with_semaphore(self, coro, index: int):
        """wrap a report task with semaphore and staggered delay"""
        async with self._report_semaphore:
            # stagger start to avoid burst patterns
            stagger_ms = random.randint(50, 400)
            await asyncio.sleep(stagger_ms / 1000)

            try:
                return await coro
            except Exception as e:
                logger.error("[orchestrator] report task %d crashed: %s", index, e)
                return None

    async def _fire_single_report(
        self,
        wave_id: str,
        target_info,
        category: str,
        report_index: int,
    ) -> Optional[ReportAttempt]:
        """
        full lifecycle of a single report:
        get account → get proxy → build payload → fire → handle result → retire if needed
        """

        # get burner account
        burner = None
        if self.account_pool:
            burner = await self.account_pool.get_available()
            if burner is None:
                logger.debug("[orchestrator] no burner available for report %d", report_index)
                wave = self._active_waves.get(wave_id)
                if wave:
                    wave.total_failed += 1
                return None

        if burner is None:
            logger.warning("[orchestrator] no account pool — cannot fire report")
            return None

        # get proxy
        proxy = None
        if self.proxy_rotator:
            proxy = await self.proxy_rotator.get_proxy()

        # execute with retries
        attempt = None
        for retry in range(self._max_retries_per_report + 1):
            attempt = await self._execute_report(
                target_info=target_info,
                category=category,
                burner=burner,
                proxy=proxy,
            )

            # if delivered or hard failure, stop retrying
            if attempt.status == ReportDeliveryStatus.DELIVERED:
                break
            if attempt.status == ReportDeliveryStatus.INVALID_SESSION:
                break  # account dead, don't retry
            if attempt.status == ReportDeliveryStatus.BLOCKED:
                # maybe proxy blocked — try once more with new proxy
                if self.proxy_rotator:
                    proxy = await self.proxy_rotator.get_proxy()
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    continue
                break

            # rate limited — backoff and retry
            if attempt.status == ReportDeliveryStatus.RATE_LIMITED:
                backoff = (2 ** retry) + random.uniform(0, 1)
                await asyncio.sleep(backoff)
                if self.proxy_rotator:
                    proxy = await self.proxy_rotator.get_proxy()

        # update wave stats
        wave = self._active_waves.get(wave_id)
        if wave and attempt:
            wave.attempts.append(attempt)
            if attempt.status == ReportDeliveryStatus.DELIVERED:
                wave.total_delivered += 1
            elif attempt.status == ReportDeliveryStatus.RATE_LIMITED:
                wave.total_rate_limited += 1
            elif attempt.status == ReportDeliveryStatus.BLOCKED:
                wave.total_blocked += 1
            else:
                wave.total_failed += 1

        # update burner report count
        if burner:
            burner.report_count += 1
            wave.accounts_used = wave.accounts_used + 1 if wave else 1

            # retire burner after 3-4 reports
            if burner.report_count >= random.randint(3, 4):
                burner.is_active = False
                if self.account_pool:
                    await self.account_pool.retire(burner)
                if wave:
                    wave.accounts_burned += 1
                logger.debug("[orchestrator] burner %s retired after %d reports", burner.account_id, burner.report_count)
            else:
                # return to pool if still usable
                if self.account_pool and burner.is_active:
                    await self.account_pool.release(burner)

        # save individual report to DB
        if self.db and attempt:
            await self.db.save_report_attempt(attempt)

        # random delay between reports in the wave
        delay_ms = random.randint(self._min_delay, self._max_delay) / 1000
        await asyncio.sleep(delay_ms)

        return attempt

    async def _execute_report(
        self,
        target_info,
        category: str,
        burner,
        proxy: Optional[str] = None,
    ) -> ReportAttempt:
        """
        actually send the report to tiktok's endpoint
        """
        attempt = ReportAttempt(
            target_uid=target_info.uid,
            burner_account_id=burner.account_id,
            category=category,
            report_text="",
            proxy_used=_mask_proxy(proxy) if proxy else "direct",
        )

        # generate report text
        attempt.report_text = self._generate_report_text(category)

        # build payload
        cat_config = self.REPORT_CATEGORIES.get(category, self.REPORT_CATEGORIES["harassment"])
        payload = self._build_report_payload(
            target_uid=target_info.uid,
            category=cat_config,
            report_text=attempt.report_text,
            target_username=target_info.username,
        )

        # format proxy
        proxy_url = _format_proxy(proxy) if proxy else None

        # build headers with burner session
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.6478.122 Mobile Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": f"https://www.tiktok.com/@{target_info.username}",
            "Origin": "https://www.tiktok.com",
            "Cookie": burner.session_cookie,
        }

        http = await self._get_http()
        start_time = time.monotonic()

        try:
            # try each known endpoint
            for endpoint in self.REPORT_ENDPOINTS:
                try:
                    async with http.post(
                        endpoint,
                        data=payload,
                        headers=headers,
                        proxy=proxy_url,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        attempt.http_code = resp.status
                        attempt.response_time_ms = (time.monotonic() - start_time) * 1000

                        if resp.status == 200:
                            body = await resp.json()
                            if body.get("code") == 0 or body.get("status_code") == 0:
                                attempt.status = ReportDeliveryStatus.DELIVERED
                                return attempt

                            # check for captcha requirement
                            if body.get("code") in (8, 1008, 2122):
                                attempt.status = ReportDeliveryStatus.CAPTCHA_REQUIRED
                                attempt.error_detail = body.get("message", "captcha required")
                                return attempt

                            # some other rejection
                            attempt.status = ReportDeliveryStatus.FAILED
                            attempt.error_detail = body.get("message", f"code: {body.get('code')}")

                        elif resp.status == 429:
                            attempt.status = ReportDeliveryStatus.RATE_LIMITED
                            attempt.error_detail = "rate limited"

                        elif resp.status in (401, 403):
                            attempt.status = ReportDeliveryStatus.INVALID_SESSION if resp.status == 401 else ReportDeliveryStatus.BLOCKED
                            attempt.error_detail = f"HTTP {resp.status}"
                            # mark burner as dead
                            burner.is_active = False
                            if self.account_pool:
                                await self.account_pool.retire(burner)

                        else:
                            attempt.status = ReportDeliveryStatus.FAILED
                            attempt.error_detail = f"HTTP {resp.status}"

                except asyncio.TimeoutError:
                    continue  # try next endpoint
                except aiohttp.ClientError:
                    continue

            # all endpoints failed
            if attempt.status == ReportDeliveryStatus.FAILED and not attempt.error_detail:
                attempt.error_detail = "all endpoints unreachable"

        except Exception as e:
            attempt.status = ReportDeliveryStatus.FAILED
            attempt.error_detail = f"{type(e).__name__}: {e}"

        attempt.response_time_ms = (time.monotonic() - start_time) * 1000
        return attempt

    # ─── helpers ────────────────────────────────────────────────────

    def _build_report_payload(
        self,
        target_uid: str,
        category: dict,
        report_text: str,
        target_username: str,
    ) -> dict:
        """build the form-encoded payload tiktok expects"""

        sub_reason = random.choice(category["sub_reasons"])

        # tiktok's actual report payload structure (varies by endpoint)
        payload = {
            "object_id": target_uid,
            "owner_id": target_uid,
            "report_type": "user",
            "reason": category["reason_id"],
            "sub_reason": sub_reason,
            "description": report_text[:500],  # tiktok caps descriptions
            "target": f"@{target_username}",
            "source": "profile",
            "scene": 1,
        }

        return payload

    def _generate_report_text(self, category: str) -> str:
        """generate varied report text to avoid pattern detection"""

        templates = self.REPORT_TEXT_TEMPLATES.get(
            category,
            self.REPORT_TEXT_TEMPLATES["harassment"],
        )

        base = random.choice(templates)

        # add small variations — timestamp-like number, slightly different phrasing
        variations = [
            base,
            base + f" Ref: {random.randint(10000, 99999)}",
            base.replace(".", "!"),
            base.lower(),
            base + " Please review urgently.",
            "URGENT: " + base,
        ]

        return random.choice(variations)

    def _weighted_category_pick(self, weights: dict, count: int) -> list[str]:
        """generate a list of categories based on weight distribution"""
        categories = list(weights.keys())
        weights_list = [weights[c] for c in categories]

        # normalize
        total = sum(weights_list)
        if total == 0:
            return [random.choice(categories) for _ in range(count)]

        probs = [w / total for w in weights_list]

        return random.choices(categories, weights=probs, k=count)

    @property
    def total_reports_fired(self) -> int:
        return self._total_reports_fired

    @property
    def total_reports_delivered(self) -> int:
        return self._total_reports_delivered


# ─── module utilities ───────────────────────────────────────────────

def _format_proxy(proxy: str) -> Optional[str]:
    if not proxy:
        return None
    if proxy.startswith(("http://", "socks5://", "socks4://")):
        return proxy
    return f"socks5://{proxy}"


def _mask_proxy(proxy: str) -> str:
    if "@" in proxy:
        return proxy.split("@")[-1]
    return proxy