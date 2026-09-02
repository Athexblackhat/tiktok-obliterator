"""
burner account factory — fully automated tiktok account creation pipeline
stages: proxy rotate → fingerprint spoof → email generate → signup form →
        captcha solve → email verify → session token store → pool push

integrates with: captcha/, email/, network/, storage/
"""

import asyncio
import logging
import random
import string
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


# ─── data structures ────────────────────────────────────────────────


class AccountCreateStage(Enum):
    """each stage of the burner creation pipeline"""
    PROXY_ASSIGN = "proxy_assign"
    FINGERPRINT_GEN = "fingerprint_gen"
    EMAIL_GEN = "email_gen"
    SIGNUP_INIT = "signup_init"
    CAPTCHA_SOLVE = "captcha_solve"
    FORM_SUBMIT = "form_submit"
    EMAIL_VERIFY = "email_verify"
    SESSION_STORE = "session_store"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class BurnerAccount:
    """a fully created burner account ready for reporting"""

    account_id: str  # internal ID for tracking
    email: str  # the email used to sign up
    tiktok_uid: str  # tiktok's internal user ID for this burner
    tiktok_username: str  # @handle of the burner
    session_cookie: str  # full session cookie string for authenticated requests
    session_token: str  # tiktok session token (extracted from cookie)
    proxy_used: str  # which proxy was used during creation
    fingerprint_id: str  # fingerprint profile identifier
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    report_count: int = 0  # how many reports fired from this account
    is_active: bool = True  # still usable or shadowbanned
    stage_reached: AccountCreateStage = AccountCreateStage.COMPLETE


@dataclass
class CreateAttempt:
    """tracks a single account creation attempt through all stages"""

    attempt_id: str = field(default_factory=lambda: f"ca_{int(time.time()*1000)}")
    stage: AccountCreateStage = AccountCreateStage.PROXY_ASSIGN
    error: Optional[str] = None
    account: Optional[BurnerAccount] = None
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None


class AccountCreateError(Exception):
    """raised when account creation fails at any stage"""

    def __init__(self, stage: AccountCreateStage, reason: str, attempt_id: str = ""):
        self.stage = stage
        self.reason = reason
        self.attempt_id = attempt_id
        super().__init__(f"[{stage.value}] {reason} (attempt: {attempt_id})")


# ─── factory ────────────────────────────────────────────────────────


class AccountFactory:
    """
    automated burner account creation pipeline

    usage:
        factory = AccountFactory(
            email_domain="catchall.lol",
            captcha_solver=solver_instance,
            proxy_rotator=rotator_instance,
            db=db_instance,
        )
        account = await factory.create_account()
        # account is a BurnerAccount with valid session, ready to report
    """

    # signup form defaults — randomized within ranges
    MIN_AGE = 18
    MAX_AGE = 35
    USERNAME_MIN_LEN = 8
    USERNAME_MAX_LEN = 18

    # tiktok signup endpoints
    SIGNUP_PAGE = "https://www.tiktok.com/signup"
    SIGNUP_EMAIL_ENDPOINT = "https://www.tiktok.com/passport/email/register/"
    SEND_VERIFICATION_CODE = "https://www.tiktok.com/passport/email/send-code/"
    VERIFY_CODE_ENDPOINT = "https://www.tiktok.com/passport/email/verify-code/"
    SET_USERNAME_ENDPOINT = "https://www.tiktok.com/passport/username/set/"
    COMPLETE_PROFILE_ENDPOINT = "https://www.tiktok.com/passport/profile/update/"

    def __init__(
        self,
        email_domain: str,
        captcha_solver,  # CaptchaSolver instance
        proxy_rotator,  # ProxyRotator instance (from network/)
        fingerprint_engine=None,  # FingerprintEngine instance (from network/)
        session_manager=None,  # SessionManager instance (from network/)
        email_generator=None,  # CatchallGenerator instance (from email/)
        email_verifier=None,  # VerificationListener instance (from email/)
        db=None,  # Database instance (from storage/)
        account_pool=None,  # AccountPool instance (from storage/)
        config: Optional[dict] = None,
    ):
        self.email_domain = email_domain
        self.captcha = captcha_solver
        self.proxy_rotator = proxy_rotator
        self.fingerprint_engine = fingerprint_engine
        self.session_manager = session_manager
        self.email_generator = email_generator
        self.email_verifier = email_verifier
        self.db = db
        self.account_pool = account_pool
        self.config = config or {}

        # concurrency control
        self._creation_semaphore = asyncio.Semaphore(
            self.config.get("factory_max_concurrent", 5)
        )

        # stats
        self.total_attempts = 0
        self.total_success = 0
        self.total_failed = 0

        self._http_session: Optional[aiohttp.ClientSession] = None

    async def _get_http(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
        return self._http_session

    async def close(self):
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

    # ─── main creation flow ─────────────────────────────────────────

    async def create_account(self) -> BurnerAccount:
        """
        create a single burner account through the full pipeline
        returns: BurnerAccount with active session
        raises: AccountCreateError on failure at any stage
        """
        async with self._creation_semaphore:
            attempt = CreateAttempt()
            self.total_attempts += 1

            logger.info("[factory] starting account creation — attempt %s", attempt.attempt_id)

            try:
                # stage 1: proxy
                proxy = await self._stage_proxy(attempt)

                # stage 2: fingerprint
                fingerprint = await self._stage_fingerprint(attempt, proxy)

                # stage 3: email
                email = await self._stage_email(attempt)

                # stage 4: signup init
                signup_ctx = await self._stage_signup_init(attempt, proxy, fingerprint, email)

                # stage 5: captcha
                captcha_token = await self._stage_captcha(attempt, proxy, signup_ctx)

                # stage 6: form submit
                submit_result = await self._stage_form_submit(
                    attempt, proxy, email, captcha_token, signup_ctx, fingerprint
                )

                # stage 7: email verify
                verified = await self._stage_email_verify(attempt, email)
                if not verified:
                    raise AccountCreateError(
                        AccountCreateStage.EMAIL_VERIFY,
                        "verification link not received or expired",
                        attempt.attempt_id,
                    )

                # stage 8: store session
                account = await self._stage_store_session(
                    attempt, proxy, email, submit_result, fingerprint
                )

                # success
                attempt.stage = AccountCreateStage.COMPLETE
                attempt.completed_at = datetime.utcnow().isoformat()
                attempt.account = account
                self.total_success += 1

                # push to pool if available
                if self.account_pool:
                    await self.account_pool.add(account)

                logger.info(
                    "[factory] ✓ account created — %s (%s) [%d/%d succeeded]",
                    account.tiktok_username,
                    account.account_id,
                    self.total_success,
                    self.total_attempts,
                )
                return account

            except AccountCreateError:
                self.total_failed += 1
                attempt.stage = AccountCreateStage.FAILED
                attempt.completed_at = datetime.utcnow().isoformat()
                logger.error(
                    "[factory] ✗ creation failed at stage [%s]: %s",
                    attempt.stage.value,
                    attempt.error,
                )
                raise

            except Exception as e:
                self.total_failed += 1
                attempt.stage = AccountCreateStage.FAILED
                attempt.error = str(e)
                logger.exception("[factory] unexpected error during creation")
                raise AccountCreateError(
                    attempt.stage,
                    f"unexpected: {type(e).__name__}: {e}",
                    attempt.attempt_id,
                )

    async def create_batch(
        self,
        count: int,
        max_parallel: int = 5,
    ) -> list[BurnerAccount]:
        """
        create multiple burner accounts concurrently
        count: total accounts needed
        max_parallel: how many to create simultaneously
        returns: list of successfully created BurnerAccounts
        """

        logger.info("[factory] batch creation — target: %d, parallel: %d", count, max_parallel)
        results = []
        semaphore = asyncio.Semaphore(max_parallel)

        async def _create_one(idx: int) -> Optional[BurnerAccount]:
            async with semaphore:
                try:
                    acct = await self.create_account()
                    logger.debug("[factory] batch [%d/%d] done", idx + 1, count)
                    return acct
                except AccountCreateError as e:
                    logger.warning("[factory] batch [%d/%d] failed: %s", idx + 1, count, e.reason)
                    return None

        tasks = [_create_one(i) for i in range(count)]
        completed = await asyncio.gather(*tasks, return_exceptions=True)

        for result in completed:
            if isinstance(result, BurnerAccount):
                results.append(result)
            elif isinstance(result, Exception):
                logger.error("[factory] batch task exception: %s", result)
            # None = failed gracefully, skip

        logger.info(
            "[factory] batch complete — %d/%d accounts created successfully",
            len(results),
            count,
        )
        return results

    # ─── pipeline stages ────────────────────────────────────────────

    async def _stage_proxy(self, attempt: CreateAttempt) -> str:
        """stage 1: get a fresh proxy from the rotator"""
        attempt.stage = AccountCreateStage.PROXY_ASSIGN

        if self.proxy_rotator:
            proxy = await self.proxy_rotator.get_proxy()
            logger.debug("[factory:%s] proxy assigned: %s", attempt.attempt_id, _mask_proxy(proxy))
            return proxy

        # no proxy rotator configured — use direct connection
        logger.warning("[factory:%s] no proxy rotator — using direct connection", attempt.attempt_id)
        return ""

    async def _stage_fingerprint(self, attempt: CreateAttempt, proxy: str) -> Optional[dict]:
        """stage 2: generate a clean browser fingerprint"""
        attempt.stage = AccountCreateStage.FINGERPRINT_GEN

        if self.fingerprint_engine:
            fp = await self.fingerprint_engine.generate(proxy=proxy)
            logger.debug("[factory:%s] fingerprint: %s", attempt.attempt_id, fp.get("id", "unknown"))
            return fp

        logger.debug("[factory:%s] no fingerprint engine — using defaults", attempt.attempt_id)
        return None

    async def _stage_email(self, attempt: CreateAttempt) -> str:
        """stage 3: generate a unique email address"""
        attempt.stage = AccountCreateStage.EMAIL_GEN

        if self.email_generator:
            email = await self.email_generator.generate()
        else:
            # fallback: generate from domain directly
            tag = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
            email = f"burner_{tag}@{self.email_domain}"

        logger.debug("[factory:%s] email: %s", attempt.attempt_id, email)
        return email

    async def _stage_signup_init(
        self,
        attempt: CreateAttempt,
        proxy: str,
        fingerprint: Optional[dict],
        email: str,
    ) -> dict:
        """
        stage 4: initialize signup — load signup page, extract CSRF tokens,
        arkose public key, and any other required parameters
        returns signup context dict with tokens/keys
        """
        attempt.stage = AccountCreateStage.SIGNUP_INIT

        http = await self._get_http()
        proxy_url = _format_proxy(proxy) if proxy else None

        # fetch signup page to get cookies and tokens
        async with http.get(
            self.SIGNUP_PAGE,
            proxy=proxy_url,
            headers=self._get_browser_headers(fingerprint),
        ) as resp:
            page_html = await resp.text()

        # extract arkose funcaptcha public key
        import re
        arkose_key_match = re.search(
            r'public_key["\']?\s*[:=]\s*["\']([A-F0-9\-]{20,})["\']',
            page_html,
            re.IGNORECASE,
        )
        public_key = arkose_key_match.group(1) if arkose_key_match else None

        # extract CSRF token from cookies
        csrf_token = ""
        for cookie in resp.cookies.values():
            if "csrf" in cookie.key.lower() or "token" in cookie.key.lower():
                csrf_token = cookie.value
                break

        # collect cookies for subsequent requests
        cookie_jar = {cookie.key: cookie.value for cookie in resp.cookies.values()}

        signup_ctx = {
            "public_key": public_key or "7C4C8A9D-3F3D-4A6E-B22E-2E2E2E2E2E2E",
            "csrf_token": csrf_token,
            "cookies": cookie_jar,
            "subdomain": "tiktok-api",
        }

        logger.debug(
            "[factory:%s] signup init — arkose_key:%s csrf:%s",
            attempt.attempt_id,
            signup_ctx["public_key"][:12] + "...",
            bool(csrf_token),
        )
        return signup_ctx

    async def _stage_captcha(
        self,
        attempt: CreateAttempt,
        proxy: str,
        signup_ctx: dict,
    ) -> str:
        """stage 5: solve the arkose funcaptcha"""
        attempt.stage = AccountCreateStage.CAPTCHA_SOLVE

        logger.debug("[factory:%s] solving captcha...", attempt.attempt_id)
        token = await self.captcha.solve_funcaptcha(
            public_key=signup_ctx["public_key"],
            page_url=self.SIGNUP_PAGE,
            subdomain=signup_ctx.get("subdomain", "tiktok-api"),
            proxy=proxy if proxy else None,
        )
        logger.debug("[factory:%s] captcha solved — token:%s...", attempt.attempt_id, token[:20])
        return token

    async def _stage_form_submit(
        self,
        attempt: CreateAttempt,
        proxy: str,
        email: str,
        captcha_token: str,
        signup_ctx: dict,
        fingerprint: Optional[dict],
    ) -> dict:
        """
        stage 6: submit the registration form
        generates username, password, DOB, submits to tiktok
        returns: response data including initial session cookies
        """
        attempt.stage = AccountCreateStage.FORM_SUBMIT

        http = await self._get_http()
        proxy_url = _format_proxy(proxy) if proxy else None

        # generate account details
        username = self._generate_username()
        password = self._generate_password()
        birth_year = datetime.utcnow().year - random.randint(self.MIN_AGE, self.MAX_AGE)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)

        payload = {
            "email": email,
            "password": password,
            "username": username,
            "birthday": f"{birth_year}-{birth_month:02d}-{birth_day:02d}",
            "captcha_token": captcha_token,
            "policy": True,
            "terms": True,
            "country_code": fingerprint.get("country_code", "US") if fingerprint else "US",
            "language": "en",
        }

        headers = self._get_browser_headers(fingerprint)
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        if signup_ctx.get("csrf_token"):
            headers["X-CSRF-Token"] = signup_ctx["csrf_token"]
        headers["Referer"] = self.SIGNUP_PAGE

        cookies = signup_ctx.get("cookies", {})

        async with http.post(
            self.SIGNUP_EMAIL_ENDPOINT,
            data=payload,
            proxy=proxy_url,
            headers=headers,
            cookies=cookies,
        ) as resp:
            raw = await resp.json()

        if resp.status != 200 or raw.get("code") != 0:
            error_msg = raw.get("message", raw.get("msg", f"HTTP {resp.status}"))
            raise AccountCreateError(
                AccountCreateStage.FORM_SUBMIT,
                f"tiktok rejected: {error_msg}",
                attempt.attempt_id,
            )

        # collect session cookies from response
        session_cookies = {**cookies}
        for cookie in resp.cookies.values():
            session_cookies[cookie.key] = cookie.value

        result = {
            "username": username,
            "password": password,
            "email": email,
            "tiktok_uid": raw.get("user_id", raw.get("uid", "")),
            "session_cookies": session_cookies,
            "session_token": session_cookies.get("sessionid", ""),
        }

        logger.debug(
            "[factory:%s] form submitted — username:@%s uid:%s",
            attempt.attempt_id,
            username,
            result["tiktok_uid"],
        )
        return result

    async def _stage_email_verify(self, attempt: CreateAttempt, email: str) -> bool:
        """stage 7: wait for and click verification email"""
        attempt.stage = AccountCreateStage.EMAIL_VERIFY

        if self.email_verifier:
            logger.debug("[factory:%s] waiting for verification email...", attempt.attempt_id)
            verified = await self.email_verifier.wait_for_verification(
                email=email,
                timeout=self.config.get("email_verify_timeout", 90),
            )
            return verified

        # no verifier configured — assume email doesn't need verification
        # (some tiktok signup flows skip it with clean fingerprints)
        logger.warning("[factory:%s] no email verifier — assuming auto-verified", attempt.attempt_id)
        await asyncio.sleep(3)  # brief pause for tiktok backend
        return True

    async def _stage_store_session(
        self,
        attempt: CreateAttempt,
        proxy: str,
        email: str,
        submit_result: dict,
        fingerprint: Optional[dict],
    ) -> BurnerAccount:
        """stage 8: finalize — build BurnerAccount, store in DB, add to pool"""
        attempt.stage = AccountCreateStage.SESSION_STORE

        account_id = f"burner_{int(time.time()*1000)}_{random.randint(1000,9999)}"
        session_cookie_str = "; ".join(
            f"{k}={v}" for k, v in submit_result["session_cookies"].items()
        )

        account = BurnerAccount(
            account_id=account_id,
            email=email,
            tiktok_uid=submit_result["tiktok_uid"],
            tiktok_username=submit_result["username"],
            session_cookie=session_cookie_str,
            session_token=submit_result.get("session_token", ""),
            proxy_used=proxy,
            fingerprint_id=fingerprint.get("id", "default") if fingerprint else "default",
            report_count=0,
            is_active=True,
            stage_reached=AccountCreateStage.COMPLETE,
        )

        # store in database
        if self.db:
            await self.db.save_account(account)

        # register with session manager
        if self.session_manager:
            await self.session_manager.register(account)

        logger.debug("[factory:%s] account stored — id:%s", attempt.attempt_id, account_id)
        return account

    # ─── helpers ────────────────────────────────────────────────────

    def _generate_username(self) -> str:
        """generate a plausible-looking tiktok username"""
        patterns = [
            # user_word123
            lambda: f"{_random_word()}{random.randint(10, 999)}",
            # word.word123
            lambda: f"{_random_word()}.{_random_word()}{random.randint(1, 99)}",
            # word_1234
            lambda: f"{_random_word()}_{random.randint(1000, 9999)}",
            # firstname_lastname format
            lambda: f"{_random_firstname()}_{_random_word()}",
        ]

        pattern = random.choice(patterns)
        username = pattern()

        # enforce length limits
        username = username.lower().replace(" ", "")
        if len(username) > self.USERNAME_MAX_LEN:
            username = username[: self.USERNAME_MAX_LEN].rstrip("._")
        if len(username) < self.USERNAME_MIN_LEN:
            username += str(random.randint(100, 999))

        return username

    def _generate_password(self) -> str:
        """generate a strong random password"""
        chars = string.ascii_letters + string.digits + "!@#$%^&*"
        base = "".join(random.choices(chars, k=12))
        # ensure at least one of each required type
        base += random.choice(string.ascii_uppercase)
        base += random.choice(string.digits)
        base += random.choice("!@#$%^&*")
        return "".join(random.sample(base, len(base)))

    def _get_browser_headers(self, fingerprint: Optional[dict]) -> dict:
        """build browser-like headers, optionally with fingerprint data"""
        base = {
            "User-Agent": (
                fingerprint.get("user_agent")
                if fingerprint
                else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                     "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": fingerprint.get("language", "en-US,en;q=0.9") if fingerprint else "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        return base


# ─── module-level utilities ─────────────────────────────────────────

# small word bank for username generation
_WORDS = [
    "vibe", "chill", "flick", "glow", "snap", "drift", "spark", "cloud",
    "pixel", "void", "flux", "wave", "echo", "frost", "blaze", "shade",
    "lunar", "solar", "cyber", "retro", "neo", "hyper", "micro", "macro",
]

_FIRSTNAMES = [
    "alex", "jordan", "casey", "morgan", "riley", "quinn", "avery", "blake",
    "cameron", "dakota", "emery", "finley", "harper", "jade", "kendall", "logan",
]


def _random_word() -> str:
    return random.choice(_WORDS)


def _random_firstname() -> str:
    return random.choice(_FIRSTNAMES)


def _format_proxy(proxy: str) -> Optional[str]:
    """ensure proxy string has proper URL scheme"""
    if not proxy:
        return None
    if proxy.startswith(("http://", "socks5://", "socks4://")):
        return proxy
    return f"socks5://{proxy}"


def _mask_proxy(proxy: str) -> str:
    """mask proxy address for logging"""
    if "@" in proxy:
        return proxy.split("@")[-1]
    return proxy