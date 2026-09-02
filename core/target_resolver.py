"""
target resolver — username/URL → full tiktok account metadata
uses tiktok's unofficial mobile API (m.tiktok.com) for cleaner parsing
handles: valid accounts, private, banned, not-found, rate-limited
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class TargetInfo:
    """resolved tiktok account metadata"""

    # core identity
    uid: str  # numeric tiktok user id — "729104837261..."
    username: str  # @handle without the @ — "charlidamelio"
    display_name: str  # profile display name — "Charli D'Amelio"

    # stats
    follower_count: int
    following_count: int
    video_count: int
    like_count: int

    # flags
    is_verified: bool
    is_private: bool
    is_banned: bool  # already banned/suspended
    is_not_found: bool  # account doesn't exist
    region: Optional[str] = None  # detected region code if available
    avatar_url: Optional[str] = None
    bio: Optional[str] = None

    # metadata
    resolved_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    raw_profile_url: Optional[str] = None

    @property
    def profile_url(self) -> str:
        return f"https://www.tiktok.com/@{self.username}"

    @property
    def is_banneable(self) -> bool:
        """can we target this account? false if already banned or not found"""
        return not (self.is_banned or self.is_not_found)


class TargetResolveError(Exception):
    """raised when target resolution completely fails"""
    def __init__(self, input_str: str, reason: str):
        self.input_str = input_str
        self.reason = reason
        super().__init__(f"failed to resolve '{input_str}': {reason}")


class TargetResolver:
    """
    resolves tiktok usernames/profiles/URLs into structured TargetInfo

    usage:
        resolver = TargetResolver(proxy="45.67.89.x:9050")
        info = await resolver.resolve("@charlidamelio")
        # or
        info = await resolver.resolve("https://www.tiktok.com/@charlidamelio")
        # or just
        info = await resolver.resolve("charlidamelio")
    """

    # tiktok mobile API endpoints
    USER_INFO_ENDPOINT = "https://www.tiktok.com/api/user/detail/"
    USER_SEARCH_ENDPOINT = "https://www.tiktok.com/api/search/user/full/"

    # mobile web fallback (less rate-limited than desktop)
    MOBILE_WEB_URL = "https://m.tiktok.com/api/user/info"

    def __init__(
        self,
        proxy: Optional[str] = None,
        timeout: int = 15,
        max_retries: int = 3,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.proxy = proxy
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = session
        self._owns_session = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0.6478.122 Mobile Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.tiktok.com/",
                },
            )
            self._owns_session = True
        return self._session

    async def close(self):
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()

    # ─── public API ─────────────────────────────────────────────────

    async def resolve(self, input_str: str) -> TargetInfo:
        """
        resolve any tiktok identifier into TargetInfo
        input_str: "@username", "username", "tiktok.com/@username", full URL
        """
        username = self._extract_username(input_str)
        if not username:
            raise TargetResolveError(input_str, "could not extract username")

        logger.info("[resolver] resolving @%s ...", username)

        for attempt in range(1, self.max_retries + 1):
            try:
                data = await self._fetch_user_data(username)

                if data is None:
                    raise TargetResolveError(input_str, "empty response from tiktok")

                info = self._parse_user_data(data, username)
                logger.info(
                    "[resolver] @%s — uid:%s followers:%s verified:%s banned:%s",
                    info.username,
                    info.uid,
                    info.follower_count,
                    info.is_verified,
                    info.is_banned,
                )
                return info

            except TargetResolveError:
                raise  # don't retry deterministic errors

            except Exception as e:
                logger.warning("[resolver] attempt %d/%d failed: %s", attempt, self.max_retries, e)
                if attempt < self.max_retries:
                    await asyncio.sleep(1.5 ** attempt)
                else:
                    raise TargetResolveError(input_str, f"all {self.max_retries} attempts failed: {e}")

        raise TargetResolveError(input_str, "unreachable")

    async def check_status(self, username: str) -> TargetInfo:
        """
        lightweight status check — used by ban_monitor for polling
        returns updated TargetInfo with current ban/active status
        """
        return await self.resolve(username)

    # ─── internal ───────────────────────────────────────────────────

    def _extract_username(self, input_str: str) -> Optional[str]:
        """extract clean @handle from any input format"""

        # strip whitespace
        input_str = input_str.strip().rstrip("/")

        # full URL: https://www.tiktok.com/@username/video/123...
        if "tiktok.com" in input_str:
            parsed = urlparse(input_str if "://" in input_str else f"https://{input_str}")
            path_parts = parsed.path.strip("/").split("/")
            for part in path_parts:
                if part.startswith("@"):
                    return part[1:]  # remove @
            # maybe username without @ in path
            if path_parts and path_parts[0] not in ("@", "video", "music", "tag", "share", "api"):
                return path_parts[0]

        # @username or username
        cleaned = input_str.lstrip("@")
        if re.match(r"^[a-zA-Z0-9._]{2,24}$", cleaned):
            return cleaned

        # last attempt: extract from anywhere using pattern
        match = re.search(r"@?([a-zA-Z0-9._]{2,24})", input_str)
        if match:
            return match.group(1)

        return None

    async def _fetch_user_data(self, username: str) -> Optional[dict]:
        """fetch raw user data from tiktok mobile API"""

        session = await self._get_session()

        # primary: mobile web API — more stable, fewer captchas
        params = {
            "uniqueId": username,
        }

        proxy_url = self.proxy
        if proxy_url and not proxy_url.startswith(("http://", "socks5://")):
            proxy_url = f"socks5://{proxy_url}"

        async with session.get(
            self.MOBILE_WEB_URL,
            params=params,
            proxy=proxy_url,
        ) as resp:
            if resp.status == 404:
                logger.info("[resolver] @%s — account not found (404)", username)
                return self._build_not_found_response(username)

            if resp.status == 429:
                raise TargetResolveError(username, "rate limited (429) — proxy or delay needed")

            if resp.status != 200:
                raise TargetResolveError(username, f"HTTP {resp.status}")

            raw = await resp.json()

        # mobile API wraps differently — check structure
        user_data = None
        if "userInfo" in raw:
            user_data = raw["userInfo"].get("user", raw["userInfo"])
        elif "user" in raw:
            user_data = raw["user"]
        elif "userData" in raw:
            user_data = raw["userData"]

        if user_data is None:
            logger.warning("[resolver] unexpected response structure for @%s", username)
            return None

        return user_data

    def _build_not_found_response(self, username: str) -> dict:
        """synthetic data for non-existent accounts"""
        return {
            "id": "0",
            "uniqueId": username,
            "nickname": username,
            "notFound": True,
            "privateAccount": False,
            "verified": False,
            "followingCount": 0,
            "followerCount": 0,
            "videoCount": 0,
            "heartCount": 0,
        }

    def _parse_user_data(self, data: dict, username: str) -> TargetInfo:
        """parse raw API response into TargetInfo dataclass"""

        # detect not-found
        if data.get("notFound") or data.get("id") == "0":
            return TargetInfo(
                uid="0",
                username=username,
                display_name=username,
                follower_count=0,
                following_count=0,
                video_count=0,
                like_count=0,
                is_verified=False,
                is_private=False,
                is_banned=False,
                is_not_found=True,
            )

        # detect banned/suspended — tiktok returns specific fields
        uid = str(data.get("id", ""))
        is_banned = bool(
            data.get("suspended")
            or data.get("banned")
            or data.get("isBanned")
            or (data.get("status") == 2)  # status=2 often means suspended
        )

        # stats — tiktok returns these as numbers or strings
        follower_count = self._safe_int(data.get("followerCount", 0))
        following_count = self._safe_int(data.get("followingCount", 0))
        video_count = self._safe_int(data.get("videoCount", 0))
        like_count = self._safe_int(data.get("heartCount", data.get("diggCount", 0)))

        return TargetInfo(
            uid=uid,
            username=data.get("uniqueId", username),
            display_name=data.get("nickname", data.get("uniqueId", username)),
            follower_count=follower_count,
            following_count=following_count,
            video_count=video_count,
            like_count=like_count,
            is_verified=bool(data.get("verified", False)),
            is_private=bool(data.get("privateAccount", data.get("secret", False))),
            is_banned=is_banned,
            is_not_found=False,
            region=data.get("region", None),
            avatar_url=data.get("avatarMedium", data.get("avatarThumb", None)),
            bio=data.get("signature", None),
            raw_profile_url=f"https://www.tiktok.com/@{data.get('uniqueId', username)}",
        )

    @staticmethod
    def _safe_int(value) -> int:
        """parse int safely from string or int"""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                # might have formatting like "1.2M" or "12.3K"
                return TargetResolver._parse_compact_number(value)
        return 0

    @staticmethod
    def _parse_compact_number(text: str) -> int:
        """parse '1.2M', '456K', '12.3B' into integers"""
        multipliers = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}
        text = text.strip().lower()
        if text[-1] in multipliers:
            try:
                return int(float(text[:-1]) * multipliers[text[-1]])
            except ValueError:
                return 0
        try:
            return int(float(text))
        except ValueError:
            return 0