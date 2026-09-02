"""
fingerprint engine — browser fingerprint randomization
generates unique, believable browser fingerprints for each session
handles: canvas hash, WebGL vendor/renderer, screen resolution,
         timezone, language, font list, platform, hardware concurrency
syncs timezone & locale with proxy geo for consistency
"""

import hashlib
import logging
import random
import string
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Fingerprint:
    """a complete browser fingerprint profile"""
    fingerprint_id: str
    user_agent: str
    platform: str
    screen_width: int
    screen_height: int
    viewport_width: int
    viewport_height: int
    device_pixel_ratio: float
    timezone: str
    timezone_offset: int
    language: str
    languages: list[str]
    hardware_concurrency: int
    device_memory: int
    canvas_hash: str
    webgl_vendor: str
    webgl_renderer: str
    fonts: list[str]
    audio_fingerprint: str
    do_not_track: bool
    touch_support: bool
    generated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "id": self.fingerprint_id,
            "user_agent": self.user_agent,
            "platform": self.platform,
            "screen": f"{self.screen_width}x{self.screen_height}",
            "viewport": f"{self.viewport_width}x{self.viewport_height}",
            "dpr": self.device_pixel_ratio,
            "timezone": self.timezone,
            "language": self.language,
            "cores": self.hardware_concurrency,
            "memory": self.device_memory,
            "webgl_vendor": self.webgl_vendor,
            "webgl_renderer": self.webgl_renderer,
            "touch": self.touch_support,
        }


class FingerprintEngine:
    """
    generates randomized browser fingerprints for burner sessions

    usage:
        engine = FingerprintEngine()
        fp = await engine.generate(proxy="45.67.89.x:9050")
        # fp.to_dict() → use for playwright-stealth or request headers
    """

    # realistic device profiles
    DEVICE_PROFILES = {
        "android_high": {
            "platforms": ["Linux armv8l", "Linux aarch64"],
            "screen_presets": [
                (1080, 2400, 2.75),  # common Android flagship
                (1440, 3120, 3.0),   # high-end
                (1080, 2340, 2.5),   # mid-high
            ],
            "memory_range": (4, 8),
            "cores_range": (6, 8),
            "touch_support": True,
        },
        "android_mid": {
            "platforms": ["Linux armv8l", "Linux aarch64"],
            "screen_presets": [
                (720, 1600, 2.0),
                (1080, 1920, 2.5),
                (720, 1520, 2.25),
            ],
            "memory_range": (2, 4),
            "cores_range": (4, 8),
            "touch_support": True,
        },
        "ios_high": {
            "platforms": ["iPhone", "iPhone"],
            "screen_presets": [
                (1179, 2556, 3.0),  # iPhone 15 Pro
                (1290, 2796, 3.0),  # iPhone 15 Pro Max
            ],
            "memory_range": (6, 8),
            "cores_range": (6, 6),
            "touch_support": True,
        },
        "desktop_windows": {
            "platforms": ["Win32", "Win64"],
            "screen_presets": [
                (1920, 1080, 1.0),
                (2560, 1440, 1.25),
                (1366, 768, 1.0),
            ],
            "memory_range": (4, 16),
            "cores_range": (4, 12),
            "touch_support": False,
        },
    }

    # WebGL vendor/renderer pairs — realistic combinations
    WEBGL_PAIRS = [
        ("Google Inc.", "ANGLE (Qualcomm, Adreno 750, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (Qualcomm, Adreno 740, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (ARM, Mali-G710, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (ARM, Mali-G78, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (Apple, Apple A17 Pro, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (Apple, Apple A16 Bionic, OpenGL ES 3.2)"),
        ("Google Inc.", "ANGLE (NVIDIA, GeForce RTX 4060, OpenGL 4.5)"),
        ("Google Inc.", "ANGLE (Intel, UHD Graphics 770, OpenGL 4.5)"),
        ("Google Inc.", "ANGLE (AMD, Radeon RX 7600, OpenGL 4.5)"),
        ("Google Inc.", "ANGLE (Intel, Iris Xe Graphics, OpenGL 4.5)"),
    ]

    # timezone database — mapped to regions
    TIMEZONES = {
        "US": ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"],
        "GB": ["Europe/London"],
        "DE": ["Europe/Berlin"],
        "FR": ["Europe/Paris"],
        "IN": ["Asia/Kolkata"],
        "ID": ["Asia/Jakarta"],
        "BR": ["America/Sao_Paulo"],
        "JP": ["Asia/Tokyo"],
        "KR": ["Asia/Seoul"],
        "AU": ["Australia/Sydney"],
        "CA": ["America/Toronto", "America/Vancouver"],
        "MX": ["America/Mexico_City"],
        "PH": ["Asia/Manila"],
        "NG": ["Africa/Lagos"],
        "ZA": ["Africa/Johannesburg"],
        "DEFAULT": ["America/New_York", "Europe/London", "Asia/Tokyo"],
    }

    def __init__(
        self,
        device_type: str = "random",
        use_consistent_geo: bool = True,
        seed: Optional[int] = None,
    ):
        """
        device_type: "android_high", "android_mid", "ios_high", "desktop_windows", "random"
        use_consistent_geo: if True, sync timezone+language with proxy geo region
        seed: optional random seed for reproducibility
        """
        self.device_type = device_type
        self.use_consistent_geo = use_consistent_geo
        self._rng = random.Random(seed) if seed else random.Random()
        self._generated_hashes: set[str] = set()  # avoid duplicate fingerprints

    async def generate(
        self,
        proxy: Optional[str] = None,
        device_type: Optional[str] = None,
    ) -> Fingerprint:
        """
        generate a unique fingerprint

        proxy: optional proxy string — used to determine geo for consistency
        device_type: override device type for this generation
        """
        dt = device_type or self.device_type
        if dt == "random":
            dt = self._rng.choice(list(self.DEVICE_PROFILES.keys()))

        profile = self.DEVICE_PROFILES.get(dt, self.DEVICE_PROFILES["android_high"])

        # determine geo
        region = self._detect_proxy_region(proxy) if proxy else "DEFAULT"
        timezone = self._rng.choice(self.TIMEZONES.get(region, self.TIMEZONES["DEFAULT"]))

        # screen
        screen_w, screen_h, dpr = self._rng.choice(profile["screen_presets"])

        # canvas hash — random noise
        canvas_hash = self._generate_canvas_hash()

        # WebGL
        vendor, renderer = self._rng.choice(self.WEBGL_PAIRS)

        # user agent
        ua = self._generate_user_agent(dt, region)

        # build fingerprint
        fp = Fingerprint(
            fingerprint_id=self._generate_id(),
            user_agent=ua,
            platform=self._rng.choice(profile["platforms"]),
            screen_width=screen_w,
            screen_height=screen_h,
            viewport_width=screen_w,
            viewport_height=screen_h - self._rng.randint(50, 120),  # account for status bar
            device_pixel_ratio=dpr,
            timezone=timezone,
            timezone_offset=self._get_timezone_offset(timezone),
            language=self._get_language_for_region(region),
            languages=self._get_languages_for_region(region),
            hardware_concurrency=self._rng.randint(*profile["cores_range"]),
            device_memory=self._rng.randint(*profile["memory_range"]),
            canvas_hash=canvas_hash,
            webgl_vendor=vendor,
            webgl_renderer=renderer,
            fonts=self._generate_font_list(),
            audio_fingerprint=self._generate_audio_hash(),
            do_not_track=self._rng.choice([True, False]),
            touch_support=profile["touch_support"],
        )

        # ensure uniqueness
        retries = 0
        while fp.fingerprint_id in self._generated_hashes and retries < 10:
            fp.fingerprint_id = self._generate_id()
            retries += 1

        self._generated_hashes.add(fp.fingerprint_id)
        logger.debug("[fingerprint] generated: %s (device: %s, region: %s)", fp.fingerprint_id, dt, region)

        return fp

    # ─── internal generators ────────────────────────────────────────

    def _generate_canvas_hash(self) -> str:
        """generate a random canvas fingerprint hash"""
        noise = "".join(
            self._rng.choices(string.ascii_letters + string.digits, k=64)
        )
        return hashlib.sha256(noise.encode()).hexdigest()[:32]

    def _generate_audio_hash(self) -> str:
        """generate a random audio fingerprint hash"""
        noise = "".join(
            self._rng.choices(string.ascii_letters + string.digits, k=48)
        )
        return hashlib.md5(noise.encode()).hexdigest()[:16]

    def _generate_id(self) -> str:
        """generate a unique fingerprint ID"""
        timestamp = int(time.time() * 1000)
        random_part = "".join(self._rng.choices(string.hexdigits.lower(), k=8))
        return f"fp_{timestamp}_{random_part}"

    def _generate_user_agent(self, device_type: str, region: str) -> str:
        """generate a realistic user agent string"""
        chrome_versions = ["126.0.6478.122", "126.0.6478.110", "125.0.6422.165", "125.0.6422.146", "124.0.6367.179"]
        chrome_ver = self._rng.choice(chrome_versions)

        if "android" in device_type:
            return (
                f"Mozilla/5.0 (Linux; Android {self._rng.choice(['14', '13', '12'])}; "
                f"{self._rng.choice(['SM-S908B', 'Pixel 8 Pro', 'SM-A556B', 'OnePlus 11', 'Xiaomi 14'])}) "
                f"AppleWebKit/537.36 (KHTML, like Gecko) "
                f"Chrome/{chrome_ver} Mobile Safari/537.36"
            )

        if "ios" in device_type:
            ios_ver = self._rng.choice(["17_5_1", "17_4", "17_3_1"])
            return (
                f"Mozilla/5.0 (iPhone; CPU iPhone OS {ios_ver} like Mac OS X) "
                f"AppleWebKit/605.1.15 (KHTML, like Gecko) "
                f"Version/17.5 Mobile/15E148 Safari/604.1"
            )

        # desktop
        return (
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{chrome_ver} Safari/537.36"
        )

    def _generate_font_list(self) -> list[str]:
        """generate a realistic font list"""
        common_fonts = [
            "Arial", "Helvetica", "Times New Roman", "Courier New",
            "Verdana", "Georgia", "Trebuchet MS", "Comic Sans MS",
            "Impact", "Lucida Console", "Tahoma", "Palatino Linotype",
            "Roboto", "Noto Sans", "Open Sans", "Montserrat",
        ]
        selected = self._rng.sample(common_fonts, k=self._rng.randint(8, 14))
        return sorted(selected)

    def _detect_proxy_region(self, proxy: str) -> str:
        """
        attempt to detect proxy geo region from IP
        simplified — in production you'd use a geo-IP database
        """
        # extract IP
        ip = proxy
        if "@" in ip:
            ip = ip.split("@")[-1]
        if "://" in ip:
            ip = ip.split("://")[-1]
        ip = ip.split(":")[0]

        # simple heuristic — first octet patterns
        # this is intentionally crude; real implementation uses MaxMind or similar
        first_octet = int(ip.split(".")[0]) if ip.replace(".", "").isdigit() else 0

        if first_octet in (1, 3, 4, 5, 8, 12, 13, 23, 24, 45, 47, 50, 52, 54, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 96, 97, 98, 99, 100, 104, 107, 108, 128, 129, 130, 131, 132, 134, 135, 136, 137, 138, 139, 140, 142, 143, 144, 146, 147, 148, 149, 152, 155, 156, 157, 158, 159, 160, 161, 162, 164, 165, 166, 167, 168, 169, 170, 172, 173, 174, 192, 198, 199, 204, 205, 206, 207, 208, 209, 216):
            return "US"
        elif first_octet in (2, 25, 31, 37, 46, 51, 53, 57, 62, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 109, 141, 145, 151, 176, 178, 185, 188, 193, 194, 195, 212, 213, 217):
            return "GB"
        elif first_octet in (27, 41, 105, 106, 154, 160, 196, 197):
            return "ZA"
        elif first_octet in (14, 27, 42, 49, 58, 59, 60, 61, 101, 103, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 133, 150, 153, 163, 171, 175, 180, 182, 183, 202, 203, 210, 211, 218, 219, 220, 221, 222, 223):
            return "IN"
        elif first_octet in (43, 44, 200, 201, 214, 215):
            return "JP"
        else:
            return "DEFAULT"

    def _get_timezone_offset(self, timezone: str) -> int:
        """get UTC offset for timezone (simplified)"""
        offsets = {
            "America/New_York": -5, "America/Chicago": -6, "America/Denver": -7,
            "America/Los_Angeles": -8, "America/Toronto": -5, "America/Vancouver": -8,
            "America/Mexico_City": -6, "America/Sao_Paulo": -3,
            "Europe/London": 0, "Europe/Berlin": 1, "Europe/Paris": 1,
            "Asia/Kolkata": 5, "Asia/Jakarta": 7, "Asia/Tokyo": 9,
            "Asia/Seoul": 9, "Asia/Manila": 8,
            "Australia/Sydney": 10,
            "Africa/Lagos": 1, "Africa/Johannesburg": 2,
        }
        return offsets.get(timezone, 0)

    def _get_language_for_region(self, region: str) -> str:
        lang_map = {
            "US": "en-US", "GB": "en-GB", "CA": "en-CA",
            "DE": "de-DE", "FR": "fr-FR",
            "IN": "en-IN", "ID": "id-ID",
            "BR": "pt-BR", "JP": "ja-JP", "KR": "ko-KR",
            "MX": "es-MX", "PH": "en-PH", "NG": "en-NG",
            "AU": "en-AU", "ZA": "en-ZA",
            "DEFAULT": "en-US",
        }
        return lang_map.get(region, "en-US")

    def _get_languages_for_region(self, region: str) -> list[str]:
        primary = self._get_language_for_region(region)
        if region in ("US", "GB", "CA", "AU", "DEFAULT"):
            return [primary, "en", "en-GB"]
        return [primary, "en-US", "en"]