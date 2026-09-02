"""
catch-all email generator — produces unique burner emails from a custom domain
a catch-all domain accepts *@yourdomain.com → all emails route to one inbox
this module generates trackable, unique addresses and maps them back to burner accounts
"""

import hashlib
import logging
import random
import string
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class GeneratedEmail:
    """a generated burner email with metadata"""
    email: str
    local_part: str  # the part before @
    domain: str      # the catch-all domain
    tag: str         # unique identifier for tracking
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    used: bool = False
    account_id: Optional[str] = None  # linked burner account ID after creation


class CatchallGenerator:
    """
    generates unique burner email addresses using a catch-all domain

    usage:
        gen = CatchallGenerator(domain="catchall.mydomain.com")
        email_obj = await gen.generate()
        # email_obj.email = "burner_a7f3x9@catchall.mydomain.com"
        # email_obj.tag = "a7f3x9" — use this to track which account used this email

        # later, when verification email arrives:
        matched = gen.match_email("burner_a7f3x9@catchall.mydomain.com")
        # matched.tag = "a7f3x9"
    """

    # word banks for natural-looking local parts
    ADJECTIVES = [
        "cool", "fast", "wild", "calm", "bold", "keen", "free", "warm",
        "safe", "true", "pure", "rare", "epic", "loud", "soft", "dark",
        "lite", "neon", "pink", "cosy", "gold", "hype", "chic", "snap",
    ]

    NOUNS = [
        "fox", "owl", "cat", "dog", "bee", "elk", "bat", "jay",
        "sun", "sky", "sea", "gem", "orb", "ray", "dew", "ash",
        "pod", "hub", "box", "lab", "den", "arc", "zen", "lux",
    ]

    def __init__(
        self,
        domain: str,
        prefix: str = "burner",
        use_words: bool = True,
        tag_length: int = 6,
    ):
        """
        domain: your catch-all domain — e.g. "mail.catchall.com"
        prefix: static prefix for local part — "burner" → "burner_a7f3x9@..."
        use_words: if True, generate word-based emails like "cool.fox.a7f@..."
        tag_length: length of random hex tag appended
        """
        self.domain = domain.strip().lower()
        self.prefix = prefix
        self.use_words = use_words
        self.tag_length = tag_length

        # track generated emails for matching
        self._generated: dict[str, GeneratedEmail] = {}  # email → GeneratedEmail
        self._by_tag: dict[str, GeneratedEmail] = {}     # tag → GeneratedEmail
        self._generation_count = 0

    async def generate(self, account_id: Optional[str] = None) -> GeneratedEmail:
        """
        generate a unique burner email address
        optionally link to a burner account_id for tracking
        """
        tag = self._generate_tag()
        local_part = self._build_local_part(tag)
        email = f"{local_part}@{self.domain}"

        gen_email = GeneratedEmail(
            email=email,
            local_part=local_part,
            domain=self.domain,
            tag=tag,
            account_id=account_id,
        )

        self._generated[email] = gen_email
        self._by_tag[tag] = gen_email
        self._generation_count += 1

        logger.debug("[email] generated: %s (tag: %s)", email, tag)
        return gen_email

    async def generate_batch(self, count: int) -> list[GeneratedEmail]:
        """generate multiple emails at once"""
        emails = []
        for _ in range(count):
            email_obj = await self.generate()
            emails.append(email_obj)
        logger.info("[email] batch generated: %d emails", count)
        return emails

    def match_email(self, email_address: str) -> Optional[GeneratedEmail]:
        """
        match a received email back to its GeneratedEmail metadata
        used by verification_listener when a tiktok email arrives
        """
        email_address = email_address.strip().lower()
        matched = self._generated.get(email_address)
        if matched:
            return matched

        # try matching by local part if domain matches
        if "@" in email_address:
            local, domain = email_address.split("@", 1)
            if domain == self.domain:
                # search by tag pattern
                for tag, gen_email in self._by_tag.items():
                    if tag in local:
                        return gen_email

        return None

    def match_by_tag(self, tag: str) -> Optional[GeneratedEmail]:
        """find generated email by its unique tag"""
        return self._by_tag.get(tag)

    def mark_used(self, email_address: str, account_id: str):
        """mark an email as used and link to burner account"""
        gen_email = self.match_email(email_address)
        if gen_email:
            gen_email.used = True
            gen_email.account_id = account_id

    def _generate_tag(self) -> str:
        """generate a unique hex tag"""
        timestamp = int(time.time() * 1000)
        random_part = "".join(random.choices(string.hexdigits.lower(), k=self.tag_length))
        combined = f"{timestamp}{random_part}"
        return hashlib.md5(combined.encode()).hexdigest()[:self.tag_length]

    def _build_local_part(self, tag: str) -> str:
        """build the local part (before @) of the email"""
        if self.use_words:
            adj = random.choice(self.ADJECTIVES)
            noun = random.choice(self.NOUNS)
            return f"{self.prefix}.{adj}.{noun}.{tag}"
        return f"{self.prefix}_{tag}"

    @property
    def generated_count(self) -> int:
        return self._generation_count

    @property
    def unused_count(self) -> int:
        return sum(1 for e in self._generated.values() if not e.used)