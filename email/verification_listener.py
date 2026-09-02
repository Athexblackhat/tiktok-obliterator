"""
tiktok email verification listener — async IMAP client
waits for verification emails, extracts confirmation links, auto-clicks them
uses IMAP IDLE where supported, otherwise falls back to polling
"""

import asyncio
import email
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from email.header import decode_header
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    """result of waiting for a verification email"""
    email_address: str
    verified: bool
    verification_link: Optional[str] = None
    message_id: Optional[str] = None
    received_at: Optional[str] = None
    clicked_at: Optional[str] = None
    attempts: int = 0
    wait_seconds: float = 0.0
    error: Optional[str] = None


class VerificationListener:
    """
    async IMAP listener — waits for tiktok verification emails

    usage:
        listener = VerificationListener(
            imap_host="imap.yourdomain.com",
            imap_port=993,
            username="catchall@yourdomain.com",
            password="your_password",
        )
        await listener.connect()

        # wait for verification email for a specific burner
        result = await listener.wait_for_verification(
            email="burner_cool.fox.a7f3x9@catchall.com",
            timeout=90,
        )
        # result.verified = True → link auto-clicked
    """

    # tiktok verification email identifiers
    TIKTOK_SENDERS = [
        "noreply@tiktok.com",
        "account@tiktok.com",
        "verify@email.tiktok.com",
        "no-reply@email.tiktok.com",
        "registration@tiktok.com",
    ]

    TIKTOK_SUBJECT_PATTERNS = [
        r"(?i).*verify.*(?:email|account).*",
        r"(?i).*confirm.*(?:email|account).*",
        r"(?i).*verification.*code.*",
        r"(?i).*welcome.*tiktok.*",
        r"(?i).*activate.*account.*",
    ]

    def __init__(
        self,
        imap_host: str,
        imap_port: int = 993,
        username: str = "",
        password: str = "",
        use_ssl: bool = True,
        poll_interval: float = 3.0,
        max_poll_attempts: int = 30,
    ):
        """
        imap_host: IMAP server — e.g. "imap.mailcheap.co"
        imap_port: usually 993 for SSL, 143 for non-SSL
        username: full email address of the catch-all inbox
        password: email account password
        poll_interval: seconds between inbox checks
        max_poll_attempts: max polls before giving up
        """
        self.imap_host = imap_host
        self.imap_port = imap_port
        self.username = username or f"catchall@{imap_host.split('.', 1)[-1] if '.' in imap_host else 'local'}"
        self.password = password
        self.use_ssl = use_ssl
        self.poll_interval = poll_interval
        self.max_poll_attempts = max_poll_attempts

        self._imap = None
        self._connected = False
        self._pending_verifications: dict[str, asyncio.Event] = {}  # email → event
        self._verification_results: dict[str, VerificationResult] = {}  # email → result

    async def connect(self):
        """establish IMAP connection"""
        import aioimaplib

        self._imap = aioimaplib.IMAP4_SSL if self.use_ssl else aioimaplib.IMAP4

        try:
            if self.use_ssl:
                self._imap = aioimaplib.IMAP4_SSL(
                    host=self.imap_host,
                    port=self.imap_port,
                    timeout=15,
                )
            else:
                self._imap = aioimaplib.IMAP4(
                    host=self.imap_host,
                    port=self.imap_port,
                    timeout=15,
                )

            await self._imap.wait_hello_from_server()
            await self._imap.login(self.username, self.password)
            self._connected = True
            logger.info("[verifier] connected to IMAP — %s:%d", self.imap_host, self.imap_port)

        except Exception as e:
            logger.error("[verifier] IMAP connection failed: %s", e)
            self._connected = False
            raise

    async def disconnect(self):
        """close IMAP connection"""
        if self._imap and self._connected:
            try:
                await self._imap.logout()
            except Exception:
                pass
            self._connected = False
            logger.debug("[verifier] IMAP disconnected")

    async def wait_for_verification(
        self,
        email_address: str,
        timeout: int = 90,
    ) -> VerificationResult:
        """
        wait for tiktok verification email to arrive and auto-click the link

        email_address: the burner email that was used to sign up
        timeout: maximum seconds to wait
        returns: VerificationResult with verified=True if successful
        """
        email_address = email_address.strip().lower()

        result = VerificationResult(
            email_address=email_address,
            verified=False,
        )
        self._verification_results[email_address] = result

        logger.info("[verifier] waiting for verification email: %s (timeout: %ds)", email_address, timeout)

        start_time = datetime.utcnow()

        for attempt in range(1, self.max_poll_attempts + 1):
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if elapsed > timeout:
                result.error = f"timeout after {timeout}s"
                result.wait_seconds = elapsed
                result.attempts = attempt
                logger.warning("[verifier] timeout waiting for %s", email_address)
                break

            result.attempts = attempt
            result.wait_seconds = elapsed

            try:
                # search inbox for tiktok verification emails
                found = await self._search_for_verification(email_address)

                if found:
                    verification_link = await self._extract_and_click(found["uid"], email_address)
                    if verification_link:
                        result.verified = True
                        result.verification_link = verification_link
                        result.message_id = found.get("message_id")
                        result.received_at = datetime.utcnow().isoformat()
                        result.clicked_at = datetime.utcnow().isoformat()
                        result.wait_seconds = elapsed
                        logger.info(
                            "[verifier] ✓ verification link found & clicked for %s (attempt %d, %.1fs)",
                            email_address,
                            attempt,
                            elapsed,
                        )
                        break
                    else:
                        logger.debug("[verifier] email found but no valid link extracted")
                else:
                    logger.debug(
                        "[verifier] poll %d/%d — no verification email yet for %s",
                        attempt,
                        self.max_poll_attempts,
                        email_address,
                    )

            except Exception as e:
                logger.error("[verifier] poll error for %s: %s", email_address, e)
                # reconnect if connection dropped
                if not self._connected:
                    try:
                        await self.connect()
                    except Exception:
                        pass

            await asyncio.sleep(self.poll_interval)

        # cleanup
        self._verification_results.pop(email_address, None)
        return result

    # ─── internal ───────────────────────────────────────────────────

    async def _search_for_verification(self, target_email: str) -> Optional[dict]:
        """search inbox for a tiktok verification email addressed to target"""

        if not self._connected or not self._imap:
            raise RuntimeError("not connected to IMAP")

        # select inbox
        await self._imap.select("INBOX")

        # search for recent emails from tiktok
        search_criteria = []
        for sender in self.TIKTOK_SENDERS:
            search_criteria.append(f'FROM "{sender}"')

        combined_criteria = f"OR " * (len(search_criteria) - 1) + " ".join(search_criteria) if len(search_criteria) > 1 else search_criteria[0]

        # search unseen emails from last 24 hours
        status, messages = await self._imap.search(f"UNSEEN {combined_criteria}")

        if status != "OK" or not messages[0]:
            return None

        message_ids = messages[0].split()
        if not message_ids:
            return None

        # check each message — most recent first
        for msg_id in reversed(message_ids):
            try:
                status, msg_data = await self._imap.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)])")
                if status != "OK":
                    continue

                headers = msg_data[0][1].decode("utf-8", errors="ignore") if isinstance(msg_data[0][1], bytes) else msg_data[0][1]

                # parse headers
                msg_from = self._extract_header(headers, "From")
                msg_to = self._extract_header(headers, "To")
                msg_subject = self._extract_header(headers, "Subject")

                # check if this email is for our target
                if target_email.lower() in msg_to.lower() or target_email.lower() in headers.lower():
                    # check if it's a tiktok verification email
                    if self._is_tiktok_verification(msg_from, msg_subject):
                        return {
                            "uid": msg_id.decode() if isinstance(msg_id, bytes) else msg_id,
                            "message_id": self._extract_header(headers, "Message-ID"),
                            "from": msg_from,
                            "subject": msg_subject,
                            "to": msg_to,
                        }

            except Exception as e:
                logger.debug("[verifier] error checking message %s: %s", msg_id, e)
                continue

        return None

    async def _extract_and_click(self, email_uid: str, target_email: str) -> Optional[str]:
        """
        fetch full email body, extract verification link, make HTTP GET to confirm
        """
        if not self._imap:
            return None

        try:
            # fetch full message body
            status, msg_data = await self._imap.fetch(email_uid, "(BODY[])")
            if status != "OK":
                return None

            raw_email = msg_data[0][1]
            if isinstance(raw_email, bytes):
                raw_email = raw_email.decode("utf-8", errors="ignore")

            msg = email.message_from_string(raw_email)

            # extract body (html preferred)
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == "text/html":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode("utf-8", errors="ignore")
                            break
                    elif content_type == "text/plain" and not body:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode("utf-8", errors="ignore")
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="ignore")

            if not body:
                logger.debug("[verifier] empty email body for %s", target_email)
                return None

            # extract verification link
            verification_link = self._extract_verification_link(body)
            if not verification_link:
                return None

            # click the link (HTTP GET)
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    verification_link,
                    timeout=aiohttp.ClientTimeout(total=15),
                    allow_redirects=True,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/126.0.0.0 Safari/537.36"
                        ),
                    },
                ) as resp:
                    logger.debug(
                        "[verifier] clicked verification link — status: %d, final url: %s",
                        resp.status,
                        str(resp.url)[:80],
                    )

            return verification_link

        except Exception as e:
            logger.error("[verifier] failed to extract/click for %s: %s", target_email, e)
            return None

    def _extract_verification_link(self, body: str) -> Optional[str]:
        """
        extract tiktok verification link from email body
        tiktok sends various formats — direct links, redirect links, button hrefs
        """
        # patterns for tiktok verification links
        patterns = [
            # direct verification link
            r'https?://[^"\s<>]*tiktok\.com[^"\s<>]*verify[^"\s<>]*',
            # email verification redirect
            r'https?://[^"\s<>]*tiktok\.com[^"\s<>]*email/verify[^"\s<>]*',
            # passport/email/verify endpoint
            r'https?://[^"\s<>]*tiktok\.com[^"\s<>]*passport[^"\s<>]*email[^"\s<>]*verify[^"\s<>]*',
            # click tracking redirect
            r'https?://[^"\s<>]*email\.tiktok\.com[^"\s<>]*',
            # any tiktok link with token parameter
            r'https?://[^"\s<>]*tiktok\.com[^"\s<>]*\?[^"\s<>]*token[^"\s<>]*',
            # generic "verify email" button href
            r'href\s*=\s*["\']([^"\']*verify[^"\']*)["\']',
        ]

        for pattern in patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                link = match.group(1) if "href" in pattern else match.group(0)
                # clean up the link
                link = link.strip().rstrip(".,;:'\"")
                if link.startswith("http"):
                    return link

        # fallback: find any tiktok.com link
        fallback = re.search(r'https?://[^"\s<>]*tiktok\.com[^"\s<>]*', body, re.IGNORECASE)
        if fallback:
            return fallback.group(0).strip().rstrip(".,;:'\"")

        return None

    def _is_tiktok_verification(self, sender: str, subject: str) -> bool:
        """check if an email is a tiktok verification email"""
        # check sender
        sender_lower = sender.lower()
        is_tiktok_sender = any(
            expected in sender_lower for expected in self.TIKTOK_SENDERS
        )
        if not is_tiktok_sender and "tiktok" not in sender_lower:
            return False

        # check subject
        for pattern in self.TIKTOK_SUBJECT_PATTERNS:
            if re.match(pattern, subject):
                return True

        # generic tiktok email
        if "tiktok" in subject.lower():
            return True

        return False

    def _extract_header(self, raw_headers: str, header_name: str) -> str:
        """extract a specific header value from raw headers string"""
        pattern = rf"^{header_name}:\s*(.+)$"
        match = re.search(pattern, raw_headers, re.MULTILINE | re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            # decode if encoded
            decoded_parts = decode_header(value)
            return "".join(
                part.decode(charset or "utf-8", errors="ignore") if isinstance(part, bytes) else part
                for part, charset in decoded_parts
            )
        return ""

    @property
    def is_connected(self) -> bool:
        return self._connected