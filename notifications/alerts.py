"""
alert manager — sends notifications via telegram bot + discord webhook
handles: ban confirmations, status changes, escalation triggers,
         pool health warnings, campaign summaries
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """severity of an alert"""
    INFO = "info"          # campaign start, wave complete, pool stats
    SUCCESS = "success"    # ban confirmed — the big one
    WARNING = "warning"    # pool low, rate limit spike, escalation triggered
    ERROR = "error"        # critical failure, account factory down
    DEBUG = "debug"        # verbose — only sent if debug mode enabled


@dataclass
class Alert:
    """a single alert to be dispatched"""
    level: AlertLevel
    title: str
    message: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().strftime("%H:%M:%S UTC"))
    metadata: Optional[dict] = None

    def format_telegram(self) -> str:
        """format for telegram HTML parse mode"""
        emoji_map = {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.SUCCESS: "✅",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.ERROR: "🚨",
            AlertLevel.DEBUG: "🔍",
        }
        emoji = emoji_map.get(self.level, "📢")

        text = f"{emoji} <b>{self.title}</b>\n"
        text += f"<i>{self.timestamp}</i>\n\n"
        text += f"{self.message}"

        if self.metadata:
            text += "\n\n<pre>"
            for key, value in self.metadata.items():
                text += f"{key}: {value}\n"
            text += "</pre>"

        return text

    def format_discord(self) -> dict:
        """format for discord webhook JSON"""
        color_map = {
            AlertLevel.INFO: 3447003,      # blue
            AlertLevel.SUCCESS: 5763719,    # green
            AlertLevel.WARNING: 16705372,   # orange
            AlertLevel.ERROR: 15548997,     # red
            AlertLevel.DEBUG: 10197915,     # grey
        }

        embed = {
            "title": f"{self.title}",
            "description": self.message,
            "color": color_map.get(self.level, 3447003),
            "timestamp": datetime.utcnow().isoformat(),
            "footer": {"text": f"tiktok_obliterator • {self.timestamp}"},
        }

        if self.metadata:
            fields = []
            for key, value in self.metadata.items():
                fields.append({
                    "name": key,
                    "value": str(value),
                    "inline": True,
                })
            embed["fields"] = fields[:6]  # discord max 6 inline fields

        return {"embeds": [embed]}


class AlertManager:
    """
    dispatches alerts to telegram and/or discord

    usage:
        alerts = AlertManager(
            telegram_bot_token="123:abc",
            telegram_chat_id="-100xxx",
            discord_webhook_url="https://discord.com/api/webhooks/...",
        )
        await alerts.send(Alert(
            level=AlertLevel.SUCCESS,
            title="Ban Confirmed",
            message="@targetuser has been permanently banned",
            metadata={"uid": "72910...", "time_to_ban": "342.5s", "reports_fired": "240"}
        ))
    """

    TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
    TELEGRAM_POLL_LIMIT = 20  # max messages per minute (telegram rate limit)

    def __init__(
        self,
        telegram_bot_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
        discord_webhook_url: Optional[str] = None,
        debug_mode: bool = False,
        max_queue_size: int = 100,
        batch_interval: float = 2.0,
    ):
        """
        telegram_bot_token: bot token from @BotFather
        telegram_chat_id: chat or channel ID to send to
        discord_webhook_url: discord webhook URL
        debug_mode: if True, also sends DEBUG level alerts
        max_queue_size: max alerts to queue before dropping old ones
        batch_interval: seconds between telegram messages to avoid rate limit
        """
        self.telegram_token = telegram_bot_token
        self.telegram_chat_id = telegram_chat_id
        self.discord_url = discord_webhook_url
        self.debug_mode = debug_mode

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._telegram_last_sent: float = 0.0
        self._telegram_lock = asyncio.Lock()
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """start the background dispatch worker"""
        if self._running:
            return

        self._http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
        )
        self._running = True
        self._worker_task = asyncio.create_task(self._dispatch_loop())
        logger.info("[alerts] manager started — telegram:%s discord:%s",
                    bool(self.telegram_token), bool(self.discord_url))

    async def stop(self):
        """stop the worker and flush remaining alerts"""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        # flush remaining
        while not self._queue.empty():
            try:
                alert = self._queue.get_nowait()
                await self._dispatch_alert(alert)
            except asyncio.QueueEmpty:
                break

        if self._http_session and not self._http_session.closed:
            await self._http_session.close()

        logger.debug("[alerts] manager stopped")

    async def send(self, alert: Alert):
        """queue an alert for dispatch"""
        if not self._running:
            logger.warning("[alerts] not running — alert dropped: %s", alert.title)
            return

        if alert.level == AlertLevel.DEBUG and not self.debug_mode:
            return

        try:
            self._queue.put_nowait(alert)
        except asyncio.QueueFull:
            logger.warning("[alerts] queue full — dropping oldest alert")
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(alert)
            except asyncio.QueueFull:
                pass

    # ─── convenience methods ────────────────────────────────────────

    async def ban_confirmed(
        self,
        target_username: str,
        target_uid: str,
        time_to_ban: float,
        total_reports: int,
        escalation_level: str = "N/A",
    ):
        """send ban confirmation alert"""
        minutes = int(time_to_ban // 60)
        seconds = int(time_to_ban % 60)
        time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        await self.send(Alert(
            level=AlertLevel.SUCCESS,
            title=f"★ BAN CONFIRMED: @{target_username}",
            message=(
                f"@{target_username} has been <b>permanently banned</b>.\n"
                f"The target is no longer accessible on TikTok."
            ),
            metadata={
                "uid": target_uid,
                "time_to_ban": time_str,
                "reports_fired": str(total_reports),
                "escalation_level": escalation_level,
            },
        ))

    async def campaign_started(self, target_username: str, burner_count: int):
        """send campaign start notification"""
        await self.send(Alert(
            level=AlertLevel.INFO,
            title=f"Campaign Started: @{target_username}",
            message=f"Mass reporting campaign initiated against @{target_username}.",
            metadata={
                "target": f"@{target_username}",
                "burner_accounts_ready": str(burner_count),
            },
        ))

    async def escalation_triggered(
        self,
        target_username: str,
        old_level: str,
        new_level: str,
        reports_so_far: int,
    ):
        """send escalation alert"""
        await self.send(Alert(
            level=AlertLevel.WARNING,
            title=f"Escalation: @{target_username} — {old_level} → {new_level}",
            message=(
                f"Target @{target_username} is still active. "
                f"Escalating from <b>{old_level}</b> to <b>{new_level}</b>."
            ),
            metadata={
                "target": f"@{target_username}",
                "from": old_level,
                "to": new_level,
                "reports_so_far": str(reports_so_far),
            },
        ))

    async def pool_health(self, stats: dict):
        """send proxy pool health report"""
        is_healthy = stats.get("healthy", True)
        level = AlertLevel.INFO if is_healthy else AlertLevel.WARNING

        await self.send(Alert(
            level=level,
            title=f"Proxy Pool: {'Healthy' if is_healthy else 'LOW'} ({stats.get('available', 0)}/{stats.get('total', 0)})",
            message=(
                f"Available: <b>{stats.get('available', 0)}</b> / Total: {stats.get('total', 0)}\n"
                f"Active: {stats.get('active', 0)} | Cooldown: {stats.get('cooldown', 0)} | Dead: {stats.get('dead', 0)}"
            ),
            metadata=stats,
        ))

    async def wave_complete(self, target_username: str, wave_result):
        """send report wave summary"""
        await self.send(Alert(
            level=AlertLevel.INFO,
            title=f"Wave Complete: @{target_username}",
            message=(
                f"Delivered: <b>{wave_result.total_delivered}/{wave_result.total_requested}</b> "
                f"({wave_result.delivery_rate:.0%})\n"
                f"Rate limited: {wave_result.total_rate_limited} | "
                f"Blocked: {wave_result.total_blocked}\n"
                f"Accounts used: {wave_result.accounts_used} | "
                f"Burned: {wave_result.accounts_burned}"
            ),
            metadata={
                "wave_id": wave_result.wave_id,
                "delivery_rate": f"{wave_result.delivery_rate:.1%}",
                "accounts_burned": str(wave_result.accounts_burned),
            },
        ))

    async def error_alert(self, title: str, message: str, metadata: Optional[dict] = None):
        """send critical error alert"""
        await self.send(Alert(
            level=AlertLevel.ERROR,
            title=title,
            message=message,
            metadata=metadata,
        ))

    # ─── internal dispatch ──────────────────────────────────────────

    async def _dispatch_loop(self):
        """background worker — processes alert queue"""
        while self._running:
            try:
                alert = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._dispatch_alert(alert)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[alerts] dispatch error: %s", e)

    async def _dispatch_alert(self, alert: Alert):
        """send a single alert to all configured channels"""
        tasks = []

        if self.telegram_token and self.telegram_chat_id:
            tasks.append(self._send_telegram(alert))

        if self.discord_url:
            tasks.append(self._send_discord(alert))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_telegram(self, alert: Alert):
        """send alert via telegram bot API"""
        async with self._telegram_lock:
            # rate limit — max 20 messages per minute
            now = asyncio.get_event_loop().time()
            time_since_last = now - self._telegram_last_sent
            min_interval = 60.0 / self.TELEGRAM_POLL_LIMIT
            if time_since_last < min_interval:
                await asyncio.sleep(min_interval - time_since_last)

            url = self.TELEGRAM_API.format(token=self.telegram_token)

            payload = {
                "chat_id": self.telegram_chat_id,
                "text": alert.format_telegram(),
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }

            try:
                async with self._http_session.post(url, json=payload) as resp:
                    self._telegram_last_sent = asyncio.get_event_loop().time()
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error("[alerts] telegram send failed (%d): %s", resp.status, body[:200])
                    else:
                        logger.debug("[alerts] telegram sent: %s", alert.title)
            except Exception as e:
                logger.error("[alerts] telegram error: %s", e)

    async def _send_discord(self, alert: Alert):
        """send alert via discord webhook"""
        payload = alert.format_discord()

        try:
            async with self._http_session.post(
                self.discord_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    logger.error("[alerts] discord send failed (%d): %s", resp.status, body[:200])
                else:
                    logger.debug("[alerts] discord sent: %s", alert.title)
        except Exception as e:
            logger.error("[alerts] discord error: %s", e)