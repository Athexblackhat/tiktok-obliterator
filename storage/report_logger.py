"""
report logger — structured JSON logging for campaigns and report waves
writes timestamped JSON files to output/ directory
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class ReportLogger:
    """
    writes structured campaign and report data to JSON files

    usage:
        rl = ReportLogger(output_dir="output")
        await rl.log_wave(wave_result)
        await rl.save_campaign_summary(campaign_data)
    """

    def __init__(
        self,
        output_dir: str = "output",
        auto_flush: bool = True,
        pretty_print: bool = True,
    ):
        self.output_dir = Path(output_dir)
        self.campaigns_dir = self.output_dir / "campaigns"
        self.logs_dir = self.output_dir / "logs"
        self.bans_dir = self.output_dir / "bans"
        self.auto_flush = auto_flush
        self.pretty_print = pretty_print

        # ensure directories exist
        self.campaigns_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.bans_dir.mkdir(parents=True, exist_ok=True)

        # buffer
        self._wave_buffer: list = []

    async def log_wave(self, wave_result) -> Optional[str]:
        """log a report wave result to JSON file"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"wave_{wave_result.target_username}_{timestamp}_{wave_result.wave_id}.json"
        filepath = self.logs_dir / filename

        data = {
            "wave_id": wave_result.wave_id,
            "target_username": wave_result.target_username,
            "target_uid": wave_result.target_uid,
            "total_requested": wave_result.total_requested,
            "total_delivered": wave_result.total_delivered,
            "total_rate_limited": wave_result.total_rate_limited,
            "total_blocked": wave_result.total_blocked,
            "total_failed": wave_result.total_failed,
            "accounts_used": wave_result.accounts_used,
            "accounts_burned": wave_result.accounts_burned,
            "delivery_rate": wave_result.delivery_rate,
            "escalation_level": wave_result.escalation_level,
            "started_at": wave_result.started_at,
            "completed_at": wave_result.completed_at,
            "attempts": [
                {
                    "attempt_id": a.attempt_id,
                    "category": a.category,
                    "status": a.status.value,
                    "http_code": a.http_code,
                    "proxy_used": a.proxy_used,
                    "response_time_ms": a.response_time_ms,
                }
                for a in wave_result.attempts[:50]  # limit to first 50 for file size
            ],
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2 if self.pretty_print else None, default=str)

        logger.debug("[logger] wave saved: %s", filename)
        return str(filepath)

    async def save_campaign_summary(self, campaign_data: dict) -> Optional[str]:
        """save full campaign summary when campaign ends"""
        target = campaign_data.get("target_username", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"campaign_{target}_{timestamp}.json"
        filepath = self.campaigns_dir / filename

        with open(filepath, "w") as f:
            json.dump(campaign_data, f, indent=2 if self.pretty_print else None, default=str)

        logger.info("[logger] campaign summary saved: %s", filename)
        return str(filepath)

    async def save_ban_record(self, ban_data: dict) -> Optional[str]:
        """save a confirmed ban record"""
        target = ban_data.get("target_username", "unknown")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"ban_{target}_{timestamp}.json"
        filepath = self.bans_dir / filename

        with open(filepath, "w") as f:
            json.dump(ban_data, f, indent=2 if self.pretty_print else None, default=str)

        logger.info("[logger] ★ ban record saved: %s", filename)
        return str(filepath)

    async def save_error_log(self, error_data: dict) -> Optional[str]:
        """log errors to a dedicated file"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"error_{timestamp}.json"
        filepath = self.logs_dir / filename

        with open(filepath, "w") as f:
            json.dump(error_data, f, indent=2 if self.pretty_print else None, default=str)

        logger.debug("[logger] error log saved: %s", filename)
        return str(filepath)

    async def get_recent_logs(self, limit: int = 20) -> list[dict]:
        """get list of recent log files"""
        log_files = sorted(self.logs_dir.glob("wave_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        return [{"filename": f.name, "path": str(f)} for f in log_files[:limit]]

    async def get_ban_records(self) -> list[dict]:
        """get all ban records"""
        ban_files = sorted(self.bans_dir.glob("ban_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        records = []
        for bf in ban_files:
            with open(bf, "r") as f:
                records.append(json.load(f))
        return records