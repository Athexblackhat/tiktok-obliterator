"""
escalation engine — adaptive response when target not yet banned
receives signals from ban_monitor, adjusts reporting intensity upward
triggers: more reports, heavier categories, fresh account generation
"""

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class EscalationLevel(Enum):
    """progressive escalation intensity"""
    LEVEL_0 = 0  # initial wave — standard volume, mixed categories
    LEVEL_1 = 1  # increased volume, weighted toward heavier cats
    LEVEL_2 = 2  # double volume, severe categories prioritized
    LEVEL_3 = 3  # maximum — all accounts, worst categories, continuous fire
    LEVEL_4 = 4  # desperation — phone-verified accounts, child safety cats


@dataclass
class EscalationState:
    """current escalation state for a target"""
    target_uid: str
    current_level: EscalationLevel = EscalationLevel.LEVEL_0
    total_escalations: int = 0
    total_reports_requested: int = 0
    last_escalated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    active: bool = True


class EscalationEngine:
    """
    adaptive escalation — increases reporting pressure over time

    usage:
        engine = EscalationEngine(
            orchestrator=report_orchestrator_instance,
            account_factory=factory_instance,
            config={"escalation_threshold_checks": 3}
        )
        # called automatically by BanMonitor, or manually:
        await engine.request_escalation(target_info, current_status, check_count, elapsed)
    """

    # escalation triggers — after this many monitor checks without ban, escalate
    DEFAULT_THRESHOLDS = {
        EscalationLevel.LEVEL_0: 0,
        EscalationLevel.LEVEL_1: 4,
        EscalationLevel.LEVEL_2: 10,
        EscalationLevel.LEVEL_3: 20,
        EscalationLevel.LEVEL_4: 40,
    }

    # report volume multipliers per level
    VOLUME_MULTIPLIERS = {
        EscalationLevel.LEVEL_0: 1.0,
        EscalationLevel.LEVEL_1: 1.8,
        EscalationLevel.LEVEL_2: 3.0,
        EscalationLevel.LEVEL_3: 5.0,
        EscalationLevel.LEVEL_4: 8.0,
    }

    # category weights shift toward severe as level increases
    CATEGORY_WEIGHTS = {
        EscalationLevel.LEVEL_0: {
            "harassment": 25, "spam": 25, "impersonation": 20,
            "underage": 10, "self_harm": 10, "illegal": 5, "terrorism": 5,
        },
        EscalationLevel.LEVEL_1: {
            "harassment": 20, "spam": 15, "impersonation": 15,
            "underage": 20, "self_harm": 15, "illegal": 10, "terrorism": 5,
        },
        EscalationLevel.LEVEL_2: {
            "harassment": 10, "spam": 5, "impersonation": 10,
            "underage": 25, "self_harm": 20, "illegal": 15, "terrorism": 15,
        },
        EscalationLevel.LEVEL_3: {
            "harassment": 5, "spam": 5, "impersonation": 5,
            "underage": 25, "self_harm": 20, "illegal": 20, "terrorism": 20,
        },
        EscalationLevel.LEVEL_4: {
            "harassment": 0, "spam": 0, "impersonation": 5,
            "underage": 30, "self_harm": 20, "illegal": 20, "terrorism": 25,
        },
    }

    def __init__(
        self,
        orchestrator=None,
        account_factory=None,
        config: Optional[dict] = None,
    ):
        self.orchestrator = orchestrator
        self.account_factory = account_factory
        self.config = config or {}

        self._states: dict[str, EscalationState] = {}
        self._escalation_lock = asyncio.Lock()

    async def request_escalation(
        self,
        target_info,
        current_status,
        check_count: int,
        elapsed_seconds: float,
    ) -> Optional[EscalationLevel]:
        """called by BanMonitor when target still active"""
        uid = target_info.uid

        async with self._escalation_lock:
            state = self._states.get(uid)
            if state is None:
                state = EscalationState(target_uid=uid)
                self._states[uid] = state

            if not state.active:
                return None

            new_level = self._calculate_level(check_count)
            if new_level == state.current_level:
                return None

            old_level = state.current_level
            state.current_level = new_level
            state.total_escalations += 1
            state.last_escalated_at = datetime.utcnow().isoformat()

            logger.info(
                "[escalation] @%s — level %s → %s (check #%d, elapsed %.0fs)",
                target_info.username,
                old_level.name,
                new_level.name,
                check_count,
                elapsed_seconds,
            )

            await self._trigger_wave(target_info, state)

            if new_level.value >= EscalationLevel.LEVEL_3.value and self.account_factory:
                await self._request_reinforcements(state)

            return new_level

    async def force_escalation(self, target_info, target_level: EscalationLevel) -> None:
        """manually force escalation to a specific level"""
        uid = target_info.uid
        state = self._states.get(uid, EscalationState(target_uid=uid))
        state.current_level = target_level
        state.total_escalations += 1
        state.last_escalated_at = datetime.utcnow().isoformat()
        self._states[uid] = state

        logger.info("[escalation] @%s — force escalated to %s", target_info.username, target_level.name)
        await self._trigger_wave(target_info, state)

    async def reset(self, uid: str):
        """reset escalation state for a target"""
        self._states.pop(uid, None)
        logger.debug("[escalation] reset state for uid:%s", uid)

    def _calculate_level(self, check_count: int) -> EscalationLevel:
        current = EscalationLevel.LEVEL_0
        for level in EscalationLevel:
            threshold = self.DEFAULT_THRESHOLDS.get(level, 999)
            if check_count >= threshold:
                current = level
        return current

    async def _trigger_wave(self, target_info, state: EscalationState):
        """trigger a reporting wave at current escalation intensity"""
        if not self.orchestrator:
            logger.warning("[escalation] no orchestrator attached — can't fire reports")
            return

        level = state.current_level
        multiplier = self.VOLUME_MULTIPLIERS.get(level, 1.0)
        category_weights = self.CATEGORY_WEIGHTS.get(level, self.CATEGORY_WEIGHTS[EscalationLevel.LEVEL_0])

        base_count = random.randint(40, 60)
        report_count = int(base_count * multiplier)

        logger.info(
            "[escalation] @%s — firing wave: %d reports at %s intensity",
            target_info.username,
            report_count,
            level.name,
        )

        state.total_reports_requested += report_count

        asyncio.create_task(
            self.orchestrator.fire_wave(
                target_info=target_info,
                report_count=report_count,
                category_weights=category_weights,
                escalation_level=level,
            )
        )

    async def _request_reinforcements(self, state: EscalationState):
        """at high escalation levels, request fresh burner accounts"""
        if not self.account_factory:
            return

        new_accounts_needed = 20 if state.current_level == EscalationLevel.LEVEL_3 else 40

        logger.info(
            "[escalation] requesting %d fresh burner accounts for level %s",
            new_accounts_needed,
            state.current_level.name,
        )

        try:
            new_accounts = await self.account_factory.create_batch(
                count=new_accounts_needed,
                max_parallel=5,
            )
            logger.info("[escalation] reinforcements arrived — %d new accounts", len(new_accounts))
        except Exception as e:
            logger.error("[escalation] reinforcement request failed: %s", e)

    @property
    def active_campaigns(self) -> int:
        return sum(1 for s in self._states.values() if s.active)