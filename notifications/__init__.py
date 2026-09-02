"""
notifications module — telegram bot & discord webhook alerts
ban confirmations, status changes, pool health, campaign updates
"""

from .alerts import AlertManager, AlertLevel, Alert

__all__ = ["AlertManager", "AlertLevel", "Alert"]