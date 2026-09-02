"""
captcha module — automated solving via 2captcha / capsolver
tiktok primarily uses arkose labs funcaptcha, with dice/capy as fallbacks
"""

from .solver import CaptchaSolver, CaptchaType, CaptchaSolveError

__all__ = ["CaptchaSolver", "CaptchaType", "CaptchaSolveError"]