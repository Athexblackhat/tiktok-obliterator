"""
email module — burner email generation & tiktok verification handling
catch-all domain email factory + async IMAP verification listener
"""

from .catchall_generator import CatchallGenerator
from .verification_listener import VerificationListener, VerificationResult

__all__ = ["CatchallGenerator", "VerificationListener", "VerificationResult"]