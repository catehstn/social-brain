"""
collect.py — backward-compatibility shim.
All collector logic has moved to the collectors/ package.
"""
from collectors import *  # noqa: F401,F403
from collectors import collect_all, PLATFORM_COLLECTORS  # explicit for static analysis
