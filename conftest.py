"""Root conftest.py — ensures the project root is on sys.path so that
collect, run, store, and analyse can be imported by tests on any platform."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
