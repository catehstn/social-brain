"""
check_secrets.py — reads a git diff from stdin and exits 1 if credential-shaped
strings are found in non-allowlisted files.

Called by .githooks/pre-commit.
"""

import re
import sys

PATTERNS = [
    ("Anthropic API key",        r"sk-ant-[A-Za-z0-9_-]{20,}"),
    ("Generic Token credential", r"Token\s+[A-Za-z0-9]{20,}"),
    ("Bearer token",             r"Bearer\s+[A-Za-z0-9._\-]{20,}"),
    ("JWT",                      r"eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{10,}"),
    ("AWS access key",           r"AKIA[0-9A-Z]{16}"),
    ("GitHub PAT",               r"ghp_[A-Za-z0-9]{36}"),
    ("Slack bot token",          r"xoxb-[0-9]+-[A-Za-z0-9]+"),
]

# Paths whose content is expected to contain key-shaped strings
ALLOW_PATH_RE = re.compile(
    r"^(?:config\.example\.yaml|README\.md|\.githooks/)"
)

diff = sys.stdin.read()
current_file = None
found = []

for line in diff.splitlines():
    if line.startswith("+++ b/"):
        current_file = line[6:]
        continue
    if not line.startswith("+") or line.startswith("+++"):
        continue
    if current_file and ALLOW_PATH_RE.match(current_file):
        continue
    content = line[1:]
    for label, pattern in PATTERNS:
        if re.search(pattern, content):
            found.append((current_file or "unknown", label, content.strip()))
            break

for path, label, line_content in found:
    print(f"  [{label}] in {path}: {line_content[:120]}")

sys.exit(1 if found else 0)
