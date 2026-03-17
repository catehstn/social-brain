# Claude Code Guidelines for social-brain

## Git workflow

- **Always create a new branch** before making any commits. Never commit directly to `main`.
- Branch names should be short and descriptive (e.g. `reduce-prompt-size`, `fix-linkedin-trimming`).
- One PR per logical change. Don't bundle unrelated changes.
- Push the branch and open a PR when the work is ready.

## Testing

- Run the full test suite before and after every change: `python3 -m pytest tests/ -v`
- All tests must pass before opening a PR.
- **Add tests for every new behaviour** — if you add a trimming rule, a new flag, or a new data transformation, write tests that cover it.
- If a change causes existing tests to fail, fix the tests to match the new correct behaviour (don't revert the change to make tests pass).
- After code changes, validate on real data: `python3 run.py --analyse-only` and verify the output looks correct.

## Prompt size and token cost

- Be conscious of how changes affect the size of the generated prompt — this directly affects Claude API cost and whether the prompt fits in a context window.
- When adding new data fields to the prompt, consider whether they need to be trimmed in `_trim_data`.
- Prefer sending counts and summaries over full lists when the detail isn't needed for analysis.
- After significant changes, measure prompt size with: `python3 run.py --analyse-only` and check the reported char count.

## README

- Update the README whenever you change user-facing behaviour: new flags, changed defaults, new platform setup, changed workflow.
- Internal implementation details (trimming logic, refactors) don't need README updates.
- The Usage section must stay accurate — if a flag changes meaning or a new one is added, update it.

## Code quality

- `_trim_data` in `analyse.py` is the single place where collected data is reduced before being sent to Claude. Keep all trimming logic there.
- When trimming, preserve the most analytically useful data (e.g. sort by engagement before capping post lists).
- Add a `_note` field to the data when truncating, so Claude knows data was trimmed (e.g. `"Showing top 15 of 42 posts"`).
- Don't embed large static assets in the prompt — reference them by URL where possible (e.g. `Dashboard.jsx` is fetched from GitHub).

## PR description

Include in every PR:
- What changed and why
- Whether tests pass
- Any validation done on real data
