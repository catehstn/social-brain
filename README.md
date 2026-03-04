# social-brain

A Python CLI tool that pulls analytics data from your social media and content platforms, sends it to the Claude API for analysis, and writes a weekly markdown report.

---

## Project structure

```
social-brain/
├── config.example.yaml  # template — copy to config.yaml and fill in your keys
├── config.yaml          # your real config — gitignored, never committed
├── collect.py           # data collectors for each platform
├── analyse.py           # Claude API analysis
├── run.py               # entry point
├── data/weekly/         # raw JSON snapshots (gitignored)
├── reports/             # markdown reports (gitignored)
├── linkedin_drops/      # drop LinkedIn CSV/XLSX exports here
└── substack_drops/      # drop Substack CSV exports here (gitignored)
```

---

## Setup

### 1. Clone and activate the pre-commit safety hook

After cloning, run this once to enable the hook that prevents secrets from being committed:

```bash
git config core.hooksPath .githooks
```

### 2. Install Python dependencies

Python 3.11+ is recommended.

```bash
pip install -r requirements.txt
```

### 3. Configure `config.yaml`

Copy the template and fill in your values:

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` with your credentials (see per-platform instructions below).

> **`config.yaml` is listed in `.gitignore` and will never be committed.** Only `config.example.yaml` (which contains no real credentials) lives in the repository.

---

## Platform setup

### Mastodon

No API key needed — the public API is used.

1. Set `mastodon_instance` to your server (e.g. `hachyderm.io`, `mastodon.social`).
2. Set `mastodon_handle` to your username **without** the `@` or instance suffix (e.g. `alice`, not `@alice@hachyderm.io`).

### Bluesky

No API key needed — the public AppView API is used.

1. Set `bluesky_handle` to your full Bluesky handle (e.g. `alice.bsky.social` or a custom domain like `alice.com`).

### Buttondown

A **read-only** API key is sufficient — social-brain only reads emails and subscriber data, never writes.

1. Log in to [buttondown.email](https://buttondown.email).
2. Go to **Settings → API keys** (or visit `https://buttondown.email/settings/api-keys`).
3. Create a new key. Under permissions, enable read access for **Emails** and **Subscribers** only — no write access needed.
4. Paste the key into `buttondown_api_key` in `config.yaml`.

### Jetpack / WordPress.com Stats

Your WordPress site must have the Jetpack plugin active with Stats enabled.

**Get your access token:**

1. Go to [developer.wordpress.com/apps](https://developer.wordpress.com/apps/) and create a new application:
   - **Type:** Native
   - **Website URL:** `http://localhost`
   - **Redirect URL:** `http://localhost`
2. Note the **Client ID** and **Client Secret**.
3. Use the password grant to get a token directly (use your WordPress.com **username**, not your email):
   ```bash
   curl -X POST https://public-api.wordpress.com/oauth2/token -d "client_id=YOUR_CLIENT_ID" -d "client_secret=YOUR_CLIENT_SECRET" -d "grant_type=password" -d "username=YOUR_WP_USERNAME" -d "password=YOUR_WP_PASSWORD"
   ```
4. Copy the `access_token` from the JSON response and set it as `jetpack_access_token`.
5. Set `jetpack_site` to your domain (e.g. `yourdomain.com`).

> **Tip:** A simpler alternative is to use an [application password](https://developer.wordpress.com/docs/wpcom-application-passwords/) if your plan supports it.

### LinkedIn

LinkedIn does not offer a public analytics API for individual creators. Instead, export your post analytics manually and drop the CSV into `linkedin_drops/`.

**How to export:**

1. Go to your LinkedIn profile.
2. Click **Analytics** (on your profile page, below your header).
3. Select **Posts** from the left sidebar.
4. Click the **Export** button (top right of the posts table) → choose a date range covering the last 2 weeks.
5. Download the file (CSV or XLSX) and move/copy it into the `linkedin_drops/` folder.

The tool always picks up the **most recently modified** CSV or XLSX file in that folder. You can keep old exports there — they won't interfere.

### Substack

Substack doesn't offer a public analytics API. Export your email analytics manually and drop the CSV into `substack_drops/`.

**How to export:**

1. Go to your Substack dashboard and click **Stats**.
2. Click **Emails** in the left sidebar.
3. Click **Export** (top right) → download the CSV.
4. Move/copy the file into the `substack_drops/` folder.

The tool always picks up the **most recently modified** CSV in that folder. Expected columns: `Date`, `Subject`, `Recipients`, `Opens`, `Open rate`, `Clicks`, `Click rate`, `Unsubscribes`.

---

## Usage

### Full run (collect + analyse + save report)

```bash
python run.py
```

Saves raw data to `data/weekly/YYYY-WNN.json` and the report to `reports/YYYY-WNN.md`.

### Collect only (no API analysis)

```bash
python run.py --collect-only
```

### Analyse saved data (skip collection)

```bash
python run.py --analyse-only
```

Picks up the most recent snapshot in `data/weekly/`.

### Collect a single platform

```bash
python run.py --platform mastodon
python run.py --platform bluesky
python run.py --platform buttondown
python run.py --platform jetpack
python run.py --platform linkedin
```

---

## Report structure

Each report (`reports/YYYY-WNN.md`) contains:

| Section | Description |
|---|---|
| **What Worked** | Top-performing content with reasons grounded in the data |
| **What Didn't** | Underperformers and hypotheses |
| **Cross-Platform Patterns** | Signals that appear across multiple platforms |
| **Next Week Suggestions** | 5 specific content ideas with platform recommendations |
| **Metrics Summary** | One table with key numbers per platform |

---

## Error handling

- If any single collector fails (network error, bad credentials, etc.) the error is logged and the run continues with the remaining platforms.
- If `linkedin_drops/` contains no CSV files, LinkedIn is skipped and the omission is noted in the report.
- If `config.yaml` is missing or has empty required keys, you'll see a warning and the relevant collectors will be skipped.

---

## Automating weekly runs

Add a cron job (macOS/Linux) to run every Monday morning:

```
0 8 * * 1 cd /path/to/social-brain && python run.py >> logs/cron.log 2>&1
```

---

## Privacy and security

**What stays local:**
- `config.yaml` — gitignored, your real API keys never leave your machine
- `data/weekly/*.json` — raw analytics snapshots, gitignored by default
- `reports/*.md` — generated reports, gitignored by default
- `linkedin_drops/*.csv` — LinkedIn exports, gitignored

**What is sent externally:**
- Collected analytics data is sent to the Anthropic API for analysis (same as any Claude prompt)
- Credentials are sent only to their respective platform APIs (Buttondown, Jetpack, Anthropic)
- Mastodon and Bluesky use public APIs — no credentials transmitted

**Pre-commit hook:**
The `.githooks/pre-commit` script (activated via `git config core.hooksPath .githooks`) scans every staged file for credential-shaped strings before allowing a commit. It blocks `config.yaml`, LinkedIn CSV files, Anthropic API key patterns, JWTs, and other common secret formats.
