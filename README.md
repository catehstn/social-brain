# social-brain

A Python CLI tool that collects analytics data from your social, content, and publishing platforms, then builds a prompt for Claude to produce a performance report and an interactive analytics dashboard that renders directly in claude.ai.

---

## Project structure

```
social-brain/
├── config.example.yaml   # template — copy to config.yaml and fill in your keys
├── config.yaml           # your real config — gitignored, never committed
├── collect.py            # data collectors for each platform
├── analyse.py            # prompt builder
├── run.py                # entry point
├── data/weekly/          # raw JSON snapshots (gitignored)
├── reports/              # generated prompt files (gitignored)
├── linkedin_drops/       # drop LinkedIn CSV/XLSX exports here
├── substack_drops/       # drop Substack CSV exports here (gitignored)
└── viz/
    └── Dashboard.jsx     # reference template Claude models the artifact on — never edit
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

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` with your credentials. See per-platform instructions below.

> **`config.yaml` is gitignored and will never be committed.** Only `config.example.yaml` (no real credentials) lives in the repo.

---

## Platform setup

### Mastodon

No API key needed — the public API is used for your own posts.

1. Set `mastodon_instance` (e.g. `hachyderm.io`).
2. Set `mastodon_handle` — your username **without** `@` or instance (e.g. `alice`).

**Optional — @mention notifications:**

To also collect posts where others mention you, generate an access token:

1. Go to **Settings → Development → New Application** on your instance.
2. Enable the `read:notifications` scope only.
3. Copy the access token and set `mastodon_access_token` in `config.yaml`.

### Bluesky

No API key needed — the public AppView API is used for your own posts.

1. Set `bluesky_handle` to your full handle (e.g. `alice.bsky.social`).

**Optional — @mention notifications:**

1. Go to **Settings → Privacy and Security → App Passwords**.
2. Create a new App Password (do not use your main password).
3. Set `bluesky_app_password` in `config.yaml`.

### Buttondown

A **read-only** API key is sufficient.

1. Log in to [buttondown.email](https://buttondown.email).
2. Go to **Settings → API keys**.
3. Create a key with read access for **Emails** and **Subscribers** only.
4. Set `buttondown_api_key` in `config.yaml`.

Collects: subscriber counts per newsletter, and per-issue open rate, click rate, and unsubscribe count. Also picks up any scheduled (future) emails.

### Jetpack / WordPress.com Stats

Your WordPress site must have Jetpack active with Stats enabled.

**Get your access token:**

1. Go to [developer.wordpress.com/apps](https://developer.wordpress.com/apps/) and create an application:
   - **Type:** Native
   - **Website URL / Redirect URL:** `http://localhost`
2. Note the **Client ID** and **Client Secret**.
3. Exchange credentials for a token (use your WordPress.com **username**, not email):
   ```bash
   curl -X POST https://public-api.wordpress.com/oauth2/token \
     -d "client_id=YOUR_CLIENT_ID" \
     -d "client_secret=YOUR_CLIENT_SECRET" \
     -d "grant_type=password" \
     -d "username=YOUR_WP_USERNAME" \
     -d "password=YOUR_WP_PASSWORD"
   ```
4. Set `jetpack_access_token` and `jetpack_site` (e.g. `yourdomain.com`) in `config.yaml`.

Collects: daily page views, top posts, referrer sources, and scheduled future posts.

### LinkedIn

LinkedIn does not offer a public analytics API for individual creators. Export manually and drop the file into `linkedin_drops/`.

**How to export:**

1. Go to your LinkedIn profile → **Analytics → Posts**.
2. Click **Export** → choose a date range → download the CSV or XLSX.
3. Move the file into `linkedin_drops/`.

The tool picks up the **most recently modified** file in that folder. Post text is automatically fetched from each post's public URL.

### Vercel Web Analytics

Your Vercel project must have Web Analytics enabled.

> **Note:** social-brain uses the internal dashboard API endpoints. These may change without notice. You need at least **Member** access on the team that owns the project.

1. Go to [vercel.com/account/tokens](https://vercel.com/account/tokens) → **Create token**.
   - Scope: **Full Account**.
2. Set `vercel_token` in `config.yaml`.
3. Set `vercel_project_id` to the project **slug** from the URL:
   `https://vercel.com/my-team/my-app` → `my-app`.
4. If under a team, set `vercel_team_id` (looks like `team_xxxxxxxx`). Find it at **Team Settings → General**.

### Amazon

No API key needed — public product pages are scraped.

1. Copy the ASIN from each edition's Amazon URL (e.g. `amazon.com/dp/B0XXXXXXXXXX` → `B0XXXXXXXXXX`).
2. Add them to `amazon_asins` in `config.yaml`.
3. List marketplaces in `amazon_marketplaces` (default: `amazon.com` and `amazon.co.uk`).

Collects: sales rank, star rating, and review count per edition per marketplace.

### Substack

Export analytics manually and drop the CSV into `substack_drops/`.

**How to export:**

1. Go to your Substack dashboard → **Stats → Emails**.
2. Click **Export** → download the CSV.
3. Move the file into `substack_drops/`.

Expected columns: `Date`, `Subject`, `Recipients`, `Opens`, `Open rate`, `Clicks`, `Click rate`, `Unsubscribes`.

### Buffer

Uses the Buffer GraphQL API (personal access token, currently in beta).

1. Go to [buffer.com/manage/apps-and-extras/apps](https://buffer.com/manage/apps-and-extras/apps).
2. Generate a personal access token.
3. Set `buffer_token` in `config.yaml`.

Collects: all scheduled and draft posts across connected channels (Mastodon, LinkedIn, Bluesky, etc.).

### Mentions and inbound links

Controls which domains are monitored for external references.

```yaml
monitored_domains:
  - yourdomain.com
  - yourcoursedomain.com
```

**Hacker News** (no auth): automatically searches stories and comments for any monitored domain using the Algolia API.

**Mastodon mentions**: set `mastodon_access_token` (see Mastodon section above).

**Bluesky mentions**: set `bluesky_app_password` (see Bluesky section above).

**Google Search Console** (optional):

GSC gives you the search queries that brought people to your site and which pages they landed on.

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → create or select a project.
2. Enable the **Google Search Console API**.
3. Create a **Service Account** → generate a JSON key → download it.
4. In [Search Console](https://search.google.com/search-console), add the service account email as a **Full user** on each property.
5. Set `gsc_credentials_file` in `config.yaml` to the path of the JSON key file.
6. Install the extra dependency: `pip install google-api-python-client google-auth`.

### Upcoming / scheduled content

Automatically collected alongside the other sources — no extra config needed once the relevant platforms are set up. Pulls:

- **WordPress** scheduled posts (future publish dates)
- **Buttondown** scheduled emails
- **Buffer** queued posts

The prompt instructs Claude to treat upcoming content as already planned and suggest complementary ideas in section 4, rather than duplicating what's already in the queue.

---

## Usage

### Full run (collect all platforms + build prompt)

```bash
python run.py
```

Saves raw data to `data/weekly/YYYY-WNN.json` and the prompt to `reports/prompt-YYYY-WNN.txt`.

### Longer lookback window

```bash
python run.py --months 3
```

### Collect only (skip prompt generation)

```bash
python run.py --collect-only
```

### Build prompt from saved data (skip collection)

```bash
python run.py --analyse-only
```

### Collect a single platform

```bash
python run.py --platform mastodon
python run.py --platform bluesky
python run.py --platform buttondown
python run.py --platform jetpack
python run.py --platform linkedin
python run.py --platform substack
python run.py --platform vercel
python run.py --platform amazon
python run.py --platform buffer      # (via upcoming)
python run.py --platform upcoming
python run.py --platform mentions
```

---

## What Claude produces

Paste the generated prompt file (`reports/prompt-YYYY-WNN.txt`) into claude.ai. Claude will produce **two outputs**:

### 1. Markdown report

| Section | Description |
|---|---|
| **What Worked** | Top-performing content with reasons grounded in the data |
| **What Didn't** | Underperformers and hypotheses |
| **Cross-Platform Patterns** | Signals that appear across two or more platforms |
| **Next Period Suggestions** | 5 specific ideas, each justified by a data signal; complements already-scheduled content |
| **Metrics Summary** | One table with key numbers per platform |

### 2. Interactive dashboard artifact

Claude produces a self-contained React component that renders directly in claude.ai as an interactive artifact — no local server, no file copying.

**Tabs:** Overview (funnel chart + headline stats) · Blog · LinkedIn · Mastodon · Courses · Book

**Workflow each period:**
1. Run `python run.py` (or `--months 3` for a quarterly view).
2. Paste `reports/prompt-YYYY-WNN.txt` into claude.ai.
3. The dashboard renders inline in the chat.

`viz/Dashboard.jsx` is the reference template Claude models the artifact on. Never edit it manually.

---

## Automating weekly runs

```
0 8 * * 1 cd /path/to/social-brain && python run.py >> logs/cron.log 2>&1
```

---

## Error handling

- Any collector that fails is skipped with a log warning — a partial run always produces a report.
- If `linkedin_drops/` has no files, LinkedIn is skipped.
- If `monitored_domains` is empty, the mentions collector is skipped.
- Optional auth keys (`mastodon_access_token`, `bluesky_app_password`, `gsc_credentials_file`) are silently skipped if not set.

---

## Privacy and security

**What stays local:**
- `config.yaml` — gitignored, credentials never leave your machine
- `data/weekly/*.json` — raw analytics snapshots, gitignored
- `reports/*.txt` — prompt files, gitignored
- `linkedin_drops/` and `substack_drops/` — exports, gitignored
- `viz/data.js` — dashboard data, gitignored

**What is sent externally:**
- The prompt (including collected analytics) is sent to Claude via claude.ai — same as any prompt you type
- Credentials are sent only to their respective platform APIs
- Mastodon and Bluesky post collection uses public APIs — no credentials transmitted for that part

**Pre-commit hook:**
`.githooks/pre-commit` (activated via `git config core.hooksPath .githooks`) scans staged files for credential-shaped strings before allowing a commit.
