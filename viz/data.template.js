// ─────────────────────────────────────────────────────────────────────────────
// data.template.js
//
// This is the file Claude populates each reporting period.
// Commit this template to the repo. Each period, Claude produces a filled
// data.js by reading the prompt JSON and replacing all placeholder values.
//
// WHAT CLAUDE NEEDS TO DO EACH PERIOD:
//   1. Copy this file to data.js
//   2. Fill every REPLACE_WITH_* placeholder from the JSON analytics data
//   3. Do not touch Dashboard.jsx
//
// SCHEMA NOTES (don't change these — Dashboard.jsx depends on them):
//   blogDaily        → { d: "Mon DD", v: number }[]
//   linkedinDaily    → { d: "Mon DD", imp: number, eng: number, fol: number }[]
//   mastodonPosts    → { label: string, fav: number, boost: number, reply: number, total: number }[]
//   vercelDaily      → { d: "Mon DD", pv: number }[]
//   vercelReferrers  → { name: string, pv: number }[]   sorted descending
//   blogTopPosts     → { title: string, views: number }[]  sorted descending, max 10
//   amazonBooks      → { format: string, bsr: number }[]   sorted ascending by bsr (best first)
//   newsletterIssues → { subject, sent, recipients, newSubs, unsubs, clicks }[]
//   monthlyFunnel    → see below — aggregated manually from the daily data
//   funnelInsights   → { head: string, body: string }[]  exactly 4 items
//   STATS            → { label, value, sub }[]  order matters for colour cycling
// ─────────────────────────────────────────────────────────────────────────────

// ── PERIOD ───────────────────────────────────────────────────────────────────

export const PERIOD_LABEL = {
  id:        "REPLACE_WITH_PERIOD_ID",        // e.g. "2026-W10-3m"
  range:     "REPLACE_WITH_DATE_RANGE",       // e.g. "DEC 2025 – MAR 2026"
  collected: "REPLACE_WITH_COLLECTED_DATE",   // e.g. "2026-03-04"
};

// ── HEADLINE STATS ────────────────────────────────────────────────────────────
// Shown as cards in the Overview tab. Keep this order — colours cycle by index.

export const STATS = [
  { label: "Blog views",          value: "REPLACE",  sub: "cate.blog" },
  { label: "LinkedIn impressions",value: "REPLACE",  sub: "REPLACE members reached" },
  { label: "Course site views",   value: "REPLACE",  sub: "driyourcareer.com" },
  { label: "Mastodon followers",  value: "REPLACE",  sub: "REPLACE total posts" },
  { label: "Newsletter subs",     value: "REPLACE",  sub: "REPLACE WTHIC · REPLACE WMJA" },
  { label: "Book (best BSR)",     value: "REPLACE",  sub: "Kindle · REPLACE★ REPLACE reviews" },
  // LinkedIn detail stats (used in LinkedIn tab header cards)
  { label: "Members reached",     value: "REPLACE",  sub: "" },
  { label: "Total followers",     value: "REPLACE",  sub: "" },
  { label: "Peak day",            value: "REPLACE",  sub: "REPLACE impressions — describe the post" },
  // Mastodon detail (used in Mastodon tab)
  { label: "Mastodon posts",      value: "REPLACE",  sub: "top 30 shown" },
  // Courses detail
  { label: "Unique visitors",     value: "REPLACE",  sub: "" },
  { label: "Bounce rate",         value: "REPLACE%", sub: "" },
  { label: "Purchases",           value: "REPLACE",  sub: "visits to /purchase-success" },
  // Book detail
  { label: "Rating",              value: "REPLACE★", sub: "REPLACE reviews" },
  { label: "Best BSR",            value: "REPLACE",  sub: "REPLACE format" },
  { label: "Book page views",     value: "REPLACE",  sub: "cate.blog/book/" },
];

// ── BLOG DAILY VIEWS ──────────────────────────────────────────────────────────
// One entry per day. d = "Mon DD" (e.g. "Dec 4"). v = integer view count.

export const blogDaily = [
  // REPLACE_WITH_DAILY_BLOG_DATA
  // { d: "Dec 4", v: 106 },
];

// ── LINKEDIN DAILY ────────────────────────────────────────────────────────────
// imp = impressions, eng = engagements, fol = new followers

export const linkedinDaily = [
  // REPLACE_WITH_DAILY_LINKEDIN_DATA
  // { d: "Dec 5", imp: 8, eng: 0, fol: 0 },
];

// ── MASTODON TOP POSTS ────────────────────────────────────────────────────────
// Top 9 by total engagement. label uses \n for the date line. total = fav+boost+reply.

export const mastodonPosts = [
  // REPLACE_WITH_MASTODON_TOP_POSTS
  // { label: "Short description\n(Mon DD)", fav: 0, boost: 0, reply: 0, total: 0 },
];

// ── COURSE SITE DAILY (VERCEL) ────────────────────────────────────────────────
// Only include days where analytics were running (i.e. non-zero or explicitly tracked).

export const vercelDaily = [
  // REPLACE_WITH_VERCEL_DAILY_DATA
  // { d: "Feb 17", pv: 3 },
];

// ── COURSE SITE REFERRERS ─────────────────────────────────────────────────────
// Top referrers, sorted descending by pv.

export const vercelReferrers = [
  // REPLACE_WITH_VERCEL_REFERRERS
  // { name: "substack.com", pv: 174 },
];

// ── BLOG TOP POSTS ────────────────────────────────────────────────────────────
// Top 10 by views. Sorted descending.

export const blogTopPosts = [
  // REPLACE_WITH_BLOG_TOP_POSTS
  // { title: "Getting More Strategic", views: 24174 },
];

// ── AMAZON BOOKS ──────────────────────────────────────────────────────────────
// All formats. Sorted ascending by bsr (best rank first).

export const amazonBooks = [
  // REPLACE_WITH_AMAZON_BOOKS
  // { format: "Kindle (ASIN)", bsr: 159450 },
];

// ── NEWSLETTER ISSUES ─────────────────────────────────────────────────────────
// What's My Job Again? issues sent this period.

export const newsletterIssues = [
  // REPLACE_WITH_NEWSLETTER_ISSUES
  // { subject: "the value of side projects", sent: "Feb 6", recipients: 48, newSubs: 1, unsubs: 2, clicks: 1 },
];

// ── MONTHLY FUNNEL ────────────────────────────────────────────────────────────
// Manually aggregated from the daily data above.
// courseVisitors: use 0 for months before analytics were enabled.
// bookPageViews: estimate if not directly tracked (note it in a comment).

const monthlyFunnel = [
  // REPLACE_WITH_MONTHLY_FUNNEL
  // {
  //   month: "Dec",
  //   posts: 0,           // posts published (from Mastodon data, approximate)
  //   engagement: 0,      // sum of mastodon fav+boost+reply + linkedin engagements
  //   liImpressions: 0,   // sum of linkedin imp for the month
  //   mastodonBoosts: 0,  // sum of mastodon boosts for the month
  //   newFollowers: 0,    // sum of linkedin new followers for the month
  //   courseVisitors: 0,  // from vercel; 0 if analytics not yet running
  //   bookPageViews: 0,   // estimate or 0
  // },
];

// ── FUNNEL NORMALISATION (derived — do not edit) ──────────────────────────────

const norm = (val, max) => max === 0 ? 0 : Math.round((val / max) * 100);
const maxEng = Math.max(...monthlyFunnel.map(m => m.engagement), 1);
const maxImp = Math.max(...monthlyFunnel.map(m => m.liImpressions), 1);
const maxFol = Math.max(...monthlyFunnel.map(m => m.newFollowers), 1);
const maxCV  = Math.max(...monthlyFunnel.map(m => m.courseVisitors), 1);
const maxP   = Math.max(...monthlyFunnel.map(m => m.posts), 1);

export const funnelNorm = monthlyFunnel.map(m => ({
  month:                    m.month,
  "Content (posts)":        norm(m.posts, maxP),
  "Engagement":             norm(m.engagement, maxEng),
  "Reach (LI impressions)": norm(m.liImpressions, maxImp),
  "New followers":          norm(m.newFollowers, maxFol),
  "Course visitors":        norm(m.courseVisitors, maxCV),
}));

// ── FUNNEL INSIGHTS ───────────────────────────────────────────────────────────
// Exactly 4 items. These are the annotation boxes below the funnel chart.
// Write these based on what the data actually shows for this period.

export const funnelInsights = [
  // REPLACE_WITH_FUNNEL_INSIGHTS
  // { head: "Short headline", body: "1-2 sentence explanation grounded in the data." },
];
