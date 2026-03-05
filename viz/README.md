# Content Analytics Dashboard

A React dashboard for quarterly content performance reports. Two files live in the repo; one gets replaced each period.

## Files

| File | Status | What it is |
|---|---|---|
| `Dashboard.jsx` | ✅ committed, never edited | Component shell — all charts, tabs, layout |
| `data.template.js` | ✅ committed, never edited | Placeholder schema with comments |
| `data.js` | 🔄 replaced each period | Current period's actual data |

`data.js` is gitignored (or committed per period as `data-PERIOD_ID.js`) — your call.

## How to generate a new period

1. Run your analytics collection script to produce the JSON prompt file (e.g. `prompt-2026-W13-3m.txt`)
2. Open Claude and paste the following instruction along with the JSON:

---

**Prompt to add to the report prompt:**

```
In addition to the markdown report, produce a filled data.js file for the
analytics dashboard at dashboard/data.template.js. 

Rules:
- Copy data.template.js exactly, replacing every REPLACE_WITH_* placeholder
  with real values derived from the JSON data above
- Do not invent numbers. If a value is unavailable, use 0 or "n/a" and add
  a brief comment explaining why
- monthlyFunnel: aggregate daily data by calendar month; use 0 for
  courseVisitors in months before Vercel analytics were running (note the
  actual start date in a comment)
- funnelInsights: write exactly 4 items grounded in what the data shows for
  this specific period — do not reuse last period's insights verbatim
- Do not touch Dashboard.jsx
```

---

3. Save the output as `dashboard/data.js`
4. Open the dashboard in Claude or your local dev environment

## Local development

```bash
# If running standalone (not in Claude artifacts):
npm create vite@latest . -- --template react
npm install recharts
# Replace src/App.jsx content with Dashboard.jsx
# Place data.js alongside Dashboard.jsx
# Update the import path in Dashboard.jsx: from "./data.js"
npm run dev
```

## Schema reference

See `data.template.js` — every export is documented inline.

Key constraint: **don't rename exports**. `Dashboard.jsx` imports by name.
