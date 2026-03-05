// ─────────────────────────────────────────────────────────────────────────────
// Dashboard.jsx — component shell, no hardcoded data
// To generate a new period: populate data.js from the report JSON, keep this
// file untouched.
// ─────────────────────────────────────────────────────────────────────────────

import { useState } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, ReferenceLine, Legend, Cell,
  ComposedChart,
} from "recharts";

import {
  PERIOD_LABEL,
  STATS,
  blogDaily,
  linkedinDaily,
  mastodonPosts,
  vercelDaily,
  vercelReferrers,
  blogTopPosts,
  amazonBooks,
  newsletterIssues,
  funnelNorm,
  funnelInsights,
} from "./data.js";

// ── COLOURS ──────────────────────────────────────────────────────────────────

const C = {
  bg:       "#0f0f11",
  surface:  "#17171a",
  border:   "#2a2a30",
  accent:   "#e8c547",
  accent2:  "#4ecdc4",
  accent3:  "#ff6b6b",
  purple:   "#a78bfa",
  muted:    "#5a5a6e",
  text:     "#e2e2e8",
  textDim:  "#8888a0",
};

// ── HELPERS ───────────────────────────────────────────────────────────────────

const fmt = (n) => n >= 1000 ? (n / 1000).toFixed(1) + "k" : n;

const CustomTooltip = ({ active, payload, label, unit = "" }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: C.surface, border: `1px solid ${C.border}`, padding: "8px 12px", borderRadius: 6, fontSize: 12, color: C.text }}>
      <div style={{ color: C.textDim, marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || C.accent }}>
          {p.name}: <strong>{fmt(p.value)}{unit}</strong>
        </div>
      ))}
    </div>
  );
};

// ── STAT CARD ─────────────────────────────────────────────────────────────────

const Stat = ({ label, value, sub, accent = C.accent }) => (
  <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 20px", flex: 1, minWidth: 120 }}>
    <div style={{ fontSize: 11, color: C.textDim, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>{label}</div>
    <div style={{ fontSize: 26, fontWeight: 700, color: accent, fontFamily: "'DM Mono',monospace", lineHeight: 1 }}>{value}</div>
    {sub && <div style={{ fontSize: 11, color: C.textDim, marginTop: 5 }}>{sub}</div>}
  </div>
);

// ── SECTION WRAPPER ───────────────────────────────────────────────────────────

const Section = ({ title, children, note }) => (
  <div style={{ marginBottom: 32 }}>
    <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 16 }}>
      <h2 style={{ margin: 0, fontSize: 13, fontWeight: 600, color: C.textDim, textTransform: "uppercase", letterSpacing: "0.1em" }}>{title}</h2>
      {note && <span style={{ fontSize: 11, color: C.muted }}>{note}</span>}
    </div>
    {children}
  </div>
);

// ── TABS ──────────────────────────────────────────────────────────────────────

const TABS = ["Overview", "Blog", "LinkedIn", "Mastodon", "Web", "Book"];

// ── MAIN ─────────────────────────────────────────────────────────────────────

export default function Dashboard() {
  const [tab, setTab] = useState("Overview");

  const s = {
    root: { fontFamily: "system-ui,sans-serif", background: C.bg, color: C.text, minHeight: "100vh", padding: "24px", boxSizing: "border-box" },
  };

  return (
    <div style={s.root}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: ${C.bg}; }
        ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 3px; }
      `}</style>

      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <div style={{ fontSize: 11, color: C.muted, fontFamily: "'DM Mono',monospace", marginBottom: 4 }}>{PERIOD_LABEL.range}</div>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, fontFamily: "'DM Sans',sans-serif", letterSpacing: "-0.02em" }}>
          Content Performance <span style={{ color: C.accent }}>{PERIOD_LABEL.id}</span>
        </h1>
      </div>

      {/* Tab bar */}
      <div style={{ display: "flex", gap: 4, marginBottom: 28, borderBottom: `1px solid ${C.border}` }}>
        {TABS.map(t => (
          <button key={t} onClick={() => setTab(t)} style={{
            background: "none", border: "none", cursor: "pointer",
            padding: "8px 14px", fontSize: 13, fontWeight: t === tab ? 600 : 400,
            color: t === tab ? C.accent : C.textDim,
            borderBottom: t === tab ? `2px solid ${C.accent}` : "2px solid transparent",
            marginBottom: -1, transition: "color 0.15s",
          }}>{t}</button>
        ))}
      </div>

      {/* ── OVERVIEW ── */}
      {tab === "Overview" && (
        <>
          <Section title="3-Month Totals">
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              {STATS.map((s, i) => (
                <Stat key={i} label={s.label} value={s.value} sub={s.sub} accent={[C.accent, C.accent2, C.accent3, C.accent, C.accent2, C.accent3][i % 6]} />
              ))}
            </div>
          </Section>

          <Section title="Content → Engagement → Reach → Outcomes" note="indexed 0–100 per month so different scales are comparable">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px 8px" }}>
              <ResponsiveContainer width="100%" height={240}>
                <ComposedChart data={funnelNorm} margin={{ top: 8, right: 24, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="month" tick={{ fontSize: 11, fill: C.textDim }} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={24} domain={[0, 100]} />
                  <Tooltip content={<CustomTooltip unit="%" />} />
                  <Legend wrapperStyle={{ fontSize: 11, color: C.textDim, paddingTop: 12 }} />
                  <Bar dataKey="Content (posts)" fill={C.muted} radius={[2, 2, 0, 0]} opacity={0.7} />
                  <Line type="monotone" dataKey="Engagement" stroke={C.accent} strokeWidth={2.5} dot={{ r: 4, fill: C.accent, strokeWidth: 0 }} />
                  <Line type="monotone" dataKey="Reach (LI impressions)" stroke={C.accent2} strokeWidth={2.5} dot={{ r: 4, fill: C.accent2, strokeWidth: 0 }} />
                  <Line type="monotone" dataKey="New followers" stroke={C.accent3} strokeWidth={2} strokeDasharray="5 3" dot={{ r: 3, fill: C.accent3, strokeWidth: 0 }} />
                  <Line type="monotone" dataKey="Web visitors" stroke={C.purple} strokeWidth={2} strokeDasharray="3 2" dot={{ r: 3, fill: C.purple, strokeWidth: 0 }} />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
            <div style={{
              background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8,
              padding: "12px 16px", marginTop: 10,
              display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(180px,1fr))", gap: "10px 20px"
            }}>
              {[
                { label: "📝 Content (posts)", colour: C.muted, note: "Posts published that month (Mastodon sample)" },
                { label: "💬 Engagement", colour: C.accent, note: "Fav + boosts + replies (Masto) + LI engagements" },
                { label: "📡 Reach", colour: C.accent2, note: "LinkedIn impressions — biggest reach signal we have" },
                { label: "👥 New followers", colour: C.accent3, note: "LinkedIn new followers — audience growth proxy" },
                { label: "🌐 Web visitors", colour: C.purple, note: "Website visitors across tracked sites" },
              ].map((item, i) => (
                <div key={i} style={{ display: "flex", gap: 8, alignItems: "flex-start" }}>
                  <div style={{ width: 10, height: 10, borderRadius: "50%", background: item.colour, flexShrink: 0, marginTop: 3 }} />
                  <div>
                    <div style={{ fontSize: 12, color: C.text, fontWeight: 500 }}>{item.label}</div>
                    <div style={{ fontSize: 11, color: C.muted, marginTop: 2, lineHeight: 1.4 }}>{item.note}</div>
                  </div>
                </div>
              ))}
            </div>
          </Section>

          <Section title="What the funnel shows">
            <div style={{
              background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8,
              padding: "14px 18px",
              display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 16
            }}>
              {funnelInsights.map((c, i) => (
                <div key={i} style={{ borderLeft: `3px solid ${[C.accent, C.accent2, C.purple, C.accent3][i % 4]}`, paddingLeft: 12 }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: C.text, marginBottom: 4 }}>{c.head}</div>
                  <div style={{ fontSize: 12, color: C.textDim, lineHeight: 1.5 }}>{c.body}</div>
                </div>
              ))}
            </div>
          </Section>
        </>
      )}

      {/* ── BLOG ── */}
      {tab === "Blog" && (
        <>
          <Section title="Daily Views" note={`${STATS.find(s=>s.label==="Blog views")?.value} total`}>
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={blogDaily} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="d" tick={{ fontSize: 10, fill: C.muted }} interval={9} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={36} />
                  <Tooltip content={<CustomTooltip unit=" views" />} />
                  <Line type="monotone" dataKey="v" name="Views" stroke={C.accent} strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </Section>

          <Section title="Top Posts" note="views this period">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
              {blogTopPosts.map((p, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 12, padding: "10px 16px",
                  borderBottom: i < blogTopPosts.length - 1 ? `1px solid ${C.border}` : "none",
                }}>
                  <div style={{ fontSize: 11, color: C.muted, fontFamily: "'DM Mono',monospace", width: 16, textAlign: "right" }}>{i + 1}</div>
                  <div style={{ flex: 1, fontSize: 13, color: C.text, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{p.title}</div>
                  <div style={{ fontSize: 13, fontWeight: 600, color: i === 0 ? C.accent : C.textDim, fontFamily: "'DM Mono',monospace", minWidth: 60, textAlign: "right" }}>
                    {p.views.toLocaleString()}
                  </div>
                  <div style={{ width: 80, height: 6, background: C.border, borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", background: i === 0 ? C.accent : C.accent2, width: `${(p.views / blogTopPosts[0].views) * 100}%`, borderRadius: 3 }} />
                  </div>
                </div>
              ))}
            </div>
          </Section>
        </>
      )}

      {/* ── LINKEDIN ── */}
      {tab === "LinkedIn" && (
        <>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
            {STATS.filter(s => ["LinkedIn impressions","Members reached","Total followers","Peak day"].includes(s.label)).map((s, i) => (
              <Stat key={i} label={s.label} value={s.value} sub={s.sub} accent={[C.accent2, C.accent2, C.accent, C.accent3][i]} />
            ))}
          </div>

          <Section title="Daily Impressions">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={linkedinDaily} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="d" tick={{ fontSize: 10, fill: C.muted }} interval={10} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={44} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="imp" name="Impressions" radius={[2, 2, 0, 0]}>
                    {linkedinDaily.map((e, i) => (
                      <Cell key={i} fill={e.imp > 3000 ? C.accent : e.imp > 1000 ? C.accent2 : C.muted} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Section>

          <Section title="Daily Engagements">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={160}>
                <LineChart data={linkedinDaily} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="d" tick={{ fontSize: 10, fill: C.muted }} interval={10} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={30} />
                  <Tooltip content={<CustomTooltip />} />
                  <Line type="monotone" dataKey="eng" name="Engagements" stroke={C.accent3} strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </Section>

          <Section title="New Followers per Day">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={130}>
                <BarChart data={linkedinDaily} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="d" tick={{ fontSize: 10, fill: C.muted }} interval={10} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={25} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="fol" name="New followers" radius={[2, 2, 0, 0]}>
                    {linkedinDaily.map((e, i) => (
                      <Cell key={i} fill={e.fol >= 20 ? C.accent : e.fol >= 5 ? C.accent2 : C.muted} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Section>
        </>
      )}

      {/* ── MASTODON ── */}
      {tab === "Mastodon" && (
        <>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
            {STATS.filter(s => ["Mastodon followers","Mastodon posts"].includes(s.label)).map((s, i) => (
              <Stat key={i} label={s.label} value={s.value} sub={s.sub} accent={C.accent} />
            ))}
          </div>

          <Section title="Top Posts by Total Engagement" note="favourites + boosts + replies">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
              {mastodonPosts.map((p, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 10, padding: "10px 14px",
                  borderBottom: i < mastodonPosts.length - 1 ? `1px solid ${C.border}` : "none",
                }}>
                  <div style={{ fontSize: 11, color: C.muted, width: 16, textAlign: "right", fontFamily: "'DM Mono',monospace" }}>{i + 1}</div>
                  <div style={{ flex: 1, fontSize: 12, color: C.text, whiteSpace: "pre-wrap", lineHeight: 1.3 }}>{p.label}</div>
                  <div style={{ display: "flex", gap: 8, fontSize: 11, fontFamily: "'DM Mono',monospace" }}>
                    <span style={{ color: C.accent }} title="favourites">★{p.fav}</span>
                    <span style={{ color: C.accent2 }} title="boosts">↑{p.boost}</span>
                    <span style={{ color: C.accent3 }} title="replies">↩{p.reply}</span>
                  </div>
                  <div style={{ width: 60, height: 6, background: C.border, borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", background: i === 0 ? C.accent : C.accent2, width: `${(p.total / mastodonPosts[0].total) * 100}%`, borderRadius: 3 }} />
                  </div>
                </div>
              ))}
            </div>
          </Section>

          <Section title="Engagement Breakdown" note="favourites vs boosts vs replies">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={mastodonPosts} layout="vertical" margin={{ top: 4, right: 16, bottom: 0, left: 80 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false} />
                  <XAxis type="number" tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} />
                  <YAxis type="category" dataKey="label" tick={{ fontSize: 10, fill: C.textDim, width: 78 }} tickLine={false} axisLine={false} width={82} />
                  <Tooltip content={<CustomTooltip />} />
                  <Legend wrapperStyle={{ fontSize: 11, color: C.textDim }} />
                  <Bar dataKey="fav" name="Favourites" stackId="a" fill={C.accent} />
                  <Bar dataKey="boost" name="Boosts" stackId="a" fill={C.accent2} />
                  <Bar dataKey="reply" name="Replies" stackId="a" fill={C.accent3} radius={[0, 2, 2, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Section>
        </>
      )}

      {/* ── COURSES ── */}
      {tab === "Web" && (
        <>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
            {STATS.filter(s => ["Web page views","Unique visitors","Bounce rate"].includes(s.label)).map((s, i) => (
              <Stat key={i} label={s.label} value={s.value} sub={s.sub} accent={[C.accent, C.accent, C.accent3][i]} />
            ))}
          </div>

          <Section title="Daily Traffic" note="across tracked sites">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, padding: "16px 8px" }}>
              <ResponsiveContainer width="100%" height={180}>
                <BarChart data={vercelDaily} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
                  <XAxis dataKey="d" tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} />
                  <YAxis tick={{ fontSize: 10, fill: C.muted }} tickLine={false} axisLine={false} width={30} />
                  <Tooltip content={<CustomTooltip unit=" views" />} />
                  <Bar dataKey="pv" name="Page views" fill={C.accent} radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </Section>

          <Section title="Top Referrers">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
              {vercelReferrers.map((r, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 12, padding: "10px 16px",
                  borderBottom: i < vercelReferrers.length - 1 ? `1px solid ${C.border}` : "none",
                }}>
                  <div style={{ flex: 1, fontSize: 13, color: i === 0 ? C.accent : C.text }}>{r.name}</div>
                  <div style={{ fontSize: 13, fontWeight: 600, fontFamily: "'DM Mono',monospace", color: i === 0 ? C.accent : C.textDim, minWidth: 30, textAlign: "right" }}>{r.pv}</div>
                  <div style={{ width: 80, height: 5, background: C.border, borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", background: i === 0 ? C.accent : C.accent2, width: `${(r.pv / vercelReferrers[0].pv) * 100}%`, borderRadius: 3 }} />
                  </div>
                </div>
              ))}
            </div>
          </Section>

          <Section title="Newsletter Issues">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
              {newsletterIssues.map((n, i) => (
                <div key={i} style={{ padding: "14px 16px", borderBottom: i < newsletterIssues.length - 1 ? `1px solid ${C.border}` : "none" }}>
                  <div style={{ fontSize: 13, color: C.text, marginBottom: 6, fontWeight: 500 }}>"{n.subject}"</div>
                  <div style={{ display: "flex", gap: 16, fontSize: 12, color: C.textDim, flexWrap: "wrap" }}>
                    <span>Sent {n.sent}</span>
                    <span>📨 {n.recipients} recipients</span>
                    <span style={{ color: n.newSubs > 10 ? C.accent2 : C.textDim }}>+{n.newSubs} new subs</span>
                    {n.unsubs > 0 && <span style={{ color: C.accent3 }}>−{n.unsubs} unsubs</span>}
                    <span>{n.clicks} click{n.clicks !== 1 ? "s" : ""}</span>
                  </div>
                </div>
              ))}
            </div>
          </Section>
        </>
      )}

      {/* ── BOOK ── */}
      {tab === "Book" && (
        <>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 24 }}>
            {STATS.filter(s => ["Rating","Best BSR","Book page views"].includes(s.label)).map((s, i) => (
              <Stat key={i} label={s.label} value={s.value} sub={s.sub} accent={[C.accent, C.accent2, C.accent3][i]} />
            ))}
          </div>

          <Section title="Amazon BSR by Format" note="lower = better rank">
            <div style={{ background: C.surface, border: `1px solid ${C.border}`, borderRadius: 8, overflow: "hidden" }}>
              {amazonBooks.map((b, i) => (
                <div key={i} style={{
                  display: "flex", alignItems: "center", gap: 12, padding: "12px 16px",
                  borderBottom: i < amazonBooks.length - 1 ? `1px solid ${C.border}` : "none",
                }}>
                  <div style={{ flex: 1, fontSize: 13, color: i === 0 ? C.accent : C.text }}>{b.format}</div>
                  <div style={{ fontSize: 13, fontFamily: "'DM Mono',monospace", color: i === 0 ? C.accent : C.textDim }}>{b.bsr.toLocaleString()}</div>
                  <div style={{ width: 80, height: 5, background: C.border, borderRadius: 3, overflow: "hidden" }}>
                    <div style={{ height: "100%", background: i === 0 ? C.accent : C.muted, width: `${(1 - (b.bsr / (amazonBooks[amazonBooks.length-1].bsr * 1.1))) * 100}%`, borderRadius: 3 }} />
                  </div>
                </div>
              ))}
            </div>
            <div style={{ fontSize: 12, color: C.muted, marginTop: 8, paddingLeft: 4 }}>
              BSR is a relative rank — lower is better. No sales data available from Amazon.
            </div>
          </Section>
        </>
      )}

      {/* Footer */}
      <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 16, marginTop: 8, fontSize: 11, color: C.muted, display: "flex", justifyContent: "space-between", flexWrap: "wrap", gap: 8 }}>
        <span>Data collected {PERIOD_LABEL.collected} · {PERIOD_LABEL.range}</span>
      </div>
    </div>
  );
}
