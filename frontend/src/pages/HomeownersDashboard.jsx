import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import AspectLogo from "../Aspect_Logo.svg";
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  BarChart,
  Bar,
  XAxis,
  YAxis,
} from "recharts";

const API_URL = import.meta.env.VITE_API_URL || "http://127.0.0.1:8001";

function Panel({ title, children, style }) {
  return (
    <div
      style={{
        background: "white",
        borderRadius: 14,
        boxShadow: "0px 2px 8px rgba(0,0,0,0.06)",
        padding: 18,
        ...style,
      }}
    >
      <div style={{ fontSize: 18, fontWeight: 750, marginBottom: 12 }}>
        {title}
      </div>
      {children}
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div
      style={{
        background: "#3F5B9D",
        padding: 18,
        borderRadius: 14,
        textAlign: "center",
        color: "#ffffff",
        minHeight: 96,
        boxShadow: "0px 2px 4px rgba(0,0,0,0.10)",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        gap: 6,
      }}
    >
      <div style={{ fontSize: 22, fontWeight: 800, lineHeight: 1.1 }}>
        {value}
      </div>
      <div style={{ fontSize: 13, opacity: 0.9 }}>{label}</div>
    </div>
  );
}

function fmtGBP(n) {
  const v = Number(n || 0);
  return `£${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function normalizeSegment(seg) {
  const s = String(seg || "").trim().toLowerCase();
  if (!s) return "other";
  if (s.includes("premium")) return "premium";
  if (s.includes("gold")) return "gold";
  if (s.includes("silver")) return "silver";
  if (s.includes("dormant")) return "dormant";
  if (
    s.includes("at risk") ||
    s.includes("at-risk") ||
    s.includes("atrisk") ||
    s === "risk" ||
    s.includes("risk")
  )
    return "at_risk";
  return "other";
}

const SEGMENT_LABEL = {
  premium: "Premium",
  gold: "Gold",
  silver: "Silver",
  dormant: "Dormant",
  at_risk: "At Risk",
};

const PIE_ORDER = ["premium", "gold", "silver", "dormant", "at_risk"];

const PIE_COLORS = ["#13531E", "#198F2D", "#2DB944", "#F29631", "#D05235"];

function meanSafe(values) {
  const nums = values.map((v) => Number(v)).filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return nums.reduce((a, b) => a + b, 0) / nums.length;
}

function SegmentTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const p = payload[0]?.payload;
  if (!p) return null;

  return (
    <div
      style={{
        background: "white",
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        padding: 12,
        boxShadow: "0 6px 18px rgba(0,0,0,0.10)",
        color: "#111",
        minWidth: 240,
      }}
    >
      <div style={{ fontWeight: 800, marginBottom: 6 }}>{p.name}</div>
      <div style={{ fontSize: 13, opacity: 0.85 }}>
        Customers: <b>{p.value.toLocaleString()}</b> ({p.pct.toFixed(1)}%)
      </div>

      <hr style={{ margin: "10px 0", opacity: 0.25 }} />

      <div style={{ fontSize: 13, lineHeight: 1.6 }}>
        <div>
          Avg Recency: <b>{Number(p.avgRecency || 0).toFixed(1)}</b>
        </div>
        <div>
          Avg Frequency: <b>{Number(p.avgFrequency || 0).toFixed(2)}</b>
        </div>
        <div>
          Avg Monetary: <b>{fmtGBP(p.avgMonetary)}</b>
        </div>
        <div>
          Avg Duration: <b>{Number(p.avgDuration || 0).toFixed(1)}</b>
        </div>
        <div>
          Avg RFMD: <b>{Number(p.avgRFMD || 0).toFixed(2)}</b>
        </div>
      </div>
    </div>
  );
}

function TradeTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const row = payload[0]?.payload;
  if (!row) return null;

  return (
    <div
      style={{
        background: "white",
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        padding: 12,
        boxShadow: "0 6px 18px rgba(0,0,0,0.10)",
        color: "#111",
        minWidth: 220,
      }}
    >
      <div style={{ fontWeight: 800, marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 13, lineHeight: 1.6 }}>
        Revenue: <b>{fmtGBP(row.Revenue)}</b>
        <br />
        Customers: <b>{(row.Customers || 0).toLocaleString()}</b>
        <br />
        At Risk %: <b>{Number(row.AtRiskPct || 0).toFixed(1)}%</b>
      </div>
    </div>
  );
}

export default function HomeownersDashboard() {
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");
  const [rows, setRows] = useState([]);

  // Region filters KPIs + charts
  const [region, setRegion] = useState("All Regions");
  // Sub-region filters ONLY the Pie + Bar charts (not KPIs)
  const [subRegion, setSubRegion] = useState("All Sub-Regions");

  useEffect(() => {
    let alive = true;

    async function load() {
      setLoading(true);
      setErr("");

      try {
        const client = axios.create({ baseURL: API_URL, timeout: 45000 });
        const res = await client.get("/homeowners");
        if (!alive) return;
        setRows(Array.isArray(res.data) ? res.data : []);
      } catch (e) {
        setErr(
          e?.response?.data?.detail || e?.message || "Failed to load homeowners"
        );
      } finally {
        if (alive) setLoading(false);
      }
    }

    load();
    return () => {
      alive = false;
    };
  }, []);

  // Region-filtered base (used by KPIs + also as base for charts)
  const regionFilteredRows = useMemo(() => {
    if (region === "All Regions") return rows;
    return rows.filter((r) => r.region === region);
  }, [rows, region]);

  // KPIs (filtered by REGION only)
  const kpi = useMemo(() => {
    const df = regionFilteredRows;

    const totalCustomers = df.length;
    const totalRevenue = df.reduce(
      (acc, r) => acc + (Number(r.monetary) || 0),
      0
    );

    const atRiskCustomers = df.filter(
      (r) => normalizeSegment(r.segment) === "at_risk"
    ).length;

    const regionRev = new Map();
    for (const r of df) {
      const reg = r.region || "Unknown";
      regionRev.set(reg, (regionRev.get(reg) || 0) + (Number(r.monetary) || 0));
    }

    let lowestRegion = "N/A";
    let lowestVal = Infinity;
    let highestRegion = "N/A";
    let highestVal = -Infinity;

    for (const [reg, val] of regionRev.entries()) {
      if (val < lowestVal) {
        lowestVal = val;
        lowestRegion = reg;
      }
      if (val > highestVal) {
        highestVal = val;
        highestRegion = reg;
      }
    }

    if (!Number.isFinite(lowestVal)) lowestVal = 0;
    if (!Number.isFinite(highestVal)) highestVal = 0;

    return {
      totalCustomers,
      totalRevenue,
      atRiskCustomers,
      lowestRegion,
      lowestVal,
      highestRegion,
      highestVal,
    };
  }, [regionFilteredRows]);

  // Region/Sub-region lists (sub-region options depend on region)
  const regionList = useMemo(() => {
    const s = new Set(rows.map((r) => r.region).filter(Boolean));
    return ["All Regions", ...Array.from(s).sort()];
  }, [rows]);

  const subRegionList = useMemo(() => {
    const base = regionFilteredRows;
    const s = new Set(base.map((r) => r.sub_region).filter(Boolean));
    return ["All Sub-Regions", ...Array.from(s).sort()];
  }, [regionFilteredRows]);

  // Pie data (filtered by region + subRegion)
  const segmentPie = useMemo(() => {
    let df = regionFilteredRows;
    if (subRegion !== "All Sub-Regions")
      df = df.filter((r) => r.sub_region === subRegion);

    const total = df.length || 1;

    const bucketRows = {
      premium: [],
      gold: [],
      silver: [],
      dormant: [],
      at_risk: [],
    };

    for (const r of df) {
      const b = normalizeSegment(r.segment);
      if (bucketRows[b]) bucketRows[b].push(r);
    }

    return PIE_ORDER.map((key) => {
      const rs = bucketRows[key] || [];
      const value = rs.length;

      return {
        key,
        name: SEGMENT_LABEL[key],
        value,
        pct: (value / total) * 100,
        avgRecency: meanSafe(rs.map((r) => r.recency)),
        avgFrequency: meanSafe(rs.map((r) => r.frequency)),
        avgMonetary: meanSafe(rs.map((r) => r.monetary)),
        avgDuration: meanSafe(rs.map((r) => r.duration)),
        avgRFMD: meanSafe(rs.map((r) => r.RFMD_score)),
      };
    });
  }, [regionFilteredRows, subRegion]);

  // Revenue by Trade (Top 10) filtered by region + subRegion
  const tradeData = useMemo(() => {
    let df = regionFilteredRows;
    if (subRegion !== "All Sub-Regions")
      df = df.filter((r) => r.sub_region === subRegion);

    const agg = new Map(); // trade -> {rev, customers, atRisk}
    for (const r of df) {
      const trade = r.Trade || "Unknown";
      const rev = Number(r.monetary) || 0;
      const isAtRisk = normalizeSegment(r.segment) === "at_risk";

      if (!agg.has(trade)) agg.set(trade, { rev: 0, customers: 0, atRisk: 0 });
      const o = agg.get(trade);
      o.rev += rev;
      o.customers += 1;
      o.atRisk += isAtRisk ? 1 : 0;
    }

    const rowsOut = Array.from(agg.entries())
      .map(([Trade, o]) => ({
        Trade,
        Revenue: Number(o.rev.toFixed(2)),
        Customers: o.customers,
        AtRiskPct: o.customers ? (o.atRisk / o.customers) * 100 : 0,
      }))
      .sort((a, b) => b.Revenue - a.Revenue)
      .slice(0, 10);

    const filteredTotalRev = rowsOut.reduce((acc, r) => acc + r.Revenue, 0);

    return { rows: rowsOut, filteredTotalRev };
  }, [regionFilteredRows, subRegion]);

  // Layout: balanced padding + centered content (between old + new)
  const PAGE_SIDE_PADDING = 90;
  const CONTENT_MAX_WIDTH = 1700;

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#f7f7f7",
        overflowX: "hidden",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Header */}
      <div
        style={{
          width: "100%",
          padding: `80px ${PAGE_SIDE_PADDING}px 36px`,
          boxSizing: "border-box",
        }}
      >
        <div
          style={{
            maxWidth: CONTENT_MAX_WIDTH,
            margin: "0 auto",
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-start",
            gap: 12,
          }}
        >
          <img
            src={AspectLogo}
            alt="Aspect logo"
            style={{ height: 30, width: "auto" }}
          />
          <div
            style={{
              fontSize: 34,
              fontWeight: 850,
              color: "#111",
              lineHeight: 1,
            }}
          >
            RFMD Dashboard
          </div>
        </div>
      </div>

      {/* Content */}
      <div
        style={{
          width: "100%",
          padding: `0 ${PAGE_SIDE_PADDING}px`,
          boxSizing: "border-box",
          flex: 1,
        }}
      >
        <div style={{ maxWidth: CONTENT_MAX_WIDTH, margin: "0 auto" }}>
          {err && (
            <div
              style={{
                background: "#ffe9e9",
                border: "1px solid #ffb7b7",
                padding: 12,
                borderRadius: 12,
                marginBottom: 12,
                color: "#111",
              }}
            >
              <b>Error:</b> {err}
            </div>
          )}

          {/* KPI Row (5) */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(5, 1fr)",
              gap: 16,
              marginBottom: 18,
            }}
          >
            <StatCard
              label="Total Customers"
              value={loading ? "…" : kpi.totalCustomers.toLocaleString()}
            />
            <StatCard
              label="Total Revenue"
              value={loading ? "…" : fmtGBP(kpi.totalRevenue)}
            />
            <StatCard
              label="At Risk Customers"
              value={loading ? "…" : kpi.atRiskCustomers.toLocaleString()}
            />
            <StatCard
              label="Lowest Revenue Region"
              value={
                loading ? "…" : `${kpi.lowestRegion} (${fmtGBP(kpi.lowestVal)})`
              }
            />
            <StatCard
              label="Highest Revenue Region"
              value={
                loading
                  ? "…"
                  : `${kpi.highestRegion} (${fmtGBP(kpi.highestVal)})`
              }
            />
          </div>

          {/* Filters */}
          <Panel title="Filters" style={{ marginBottom: 18 }}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: 14,
                alignItems: "center",
              }}
            >
              <div>
                <div
                  style={{ fontSize: 12, opacity: 0.7, marginBottom: 4 }}
                >
                  Region (filters KPIs + charts)
                </div>
                <select
                  value={region}
                  onChange={(e) => {
                    setRegion(e.target.value);
                    setSubRegion("All Sub-Regions");
                  }}
                  style={{
                    width: "100%",
                    padding: 10,
                    borderRadius: 10,
                    border: "1px solid #e5e7eb",
                    background: "#fff",
                  }}
                >
                  {regionList.map((r) => (
                    <option key={r} value={r}>
                      {r}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <div
                  style={{ fontSize: 12, opacity: 0.7, marginBottom: 4 }}
                >
                  Sub-Region (filters charts only)
                </div>
                <select
                  value={subRegion}
                  onChange={(e) => setSubRegion(e.target.value)}
                  style={{
                    width: "100%",
                    padding: 10,
                    borderRadius: 10,
                    border: "1px solid #e5e7eb",
                    background: "#fff",
                  }}
                >
                  {subRegionList.map((sr) => (
                    <option key={sr} value={sr}>
                      {sr}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          </Panel>

          {/* Charts */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 2fr",
              gap: 18,
              paddingBottom: 16,
            }}
          >
            {/* Pie */}
            <Panel title="Customer Mix by Segment" style={{ height: "100%" }}>
              {loading ? (
                <div style={{ color: "#111" }}>Loading…</div>
              ) : (
                <div style={{ width: "100%", height: 420 }}>
                  <ResponsiveContainer>
                    <PieChart>
                      <Pie
                        data={segmentPie}
                        dataKey="value"
                        nameKey="name"
                        innerRadius={82}
                        outerRadius={145}
                        paddingAngle={2}
                        label={({ name, pct }) => `${name}: ${pct.toFixed(1)}%`}
                      >
                        {segmentPie.map((entry, idx) => (
                          <Cell
                            key={entry.key}
                            fill={PIE_COLORS[idx % PIE_COLORS.length]}
                          />
                        ))}
                      </Pie>
                      <Tooltip content={<SegmentTooltip />} />
                      <Legend />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              )}
            </Panel>

            {/* Revenue by Trade (Top 10) */}
            <Panel title="Revenue by Trade (Top 10)" style={{ height: "100%" }}>
              <div style={{ marginBottom: 10, fontSize: 13, opacity: 0.8 }}>
                Filtered revenue (Top 10 sum):{" "}
                <b>{fmtGBP(tradeData.filteredTotalRev)}</b>
              </div>

              {loading ? (
                <div style={{ color: "#111" }}>Loading…</div>
              ) : tradeData.rows.length === 0 ? (
                <div style={{ color: "#111" }}>
                  No trade data for the selected region/sub-region.
                </div>
              ) : (
                <div style={{ width: "100%", height: 420 }}>
                  <ResponsiveContainer>
                    <BarChart
                      data={tradeData.rows}
                      layout="vertical"
                      margin={{ top: 10, right: 28, bottom: 10, left: 10 }}
                    >
                      <XAxis
                        type="number"
                        tickFormatter={(v) => `£${Number(v).toLocaleString()}`}
                        axisLine={{ stroke: "#d1d5db" }}
                        tickLine={{ stroke: "#d1d5db" }}
                      />
                      <YAxis
                        type="category"
                        dataKey="Trade"
                        width={170}
                        axisLine={{ stroke: "#d1d5db" }}
                        tickLine={{ stroke: "#d1d5db" }}
                      />
                      <Tooltip content={<TradeTooltip />} />
                      <Bar
                        dataKey="Revenue"
                        fill="#3F5B9D"
                        radius={[10, 10, 10, 10]}
                        barSize={18}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </Panel>
          </div>
        </div>
      </div>
    </div>
  );
}