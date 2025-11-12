import { useEffect, useMemo, useState } from "react";

interface KpiRow {
  day: string;
  rule_type: string;
  signals: number;
  p1_signals: number;
  avg_gap?: number;
  est_edge_bps?: number;
}

export default function KpiCards() {
  const [rows, setRows] = useState<KpiRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/kpi/daily")
      .then(async (res) => {
        if (!res.ok) throw new Error(`Failed KPI ${res.status}`);
        return res.json();
      })
      .then(setRows)
      .catch((err) => setError((err as Error).message));
  }, []);

  const summary = useMemo(() => {
    const data: Record<
      string,
      { signals: number; p1: number; edge: number; gap: number; days: number }
    > = {};
    rows.forEach((row) => {
      const bucket = data[row.rule_type] ?? { signals: 0, p1: 0, edge: 0, gap: 0, days: 0 };
      bucket.signals += row.signals;
      bucket.p1 += row.p1_signals;
      bucket.edge += row.est_edge_bps || 0;
      bucket.gap += row.avg_gap || 0;
      bucket.days += 1;
      data[row.rule_type] = bucket;
    });
    return Object.entries(data).map(([rule, value]) => ({
      rule,
      signals: value.signals,
      p1: value.p1,
      avgEdge: value.edge / Math.max(value.days, 1),
      avgGap: value.gap / Math.max(value.days, 1),
    }));
  }, [rows]);

  if (error) return <div className="glass-panel error">{error}</div>;
  if (summary.length === 0) return null;

  const totalSignals = summary.reduce((sum, item) => sum + item.signals, 0);
  const totalP1 = summary.reduce((sum, item) => sum + item.p1, 0);

  return (
    <div className="glass-panel kpi-panel">
      <div className="kpi-hero">
        <div>
          <p className="signal-eyebrow" style={{ letterSpacing: "0.35rem" }}>
            近 7 日
          </p>
          <h3>{totalSignals} Signals</h3>
          <p>{totalP1} escalated (P1)</p>
        </div>
        <div>
          <p className="kpi-label">活跃规则</p>
          <h4>{summary.length}</h4>
        </div>
      </div>
      <div className="kpi-scroll">
        {summary.map((item) => (
          <div key={item.rule} className="kpi-chip">
            <strong>{item.rule}</strong>
            <p>Signals · {item.signals}</p>
            <p>P1 · {item.p1}</p>
            <p>Avg Gap · {item.avgGap?.toFixed(3) ?? "-"}</p>
            <p>Edge bps · {item.avgEdge?.toFixed(2) ?? "-"}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
