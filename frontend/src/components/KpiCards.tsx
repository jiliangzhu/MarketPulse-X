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
    const data: Record<string, { signals: number; p1: number; edge: number; gap: number; days: number }> = {};
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

  if (error) return <p>Error loading KPI: {error}</p>;
  if (summary.length === 0) return null;

  return (
    <div className="card">
      <h3>近 7 日规则 KPI</h3>
      <div className="kpi-grid">
        {summary.map((item) => (
          <div key={item.rule} className="kpi-item">
            <strong>{item.rule}</strong>
            <p>Signals: {item.signals}</p>
            <p>P1: {item.p1}</p>
            <p>Avg gap: {item.avgGap?.toFixed(3) ?? "-"}</p>
            <p>Edge bps: {item.avgEdge?.toFixed(2) ?? "-"}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
