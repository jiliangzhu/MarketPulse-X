import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
import { fetchMarketDetail } from "../api";

interface SparkPoint {
  ts: string;
  option_id: string;
  price: number;
}

interface MarketOption {
  option_id: string;
  label: string;
  last_price?: number;
  last_ts?: string;
}

interface MarketDetail {
  market_id: string;
  title: string;
  status: string;
  sparkline: SparkPoint[];
  options: MarketOption[];
  synonyms?: string[];
}

export default function MarketDetail() {
  const { id } = useParams();
  const [market, setMarket] = useState<MarketDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const data = await fetchMarketDetail(id);
        if (!cancelled) {
          setMarket(data);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) setError((err as Error).message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    const timer = setInterval(load, 5000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [id]);

  const sparkline = useMemo(() => {
    if (!market || market.sparkline.length === 0) return null;
    const points = [...market.sparkline].reverse();
    const series = points.map((p) => p.price);
    const min = Math.min(...series);
    const max = Math.max(...series);
    const range = max - min || 1;
    const width = 300;
    const height = 80;
    const step = width / Math.max(series.length - 1, 1);
    const path = points
      .map((point, idx) => {
        const x = idx * step;
        const normalized = (point.price - min) / range;
        const y = height - normalized * height;
        return `${idx === 0 ? "M" : "L"}${x},${y}`;
      })
      .join(" ");
    return { path, width, height };
  }, [market]);

  if (loading) return <p>Loading market...</p>;
  if (error) return <p>Error: {error}</p>;
  if (!market) return <p>Market not found.</p>;

  return (
    <div className="card">
      <h2>{market.title}</h2>
      {sparkline && (
        <svg width={sparkline.width} height={sparkline.height} style={{ background: "#0f172a" }}>
          <path d={sparkline.path} fill="none" stroke="#38bdf8" strokeWidth={2} />
        </svg>
      )}
      <h3>Options</h3>
      {market.options.map((opt) => (
        <div className="list-item" key={opt.option_id}>
          <div>{opt.label}</div>
          <div>{(opt.last_price ?? 0).toFixed(2)}</div>
        </div>
      ))}
      {market.synonyms && market.synonyms.length > 0 && (
        <div>
          <h3>相似市场</h3>
          {market.synonyms.map((id) => (
            <div key={id}>
              <a href={`/markets/${id}`}>{id}</a>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
