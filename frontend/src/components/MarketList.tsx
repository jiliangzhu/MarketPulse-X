import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchMarkets } from "../api";

interface MarketOption {
  option_id: string;
  label: string;
  last_price?: number;
}

interface MarketSummary {
  market_id: string;
  title: string;
  status: string;
  ends_at?: string;
  options: MarketOption[];
}

const PAGE_SIZES = [10, 20, 50];

export default function MarketList() {
  const [markets, setMarkets] = useState<MarketSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(20);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const data = await fetchMarkets(limit, page * limit);
        if (!cancelled) {
          setMarkets(data);
          setHasMore(data.length === limit);
          setError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setError((err as Error).message);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    load();
    const timer = setInterval(load, 8000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [limit, page]);

  const changePageSize = (value: number) => {
    setLimit(value);
    setPage(0);
  };

  const prevPage = () => {
    setPage((p) => Math.max(0, p - 1));
  };

  const nextPage = () => {
    if (hasMore) {
      setPage((p) => p + 1);
    }
  };

  return (
    <div className="card">
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <h2>Markets</h2>
        <label style={{ fontSize: 12 }}>
          每页
          <select
            value={limit}
            onChange={(e) => changePageSize(Number(e.target.value))}
            style={{ marginLeft: 6 }}
          >
            {PAGE_SIZES.map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </label>
      </div>
      {loading && <p>Loading markets...</p>}
      {error && <p>Error: {error}</p>}
      {!loading && markets.length === 0 && <p>No markets found.</p>}
      {markets.map((market) => (
        <div className="list-item" key={market.market_id}>
          <div>
            <Link to={`/markets/${market.market_id}`}>{market.title}</Link>
            <div>
              {market.options.map((opt) => (
                <span key={opt.option_id} style={{ marginRight: 8 }}>
                  {opt.label}: {(opt.last_price ?? 0).toFixed(2)}
                </span>
              ))}
            </div>
          </div>
          <div className="badge">{market.status}</div>
        </div>
      ))}
      <div className="pagination">
        <button onClick={prevPage} disabled={page === 0 || loading}>
          上一页
        </button>
        <span>第 {page + 1} 页</span>
        <button onClick={nextPage} disabled={!hasMore || loading}>
          下一页
        </button>
      </div>
    </div>
  );
}
