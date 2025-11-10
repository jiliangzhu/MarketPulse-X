import { useEffect, useState } from "react";
import { fetchSignals } from "../api";
import type { BookRow, SuggestedTrade } from "../types";
import ExecutionModal from "./ExecutionModal";

interface SignalPayload extends Record<string, unknown> {
  edge_score?: number;
  rule_type?: string;
  suggested_trade?: SuggestedTrade;
  book_snapshot?: BookRow[];
}

interface Signal {
  signal_id: number;
  market_id: string;
  level: string;
  score?: number;
  edge_score?: number;
  payload_json?: SignalPayload;
  created_at: string;
  source?: string;
  confidence?: number;
  reason?: string;
}

const levels = ["all", "P1", "P2", "P3"] as const;
const PAGE_SIZES = [10, 20, 50];

type LevelFilter = (typeof levels)[number];

export default function SignalList() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [level, setLevel] = useState<LevelFilter>("all");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeSignal, setActiveSignal] = useState<number | null>(null);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [pageSize, setPageSize] = useState(10);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const data = await fetchSignals(
          level === "all" ? undefined : level,
          pageSize,
          page * pageSize,
        );
        if (!cancelled) {
          setSignals(data);
          setError(null);
           setHasMore(data.length === pageSize);
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
    const timer = setInterval(load, 5000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [level, page, pageSize]);

  const onChangeLevel = (lvl: LevelFilter) => {
    setLevel(lvl);
    setPage(0);
  };

  const onChangePageSize = (size: number) => {
    setPageSize(size);
    setPage(0);
  };

  const prevPage = () => {
    setPage((prev) => Math.max(0, prev - 1));
  };

  const nextPage = () => {
    if (hasMore) {
      setPage((prev) => prev + 1);
    }
  };

  const renderEdge = (signal: Signal) => {
    const payloadEdge = signal.payload_json?.edge_score;
    const value = signal.edge_score ?? payloadEdge;
    if (value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return `${(value * 100).toFixed(2)}%`;
  };

  const renderRule = (signal: Signal) => {
    const ruleType = signal.payload_json?.rule_type;
    return ruleType ?? "N/A";
  };

  const renderTrade = (signal: Signal) => {
    const trade = signal.payload_json?.suggested_trade;
    if (!trade || !trade.legs || trade.legs.length === 0) return null;
    const legsDesc = trade.legs
      .slice(0, 3)
      .map((leg) => {
        const side = leg.side?.toUpperCase() ?? "?";
        const label = leg.label ?? leg.option_id ?? "-";
        const price = leg.reference_price ?? leg.limit_price;
        return `${side} ${label}@${price === undefined ? "-" : price.toFixed(3)}`;
      })
      .join(" | ");
    return (
      <div style={{ marginTop: 4, fontSize: 12 }}>
        <strong>Trade:</strong> {trade.action ?? "plan"} → {legsDesc}
        {trade.rationale && <div style={{ color: "#6b7280" }}>{trade.rationale}</div>}
      </div>
    );
  };

  const renderBook = (signal: Signal) => {
    const book = signal.payload_json?.book_snapshot;
    if (!book || book.length === 0) return null;
    const summary = book
      .slice(0, 3)
      .map((row) => {
        const label = row.label ?? row.option_id ?? "-";
        const price = row.price ?? 0;
        return `${label}:${price.toFixed(3)}`;
      })
      .join(", ");
    return (
      <div style={{ marginTop: 2, fontSize: 12, color: "#6b7280" }}>
        <strong>Book:</strong> {summary}
      </div>
    );
  };

  return (
    <div className="card">
      <div className="flex" style={{ justifyContent: "space-between", alignItems: "center", gap: 8 }}>
        <h2>Signal Stream</h2>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {levels.map((lvl) => (
            <button
              key={lvl}
              className={lvl === level ? "badge" : ""}
              style={{ marginRight: 8 }}
              onClick={() => onChangeLevel(lvl)}
            >
              {lvl.toUpperCase()}
            </button>
          ))}
          <label style={{ fontSize: 12 }}>
            每页
            <select
              value={pageSize}
              onChange={(e) => onChangePageSize(Number(e.target.value))}
              style={{ marginLeft: 4 }}
            >
              {PAGE_SIZES.map((size) => (
                <option key={size} value={size}>
                  {size}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>
      {loading && <p>Loading signals...</p>}
      {error && <p>Error: {error}</p>}
      {!loading && signals.length === 0 && <p>No signals yet.</p>}
      {signals.map((signal) => (
        <div className="list-item" key={signal.signal_id}>
          <div>
            <div className={`signal-level-${signal.level}`}>{signal.level}</div>
            <small>{new Date(signal.created_at).toLocaleTimeString()}</small>
          </div>
          <div>
            <div>
              Market: {signal.market_id} / {signal.source?.toUpperCase() ?? "RULE"}
            </div>
            <div>Score: {signal.score ?? "-"}</div>
            <div>Edge: {renderEdge(signal)}</div>
            {signal.confidence !== undefined && (
              <div>Confidence: {(signal.confidence * 100).toFixed(0)}%</div>
            )}
            <div>Rule: {renderRule(signal)}</div>
            <div>Reason: {signal.reason ?? "N/A"}</div>
            {renderTrade(signal)}
            {renderBook(signal)}
          </div>
          <button onClick={() => setActiveSignal(signal.signal_id)}>下单</button>
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
      {activeSignal && <ExecutionModal signalId={activeSignal} onClose={() => setActiveSignal(null)} />}
    </div>
  );
}
