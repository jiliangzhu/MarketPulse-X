import { useCallback, useEffect, useMemo, useState } from "react";
import { apiRequest, fetchSignals } from "../api";
import type { SignalRecord } from "../types";
import ExecutionModal from "./ExecutionModal";

const levels = ["all", "P1", "P2", "P3"] as const;
const PAGE_SIZES = [10, 20, 50];

type LevelFilter = (typeof levels)[number];

export default function SignalList() {
  const [signals, setSignals] = useState<SignalRecord[]>([]);
  const [level, setLevel] = useState<LevelFilter>("all");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeSignal, setActiveSignal] = useState<number | null>(null);
  const [page, setPage] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [pageSize, setPageSize] = useState(10);

  const loadSignals = useCallback(async () => {
    setLoading(true);
    const controller = new AbortController();
    try {
      const data = await fetchSignals(level === "all" ? undefined : level, pageSize, page * pageSize);
      setSignals(data);
      setHasMore(data.length === pageSize);
      setError(null);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
      controller.abort();
    }
  }, [level, page, pageSize]);

  useEffect(() => {
    let isMounted = true;
    const run = async () => {
      if (!isMounted) return;
      await loadSignals();
    };
    run();
    const timer = setInterval(run, 5000);
    return () => {
      isMounted = false;
      clearInterval(timer);
    };
  }, [loadSignals]);

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

  const renderEdge = (signal: SignalRecord) => {
    const payloadEdge = signal.payload_json?.edge_score;
    const value = signal.edge_score ?? payloadEdge;
    if (value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return `${(value * 100).toFixed(2)}%`;
  };

  const renderRule = (signal: SignalRecord) => {
    const ruleType = signal.payload_json?.rule_type;
    return ruleType ?? "N/A";
  };

  const renderTrade = (signal: SignalRecord) => {
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
      .join(" · ");
    return (
      <div className="signal-trade-row">
        {trade.action && <span className="chip">{trade.action}</span>}
        <span>{legsDesc}</span>
      </div>
    );
  };

  const renderTitle = (signal: SignalRecord) => {
    return signal.payload_json?.market_title ?? signal.reason ?? signal.market_id;
  };

  const renderBook = (signal: SignalRecord) => {
    const book = signal.payload_json?.book_snapshot;
    if (!book || book.length === 0) return null;
    const summary = book
      .slice(0, 3)
      .map((row) => {
        const label = row.label ?? row.option_id ?? "-";
        const price = row.price ?? 0;
        return `${label}:${price.toFixed(3)}`;
      })
      .join(" · ");
    return <div className="signal-book-row">{summary}</div>;
  };

  const signalCards = useMemo(
    () =>
      signals.map((signal) => (
        <button
          type="button"
          key={signal.signal_id}
          className="signal-card"
          onClick={() => setActiveSignal(signal.signal_id)}
        >
          <div className="signal-card-top">
            <div>
              <span className={`pill pill-${signal.level.toLowerCase()}`}>{signal.level}</span>
              <span className="pill pill-muted">{signal.source?.toUpperCase() ?? "RULE"}</span>
            </div>
            <span className="signal-score">{renderEdge(signal)}</span>
          </div>
          <div className="signal-title">{renderTitle(signal)}</div>
          <div className="signal-meta">
            <span>{new Date(signal.created_at).toLocaleTimeString()}</span>
            {signal.confidence !== undefined && (
              <span>Confidence {(signal.confidence * 100).toFixed(0)}%</span>
            )}
            <span>{renderRule(signal)}</span>
          </div>
          {renderTrade(signal)}
          {renderBook(signal)}
        </button>
      )),
    [signals],
  );

  return (
    <section className="signal-section">
      <header className="signal-header">
        <div>
          <p className="signal-eyebrow">Real-time Alpha</p>
          <h2>Signal Stream</h2>
        </div>
        <div className="signal-controls">
          <div className="segmented">
            {levels.map((lvl) => (
              <button
                key={lvl}
                className={lvl === level ? "active" : ""}
                onClick={() => onChangeLevel(lvl)}
              >
                {lvl.toUpperCase()}
              </button>
            ))}
          </div>
          <select value={pageSize} onChange={(e) => onChangePageSize(Number(e.target.value))}>
            {PAGE_SIZES.map((size) => (
              <option key={size} value={size}>
                每页 {size}
              </option>
            ))}
          </select>
        </div>
      </header>
      {loading && <div className="glass-panel">Loading signals…</div>}
      {error && <div className="glass-panel error">{error}</div>}
      {!loading && signals.length === 0 && <div className="glass-panel">No signals yet.</div>}
      <div className="signal-grid">{signalCards}</div>
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
    </section>
  );
}
