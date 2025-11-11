import { useEffect, useMemo, useState } from "react";
import { apiRequest } from "../api";
import type { BookRow, SuggestedTrade } from "../types";

interface Intent {
  intent_id: number;
  status: string;
  detail_json?: Record<string, unknown>;
}

interface Props {
  signalId: number | null;
  onClose: () => void;
}

export default function ExecutionModal({ signalId, onClose }: Props) {
  const [intent, setIntent] = useState<Intent | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!signalId) return;
    const controller = new AbortController();
    setLoading(true);
    apiRequest<Intent>("/api/execution/intent", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ signal_id: signalId }),
      signal: controller.signal,
    })
      .then((data) => {
        setIntent(data);
        setError(null);
      })
      .catch((err) => {
        if ((err as DOMException).name === "AbortError") return;
        setError((err as Error).message);
        setIntent(null);
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [signalId]);

  const confirm = async () => {
    if (!intent) return;
    setLoading(true);
    try {
      const data = await apiRequest<Intent>(`/api/execution/confirm/${intent.intent_id}`, {
        method: "POST",
      });
      setIntent((prev) => (prev ? { ...prev, ...data } : data));
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  if (!signalId) return null;

  const detail = intent?.detail_json as {
    rule_type?: string;
    edge_score?: number;
    estimated_edge_bps?: number;
    transport?: string;
    payload?: Record<string, unknown>;
    checks?: { approved?: boolean; reasons?: string[] };
    trade_plan_hint?: SuggestedTrade;
  };
  const checks = (detail?.checks || {}) as {
    approved?: boolean;
    reasons?: string[];
  };
  const reasons = useMemo(() => checks.reasons?.filter(Boolean) ?? [], [checks]);
  const payload = detail?.payload as (Record<string, unknown> & {
    suggested_trade?: SuggestedTrade;
    book_snapshot?: BookRow[];
  }) | undefined;
  const tradePlan = payload?.suggested_trade ?? detail?.trade_plan_hint;
  const bookSnapshot = payload?.book_snapshot;
  const riskState = checks.approved === undefined ? "pending" : checks.approved ? "approved" : "rejected";

  const statusColor =
    intent?.status === "filled"
      ? "#34d399"
      : intent?.status === "rejected"
        ? "#f87171"
        : intent?.status === "sent" || intent?.status === "confirmed"
          ? "#facc15"
          : "#94a3b8";

  const riskColor =
    riskState === "approved" ? "#34d399" : riskState === "rejected" ? "#f87171" : "#facc15";

  const disableConfirm =
    loading || !intent || ["filled", "rejected"].includes(intent.status.toLowerCase());

  const formatEdge = (value?: number) => {
    if (!value && value !== 0) return "-";
    return `${(value * 100).toFixed(2)}%`;
  };

  return (
    <div className="modal-backdrop">
      <div className="modal-card">
        <div className="modal-title-row">
          <div>
            <p className="signal-eyebrow">Execution</p>
            <h3>半自动下单</h3>
          </div>
          {intent && <span className="pill" style={{ borderColor: statusColor, color: statusColor }}>{intent.status.toUpperCase()}</span>}
        </div>
        {loading && <div className="glass-panel">Processing…</div>}
        {error && <div className="glass-panel error">{error}</div>}
        {intent && !loading && !error && (
          <>
            <div className="modal-summary">
              <p>Rule: {detail?.rule_type ?? "-"}</p>
              <p>Edge Score: {formatEdge(detail?.edge_score)}</p>
              <p>
                Est. Edge (bps):{" "}
                {detail?.estimated_edge_bps === undefined ? "-" : detail.estimated_edge_bps.toFixed(1)}
              </p>
              <p>Transport: {detail?.transport ?? "-"}</p>
            </div>
            <div className="risk-summary" style={{ borderColor: riskColor }}>
              <strong style={{ color: riskColor }}>
                风控校验：{riskState === "pending" ? "待确认" : riskState === "approved" ? "通过" : "拒绝"}
              </strong>
              {reasons.length > 0 ? (
                <ul>
                  {reasons.map((msg) => (
                    <li key={msg}>{msg}</li>
                  ))}
                </ul>
              ) : (
                <p style={{ margin: 0, color: "#94a3b8" }}>暂无风险提示</p>
              )}
            </div>
            {tradePlan && tradePlan.legs && tradePlan.legs.length > 0 && (
              <div className="modal-summary" style={{ marginTop: 12 }}>
                <h4 style={{ margin: "4px 0" }}>建议操作</h4>
                <p style={{ margin: "4px 0", fontSize: 13 }}>
                  {tradePlan.action ?? "plan"} {tradePlan.rationale ? `— ${tradePlan.rationale}` : null}
                </p>
                <table className="modal-table">
                  <thead>
                    <tr>
                      <th>Side</th>
                      <th>Leg</th>
                      <th>Qty</th>
                      <th>Ref</th>
                      <th>Limit</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tradePlan.legs.map((leg, idx) => (
                      <tr key={`${leg.option_id}-${idx}`}>
                        <td>{leg.side?.toUpperCase() ?? "-"}</td>
                        <td>{leg.label ?? leg.option_id ?? "-"}</td>
                        <td>{leg.qty ?? 1}</td>
                        <td>{leg.reference_price === undefined ? "-" : leg.reference_price.toFixed(3)}</td>
                        <td>{leg.limit_price === undefined ? "-" : leg.limit_price.toFixed(3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {bookSnapshot && bookSnapshot.length > 0 && (
              <div className="modal-summary" style={{ marginTop: 12 }}>
                <h4 style={{ margin: "4px 0" }}>盘口快照</h4>
                <table className="modal-table">
                  <thead>
                    <tr>
                      <th>Leg</th>
                      <th>Price</th>
                      <th>Bid</th>
                      <th>Ask</th>
                      <th>Liquidity</th>
                    </tr>
                  </thead>
                  <tbody>
                    {bookSnapshot.slice(0, 5).map((row) => (
                      <tr key={row.option_id}>
                        <td>{row.label ?? row.option_id}</td>
                        <td>{row.price === undefined ? "-" : row.price.toFixed(3)}</td>
                        <td>{row.best_bid === undefined ? "-" : row.best_bid.toFixed(3)}</td>
                        <td>{row.best_ask === undefined ? "-" : row.best_ask.toFixed(3)}</td>
                        <td>{row.liquidity === undefined ? "-" : row.liquidity.toFixed(0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            <pre className="modal-detail">{JSON.stringify(intent.detail_json ?? {}, null, 2)}</pre>
          </>
        )}
        <div className="modal-actions">
          <button className="button-secondary" onClick={onClose}>
            关闭
          </button>
          <button className="button-primary" disabled={disableConfirm} onClick={confirm}>
            确认执行
          </button>
        </div>
      </div>
    </div>
  );
}
