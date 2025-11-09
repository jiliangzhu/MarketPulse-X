import { useEffect, useMemo, useState } from "react";

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
    setLoading(true);
    fetch("/api/execution/intent", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ signal_id: signalId }),
    })
      .then(async (res) => {
        if (!res.ok) throw new Error(`Failed: ${res.status}`);
        return res.json();
      })
      .then((data) => {
        setIntent(data);
        setError(null);
      })
      .catch((err) => setError((err as Error).message))
      .finally(() => setLoading(false));
  }, [signalId]);

  const confirm = async () => {
    if (!intent) return;
    setLoading(true);
    try {
      const res = await fetch(`/api/execution/confirm/${intent.intent_id}`, { method: "POST" });
      if (!res.ok) throw new Error(`Confirm failed ${res.status}`);
      const data = await res.json();
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
  };
  const checks = (detail?.checks || {}) as {
    approved?: boolean;
    reasons?: string[];
  };
  const reasons = useMemo(() => checks.reasons?.filter(Boolean) ?? [], [checks]);
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
        <h3>半自动下单</h3>
        {loading && <p>Processing...</p>}
        {error && <p style={{ color: "#f87171" }}>{error}</p>}
        {intent && (
          <div>
            <p>Intent #{intent.intent_id}</p>
            <p>
              Status: <span style={{ color: statusColor }}>{intent.status}</span>
            </p>
            <div className="modal-summary">
              <p>Rule: {detail?.rule_type ?? "-"}</p>
              <p>Edge Score: {formatEdge(detail?.edge_score)}</p>
              <p>
                Est. Edge (bps):{" "}
                {detail?.estimated_edge_bps === undefined
                  ? "-"
                  : detail.estimated_edge_bps.toFixed(1)}
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
            <pre className="modal-detail">{JSON.stringify(intent.detail_json ?? {}, null, 2)}</pre>
          </div>
        )}
        <div className="modal-actions">
          <button onClick={onClose}>关闭</button>
          <button disabled={disableConfirm} onClick={confirm}>
            确认执行
          </button>
        </div>
      </div>
    </div>
  );
}
