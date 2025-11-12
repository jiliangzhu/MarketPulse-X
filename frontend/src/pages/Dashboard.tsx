import { useEffect, useState } from "react";
import MarketList from "../components/MarketList";
import SignalList from "../components/SignalList";
import KpiCards from "../components/KpiCards";

interface Health {
  status: string;
  db: string;
  rules_heartbeat: string;
}

export default function Dashboard() {
  const [health, setHealth] = useState<Health | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function loadHealth() {
      try {
        const res = await fetch("/api/healthz");
        if (res.ok) {
          const data = (await res.json()) as Health;
          if (!cancelled) setHealth(data);
        }
      } catch (err) {
        console.error(err);
      }
    }
    loadHealth();
    const timer = setInterval(loadHealth, 5000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  return (
    <div className="dashboard-grid">
      <div className="column wide">
        <KpiCards />
        <SignalList />
      </div>
      <div className="column narrow">
        <MarketList />
        {health && (
          <div className="glass-panel health-card">
            <p className="signal-eyebrow" style={{ letterSpacing: "0.4rem" }}>
              STATUS
            </p>
            <h3>System Health</h3>
            <p>Data Plane: {health.db}</p>
            <p>Rules Engine: {health.rules_heartbeat}</p>
          </div>
        )}
      </div>
    </div>
  );
}
