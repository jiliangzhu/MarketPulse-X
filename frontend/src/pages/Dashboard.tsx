import { useEffect, useState } from "react";
import MarketList from "../components/MarketList";
import SignalList from "../components/SignalList";
import KpiCards from "../components/KpiCards";
import RunbookCard from "../components/RunbookCard";

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
    <div className="flex">
      <div className="column">
        <KpiCards />
        <SignalList />
        {health && (
          <div className="card">
            <h3>Health</h3>
            <p>DB: {health.db}</p>
            <p>Rules: {health.rules_heartbeat}</p>
          </div>
        )}
      </div>
      <div className="column">
        <MarketList />
        <RunbookCard />
      </div>
    </div>
  );
}
