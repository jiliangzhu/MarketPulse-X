import { Routes, Route, Link } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import MarketDetail from "./pages/MarketDetail";

function App() {
  return (
    <div className="app-shell">
      <header>
        <Link to="/">
          <h1>MarketPulse-X</h1>
        </Link>
      </header>
      <main>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/markets/:id" element={<MarketDetail />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;
