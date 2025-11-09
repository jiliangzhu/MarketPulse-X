const API_BASE = import.meta.env.VITE_API_BASE || "";

async function request<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status}`);
  }
  return res.json();
}

export function fetchMarkets(limit = 20, offset = 0) {
  const params = new URLSearchParams();
  params.append("limit", String(limit));
  params.append("offset", String(offset));
  return request(`/api/markets?${params.toString()}`);
}

export function fetchSignals(level?: string, limit = 10, offset = 0) {
  const searchParams = new URLSearchParams();
  if (level) searchParams.append("level", level);
  searchParams.append("limit", String(limit));
  searchParams.append("offset", String(offset));
  const query = searchParams.toString();
  return request(`/api/signals${query ? `?${query}` : ""}`);
}

export function fetchMarketDetail(id: string) {
  return request(`/api/markets/${id}`);
}
