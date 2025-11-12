import type { MarketDetail, MarketSummary, SignalRecord } from "./types";

const inferApiBase = () => {
  if (typeof window === "undefined") return "";
  const { protocol, hostname } = window.location;
  return `${protocol}//${hostname}:8080`;
};

let cachedDirectBase: string | null = import.meta.env.VITE_API_BASE ?? null;

const getDirectBase = () => {
  if (cachedDirectBase === null) {
    const inferred = inferApiBase();
    cachedDirectBase = inferred || "";
  }
  return cachedDirectBase;
};

async function fetchWithBase<T>(
  path: string,
  init: RequestInit | undefined,
  base: string,
): Promise<T> {
  const url = base ? `${base}${path}` : path;
  const res = await fetch(url, init);
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status}`);
  }
  return res.json();
}

export async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const baseChain: string[] = [""];
  const direct = getDirectBase();
  if (direct) baseChain.push(direct);
  let lastError: unknown = null;
  for (const base of baseChain) {
    try {
      return await fetchWithBase<T>(path, init, base);
    } catch (err) {
      lastError = err;
      continue;
    }
  }
  throw lastError ?? new Error("API request failed");
}

export function fetchMarkets(limit = 20, offset = 0) {
  const params = new URLSearchParams();
  params.append("limit", String(limit));
  params.append("offset", String(offset));
  return apiRequest<MarketSummary[]>(`/api/markets?${params.toString()}`);
}

export function fetchSignals(level?: string, limit = 10, offset = 0) {
  const searchParams = new URLSearchParams();
  if (level) searchParams.append("level", level);
  searchParams.append("limit", String(limit));
  searchParams.append("offset", String(offset));
  const query = searchParams.toString();
  return apiRequest<SignalRecord[]>(`/api/signals${query ? `?${query}` : ""}`);
}

export function fetchMarketDetail(id: string) {
  return apiRequest<MarketDetail>(`/api/markets/${id}`);
}
