const inferApiBase = () => {
  if (typeof window === "undefined") return "";
  const { protocol, hostname, port } = window.location;
  if (port === "5173" || port === "4173") {
    return `${protocol}//${hostname}:8080`;
  }
  return "";
};

let resolvedBase: string | null = import.meta.env.VITE_API_BASE ?? null;

const getApiBase = () => {
  if (resolvedBase !== null) {
    return resolvedBase;
  }
  const candidate = inferApiBase();
  if (candidate) {
    resolvedBase = candidate;
    return candidate;
  }
  return "";
};

async function doFetch<T>(path: string, init: RequestInit | undefined, attempt: "direct" | "proxy"): Promise<T> {
  const base = attempt === "direct" ? getApiBase() : "";
  const url = base ? `${base}${path}` : path;
  try {
    const res = await fetch(url, init);
    if (!res.ok) {
      throw new Error(`Request failed: ${res.status}`);
    }
    return res.json();
  } catch (err) {
    if (attempt === "direct") {
      return doFetch(path, init, "proxy");
    }
    throw err;
  }
}

export async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  return doFetch<T>(path, init, "direct");
}

export function fetchMarkets(limit = 20, offset = 0) {
  const params = new URLSearchParams();
  params.append("limit", String(limit));
  params.append("offset", String(offset));
  return apiRequest(`/api/markets?${params.toString()}`);
}

export function fetchSignals(level?: string, limit = 10, offset = 0) {
  const searchParams = new URLSearchParams();
  if (level) searchParams.append("level", level);
  searchParams.append("limit", String(limit));
  searchParams.append("offset", String(offset));
  const query = searchParams.toString();
  return apiRequest(`/api/signals${query ? `?${query}` : ""}`);
}

export function fetchMarketDetail(id: string) {
  return apiRequest(`/api/markets/${id}`);
}
