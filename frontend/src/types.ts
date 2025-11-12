export interface BookRow {
  option_id?: string;
  label?: string;
  price?: number;
  best_bid?: number;
  best_ask?: number;
  liquidity?: number;
  ts?: string;
}

export interface TradeLeg {
  market_id?: string;
  option_id?: string;
  side?: string;
  qty?: number;
  reference_price?: number;
  limit_price?: number;
  label?: string;
}

export interface SuggestedTrade {
  action?: string;
  rationale?: string;
  legs?: TradeLeg[];
  estimated_edge_bps?: number;
  confidence?: number;
}

export interface MarketOption {
  option_id: string;
  label: string;
  last_price?: number;
  last_ts?: string;
}

export interface MarketSummary {
  market_id: string;
  title: string;
  status: string;
  ends_at?: string;
  options: MarketOption[];
}

export interface SparkPoint {
  ts: string;
  option_id: string;
  price: number;
}

export interface MarketDetail extends MarketSummary {
  sparkline: SparkPoint[];
  synonyms?: string[];
}

export interface SignalPayload {
  edge_score?: number;
  rule_type?: string;
  suggested_trade?: SuggestedTrade;
  book_snapshot?: BookRow[];
}

export interface SignalRecord {
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
