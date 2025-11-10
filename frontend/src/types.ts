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
