CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE market
    ADD COLUMN IF NOT EXISTS embedding vector(384);

CREATE INDEX IF NOT EXISTS idx_market_embedding
    ON market
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
