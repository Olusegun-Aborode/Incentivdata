-- ============================================================
-- ENRICHED BRIDGE ANALYTICS TABLES
-- Cross-chain wallet behavior for bridge users
-- ============================================================

-- Protocol interactions per wallet per chain
-- Tracks what protocols bridge users interact with on source/destination chains
CREATE TABLE IF NOT EXISTS bridge_wallet_protocols (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    chain_name TEXT NOT NULL,
    protocol_name TEXT NOT NULL,
    protocol_category TEXT,          -- 'dex', 'lending', 'bridge', 'nft', 'staking', etc.
    volume_usd NUMERIC DEFAULT 0,
    tx_count INTEGER DEFAULT 0,
    last_active TIMESTAMPTZ,
    bridge_direction TEXT NOT NULL,  -- 'inbound' or 'outbound'
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bwp_wallet_chain_proto
    ON bridge_wallet_protocols(wallet_address, chain_id, protocol_name, bridge_direction);
CREATE INDEX IF NOT EXISTS idx_bwp_wallet ON bridge_wallet_protocols(wallet_address);
CREATE INDEX IF NOT EXISTS idx_bwp_protocol ON bridge_wallet_protocols(protocol_name);
CREATE INDEX IF NOT EXISTS idx_bwp_chain ON bridge_wallet_protocols(chain_id);
CREATE INDEX IF NOT EXISTS idx_bwp_direction ON bridge_wallet_protocols(bridge_direction);

-- Multi-chain wallet presence
-- Shows which chains each bridge wallet is active on
CREATE TABLE IF NOT EXISTS bridge_wallet_chains (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    chain_name TEXT NOT NULL,
    tx_count INTEGER DEFAULT 0,
    volume_usd NUMERIC DEFAULT 0,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bwc_wallet_chain
    ON bridge_wallet_chains(wallet_address, chain_id);
CREATE INDEX IF NOT EXISTS idx_bwc_wallet ON bridge_wallet_chains(wallet_address);

-- Post-bridge-out actions
-- Tracks what users do immediately after bridging out of Incentiv
CREATE TABLE IF NOT EXISTS bridge_post_actions (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    bridge_out_tx_hash TEXT NOT NULL,
    bridge_out_timestamp TIMESTAMPTZ,
    bridge_out_chain_id INTEGER,
    bridge_out_chain_name TEXT,
    next_action_tx_hash TEXT,
    next_action_type TEXT,           -- 'swap', 'transfer', 'deposit', 'mint', 'stake', etc.
    next_action_protocol TEXT,
    next_action_chain_id INTEGER,
    next_action_chain_name TEXT,
    next_action_timestamp TIMESTAMPTZ,
    time_to_next_action_seconds INTEGER,
    amount_usd NUMERIC,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bpa_bridge_tx
    ON bridge_post_actions(bridge_out_tx_hash);
CREATE INDEX IF NOT EXISTS idx_bpa_wallet ON bridge_post_actions(wallet_address);
CREATE INDEX IF NOT EXISTS idx_bpa_action_type ON bridge_post_actions(next_action_type);
CREATE INDEX IF NOT EXISTS idx_bpa_chain ON bridge_post_actions(next_action_chain_id);

-- Pipeline state tracking
INSERT INTO extraction_state (extraction_type, last_block_processed, status)
VALUES ('bridge_enrichment', 0, 'idle')
ON CONFLICT (extraction_type) DO NOTHING;
