#!/usr/bin/env python3
"""
Enriched Bridge Analytics Pipeline

Extracts bridge wallet addresses from decoded_events,
queries Dune Analytics for their cross-chain protocol interactions,
and stores the results in Neon DB.

Usage:
  python3 -u scripts/enrich_bridge_wallets.py
  python3 -u scripts/enrich_bridge_wallets.py --dry-run
  python3 -u scripts/enrich_bridge_wallets.py --step wallets  # Extract wallets only
"""

import argparse
import os
import sys
import time
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv(Path(__file__).resolve().parent.parent / '.env.neon')

from src.loaders.neon import NeonLoader

# ============================================================
# Configuration
# ============================================================

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
DUNE_BASE_URL = "https://api.dune.com/api/v1"
NEON_DATABASE_URL = os.environ.get("NEON_DATABASE_URL", "")

# Chain ID -> Dune blockchain name mapping
CHAIN_ID_TO_DUNE = {
    1: "ethereum",
    10: "optimism",
    137: "polygon",
    8453: "base",
    42161: "arbitrum",
    56: "bnb",
}

CHAIN_ID_TO_NAME = {
    1: "Ethereum",
    10: "Optimism",
    137: "Polygon",
    8453: "Base",
    42161: "Arbitrum",
    56: "BSC",
    1399811149: "Solana",
}

WALLET_BATCH_SIZE = 50
DUNE_POLL_INTERVAL = 5  # seconds between status checks
DUNE_MAX_WAIT = 300     # max seconds to wait for query


# ============================================================
# Dune API helpers
# ============================================================

def dune_headers():
    return {"X-Dune-Api-Key": DUNE_API_KEY}


def dune_execute_query(query_sql: str, params: Optional[Dict] = None) -> Optional[List[Dict]]:
    """Execute an inline SQL query on Dune and return results."""
    payload = {
        "query_parameters": params or {},
        "sql": query_sql,
        "performance": "medium",
    }

    # Execute query via SQL endpoint
    r = requests.post(
        f"{DUNE_BASE_URL}/sql/execute",
        headers={**dune_headers(), "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )

    if r.status_code != 200:
        print(f"  [WARN] Dune execute failed ({r.status_code}): {r.text[:200]}")
        return None

    execution_id = r.json().get("execution_id")
    if not execution_id:
        print("  [WARN] No execution_id returned")
        return None

    # Poll for results
    start = time.time()
    while time.time() - start < DUNE_MAX_WAIT:
        time.sleep(DUNE_POLL_INTERVAL)
        status_r = requests.get(
            f"{DUNE_BASE_URL}/execution/{execution_id}/status",
            headers=dune_headers(),
            timeout=30,
        )
        status_data = status_r.json()
        state = status_data.get("state", "")
        if state == "QUERY_STATE_COMPLETED":
            break
        elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            error_msg = status_data.get("error", {}).get("message", "unknown error")
            print(f"  [WARN] Dune query {state}: {error_msg[:150]}")
            return None

    # Fetch results
    results_r = requests.get(
        f"{DUNE_BASE_URL}/execution/{execution_id}/results",
        headers=dune_headers(),
        timeout=30,
    )
    if results_r.status_code != 200:
        print(f"  [WARN] Dune results fetch failed ({results_r.status_code})")
        return None

    return results_r.json().get("result", {}).get("rows", [])


# ============================================================
# Step 1: Extract bridge wallets from Neon
# ============================================================

def extract_bridge_wallets(neon: NeonLoader) -> Tuple[List[Dict], List[Dict]]:
    """Extract unique bridge wallets from decoded_events (past 180 days)."""
    print("\n  Step 1: Extracting bridge wallets from decoded_events...")

    # Inbound wallets (bridged INTO Incentiv)
    inbound_df = neon.query_df("""
        SELECT DISTINCT
            LOWER(t.from_address) as wallet_address,
            (params->>'origin')::int as source_chain_id,
            'inbound' as bridge_direction
        FROM decoded_events de
        JOIN transactions t ON t.hash = de.transaction_hash
        WHERE de.event_name = 'ReceivedTransferRemote'
          AND de."timestamp" > NOW() - INTERVAL '180 days'
          AND t.from_address IS NOT NULL
    """)

    # Outbound wallets (bridged OUT of Incentiv)
    outbound_df = neon.query_df("""
        SELECT DISTINCT
            LOWER(t.from_address) as wallet_address,
            (params->>'destination')::int as dest_chain_id,
            de.transaction_hash,
            de."timestamp" as bridge_timestamp,
            'outbound' as bridge_direction
        FROM decoded_events de
        JOIN transactions t ON t.hash = de.transaction_hash
        WHERE de.event_name = 'SentTransferRemote'
          AND de."timestamp" > NOW() - INTERVAL '180 days'
          AND t.from_address IS NOT NULL
    """)

    inbound_wallets = inbound_df.to_dict('records') if not inbound_df.empty else []
    outbound_wallets = outbound_df.to_dict('records') if not outbound_df.empty else []

    # Unique wallet counts
    unique_inbound = set(w['wallet_address'] for w in inbound_wallets)
    unique_outbound = set(w['wallet_address'] for w in outbound_wallets)

    print(f"    Inbound wallets: {len(unique_inbound):,} unique ({len(inbound_wallets):,} records)")
    print(f"    Outbound wallets: {len(unique_outbound):,} unique ({len(outbound_wallets):,} records)")
    print(f"    Total unique wallets: {len(unique_inbound | unique_outbound):,}")

    return inbound_wallets, outbound_wallets


# ============================================================
# Step 2: Query Dune for protocol interactions
# ============================================================

def query_protocol_interactions(wallets: List[str], chain_dune_name: str, chain_id: int, direction: str) -> pd.DataFrame:
    """Query Dune for protocol interactions of wallets on a specific chain."""
    if not wallets or not DUNE_API_KEY:
        return pd.DataFrame()

    wallet_list = ",".join(f"{w}" for w in wallets[:WALLET_BATCH_SIZE])

    sql = f"""
    SELECT
        CAST(t."from" AS VARCHAR) as wallet_address,
        COALESCE(c.namespace, 'unknown') as protocol_name,
        COUNT(*) as tx_count,
        MAX(t.block_time) as last_active
    FROM {chain_dune_name}.transactions t
    LEFT JOIN {chain_dune_name}.contracts c ON t."to" = c.address
    WHERE t."from" IN ({wallet_list})
        AND t.block_time > NOW() - INTERVAL '180' DAY
    GROUP BY t."from", c.namespace
    HAVING COUNT(*) >= 1
    ORDER BY tx_count DESC
    """

    rows = dune_execute_query(sql)
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df['chain_id'] = chain_id
    df['chain_name'] = CHAIN_ID_TO_NAME.get(chain_id, f'Chain {chain_id}')
    df['bridge_direction'] = direction
    df['volume_usd'] = 0  # Placeholder — can be enriched later
    df['updated_at'] = datetime.now(timezone.utc)

    return df


def enrich_protocol_data(inbound_wallets: List[Dict], outbound_wallets: List[Dict]) -> pd.DataFrame:
    """Query Dune for protocol interactions across all relevant chains."""
    print("\n  Step 2: Querying Dune for protocol interactions...")

    if not DUNE_API_KEY:
        print("    [SKIP] DUNE_API_KEY not set. Skipping Dune queries.")
        return pd.DataFrame()

    all_dfs = []

    # Group wallets by chain
    inbound_by_chain: Dict[int, List[str]] = {}
    for w in inbound_wallets:
        chain_id = w.get('source_chain_id')
        if chain_id and chain_id in CHAIN_ID_TO_DUNE:
            inbound_by_chain.setdefault(chain_id, []).append(w['wallet_address'])

    outbound_by_chain: Dict[int, List[str]] = {}
    for w in outbound_wallets:
        chain_id = w.get('dest_chain_id')
        if chain_id and chain_id in CHAIN_ID_TO_DUNE:
            outbound_by_chain.setdefault(chain_id, []).append(w['wallet_address'])

    # Query each chain
    for chain_id, wallets in {**inbound_by_chain}.items():
        unique_wallets = list(set(wallets))
        dune_chain = CHAIN_ID_TO_DUNE[chain_id]
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, '')

        for i in range(0, len(unique_wallets), WALLET_BATCH_SIZE):
            batch = unique_wallets[i:i + WALLET_BATCH_SIZE]
            print(f"    Querying {chain_name} inbound batch {i//WALLET_BATCH_SIZE + 1} ({len(batch)} wallets)...")

            df = query_protocol_interactions(batch, dune_chain, chain_id, 'inbound')
            if not df.empty:
                all_dfs.append(df)
                print(f"      Got {len(df):,} protocol interactions")

            time.sleep(2)  # Rate limit

    for chain_id, wallets in {**outbound_by_chain}.items():
        unique_wallets = list(set(wallets))
        dune_chain = CHAIN_ID_TO_DUNE[chain_id]
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, '')

        for i in range(0, len(unique_wallets), WALLET_BATCH_SIZE):
            batch = unique_wallets[i:i + WALLET_BATCH_SIZE]
            print(f"    Querying {chain_name} outbound batch {i//WALLET_BATCH_SIZE + 1} ({len(batch)} wallets)...")

            df = query_protocol_interactions(batch, dune_chain, chain_id, 'outbound')
            if not df.empty:
                all_dfs.append(df)
                print(f"      Got {len(df):,} protocol interactions")

            time.sleep(2)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


# ============================================================
# Step 3: Query Dune for multi-chain presence
# ============================================================

def enrich_chain_presence(all_wallets: List[str]) -> pd.DataFrame:
    """Query Dune for which chains each wallet is active on."""
    print("\n  Step 3: Querying Dune for multi-chain wallet presence...")

    if not DUNE_API_KEY or not all_wallets:
        print("    [SKIP] No API key or no wallets.")
        return pd.DataFrame()

    all_dfs = []
    unique_wallets = list(set(all_wallets))

    for i in range(0, len(unique_wallets), WALLET_BATCH_SIZE):
        batch = unique_wallets[i:i + WALLET_BATCH_SIZE]
        wallet_list = ",".join(f"{w}" for w in batch)

        # Build UNION query across all chains
        chain_queries = []
        for chain_id, dune_name in CHAIN_ID_TO_DUNE.items():
            chain_queries.append(f"""
                SELECT CAST("from" AS VARCHAR) as wallet_address, '{dune_name}' as chain_name, {chain_id} as chain_id,
                       COUNT(*) as tx_count, MIN(block_time) as first_seen, MAX(block_time) as last_seen
                FROM {dune_name}.transactions
                WHERE "from" IN ({wallet_list})
                  AND block_time > NOW() - INTERVAL '180' DAY
                GROUP BY "from"
            """)

        sql = " UNION ALL ".join(chain_queries)

        print(f"    Batch {i//WALLET_BATCH_SIZE + 1} ({len(batch)} wallets)...")
        rows = dune_execute_query(sql)
        if rows:
            df = pd.DataFrame(rows)
            df['volume_usd'] = 0
            df['updated_at'] = datetime.now(timezone.utc)
            all_dfs.append(df)
            print(f"      Got {len(df):,} chain-wallet records")

        time.sleep(2)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


# ============================================================
# Step 4: Query Dune for post-bridge actions
# ============================================================

def enrich_post_bridge_actions(outbound_wallets: List[Dict]) -> pd.DataFrame:
    """Find the next on-chain action after each bridge-out transaction."""
    print("\n  Step 4: Querying Dune for post-bridge actions...")

    if not DUNE_API_KEY or not outbound_wallets:
        print("    [SKIP] No API key or no outbound wallets.")
        return pd.DataFrame()

    all_dfs = []

    # Group by destination chain
    by_chain: Dict[int, List[Dict]] = {}
    for w in outbound_wallets:
        chain_id = w.get('dest_chain_id')
        if chain_id and chain_id in CHAIN_ID_TO_DUNE:
            by_chain.setdefault(chain_id, []).append(w)

    for chain_id, wallets in by_chain.items():
        dune_chain = CHAIN_ID_TO_DUNE[chain_id]
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, f'Chain {chain_id}')

        # Process in batches
        unique_wallets = list({w['wallet_address'] for w in wallets})

        for i in range(0, len(unique_wallets), WALLET_BATCH_SIZE):
            batch = unique_wallets[i:i + WALLET_BATCH_SIZE]
            wallet_list = ",".join(f"{w}" for w in batch)

            sql = f"""
            WITH next_txs AS (
                SELECT
                    CAST(t."from" AS VARCHAR) as wallet_address,
                    CAST(t.hash AS VARCHAR) as next_action_tx_hash,
                    t.block_time as next_action_timestamp,
                    COALESCE(c.namespace, 'transfer') as protocol,
                    CASE
                        WHEN c.namespace IN ('uniswap_v3','uniswap_v2','sushiswap','curve','pancakeswap',
                                          'trader_joe','balancer','aerodrome','velodrome') THEN 'swap'
                        WHEN c.namespace IN ('aave_v3','aave_v2','compound_v3','morpho','spark') THEN 'deposit'
                        WHEN c.namespace IN ('lido','rocket_pool','eigenlayer') THEN 'stake'
                        WHEN c.namespace IS NULL THEN 'transfer'
                        ELSE 'contract_interaction'
                    END as action_type,
                    ROW_NUMBER() OVER (PARTITION BY t."from" ORDER BY t.block_time ASC) as rn
                FROM {dune_chain}.transactions t
                LEFT JOIN {dune_chain}.contracts c ON t."to" = c.address
                WHERE t."from" IN ({wallet_list})
                    AND t.block_time > NOW() - INTERVAL '180' DAY
            )
            SELECT wallet_address, next_action_tx_hash, next_action_timestamp,
                   protocol, action_type
            FROM next_txs
            WHERE rn = 1
            """

            print(f"    {chain_name} batch {i//WALLET_BATCH_SIZE + 1} ({len(batch)} wallets)...")
            rows = dune_execute_query(sql)
            if rows:
                df = pd.DataFrame(rows)
                df['next_action_chain_id'] = chain_id
                df['next_action_chain_name'] = chain_name
                df['updated_at'] = datetime.now(timezone.utc)
                all_dfs.append(df)
                print(f"      Got {len(df):,} post-bridge actions")

            time.sleep(2)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()


# ============================================================
# Step 5: Store results in Neon
# ============================================================

def store_protocols(neon: NeonLoader, df: pd.DataFrame):
    """Upsert protocol interaction data into bridge_wallet_protocols."""
    if df.empty:
        print("    No protocol data to store.")
        return

    required_cols = ['wallet_address', 'protocol_name', 'chain_id', 'chain_name',
                     'tx_count', 'bridge_direction']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    if 'volume_usd' not in df.columns:
        df['volume_usd'] = 0
    if 'last_active' not in df.columns:
        df['last_active'] = None
    if 'updated_at' not in df.columns:
        df['updated_at'] = datetime.now(timezone.utc)

    # Select and rename columns for the table
    store_df = df[['wallet_address', 'chain_id', 'chain_name', 'protocol_name',
                   'tx_count', 'volume_usd', 'last_active', 'bridge_direction', 'updated_at']].copy()

    neon.copy_dataframe('bridge_wallet_protocols', store_df)
    neon.conn.commit()
    print(f"    Stored {len(store_df):,} protocol interaction records")


def store_chains(neon: NeonLoader, df: pd.DataFrame):
    """Upsert chain presence data into bridge_wallet_chains."""
    if df.empty:
        print("    No chain data to store.")
        return

    required_cols = ['wallet_address', 'chain_id', 'chain_name', 'tx_count',
                     'volume_usd', 'first_seen', 'last_seen']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    if 'updated_at' not in df.columns:
        df['updated_at'] = datetime.now(timezone.utc)

    store_df = df[['wallet_address', 'chain_id', 'chain_name', 'tx_count',
                   'volume_usd', 'first_seen', 'last_seen', 'updated_at']].copy()

    neon.copy_dataframe('bridge_wallet_chains', store_df)
    neon.conn.commit()
    print(f"    Stored {len(store_df):,} chain presence records")


def store_post_actions(neon: NeonLoader, df: pd.DataFrame, outbound_wallets: List[Dict]):
    """Upsert post-bridge action data into bridge_post_actions."""
    if df.empty:
        print("    No post-bridge action data to store.")
        return

    # Match with outbound wallet bridge tx hashes
    outbound_map = {}
    for w in outbound_wallets:
        addr = w['wallet_address']
        if addr not in outbound_map:
            outbound_map[addr] = w

    records = []
    for _, row in df.iterrows():
        wallet = row.get('wallet_address', '')
        bridge_info = outbound_map.get(wallet, {})
        records.append({
            'wallet_address': wallet,
            'bridge_out_tx_hash': bridge_info.get('transaction_hash', ''),
            'bridge_out_timestamp': bridge_info.get('bridge_timestamp'),
            'bridge_out_chain_id': bridge_info.get('dest_chain_id'),
            'bridge_out_chain_name': CHAIN_ID_TO_NAME.get(bridge_info.get('dest_chain_id', 0), ''),
            'next_action_tx_hash': row.get('next_action_tx_hash', ''),
            'next_action_type': row.get('action_type', ''),
            'next_action_protocol': row.get('protocol', ''),
            'next_action_chain_id': row.get('next_action_chain_id'),
            'next_action_chain_name': row.get('next_action_chain_name', ''),
            'next_action_timestamp': row.get('next_action_timestamp'),
            'time_to_next_action_seconds': None,  # TODO: calculate
            'amount_usd': None,
            'updated_at': datetime.now(timezone.utc),
        })

    store_df = pd.DataFrame(records)
    neon.copy_dataframe('bridge_post_actions', store_df)
    neon.conn.commit()
    print(f"    Stored {len(store_df):,} post-bridge action records")


# ============================================================
# Main
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enriched Bridge Analytics Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Extract wallets only, don't query Dune")
    parser.add_argument("--step", choices=["wallets", "protocols", "chains", "actions", "all"],
                       default="all", help="Run a specific step only")
    args = parser.parse_args()

    print("=" * 60)
    print("  ENRICHED BRIDGE ANALYTICS PIPELINE")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"  Dune API: {'configured' if DUNE_API_KEY else 'NOT SET (will skip Dune queries)'}")
    print(f"  Mode: {'dry-run' if args.dry_run else 'full'}")
    print(f"  Step: {args.step}")
    print("=" * 60)

    neon = NeonLoader()

    try:
        # Step 1: Extract wallets
        inbound_wallets, outbound_wallets = extract_bridge_wallets(neon)

        if args.dry_run or args.step == "wallets":
            print("\n  [DRY RUN] Wallet extraction complete. Exiting.")
            neon.close()
            return

        all_wallet_addrs = list(set(
            [w['wallet_address'] for w in inbound_wallets] +
            [w['wallet_address'] for w in outbound_wallets]
        ))

        # Step 2: Protocol interactions
        if args.step in ("all", "protocols"):
            protocols_df = enrich_protocol_data(inbound_wallets, outbound_wallets)
            print(f"\n  Step 2 result: {len(protocols_df):,} protocol interaction records")
            store_protocols(neon, protocols_df)

        # Step 3: Multi-chain presence
        if args.step in ("all", "chains"):
            chains_df = enrich_chain_presence(all_wallet_addrs)
            print(f"\n  Step 3 result: {len(chains_df):,} chain presence records")
            store_chains(neon, chains_df)

        # Step 4: Post-bridge actions
        if args.step in ("all", "actions"):
            actions_df = enrich_post_bridge_actions(outbound_wallets)
            print(f"\n  Step 4 result: {len(actions_df):,} post-bridge action records")
            store_post_actions(neon, actions_df, outbound_wallets)

        # Update extraction state
        neon.update_extraction_state('bridge_enrichment', 0, 'completed')

        print(f"\n{'=' * 60}")
        print(f"  PIPELINE COMPLETE")
        print(f"  Finished: {datetime.now(timezone.utc).isoformat()}")
        print(f"{'=' * 60}")

    except Exception as e:
        print(f"\n  [ERROR] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        neon.close()


if __name__ == "__main__":
    main()
