#!/usr/bin/env python3
"""
RESILIENT SYNC — Self-healing pipeline that catches up and stays synced.

This replaces all the ad-hoc scripts (catchup_and_rebuild.sh, cron_sync, etc.)
with a single, robust, forever-running process.

Features:
  - Catches up from state.json to chain tip using REST v2 API
  - On batch failure: retries individual blocks before DLQ-ing
  - Periodic DLQ replay to recover previously failed batches
  - Auto-reconnect on DB/network failures
  - Heartbeat file for external monitoring
  - Graceful shutdown on SIGINT/SIGTERM
  - Continuous mode: sleeps and checks for new blocks

Usage:
  cd ~/Incentiveanti/Incentivtestnetdata
  python3 -u scripts/resilient_sync.py                    # catch up + continuous
  python3 -u scripts/resilient_sync.py --catch-up-only    # just close the gap
  python3 -u scripts/resilient_sync.py --replay-dlq       # just replay DLQ
  python3 -u scripts/resilient_sync.py --batch-size 25    # smaller batches
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv('.env.neon')

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pandera")

from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor
from src.extractors.full_chain import FullChainExtractor
from src.handlers.dlq import DeadLetterQueue
from src.loaders.neon import NeonLoader
from src.transformers.blocks import normalize_blocks
from src.transformers.transactions import normalize_transactions
from src.transformers.raw_logs import normalize_raw_logs
from src.transformers.decoded_logs import decode_logs


# ── Config ───────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT_DIR / "state.json"
HEARTBEAT_FILE = PROJECT_DIR / "data" / "sync_heartbeat.json"
DLQ_REPLAY_INTERVAL = 100  # Replay DLQ every N batches
CONTINUOUS_SLEEP = 30       # Seconds between checks in continuous mode
CRASH_COOLDOWN = 60         # Seconds to wait after a crash
MAX_SINGLE_BLOCK_RETRIES = 3  # Retries for individual block fallback


class GracefulShutdown:
    """Handle SIGINT/SIGTERM for clean exit."""
    def __init__(self):
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handler)
        signal.signal(signal.SIGTERM, self._handler)

    def _handler(self, signum, frame):
        print(f"\n[SHUTDOWN] Signal {signum} received. Finishing current batch...")
        self.should_stop = True


def load_state() -> Dict:
    """
    Load state. Neon DB is the source of truth; state.json is a fallback
    used only if Neon hasn't been initialized yet.
    """
    state_from_neon = _load_state_from_neon()
    state_from_file = {}
    if STATE_FILE.exists():
        try:
            state_from_file = json.loads(STATE_FILE.read_text())
        except Exception:
            pass

    # Prefer the higher block number — DB is canonical but file may be ahead
    # if a previous run wrote the file but failed to update DB
    neon_block = state_from_neon.get("last_all_activity_block", 0)
    file_block = state_from_file.get("last_all_activity_block", 0)

    if neon_block >= file_block:
        return state_from_neon if state_from_neon else state_from_file
    return state_from_file


def _load_state_from_neon() -> Dict:
    """Load all_activity extraction state from Neon DB."""
    try:
        from src.loaders.neon import NeonLoader
        neon = NeonLoader()
        try:
            row = neon.get_extraction_state("all_activity")
            if row and row.get("last_block_processed") is not None:
                return {"last_all_activity_block": int(row["last_block_processed"])}
        finally:
            try:
                neon.close()
            except Exception:
                pass
    except Exception:
        pass
    return {}


def save_state(state: Dict) -> None:
    STATE_FILE.write_text(json.dumps(state))


def write_heartbeat(block: int, status: str, extra: Optional[Dict] = None) -> None:
    """Write heartbeat for external monitoring."""
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    hb = {
        "last_block": block,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
    }
    if extra:
        hb.update(extra)
    try:
        HEARTBEAT_FILE.write_text(json.dumps(hb, indent=2))
    except Exception:
        pass


def _load_decoded_to_neon(neon, decoded_df, table_key: str) -> None:
    """Convert a decoded DataFrame to decoded_events format and load to Neon."""
    import pandas as pd

    base_cols = ["block_number", "block_timestamp", "tx_hash", "log_index",
                 "address", "event_name", "chain", "extracted_at"]

    rows = []
    for _, row in decoded_df.iterrows():
        params = {}
        for col in decoded_df.columns:
            if col not in base_cols:
                val = row.get(col)
                if pd.notna(val) and val is not None and val != "":
                    params[col] = str(val)

        rows.append({
            "event_name": row.get("event_name", "Unknown"),
            "contract_address": row.get("address", ""),
            "block_number": int(row.get("block_number", 0)),
            "transaction_hash": row.get("tx_hash", ""),
            "log_index": int(row.get("log_index", 0)),
            "params": json.dumps(params) if params else None,
            "timestamp": row.get("block_timestamp"),
            "chain": row.get("chain", "incentiv"),
        })

    if rows:
        import pandas as pd
        df = pd.DataFrame(rows)
        neon.copy_dataframe("decoded_events", df)


def process_batch(
    extractor: BlockscoutExtractor,
    full_extractor: FullChainExtractor,
    neon: NeonLoader,
    dlq: DeadLetterQueue,
    start: int,
    end: int,
    chain: str = "incentiv",
) -> bool:
    """
    Process a single batch. Returns True on success.
    On failure, tries individual blocks before DLQ-ing.
    """
    try:
        result = full_extractor.extract_full_batch(start, end)
        _load_result_to_neon(neon, result, chain)
        return True

    except Exception as batch_exc:
        print(f"    Batch {start}-{end} failed: {batch_exc}")
        print(f"    Falling back to individual blocks...")

        # Try each block individually
        success_count = 0
        fail_count = 0

        for block_num in range(start, end + 1):
            try:
                result = full_extractor.extract_full_batch(block_num, block_num)
                _load_result_to_neon(neon, result, chain)
                success_count += 1
            except Exception as block_exc:
                fail_count += 1
                dlq.send(
                    record={"batch": f"{block_num}-{block_num}", "mode": "all_activity"},
                    error=block_exc,
                    context={"from_block": block_num, "to_block": block_num},
                )

        if fail_count > 0:
            print(f"    Individual blocks: {success_count} ok, {fail_count} DLQ'd")
        else:
            print(f"    Individual blocks: all {success_count} recovered!")

        return True  # We processed what we could; state should advance


def _load_result_to_neon(neon: NeonLoader, result: Dict, chain: str) -> None:
    """Load extracted data into Neon PostgreSQL."""
    blocks = result.get("blocks", [])
    transactions = result.get("transactions", [])
    logs = result.get("logs", [])

    if blocks:
        df_blocks = normalize_blocks(blocks, chain=chain)
        neon.copy_dataframe("blocks", df_blocks)

    if transactions:
        df_txs = normalize_transactions(blocks, chain=chain)
        neon.copy_dataframe("transactions", df_txs)

    if logs:
        df_raw = normalize_raw_logs(logs, chain=chain)
        neon.copy_dataframe("raw_logs", df_raw)

        # Decode known events
        try:
            decoded_tables = decode_logs(
                logs=logs,
                chain=chain,
                abi_dir=Path("config/abis"),
                include_unknown=True,
            )
            for table_key, decoded_df in decoded_tables.items():
                if not decoded_df.empty:
                    _load_decoded_to_neon(neon, decoded_df, table_key)
        except Exception as e:
            print(f"    Decode warning (non-fatal): {e}")

    # Discover contracts
    if logs:
        try:
            from src.extractors.full_chain import FullChainExtractor
            contracts = FullChainExtractor._discover_contracts_from_logs(logs)
            if contracts:
                neon.upsert_contracts(list(contracts.values()))
        except Exception:
            pass


# Standalone contract discovery (doesn't need extractor instance)
def _discover_contracts_from_logs(logs):
    contracts = {}
    for log in logs:
        address = log.get("address", "").lower()
        if not address:
            continue
        block_num = int(log["blockNumber"], 16)
        if address not in contracts:
            contracts[address] = {
                "address": address,
                "first_seen_block": block_num,
                "last_activity_block": block_num,
                "event_count": 1,
            }
        else:
            contracts[address]["last_activity_block"] = max(
                contracts[address]["last_activity_block"], block_num
            )
            contracts[address]["event_count"] += 1
    return contracts

# Monkey-patch for use in _load_result_to_neon
from src.extractors.full_chain import FullChainExtractor
FullChainExtractor._discover_contracts_from_logs = staticmethod(_discover_contracts_from_logs)


def run_catch_up(
    extractor: BlockscoutExtractor,
    full_extractor: FullChainExtractor,
    neon: NeonLoader,
    dlq: DeadLetterQueue,
    shutdown: GracefulShutdown,
    batch_size: int = 50,
    chain: str = "incentiv",
) -> int:
    """
    Catch up from current state to chain tip.
    Returns the last block processed.
    """
    state = load_state()
    last_block = state.get("last_all_activity_block", 0)
    safe_block = extractor.get_safe_block_number()

    if last_block >= safe_block:
        print(f"Already caught up! (block {last_block} >= chain tip {safe_block})")
        return last_block

    total_gap = safe_block - last_block
    start_block = last_block + 1
    print(f"\n{'='*60}")
    print(f"  CATCH-UP: {start_block:,} → {safe_block:,} ({total_gap:,} blocks)")
    print(f"  Batch size: {batch_size}")
    print(f"{'='*60}\n")

    total_blocks = 0
    total_txs = 0
    total_logs = 0
    batches_done = 0
    start_time = time.time()

    for batch_start in range(start_block, safe_block + 1, batch_size):
        if shutdown.should_stop:
            print("[SHUTDOWN] Stopping catch-up gracefully.")
            break

        batch_end = min(batch_start + batch_size - 1, safe_block)

        try:
            print(f"  Batch {batch_start:,}-{batch_end:,}...", end=" ", flush=True)

            result = full_extractor.extract_full_batch(batch_start, batch_end)
            b = len(result.get("blocks", []))
            t = len(result.get("transactions", []))
            l = len(result.get("logs", []))

            _load_result_to_neon(neon, result, chain)

            total_blocks += b
            total_txs += t
            total_logs += l
            batches_done += 1

            # Update state
            state["last_all_activity_block"] = batch_end
            save_state(state)

            # Update Neon state
            try:
                neon.update_extraction_state(
                    "all_activity", batch_end,
                    total_items=b + t + l,
                    status="running",
                )
            except Exception:
                pass

            elapsed = time.time() - start_time
            rate = total_blocks / max(1, elapsed)
            remaining = safe_block - batch_end
            eta_min = remaining / max(0.01, rate) / 60

            print(f"{b}b {t}tx {l}logs ({result['elapsed_seconds']:.1f}s) | "
                  f"Total: {total_blocks:,}b {total_txs:,}tx {total_logs:,}logs | "
                  f"{rate:.1f} blk/s | ETA: {eta_min:.0f}m",
                  flush=True)

            write_heartbeat(batch_end, "catching_up", {
                "total_blocks": total_blocks,
                "total_txs": total_txs,
                "rate_bps": round(rate, 1),
                "eta_minutes": round(eta_min),
                "gap_remaining": remaining,
            })

            # Periodic DLQ replay (skip during initial catch-up to avoid connection issues)
            if batches_done % DLQ_REPLAY_INTERVAL == 0 and dlq.count() > 0 and remaining < 5000:
                print(f"\n  [DLQ] Replaying {dlq.count()} failed entries (gap small enough)...")
                try:
                    replay_dlq(extractor, full_extractor, neon, dlq, chain, max_entries=20)
                except Exception as dlq_exc:
                    print(f"    DLQ replay error (non-fatal): {dlq_exc}")
                print()

        except Exception as exc:
            print(f"\n    BATCH FAILED: {exc}")

            # Try individual blocks
            success = process_batch(
                extractor, full_extractor, neon, dlq,
                batch_start, batch_end, chain
            )

            # Always advance state to avoid infinite loops
            state["last_all_activity_block"] = batch_end
            save_state(state)

            try:
                neon.update_extraction_state(
                    "all_activity", batch_end, status="running"
                )
            except Exception:
                pass

            # Check if we need to reconnect
            try:
                neon.reconnect()
            except Exception:
                print("    DB reconnect failed, waiting...")
                time.sleep(CRASH_COOLDOWN)
                try:
                    neon.reconnect()
                except Exception:
                    pass

    print(f"\nCatch-up complete: {total_blocks:,} blocks, {total_txs:,} txs, "
          f"{total_logs:,} logs in {time.time()-start_time:.0f}s")

    return state.get("last_all_activity_block", last_block)


def replay_dlq(
    extractor: BlockscoutExtractor,
    full_extractor: FullChainExtractor,
    neon: NeonLoader,
    dlq: DeadLetterQueue,
    chain: str = "incentiv",
    max_entries: int = 0,
) -> Dict[str, int]:
    """Replay DLQ entries using the new REST v2 extraction."""
    def process_fn(from_block: int, to_block: int) -> bool:
        try:
            result = full_extractor.extract_full_batch(from_block, to_block)
            _load_result_to_neon(neon, result, chain)
            return True
        except Exception as e:
            print(f"    DLQ replay failed for {from_block}-{to_block}: {e}")
            return False

    return dlq.replay(process_fn, max_entries=max_entries)


def run_continuous(
    extractor: BlockscoutExtractor,
    full_extractor: FullChainExtractor,
    neon: NeonLoader,
    dlq: DeadLetterQueue,
    shutdown: GracefulShutdown,
    batch_size: int = 50,
    chain: str = "incentiv",
) -> None:
    """
    Continuous sync loop: check for new blocks, process them, sleep.
    """
    print(f"\n{'='*60}")
    print(f"  CONTINUOUS SYNC (checking every {CONTINUOUS_SLEEP}s)")
    print(f"{'='*60}\n")

    while not shutdown.should_stop:
        try:
            state = load_state()
            last_block = state.get("last_all_activity_block", 0)
            safe_block = extractor.get_safe_block_number()

            if last_block < safe_block:
                gap = safe_block - last_block
                print(f"  New blocks: {gap:,} ({last_block+1:,} → {safe_block:,})")

                run_catch_up(
                    extractor, full_extractor, neon, dlq,
                    shutdown, batch_size, chain
                )
            else:
                write_heartbeat(last_block, "synced")

            # Sleep before next check
            for _ in range(CONTINUOUS_SLEEP):
                if shutdown.should_stop:
                    break
                time.sleep(1)

        except Exception as e:
            print(f"  [ERROR] Continuous sync error: {e}")
            write_heartbeat(0, "error", {"error": str(e)})
            print(f"  Waiting {CRASH_COOLDOWN}s before retry...")
            time.sleep(CRASH_COOLDOWN)

            # Try to reconnect
            try:
                neon.reconnect()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Resilient Incentiv Sync")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--catch-up-only", action="store_true",
                        help="Just close the gap, don't enter continuous mode")
    parser.add_argument("--replay-dlq", action="store_true",
                        help="Just replay DLQ entries")
    parser.add_argument("--dlq-max", type=int, default=0,
                        help="Max DLQ entries to replay (0 = all)")
    args = parser.parse_args()

    print("=" * 60)
    print("  RESILIENT SYNC — Incentiv Blockchain Data Pipeline")
    print(f"  PID: {os.getpid()}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    shutdown = GracefulShutdown()

    # Load chain config
    chains = load_yaml("config/chains.yaml")
    chain_config = chains["incentiv"]

    extractor = BlockscoutExtractor(
        base_url=chain_config["blockscout_base_url"],
        rpc_url=chain_config["blockscout_rpc_url"],
        confirmations=int(chain_config["confirmations"]),
        batch_size=args.batch_size,
        rate_limit_per_second=float(chain_config["rate_limit_per_second"]),
    )

    full_extractor = FullChainExtractor(extractor)
    neon = NeonLoader()
    dlq = DeadLetterQueue()

    # Print current state
    state = load_state()
    print(f"\n  State: last_all_activity_block = {state.get('last_all_activity_block', 0):,}")
    print(f"  DLQ: {dlq.count():,} entries")

    try:
        safe_block = extractor.get_safe_block_number()
        print(f"  Chain tip: ~{safe_block:,}")
        gap = safe_block - state.get("last_all_activity_block", 0)
        print(f"  Gap: {gap:,} blocks")
    except Exception as e:
        print(f"  Could not fetch chain tip: {e}")

    # Print Neon table counts
    try:
        counts = neon.get_table_counts()
        print(f"\n  Neon DB:")
        for table, count in counts.items():
            print(f"    {table}: {count:,}")
    except Exception as e:
        print(f"  Could not fetch Neon counts: {e}")

    print()

    # ── Mode: DLQ Replay ────────────────────────────────────────
    if args.replay_dlq:
        print("=" * 60)
        print("  DLQ REPLAY MODE")
        print("=" * 60)
        stats = replay_dlq(
            extractor, full_extractor, neon, dlq,
            max_entries=args.dlq_max,
        )
        print(f"\nDLQ Replay: {stats}")
        neon.close()
        return

    # ── Mode: Catch-up ──────────────────────────────────────────
    run_catch_up(
        extractor, full_extractor, neon, dlq,
        shutdown, args.batch_size,
    )

    if shutdown.should_stop:
        print("\n[SHUTDOWN] Clean exit.")
        write_heartbeat(state.get("last_all_activity_block", 0), "stopped")
        neon.close()
        return

    # ── Mode: Continuous ────────────────────────────────────────
    if not args.catch_up_only:
        run_continuous(
            extractor, full_extractor, neon, dlq,
            shutdown, args.batch_size,
        )

    print("\n[SHUTDOWN] Clean exit.")
    write_heartbeat(state.get("last_all_activity_block", 0), "stopped")
    neon.close()


if __name__ == "__main__":
    main()
