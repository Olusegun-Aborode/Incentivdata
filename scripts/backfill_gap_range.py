#!/usr/bin/env python3
"""
Backfill a specific block range using the same pipeline as resilient_sync.
Handles blocks, transactions, AND logs in one pass.

Usage:
  python3 -u scripts/backfill_gap_range.py --from-block 1964081 --to-block 2041706
  python3 -u scripts/backfill_gap_range.py --from-block 1964081 --to-block 2041706 --batch-size 25
"""

import argparse
import os
import sys
import time
import signal
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()
load_dotenv(Path(__file__).resolve().parent.parent / '.env.neon')

from pathlib import Path as _Path
from src.config import load_yaml
from src.extractors.blockscout import BlockscoutExtractor
from src.extractors.full_chain import FullChainExtractor
from src.loaders.neon import NeonLoader
from src.transformers.blocks import normalize_blocks
from src.transformers.transactions import normalize_transactions
from src.transformers.raw_logs import normalize_raw_logs
from src.transformers.decoded_logs import decode_logs

# Graceful shutdown
STOP = False
def handle_signal(sig, frame):
    global STOP
    STOP = True
    print("\n[SHUTDOWN] Stopping gracefully after current batch...")
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def main():
    parser = argparse.ArgumentParser(description="Backfill specific block range (blocks + txs + logs)")
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=25)
    args = parser.parse_args()

    print("=" * 60)
    print(f"  GAP BACKFILL: {args.from_block:,} → {args.to_block:,}")
    print(f"  Total: {args.to_block - args.from_block + 1:,} blocks")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    # Load config
    chains = load_yaml("config/chains.yaml")
    chain_config = chains["incentiv"]

    extractor = BlockscoutExtractor(
        base_url=chain_config["blockscout_base_url"],
        rpc_url=chain_config["blockscout_rpc_url"],
        confirmations=0,  # No confirmations needed for historical blocks
        batch_size=args.batch_size,
        rate_limit_per_second=float(chain_config["rate_limit_per_second"]),
    )

    full_extractor = FullChainExtractor(extractor)
    neon = NeonLoader()

    total_blocks = 0
    total_txs = 0
    total_logs = 0
    errors = 0
    start_time = time.time()

    from_block = args.from_block
    to_block = args.to_block

    for batch_start in range(from_block, to_block + 1, args.batch_size):
        if STOP:
            print("[SHUTDOWN] Stopping gracefully.")
            break

        batch_end = min(batch_start + args.batch_size - 1, to_block)

        try:
            print(f"  Batch {batch_start:,}-{batch_end:,}...", end=" ", flush=True)

            result = full_extractor.extract_full_batch(batch_start, batch_end)
            b = len(result.get("blocks", []))
            t = len(result.get("transactions", []))
            l = len(result.get("logs", []))

            # Load into Neon DB (same approach as resilient_sync)
            blocks_data = result.get("blocks", [])
            txs_data = result.get("transactions", [])
            logs_data = result.get("logs", [])

            if blocks_data:
                df_blocks = normalize_blocks(blocks_data, chain="incentiv")
                neon.copy_dataframe("blocks", df_blocks)
                neon.conn.commit()
            if txs_data:
                df_txs = normalize_transactions(blocks_data, chain="incentiv")
                neon.copy_dataframe("transactions", df_txs)
                neon.conn.commit()
            if logs_data:
                df_raw = normalize_raw_logs(logs_data, chain="incentiv")
                neon.copy_dataframe("raw_logs", df_raw)
                neon.conn.commit()
                try:
                    decoded_tables = decode_logs(
                        logs=logs_data, chain="incentiv",
                        abi_dir=_Path("config/abis"), include_unknown=True,
                    )
                    for table_key, decoded_df in decoded_tables.items():
                        if not decoded_df.empty:
                            neon.copy_dataframe(table_key, decoded_df)
                            neon.conn.commit()
                except Exception as e:
                    try:
                        neon.conn.rollback()
                    except Exception:
                        pass

            total_blocks += b
            total_txs += t
            total_logs += l

            elapsed = time.time() - start_time
            processed = batch_end - from_block + 1
            total_range = to_block - from_block + 1
            rate = processed / elapsed if elapsed > 0 else 0
            remaining = total_range - processed
            eta_m = (remaining / rate) / 60 if rate > 0 else 0

            print(f"{b}b {t}tx {l}logs ({elapsed:.0f}s) | "
                  f"Total: {total_blocks}b {total_txs}tx {total_logs}logs | "
                  f"{rate:.1f} blk/s | ETA: {eta_m:.0f}m")

        except Exception as e:
            errors += 1
            print(f"\n  ERROR at batch {batch_start:,}-{batch_end:,}: {e}")
            # Rollback to clear aborted transaction, reconnect if needed
            try:
                neon.conn.rollback()
            except Exception:
                try:
                    neon.reconnect()
                except Exception:
                    neon = NeonLoader()
            time.sleep(2)
            continue

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  COMPLETE!")
    print(f"  Blocks: {total_blocks:,} | Txs: {total_txs:,} | Logs: {total_logs:,}")
    print(f"  Errors: {errors:,}")
    print(f"  Elapsed: {elapsed:.0f}s ({elapsed/60:.1f}m)")
    print(f"{'=' * 60}")

    neon.close()


if __name__ == "__main__":
    main()
