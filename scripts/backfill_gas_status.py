#!/usr/bin/env python3
"""
BACKFILL gas_used & status for existing transactions.

Problem:
  The REST v2 pipeline was not extracting gas_used or status from
  Blockscout, so all existing rows have gas_used=NULL and status=NULL.
  The dashboard was falling back to the gas LIMIT and showing everything
  as FAIL.

Solution:
  Fetch tx details from Blockscout REST v2 API in batches and UPDATE
  the Neon DB with correct gas_used and status values.

Usage:
  python scripts/backfill_gas_status.py                  # backfill all NULL rows
  python scripts/backfill_gas_status.py --limit 10000    # backfill 10k rows
  python scripts/backfill_gas_status.py --dry-run        # preview without writing
"""
import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Tuple

try:
    import httpx
except ImportError:
    print("ERROR: httpx not installed. Run: pip install httpx")
    sys.exit(1)

try:
    import asyncpg
except ImportError:
    print("ERROR: asyncpg not installed. Run: pip install asyncpg")
    sys.exit(1)

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
dotenv_path = Path(__file__).resolve().parent.parent / ".env.neon"
load_dotenv(dotenv_path)
load_dotenv()

BASE_URL = "https://explorer.incentiv.io/api/v2"

# --- Tuning ---
CONCURRENCY = 5          # parallel API requests
BATCH_SIZE = 200         # tx hashes per DB fetch batch
DB_UPDATE_BATCH = 100    # rows per UPDATE executemany
MAX_RETRIES = 4
REQUEST_DELAY = 0.15     # seconds between requests inside semaphore
REPORT_EVERY = 500       # log progress every N txs
# ---------------

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://explorer.incentiv.io/",
}


async def fetch_tx(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    tx_hash: str,
) -> Dict[str, Any] | None:
    """Fetch a single transaction from Blockscout REST v2."""
    url = f"{BASE_URL}/transactions/{tx_hash}"
    for attempt in range(1, MAX_RETRIES + 1):
        async with sem:
            await asyncio.sleep(REQUEST_DELAY)
            try:
                resp = await client.get(url, headers=HEADERS, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    logger.warning(f"TX not found on explorer: {tx_hash}")
                    return None
                if resp.status_code in (429, 503):
                    wait = min(2 ** attempt, 30)
                    logger.warning(f"Rate limited ({resp.status_code}) on {tx_hash}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                logger.warning(f"HTTP {resp.status_code} for {tx_hash} (attempt {attempt})")
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError) as e:
                wait = min(2 ** attempt, 30)
                logger.warning(f"Network error for {tx_hash}: {e}, retry in {wait}s")
                await asyncio.sleep(wait)
    logger.error(f"Failed to fetch {tx_hash} after {MAX_RETRIES} attempts")
    return None


def parse_tx_fields(data: Dict[str, Any]) -> Tuple[int | None, str | None]:
    """Extract gas_used and status from Blockscout REST v2 response."""
    # gas_used
    gas_used = None
    raw_gas = data.get("gas_used")
    if raw_gas is not None:
        try:
            gas_used = int(raw_gas)
        except (ValueError, TypeError):
            pass

    # status: "ok" -> "1", "error" -> "0"
    status_str = data.get("status", "")
    if status_str == "ok":
        status = "1"
    elif status_str == "error":
        status = "0"
    else:
        # Null or unknown — treat as success (matches explorer behavior)
        status = "1"

    return gas_used, status


async def run(limit: int | None = None, dry_run: bool = False):
    db_url = os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        logger.error("NEON_DATABASE_URL not set. Check .env.neon")
        sys.exit(1)

    # asyncpg needs postgres:// not postgresql://
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgres://", 1)
    # Remove channel_binding if present
    if "channel_binding=require" in db_url:
        db_url = db_url.replace("?channel_binding=require", "").replace("&channel_binding=require", "")

    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5, ssl="require")

    # ---- Count rows that need backfill ----
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM transactions WHERE gas_used IS NULL OR status IS NULL"
        )
        total_null = row["cnt"]
        logger.info(f"Transactions needing backfill: {total_null:,}")

    if total_null == 0:
        logger.info("Nothing to backfill — all rows have gas_used and status.")
        await pool.close()
        return

    # ---- Fetch tx hashes in batches ----
    effective_limit = limit or total_null
    logger.info(f"Will backfill up to {effective_limit:,} transactions (dry_run={dry_run})")

    sem = asyncio.Semaphore(CONCURRENCY)
    updated = 0
    skipped = 0
    offset = 0
    t0 = time.time()

    db_retries = 0
    max_db_retries = 10

    async with httpx.AsyncClient(http2=False) as client:
        while offset < effective_limit:
            try:
                batch_size = min(BATCH_SIZE, effective_limit - offset)

                # Get a batch of tx hashes that need backfill
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT hash FROM transactions
                        WHERE gas_used IS NULL OR status IS NULL
                        ORDER BY block_number DESC
                        LIMIT $1 OFFSET $2
                        """,
                        batch_size,
                        0,  # Always offset=0 since we update as we go
                    )

                if not rows:
                    logger.info("No more rows to backfill.")
                    break

                hashes = [r["hash"] for r in rows]
                logger.info(f"Fetching batch of {len(hashes)} txs (progress: {updated:,}/{effective_limit:,})")

                # Fetch all tx details concurrently
                tasks = [fetch_tx(client, sem, h) for h in hashes]
                results = await asyncio.gather(*tasks)

                # Build update pairs
                updates: List[Tuple[str, int | None, str | None]] = []
                for tx_hash, data in zip(hashes, results):
                    if data is None:
                        skipped += 1
                        continue
                    gas_used, status = parse_tx_fields(data)
                    updates.append((tx_hash, gas_used, status))

                # Write to DB
                if updates and not dry_run:
                    async with pool.acquire() as conn:
                        await conn.executemany(
                            """
                            UPDATE transactions
                            SET gas_used = $2, status = $3
                            WHERE hash = $1
                            """,
                            updates,
                        )
                    updated += len(updates)
                elif updates and dry_run:
                    # Show a sample
                    for tx_hash, gas_used, status in updates[:3]:
                        logger.info(f"  [DRY RUN] {tx_hash}: gas_used={gas_used}, status={status}")
                    updated += len(updates)

                offset += len(hashes)
                db_retries = 0  # reset on success

                if updated % REPORT_EVERY < BATCH_SIZE:
                    elapsed = time.time() - t0
                    rate = updated / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {updated:,} updated, {skipped:,} skipped, "
                        f"{rate:.1f} tx/s, elapsed {elapsed:.0f}s"
                    )

            except (asyncpg.exceptions.ConnectionDoesNotExistError,
                    asyncpg.exceptions.InterfaceError,
                    asyncpg.exceptions.InternalClientError,
                    OSError) as e:
                db_retries += 1
                if db_retries > max_db_retries:
                    logger.error(f"DB connection failed {max_db_retries} times in a row, giving up.")
                    break
                wait = min(2 ** db_retries, 120)
                logger.warning(f"DB connection lost ({e}), reconnecting in {wait}s (retry {db_retries}/{max_db_retries})")
                await asyncio.sleep(wait)
                # Reset the pool to force fresh connections — retry pool creation too
                try:
                    await pool.close()
                except Exception:
                    pass
                for pool_attempt in range(1, 4):
                    try:
                        pool = await asyncpg.create_pool(db_url, min_size=2, max_size=5, ssl="require")
                        logger.info("Reconnected to DB, resuming...")
                        break
                    except Exception as pe:
                        pool_wait = min(2 ** (pool_attempt + db_retries), 120)
                        logger.warning(f"Pool creation failed ({pe}), retry in {pool_wait}s (attempt {pool_attempt}/3)")
                        await asyncio.sleep(pool_wait)
                else:
                    logger.error("Could not recreate DB pool after 3 attempts, giving up.")
                    break

    elapsed = time.time() - t0
    logger.info(
        f"Done! Updated {updated:,} transactions, skipped {skipped:,}. "
        f"Took {elapsed:.1f}s ({updated / elapsed:.1f} tx/s)"
    )

    # ---- Also clear stale api_cache entries so dashboard picks up new data ----
    if not dry_run:
        async with pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM api_cache WHERE cache_key LIKE 'activity_%'"
            )
            logger.info(f"Cleared dashboard activity cache: {deleted}")

    await pool.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backfill gas_used & status from Blockscout")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to backfill (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    args = parser.parse_args()

    asyncio.run(run(limit=args.limit, dry_run=args.dry_run))
