import concurrent.futures
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from src.utils.http import HttpClient

# Maximum recursion depth for 413 splitting to prevent infinite loops
MAX_SPLIT_DEPTH = 12


class BlockscoutExtractor:
    def __init__(
        self,
        base_url: str,
        rpc_url: str,
        confirmations: int,
        batch_size: int,
        rate_limit_per_second: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.rpc_url = rpc_url.rstrip("/")
        self.confirmations = confirmations
        self.batch_size = batch_size
        self.rest_client = HttpClient(self.base_url, rate_limit_per_second=rate_limit_per_second)
        self.rpc_client = HttpClient(self.rpc_url, rate_limit_per_second=rate_limit_per_second)

        # REST v2 client — points at the base explorer URL for /api/v2/* endpoints
        # Uses the same base_url but with relaxed rate limiting since REST is more stable
        rest_v2_base = base_url.rstrip("/")
        if not rest_v2_base.endswith("/api/v2"):
            # base_url is like "https://explorer.incentiv.io/api/v2" or just the root
            if "/api/v2" not in rest_v2_base:
                rest_v2_base = rest_v2_base + "/api/v2"
        self.rest_v2 = HttpClient(
            rest_v2_base,
            rate_limit_per_second=rate_limit_per_second,
            max_retries=15,
            base_delay=1.0,
            max_delay=120.0,
            read_timeout=90.0,
        )

    # ══════════════════════════════════════════════════════════════
    # REST v2 API methods (reliable, rich data)
    # ══════════════════════════════════════════════════════════════

    def get_block_rest(self, block_number: int) -> Optional[Dict[str, Any]]:
        """
        Fetch a single block via REST v2 API.
        Returns rich block data with miner info, gas metrics, tx count etc.
        Converts to RPC-compatible format for downstream transformers.
        """
        try:
            data = self.rest_v2.get(f"/blocks/{block_number}")
            return self._rest_block_to_rpc(data)
        except Exception as e:
            if "404" in str(e):
                return None
            raise

    def get_blocks_rest(
        self, from_block: int, to_block: int
    ) -> Dict[int, Dict[str, Any]]:
        """
        Fetch multiple blocks via REST v2, parallelized.
        Returns {block_number: rpc_format_block}.
        """
        block_numbers = list(range(from_block, to_block + 1))
        results: Dict[int, Dict[str, Any]] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future_to_block = {
                executor.submit(self.get_block_rest, bn): bn
                for bn in block_numbers
            }
            for future in concurrent.futures.as_completed(future_to_block):
                bn = future_to_block[future]
                try:
                    block = future.result()
                    if block:
                        results[bn] = block
                except Exception as e:
                    print(f"  [REST] Failed to fetch block {bn}: {e}")

        return results

    def get_block_transactions_rest(
        self, block_number: int
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL transactions for a block via REST v2 API.
        Handles pagination automatically.
        Returns transactions in RPC-compatible format.
        """
        txs = []
        next_page = None

        while True:
            try:
                params = next_page if next_page else {}
                data = self.rest_v2.get(
                    f"/blocks/{block_number}/transactions", params=params
                )
                items = data.get("items", [])
                for item in items:
                    rpc_tx = self._rest_tx_to_rpc(item, block_number)
                    if rpc_tx:
                        txs.append(rpc_tx)

                next_params = data.get("next_page_params")
                if not next_params:
                    break
                next_page = next_params

            except Exception as e:
                if "404" in str(e):
                    break
                print(f"  [REST] Error fetching txs for block {block_number}: {e}")
                break

        return txs

    def get_transaction_logs_rest(
        self, tx_hash: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL logs for a transaction via REST v2 API.
        Returns logs in RPC-compatible format (hex blockNumber, topics, data, etc.)
        """
        logs = []
        next_page = None

        while True:
            try:
                params = next_page if next_page else {}
                data = self.rest_v2.get(
                    f"/transactions/{tx_hash}/logs", params=params
                )
                items = data.get("items", [])
                for item in items:
                    rpc_log = self._rest_log_to_rpc(item)
                    if rpc_log:
                        logs.append(rpc_log)

                next_params = data.get("next_page_params")
                if not next_params:
                    break
                next_page = next_params

            except Exception as e:
                if "404" in str(e):
                    break
                print(f"  [REST] Error fetching logs for tx {tx_hash}: {e}")
                break

        return logs

    def get_latest_block_rest(self) -> int:
        """Get the latest block number via REST v2 stats endpoint."""
        try:
            data = self.rest_v2.get("/main-page/blocks")
            if isinstance(data, list) and data:
                return data[0].get("height", 0)
            return 0
        except Exception:
            # Fallback to RPC
            return self.get_latest_block_number()

    # ── REST v2 → RPC format converters ──────────────────────────

    @staticmethod
    def _rest_block_to_rpc(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert REST v2 block response to RPC-compatible format."""
        height = data.get("height", 0)
        timestamp_str = data.get("timestamp", "")

        # Parse ISO timestamp to unix
        try:
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            unix_ts = int(dt.timestamp())
        except Exception:
            unix_ts = 0

        # Miner address
        miner = data.get("miner", {})
        miner_addr = miner.get("hash", "0x" + "0" * 40) if isinstance(miner, dict) else str(miner)

        return {
            "number": hex(height),
            "hash": data.get("hash", ""),
            "parentHash": data.get("parent_hash", ""),
            "nonce": data.get("nonce", "0x0"),
            "sha3Uncles": "0x" + "0" * 64,
            "logsBloom": None,
            "transactionsRoot": "0x" + "0" * 64,
            "stateRoot": "0x" + "0" * 64,
            "receiptsRoot": "0x" + "0" * 64,
            "miner": miner_addr.lower(),
            "difficulty": hex(int(data.get("difficulty", "0") or 0)),
            "totalDifficulty": hex(int(float(data.get("total_difficulty", "0") or 0))),
            "size": hex(int(data.get("size", 0) or 0)),
            "extraData": "0x",
            "gasLimit": hex(int(data.get("gas_limit", "0") or 0)),
            "gasUsed": hex(int(data.get("gas_used", "0") or 0)),
            "timestamp": hex(unix_ts),
            "transactions": [],  # Will be filled separately
            "_rest_data": data,   # Keep original for rich fields
        }

    @staticmethod
    def _rest_tx_to_rpc(tx: Dict[str, Any], block_number: int) -> Optional[Dict[str, Any]]:
        """Convert REST v2 transaction to RPC-compatible format."""
        tx_hash = tx.get("hash", "")
        if not tx_hash:
            return None

        # Parse from/to addresses (can be objects or strings)
        from_addr = ""
        if isinstance(tx.get("from"), dict):
            from_addr = tx["from"].get("hash", "")
        elif isinstance(tx.get("from"), str):
            from_addr = tx["from"]

        to_addr = ""
        if isinstance(tx.get("to"), dict):
            to_addr = tx["to"].get("hash", "")
        elif isinstance(tx.get("to"), str):
            to_addr = tx.get("to", "")

        # Parse timestamp
        block_ts_str = tx.get("timestamp", "")
        try:
            dt = datetime.fromisoformat(block_ts_str.replace("Z", "+00:00"))
            unix_ts = int(dt.timestamp())
        except Exception:
            unix_ts = 0

        # Parse status: REST uses "ok"/"error", RPC uses 0x1/0x0
        # Only treat explicit "error" as failure; "ok", null, or empty = success
        status_str = tx.get("status", "")
        status_hex = "0x0" if status_str == "error" else "0x1"

        return {
            "hash": tx_hash.lower(),
            "blockNumber": hex(tx.get("block_number", block_number)),
            "from": from_addr.lower() if from_addr else None,
            "to": to_addr.lower() if to_addr else None,
            "value": hex(int(tx.get("value", "0") or 0)),
            "gas": hex(int(tx.get("gas_limit", 0) or 0)),
            "gasUsed": hex(int(tx.get("gas_used", 0) or 0)),
            "gasPrice": hex(int(tx.get("gas_price", "0") or 0)),
            "nonce": hex(int(tx.get("nonce", 0) or 0)),
            "transactionIndex": hex(int(tx.get("position", 0) or 0)),
            "input": tx.get("raw_input", "0x"),
            "status": status_hex,
            "block_timestamp": hex(unix_ts),
            "_rest_data": tx,  # Keep original for rich fields
        }

    @staticmethod
    def _rest_log_to_rpc(log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert REST v2 log to RPC-compatible format."""
        # Extract topics from the log
        topics = log.get("topics", [])
        if not topics:
            # Try decoded info
            decoded = log.get("decoded", {})
            if decoded:
                # Build topic from method_id if available
                pass  # topics may genuinely be empty for some logs

        # Address can be an object or string
        address = ""
        if isinstance(log.get("address"), dict):
            address = log["address"].get("hash", "")
        elif isinstance(log.get("address"), str):
            address = log["address"]

        return {
            "blockNumber": hex(int(log.get("block_number", 0) or 0)),
            "transactionHash": (log.get("transaction_hash", "") or "").lower(),
            "logIndex": hex(int(log.get("index", 0) or 0)),
            "address": address.lower() if address else "",
            "topics": [t.lower() if isinstance(t, str) else t for t in topics],
            "data": log.get("data", "0x") or "0x",
            "blockHash": log.get("block_hash", ""),
            "_decoded": log.get("decoded"),  # Keep decoded info for bonus data
        }

    # ══════════════════════════════════════════════════════════════
    # Original RPC methods (kept for backward compatibility)
    # ══════════════════════════════════════════════════════════════

    def get_latest_block_number(self) -> int:
        payload = {"id": 1, "jsonrpc": "2.0", "method": "eth_blockNumber", "params": []}
        response = self.rpc_client.post("", payload)
        return int(response["result"], 16)

    def get_safe_block_number(self) -> int:
        try:
            latest = self.get_latest_block_rest()
            if latest > 0:
                return max(latest - self.confirmations, 0)
        except Exception:
            pass
        latest = self.get_latest_block_number()
        return max(latest - self.confirmations, 0)

    def get_logs(
        self,
        address: str,
        topics: List[Any],
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        address = address.lower()
        normalized_topics = []
        for t in topics:
            if isinstance(t, str):
                normalized_topics.append(t.lower())
            elif isinstance(t, list):
                normalized_topics.append([x.lower() for x in t])
            else:
                normalized_topics.append(t)
        topics = normalized_topics

        logs: List[Dict[str, Any]] = []
        for start in range(from_block, to_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, to_block)
            logs.extend(self._get_logs_recursive(address, topics, start, end))
        return logs

    def get_all_logs(
        self,
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        """
        Fetch ALL event logs across ALL contracts with no address or topic filtering.
        This captures the complete on-chain activity for the given block range.
        Uses the same recursive 413-splitting strategy as get_logs().
        """
        logs: List[Dict[str, Any]] = []
        for start in range(from_block, to_block + 1, self.batch_size):
            end = min(start + self.batch_size - 1, to_block)
            logs.extend(self._get_all_logs_recursive(start, end))
        return logs

    def _get_all_logs_recursive(
        self, from_block: int, to_block: int, depth: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch all logs with no filters. Recursively splits on 413 errors."""
        if depth > MAX_SPLIT_DEPTH:
            print(f"  Max split depth ({MAX_SPLIT_DEPTH}) reached for blocks {from_block}-{to_block}. Skipping.")
            return []

        try:
            topics = [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0xd78ad95fa46c994b6551d0da85fc275fac0f35649c0943926040b90f05f31292",
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                "0x49628fd1471006c1482da88028e9ce4dbb080b815c9b0a2060f19c2af60a4b3b",
                "0xba20947a325f450d232530e5f5fce293e7963499d5309a07cee84a269f2f15a6",
                "0xd229aacb94204188fe8042965fa6b269c62dc5818b21238779ab64bdd17efeec",
                "0x0d381c2a574ae8f04e213db7cfb4df8df712cdbd427d9868ffef380660ca6574"
            ]

            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "topics": [topics]
                    }
                ],
            }
            response = self.rpc_client.post("", payload)
            result = response.get("result", [])
            if result is None:
                return []
            return result
        except Exception as e:
            if "413" in str(e) and from_block < to_block:
                mid = (from_block + to_block) // 2
                if depth == 0:
                    print(f"  Range {from_block}-{to_block} too large (413). Splitting (depth={depth+1})...")
                return self._get_all_logs_recursive(
                    from_block, mid, depth + 1
                ) + self._get_all_logs_recursive(mid + 1, to_block, depth + 1)
            raise e

    def _get_logs_recursive(
        self, address: str, topics: List[Any], from_block: int, to_block: int, depth: int = 0
    ) -> List[Dict[str, Any]]:
        if depth > MAX_SPLIT_DEPTH:
            print(f"  Max split depth ({MAX_SPLIT_DEPTH}) reached for blocks {from_block}-{to_block}. Skipping.")
            return []

        try:
            payload = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "eth_getLogs",
                "params": [
                    {
                        "address": address,
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "topics": topics,
                    }
                ],
            }
            response = self.rpc_client.post("", payload)
            return response.get("result", [])
        except Exception as e:
            if "413" in str(e) and from_block < to_block:
                mid = (from_block + to_block) // 2
                if depth == 0:
                    print(f"  Range {from_block}-{to_block} too large (413). Splitting...")
                return self._get_logs_recursive(
                    address, topics, from_block, mid, depth + 1
                ) + self._get_logs_recursive(address, topics, mid + 1, to_block, depth + 1)
            raise e

    def get_blocks_by_number(
        self, block_numbers: Iterable[int], include_transactions: bool = False
    ) -> Dict[int, Dict[str, Any]]:
        unique_numbers = sorted(set(block_numbers))
        results: Dict[int, Dict[str, Any]] = {}

        chunk_size = 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_chunk = {}
            for i in range(0, len(unique_numbers), chunk_size):
                chunk = unique_numbers[i : i + chunk_size]
                future = executor.submit(self._get_blocks_recursive, chunk, include_transactions)
                future_to_chunk[future] = chunk

            for future in concurrent.futures.as_completed(future_to_chunk):
                results.update(future.result())

        return results

    def _get_blocks_recursive(self, chunk: List[int], include_transactions: bool) -> Dict[int, Dict[str, Any]]:
        try:
            payloads = [
                {
                    "id": block_number,
                    "jsonrpc": "2.0",
                    "method": "eth_getBlockByNumber",
                    "params": [hex(block_number), include_transactions],
                }
                for block_number in chunk
            ]
            responses = self.rpc_client.post_batch("", payloads)
            results = {}
            for response in responses:
                block = response.get("result")
                if not block:
                    continue
                results[int(block["number"], 16)] = block
            return results
        except Exception as e:
            if "413" in str(e) and len(chunk) > 1:
                mid = len(chunk) // 2
                print(f"  Block batch {len(chunk)} too large (413). Splitting...")
                res = self._get_blocks_recursive(chunk[:mid], include_transactions)
                res.update(self._get_blocks_recursive(chunk[mid:], include_transactions))
                return res
            raise e

    def get_transaction_receipts_parallel(self, tx_hashes: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        unique_hashes = list(set(tx_hashes))
        results: Dict[str, Dict[str, Any]] = {}

        chunk_size = 10

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            future_to_chunk = {}
            for i in range(0, len(unique_hashes), chunk_size):
                chunk = unique_hashes[i : i + chunk_size]
                future = executor.submit(self._get_receipts_recursive, chunk)
                future_to_chunk[future] = chunk

            for future in concurrent.futures.as_completed(future_to_chunk):
                results.update(future.result())

        return results

    def _get_receipts_recursive(self, chunk: List[str]) -> Dict[str, Dict[str, Any]]:
        try:
            payloads = [
                {
                    "id": i,
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": [tx_hash],
                }
                for i, tx_hash in enumerate(chunk)
            ]
            responses = self.rpc_client.post_batch("", payloads)
            results = {}
            for response in responses:
                receipt = response.get("result")
                if receipt and receipt.get("transactionHash"):
                    results[receipt["transactionHash"].lower()] = receipt
            return results
        except Exception as e:
            if "413" in str(e) and len(chunk) > 1:
                mid = len(chunk) // 2
                res = self._get_receipts_recursive(chunk[:mid])
                res.update(self._get_receipts_recursive(chunk[mid:]))
                return res
            raise e
