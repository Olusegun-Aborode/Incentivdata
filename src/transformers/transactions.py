from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pandera as pa
from pandera import Check, Column


TRANSACTION_SCHEMA = pa.DataFrameSchema(
    {
        "block_number": Column(int, Check.ge(0)),
        "block_timestamp": Column("datetime64[ns]"),
        "hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "nonce": Column(int, Check.ge(0)),
        "transaction_index": Column(int, Check.ge(0)),
        "from_address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$")),
        "to_address": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$"), nullable=True),
        "value": Column(str), # Keep as string for Dune large numbers
        "gas": Column(int, Check.ge(0)),
        "gas_used": Column("Int64", nullable=True),
        "gas_price": Column(str), # Keep as string
        "input": Column(str, nullable=True),
        "status": Column("Int64", nullable=True), # From receipt or tx
        "block_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "chain": Column(str),
        "extracted_at": Column("datetime64[ns]"),
    },
    coerce=True
)

def normalize_transactions(
    blocks: List[Dict[str, Any]], chain: str, receipts_by_hash: Optional[Dict[str, Dict[str, Any]]] = None
) -> pd.DataFrame:
    rows = []
    extracted_at = datetime.utcnow()
    receipts = receipts_by_hash or {}
    
    for block in blocks:
        block_number = int(block["number"], 16)
        block_timestamp = datetime.utcfromtimestamp(int(block["timestamp"], 16))
        block_hash = block["hash"]
        
        for tx in block.get("transactions", []):
            if isinstance(tx, str):
                continue
            
            tx_hash = tx["hash"].lower()
            receipt = receipts.get(tx_hash)
            # Status from receipt (RPC path) or from tx itself (REST v2 path)
            if receipt and "status" in receipt:
                status = int(receipt["status"], 16)
            elif "status" in tx:
                status = int(tx["status"], 16)
            else:
                status = None

            # gas_used from receipt (RPC) or from tx (REST v2)
            gas_used = None
            if receipt and "gasUsed" in receipt:
                gas_used = int(receipt["gasUsed"], 16)
            elif "gasUsed" in tx:
                gas_used = int(tx["gasUsed"], 16)

            rows.append(
                {
                    "block_number": block_number,
                    "block_timestamp": block_timestamp,
                    "hash": tx["hash"],
                    "nonce": int(tx.get("nonce", "0x0"), 16),
                    "transaction_index": int(tx.get("transactionIndex", "0x0"), 16),
                    "from_address": tx["from"].lower() if tx.get("from") else None,
                    "to_address": tx["to"].lower() if tx.get("to") else None,
                    "value": str(int(tx.get("value", "0x0"), 16)),
                    "gas": int(tx.get("gas", "0x0"), 16),
                    "gas_used": gas_used,
                    "gas_price": str(int(tx.get("gasPrice", "0x0"), 16)),
                    "input": tx.get("input", "0x"),
                    "status": status,
                    "block_hash": block_hash,
                    "chain": chain,
                    "extracted_at": extracted_at,
                }
            )
            
    if not rows:
        return pd.DataFrame(columns=TRANSACTION_SCHEMA.columns.keys())
        
    df = pd.DataFrame(rows)
    df['status'] = df['status'].astype("Int64")
    df['gas_used'] = df['gas_used'].astype("Int64")
    return TRANSACTION_SCHEMA.validate(df)
