from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import pandera as pa
from pandera import Check, Column


BLOCK_SCHEMA = pa.DataFrameSchema(
    {
        "number": Column(int, Check.ge(0)),
        "hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "parent_hash": Column(str, Check.str_matches(r"^0x[a-fA-F0-9]{64}$")),
        "nonce": Column(str, nullable=True),
        "sha3_uncles": Column(str, nullable=True),
        "logs_bloom": Column(str, nullable=True),
        "transactions_root": Column(str, nullable=True),
        "state_root": Column(str, nullable=True),
        "receipts_root": Column(str, nullable=True),
        "miner": Column(str, Check.str_matches(r"^0x[a-f0-9]{40}$")),
        "difficulty": Column(float, nullable=True),
        "total_difficulty": Column(float, nullable=True),
        "size": Column(int, Check.ge(0)),
        "extra_data": Column(str, nullable=True),
        "gas_limit": Column(int, Check.ge(0)),
        "gas_used": Column(int, Check.ge(0)),
        "timestamp": Column("datetime64[ns]"),
        "transaction_count": Column(int, Check.ge(0)),
        "chain": Column(str),
        "extracted_at": Column("datetime64[ns]"),
        "created_at": Column("datetime64[ns]"),
        "block_number": Column(int, Check.ge(0)),
    }
)


def normalize_blocks(blocks: List[Dict[str, Any]], chain: str) -> pd.DataFrame:
    rows = []
    extracted_at = datetime.utcnow()
    created_at = extracted_at
    for block in blocks:
        rows.append(
            {
                "number": int(block["number"], 16),
                "block_number": int(block["number"], 16),
                "hash": block["hash"],
                "parent_hash": block["parentHash"],
                "nonce": block.get("nonce"),
                "sha3_uncles": block.get("sha3Uncles"),
                "logs_bloom": block.get("logsBloom"),
                "transactions_root": block.get("transactionsRoot"),
                "state_root": block.get("stateRoot"),
                "receipts_root": block.get("receiptsRoot"),
                "miner": block["miner"].lower(),
                "difficulty": float(int(block.get("difficulty", "0x0"), 16)),
                "total_difficulty": float(int(block.get("totalDifficulty", "0x0"), 16)),
                "size": int(block.get("size", "0x0"), 16),
                "extra_data": block.get("extraData"),
                "gas_limit": int(block.get("gasLimit", "0x0"), 16),
                "gas_used": int(block.get("gasUsed", "0x0"), 16),
                "timestamp": datetime.utcfromtimestamp(int(block["timestamp"], 16)),
                "transaction_count": len(block.get("transactions", [])),
                "chain": chain,
                "extracted_at": extracted_at,
                "created_at": created_at,
            }
        )
    if not rows:
        return pd.DataFrame(columns=BLOCK_SCHEMA.columns.keys())

    df = pd.DataFrame(rows)

    # Ensure nullable string columns are typed correctly for Pandera
    for col in ["nonce", "sha3_uncles", "logs_bloom", "transactions_root",
                "state_root", "receipts_root", "extra_data"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return BLOCK_SCHEMA.validate(df)
