from __future__ import annotations

import os
from pathlib import Path

def get_pq_file(table_name, schema, data_dir=None):
    if data_dir is None:
        data_dir = os.environ.get("DATA_DIR")
    if data_dir is None:
        raise ValueError("You must provide `data_dir` or set the"
                         " `DATA_DIR` environment variable.")

    base = Path(data_dir).expanduser()
    schema_dir = base / schema
    schema_dir.mkdir(parents=True, exist_ok=True)

    return (schema_dir / table_name).with_suffix(".parquet")
