from __future__ import annotations

import os
import gzip
import re

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# PostgreSQL → Arrow type mapping
_PG_TO_ARROW = {
    "text": pa.string(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "float8": pa.float64(),
    "date": pa.date32(),

    # Arrow's CSV reader has limited direct support for "time".
    # If you truly want time-of-day, easiest is parse as string then post-process.
    # If you want to attempt coercion, you can map it to string for now.
    "time": pa.string(),

    # Prefer a single representation; you can later upgrade to tz-aware if desired.
    "timestamp": pa.timestamp("us"),
}

def get_modified_pq(file_name):
    if not os.path.exists(file_name):
        return ""

    md = pq.read_schema(file_name).metadata
    if not md:
        return ""

    value = md.get(b"last_modified")
    if value is None:
        return ""

    return value.decode("utf-8")

def _arrow_convert_options(names, col_types):
    col_types = col_types or {}
    column_types = {}
    for name in names:
        t = (col_types.get(name) or "").strip().lower()
        t = re.sub(r"\(.*\)$", "", t).strip()   # drop (8), (255), etc.
        pa_t = _PG_TO_ARROW.get(t)

        # If unknown, DON'T force a type (Arrow will infer)
        if pa_t is not None:
            column_types[name] = pa_t

    return pacsv.ConvertOptions(
        column_types=column_types,
        strings_can_be_null=True,
        # Optional: treat SAS missings as nulls if present in CSV
        null_values=[""],
    )

def csv_to_pq_arrow_stream(
    csv_file,
    pq_file,
    names,
    col_types,
    modified,
    row_group_size=1_048_576,
    block_size=1 << 20,
):
    with gzip.open(csv_file, "rb") as f:
        read_opts = pacsv.ReadOptions(
            use_threads=True,
            block_size=block_size,
            autogenerate_column_names=False,
            skip_rows=1,              # <-- skip SAS header row
            column_names=names,       # <-- force your canonical names (lowercase)
        )
        parse_opts = pacsv.ParseOptions(delimiter=",")
        convert_opts = _arrow_convert_options(names, col_types)
    
        reader = pacsv.open_csv(
            f,
            read_options=read_opts,
            parse_options=parse_opts,
            convert_options=convert_opts,
        )

        writer = None
        try:
            for batch in reader:
                if writer is None:
                    schema = batch.schema.with_metadata(
                        {b"last_modified": modified.encode("utf-8")}
                    )
                    writer = pq.ParquetWriter(pq_file, schema=schema)
                writer.write_batch(batch, row_group_size=row_group_size)
        finally:
            if writer is not None:
                writer.close()
