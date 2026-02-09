from __future__ import annotations

import csv
import io
import os
import re

def make_sas_code(
    table_name,
    schema,
    fpath=None,
    drop=None,
    keep=None,
    rename=None,
    sas_schema=None,
):
    if sas_schema is None:
        sas_schema = schema

    # Local SAS mode: assign libref to a directory
    libname_stmt = f"libname {sas_schema} '{fpath}';" if fpath else ""

    rename_str = f"rename=({rename})" if rename else ""
    drop_str = f"drop={drop}" if drop else ""
    keep_str = f"keep={keep}" if keep else ""

    # Build dataset options cleanly (avoid extra spaces)
    opts = " ".join(x for x in [drop_str, keep_str, rename_str] if x)

    sas_code = f"""
        {libname_stmt}
        
        * Extract variable metadata without printing the pretty report.;
        proc contents data={sas_schema}.{table_name}({opts}) out=_meta noprint;
        run;
        
        proc sort data=_meta;
          by varnum;
        run;
        
        * Emit machine-readable CSV to stdout.;
        proc export data=_meta(keep=name type format formatl formatd length)
          outfile=stdout
          dbms=csv
          replace;
        run;
    """
    return sas_code.strip() + "\n"

def proc_contents(table_name, sas_schema=None, wrds_id=None, fpath=None, encoding="utf-8"):
    from .stream import get_process_stream  # local import to avoid circular import
    if wrds_id is None and fpath is None:
        wrds_id = os.environ.get("WRDS_ID")

    if wrds_id is None and fpath is None:
        raise ValueError(
            "You must provide `fpath` or `wrds_id` "
            "(or set the `WRDS_ID` environment variable)."
        )

    # WRDS mode requires a SAS libref
    if wrds_id is not None and sas_schema is None:
        raise ValueError("`sas_schema` must be provided for WRDS mode (e.g., 'crsp').")

    # Local mode fallback
    if wrds_id is None and sas_schema is None:
        sas_schema = "work"

    sas_code = f"PROC CONTENTS data={sas_schema}.{table_name}; RUN;"

    with get_process_stream(
        sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=encoding,
    ) as stream:
        return stream.readlines()

def get_modified_str(table_name, sas_schema, wrds_id=None, encoding="utf-8"):
    contents = proc_contents(
        table_name=table_name,
        sas_schema=sas_schema,
        wrds_id=wrds_id,
        encoding=encoding,
    )
    if not contents:
        print(f"Table {sas_schema}.{table_name} not found.")
        return None

    modified = None
    next_row = False

    for line in contents:
        if next_row:
            line = re.sub(r"^\s+(.*)\s+$", r"\1", line)
            line = re.sub(r"\s+$", "", line)
            if "Protection" not in line:
                modified = (modified or "") + " " + line.rstrip()
            next_row = False

        if re.match(r"Last Modified", line):
            modified = re.sub(
                r"^Last Modified\s+(.*?)\s{2,}.*$",
                r"Last modified: \1",
                line,
            ).rstrip()
            next_row = True

    # IMPORTANT: don't silently return "" if not found
    if not modified:
        return None

    return modified.strip()

def get_table_sql(
    table_name,
    schema,                    # target Postgres schema
    wrds_id=None,
    fpath=None,                # local SAS library path (optional)
    drop=None,
    keep=None,
    rename=None,
    return_sql=True,
    alt_table_name=None,
    col_types=None,
    sas_schema=None,           # SAS libref (WRDS or local)
    encoding="utf-8",
):
    from .stream import get_process_stream  # local import to avoid circular import
    # --- resolve mode ---
    if wrds_id is None and fpath is None:
        wrds_id = os.environ.get("WRDS_ID")

    if wrds_id is None and fpath is None:
        raise ValueError(
            "You must provide `fpath` or `wrds_id` "
            "(or set the `WRDS_ID` environment variable)."
        )

    # --- defaults ---
    if alt_table_name is None:
        alt_table_name = table_name
    if sas_schema is None:
        # local mode: you can choose "work" or require explicit; historically you used fpath
        sas_schema = schema if wrds_id is not None else "work"

    # --- generate SAS that emits CSV metadata to stdout ---
    sas_code = make_sas_code(
        table_name=table_name,
        schema=schema,          # only used to default sas_schema inside make_sas_code if you want
        sas_schema=sas_schema,
        fpath=fpath,
        drop=drop,
        keep=keep,
        rename=rename,
    )

    # --- run SAS, capture stdout text ---
    with get_process_stream(
        sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=encoding,
    ) as stream:
        text = stream.read()

    # --- parse CSV metadata; normalize header keys to lowercase ---
    reader = csv.DictReader(io.StringIO(text))
    rows = [{k.strip().lower(): v for k, v in row.items()} for row in reader]

    if not rows:
        raise RuntimeError(
            f"No metadata returned for {sas_schema}.{table_name}. "
            "Check SAS log/stderr (likely table not found)."
        )
    if "name" not in rows[0]:
        raise RuntimeError(
            f"Unexpected PROC EXPORT headers: {list(rows[0].keys())}\n"
            "First 20 lines of output:\n" + "\n".join(text.splitlines()[:20])
        )

    # --- infer PostgreSQL types ---
    names = [r["name"].strip().lower() for r in rows]
    inferred = {n: code_row_dict(r) for n, r in zip(names, rows)}

    # override inferred types if provided
    inferred = {**inferred, **{k.lower(): v for k, v in (col_types or {}).items()}}

    rows_str = ", ".join([f'"{n}" {inferred[n]}' for n in names])
    create_sql = f'CREATE TABLE "{schema}"."{alt_table_name}" ({rows_str})'

    if return_sql:
        return {"sql": create_sql, "names": names, "col_types": inferred}

    # non-SQL return: a list of dicts (keeps pandas optional)
    out = []
    for n, r in zip(names, rows):
        out.append({**r, "name": n, "postgres_type": inferred[n]})
    return out

def get_table_metadata(
    table_name,
    wrds_id=None,
    fpath=None,
    drop=None,
    keep=None,
    rename=None,
    sas_schema=None,
    encoding="utf-8",
    col_types=None,
):
    if sas_schema is None:
        sas_schema = "work"

    sas_code = make_sas_code(
        table_name=table_name,
        schema=sas_schema,
        fpath=fpath,
        drop=drop,
        keep=keep,
        rename=rename,
        sas_schema=sas_schema,
    )

    # LOCAL import avoids circular import at module load time
    from .stream import get_process_stream

    with get_process_stream(
        sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=encoding,
    ) as stream:
        text = stream.read()

    reader = csv.DictReader(io.StringIO(text))
    rows = [{k.strip().lower(): v for k, v in row.items()} for row in reader]

    if not rows:
        raise RuntimeError(f"No metadata returned for {sas_schema}.{table_name}.")
    if "name" not in rows[0]:
        raise RuntimeError(
            f"Unexpected PROC EXPORT headers: {list(rows[0].keys())}\n"
            "First 20 lines:\n" + "\n".join(text.splitlines()[:20])
        )

    names = [r["name"].strip().lower() for r in rows]
    inferred = {n: code_row_dict(r) for n, r in zip(names, rows)}

    if col_types:
        for k, v in col_types.items():
            inferred[k.lower()] = v

    return {"names": names, "col_types": inferred, "rows": rows}

def code_row_dict(row):
    """
    row: dict with keys: name,type,format,formatl,formatd (strings from CSV)
    """
    # normalize
    fmt = (row.get("format") or "").strip()
    col_type = int(row.get("type") or 0)

    # SAS PROC CONTENTS: type=2 is character
    if col_type == 2:
        return "text"

    # formatd/formatl may be blank
    formatd = int(float(row.get("formatd") or 0))
    formatl = int(float(row.get("formatl") or 0))

    # date/time detection
    if fmt:
        if re.search(r"datetime", fmt, re.I):
            return "timestamp"
        if fmt.upper() == "TIME8." or fmt.upper() == "TOD" or re.search(r"time", fmt, re.I):
            return "time"
        if re.search(r"(date|yymmdd|mmddyy)", fmt, re.I):
            return "date"

    # numeric heuristics (your existing logic)
    if fmt.upper() == "BEST":
        return "float8"
    if formatd != 0:
        return "float8"
    if formatd == 0 and formatl != 0:
        return "integer"
    if formatd == 0 and formatl == 0:
        return "float8"

    return "text"

def get_wrds_tables(schema, wrds_id=None, encoding="utf-8"):
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
    if wrds_id is None:
        raise ValueError("You must provide `wrds_id` or set the `WRDS_ID` environment variable.")
    from .stream import get_process_stream  # local import to avoid circular import
    lib = schema.upper()

    sas_code = f"""
        proc sql;
            SELECT memname
            FROM dictionary.tables
            WHERE libname = "{lib}"
            ORDER BY memname;
        quit;
    """

    with get_process_stream(sas_code, wrds_id=wrds_id, encoding=encoding) as stream:
        text = stream.read()

    # Parse memnames from listing output. This is robust enough for PROC SQL output.
    # It looks for "words" in the memname column; filters out headers/blank lines.
    names = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line in {"MEMNAME"}:
            continue
        if "The SAS System" in line or "Monday" in line or "Tuesday" in line:
            continue
        # memname is usually a single token
        if re.fullmatch(r"[A-Z0-9_]+", line):
            names.append(line)

    return names
