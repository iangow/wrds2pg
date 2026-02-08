from __future__ import annotations

import io
import csv
import os
import re
import shutil
import subprocess
import tempfile
import time
import warnings
import gzip
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from io import StringIO

import pandas as pd
import paramiko
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from sqlalchemy import create_engine, inspect, text

# warnings.filterwarnings(action='ignore', module='.*paramiko.*')

def get_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

SAS_STDOUT_PREAMBLE = """\
ods listing;
ods html close;
ods pdf close;
ods results off;

options nodate nonumber nocenter;
"""


_PG_TO_ARROW = {
    "text": pa.string(),
    "varchar": pa.string(),
    "character varying": pa.string(),
    "integer": pa.int64(),
    "int": pa.int64(),
    "bigint": pa.int64(),
    "float8": pa.float64(),
    "double precision": pa.float64(),
    "real": pa.float32(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
    "timestamp without time zone": pa.timestamp("us"),
    "time": pa.time64("us"),
}

@contextmanager
def get_process_stream(sas_code, wrds_id=None, fpath=None, encoding="utf-8"):
    """
    Yield a *text* stream containing SAS output. Always cleans up resources.
    Exactly one of (fpath, wrds_id) should be provided.
    """
    sas_code = with_stdout_preamble(sas_code)
    if fpath is not None:
        proc = subprocess.Popen(
            ["sas", "-stdio", "-noterminal"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding=encoding,
        )
        try:
            assert proc.stdin is not None
            proc.stdin.write(sas_code)
            proc.stdin.close()

            assert proc.stdout is not None
            yield proc.stdout

            rc = proc.wait()
            if rc != 0:
                err = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(f"SAS exited with code {rc}.\n{err}")
        finally:
            # close pipes
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()
            # ensure process is gone
            if proc.poll() is None:
                proc.terminate()

    elif wrds_id is not None:
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect(
            "wrds-cloud-sshkey.wharton.upenn.edu",
            username=wrds_id,
            compress=False,
        )
        try:
            stdin, stdout, stderr = client.exec_command("qsas -stdio -noterminal")
            stdin.write(sas_code)
            stdin.close()

            # Paramiko stdout is a binary stream -> wrap as text
            text_stdout = io.TextIOWrapper(stdout, encoding=encoding)

            try:
                yield text_stdout
            finally:
                text_stdout.close()
                stderr.close()
        finally:
            client.close()

    else:
        raise ValueError("Either `wrds_id` or `fpath` must be provided.")

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

def sas_to_pandas(sas_code, wrds_id=None, fpath=None, encoding="utf-8"):
    """Function that runs SAS code on WRDS or local server
    and returns a Pandas data frame. One of `wrds_id`, the environment
    variable `WRDS_ID`, or `fpath` must be set.
    """

    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")

    if fpath is None and wrds_id is None:
        raise ValueError(
            "One of `wrds_id`, the environment variable `WRDS_ID`, "
            "or `fpath` must be set."
        )

    with get_process_stream(
        sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=encoding,
    ) as stream:
        df = pd.read_csv(stream)

    df.columns = df.columns.str.lower()
    return df

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
    if col_types:
        for k, v in col_types.items():
            inferred[k.lower()] = v

    rows_str = ", ".join([f'"{n}" {inferred[n]}' for n in names])
    create_sql = f'CREATE TABLE "{schema}"."{alt_table_name}" ({rows_str})'

    if return_sql:
        return {"sql": create_sql, "names": names, "col_types": inferred}

    # non-SQL return: a list of dicts (keeps pandas optional)
    out = []
    for n, r in zip(names, rows):
        out.append({**r, "name": n, "postgres_type": inferred[n]})
    return out

def get_wrds_sas(table_name, schema, wrds_id=None, fpath=None,
                     drop=None, keep=None, fix_cr = False, 
                     fix_missing = False, obs=None, where=None,
                     rename=None, encoding=None, sas_encoding=None):
    
    make_table_data = get_table_sql(table_name=table_name, schema=schema, 
                                    wrds_id=wrds_id, fpath=fpath,
                                    drop=drop, rename=rename, keep=keep)

    col_types = make_table_data["col_types"]
    
    if fix_cr:
        fix_missing = True;
        fix_cr_code = """
            * fix_cr_code;  
            array _char _character_;
            
            do over _char;
                _char = compress(_char, , 'kw');
            end;"""
    else:
        fix_cr_code = ""

    if fpath:
        libname_stmt = "libname %s '%s';" % (schema, fpath)
    else:
        libname_stmt = ""

    if rename:
        rename_str = " rename=(" + rename + ")"
    else:
        rename_str = ""
        
    if not sas_encoding:
        sas_encoding_str=""
    else:
        sas_encoding_str="(encoding='" + sas_encoding + "')"

    if fix_missing or drop or obs or keep or col_types or where:
        
        if obs:
            obs_str = " obs=" + str(obs)
        else:
            obs_str = ""

        if drop:
            drop_str = " drop=" + drop + " "
        else:
            drop_str = ""
        
        if keep:
            keep_str = " keep=" + keep + " "
        else:
            keep_str = ""
            
        if where:
            where_str = "where " + where + ";"
        else:
            where_str = ""
        
        if obs or drop or rename or keep:
            sas_table = table_name + "(" + drop_str + keep_str + \
                                           obs_str + rename_str + ")"
        else:
            sas_table = table_name

        # Cut table name to no more than 32 characters
        # (A SAS limitation)
        new_table = "%s%s" % (schema, table_name)
        new_table = new_table[0:min(len(new_table), 32)]

        if col_types:
            # print(col_types)
            unformat = [key for key in col_types if col_types[key] 
                          not in ['date', 'time', 'timestamp']]
            unformat_str = ' '.join([ 'attrib ' + var + ' format=;'
                                       for var in unformat])

            dates = [key for key in col_types if col_types[key]=='date']
            dates_str = ' '.join([ 'attrib ' + var + ' format=YYMMDD10.;'
                                       for var in dates])

            timestamps = [key for key in col_types 
                              if col_types[key]=='timestamp']
            timestamps_str = ' '.join([ 'attrib ' + var + ' format=E8601DT19.;'
                                       for var in timestamps])
        else:
            unformat_str = ""
        
        if fix_missing:
            fix_missing_str = """
                * fix_missing code;
                array allvars _numeric_ ;

                do over allvars;
                  if missing(allvars) then allvars = .;
                end;"""
        else:
            fix_missing_str = ""
        
        sas_code = f"""
            options nosource nonotes;
            {libname_stmt}
            * Fix missing values;
            data {new_table};
                set {schema}.{sas_table}{sas_encoding_str};
                {fix_cr_code}
                {fix_missing_str}
                {where_str}
            run;

            proc datasets lib=work;
                modify {new_table}; 
                    {unformat_str}
                    {dates_str}
                    {timestamps_str}
            run;

            proc export data={new_table}(encoding="utf-8") 
                outfile=stdout dbms=csv;
            run;"""
    else:

        sas_code = f"""
            options nosource nonotes;
            {libname_stmt}

            proc export data={schema}.{table_name}({rename_str} 
                              encoding="utf-8") outfile=stdout dbms=csv;
            run;"""
    return sas_code
    
def get_wrds_process_stream(
    table_name, schema, wrds_id=None, fpath=None,
    drop=None, keep=None, fix_cr=False,
    fix_missing=False, obs=None, rename=None, where=None,
    encoding=None, sas_encoding=None,
    stream_encoding="utf-8",
):
    sas_code = get_wrds_sas(
        table_name=table_name, wrds_id=wrds_id, fpath=fpath, schema=schema,
        drop=drop, rename=rename, keep=keep,
        fix_cr=fix_cr, fix_missing=fix_missing,
        obs=obs, where=where,
        encoding=encoding, sas_encoding=sas_encoding,
    )

    return get_process_stream(
        sas_code=sas_code,
        wrds_id=wrds_id,
        fpath=fpath,
        encoding=stream_encoding,
    )

def wrds_to_pandas(
    table_name,
    schema,
    wrds_id=None,
    fpath=None,
    *,
    sas_schema=None,
    encoding="utf-8",
    drop=None,
    rename=None,
    obs=None,
    where=None,
):
    if wrds_id is None and fpath is None:
        wrds_id = os.environ.get("WRDS_ID")

    if fpath is None and wrds_id is None:
        raise ValueError(
            "You must provide `fpath` or `wrds_id` "
            "(or set the `WRDS_ID` environment variable)."
        )

    if sas_schema is None:
        sas_schema = schema

    with get_wrds_process_stream(
        table_name=table_name,
        schema=sas_schema,
        wrds_id=wrds_id,
        fpath=fpath,
        drop=drop,
        rename=rename,
        obs=obs,
        where=where,
        stream_encoding=encoding,
    ) as stream:
        df = pd.read_csv(stream)

    df.columns = df.columns.str.lower()
    return df



def with_stdout_preamble(sas_code: str) -> str:
    return SAS_STDOUT_PREAMBLE + "\n" + sas_code

def proc_contents(table_name, sas_schema=None, wrds_id=None, fpath=None, encoding="utf-8"):
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

def get_table_comment(table_name, schema, engine):
    """Return the table comment from pg_class, or '' if none exists."""
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return ""

    sql = text("""
        SELECT obj_description(
            to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)),
            'pg_class'
        )
    """)

    with engine.connect() as conn:
        return conn.execute(sql, {"schema": schema, "table": table_name}).scalar() or ""
        
def set_table_comment(table_name, schema, comment, engine):

    connection = engine.connect()
    trans = connection.begin()
    sql = f"""COMMENT ON TABLE "{schema}"."{table_name}" IS '{comment}'"""

    try:
        res = connection.execute(text(sql))
        trans.commit()
    except:
        trans.rollback()
        raise

    return True

def wrds_to_pg(
    table_name,
    schema,
    engine,
    wrds_id=None,
    *,
    fix_missing=False,
    fix_cr=False,
    drop=None,
    obs=None,
    rename=None,
    keep=None,
    where=None,
    alt_table_name=None,
    encoding="utf-8",
    col_types=None,
    create_roles=True,
    sas_schema=None,
    sas_encoding=None,
    tz="UTC",
):
    # Resolve WRDS ID
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")

    if wrds_id is None:
        raise ValueError(
            "You must provide `wrds_id` or set the `WRDS_ID` environment variable."
        )

    if alt_table_name is None:
        alt_table_name = table_name

    if sas_schema is None:
        sas_schema = schema

    make_table_data = get_table_sql(
        table_name=table_name,
        wrds_id=wrds_id,
        schema=schema,
        drop=drop,
        rename=rename,
        keep=keep,
        alt_table_name=alt_table_name,
        col_types=col_types,
        sas_schema=sas_schema,
    )

    process_sql(
        f'DROP TABLE IF EXISTS "{schema}"."{alt_table_name}" CASCADE',
        engine,
    )

    # Create schema (and associated role) if necessary
    insp = inspect(engine)
    if schema not in insp.get_schema_names():
        process_sql(f"CREATE SCHEMA {schema}", engine)

        if create_roles:
            if not role_exists(engine, schema):
                create_role(engine, schema)
            process_sql(f"ALTER SCHEMA {schema} OWNER TO {schema}", engine)
            if not role_exists(engine, f"{schema}_access"):
                create_role(engine, f"{schema}_access")
            process_sql(
                f"GRANT USAGE ON SCHEMA {schema} TO {schema}_access",
                engine,
            )

    process_sql(make_table_data["sql"], engine)

    print(f"Beginning file import at {get_now()} UTC.")
    print(f"Importing data into {schema}.{alt_table_name}.")

    with get_wrds_process_stream(
        table_name=table_name,
        schema=sas_schema,
        wrds_id=wrds_id,
        drop=drop,
        keep=keep,
        fix_cr=fix_cr,
        fix_missing=fix_missing,
        obs=obs,
        rename=rename,
        where=where,
        sas_encoding=sas_encoding,
        stream_encoding=encoding,
    ) as stream:
        res = wrds_process_to_pg(
            alt_table_name,
            schema,
            engine,
            stream,
            tz=tz,
        )

    print(f"Completed file import at {get_now()} UTC.\n")
    return res

def wrds_process_to_pg(table_name, schema, engine, p, tz="UTC", copy_encoding="UTF8", chunk_size=1 << 20):
    """
    Stream CSV text from file-like object `p` into Postgres using COPY FROM STDIN.

    Parameters
    ----------
    p:
        A *text* stream (already decoded), positioned at the start of a CSV where
        the first line is the header.
        NOTE: This function does NOT close `p`; the caller owns the stream.
    copy_encoding:
        Encoding declared in the COPY command (usually 'UTF8').
        This is about how Postgres interprets incoming bytes; since psycopg sends
        encoded text, leaving this as UTF8 is usually correct.
    chunk_size:
        How many characters to read per chunk while streaming to Postgres.
    """

    # The first line has the variable names ...
    header = p.readline()
    if not header:
        raise ValueError("No data received from WRDS/SAS process (empty stream).")

    var_names = header.rstrip("\n\r").lower().split(",")
    var_str = '("' + '", "'.join(var_names) + '")'

    copy_cmd = f'COPY "{schema}"."{table_name}" {var_str} FROM STDIN CSV ENCODING \'{copy_encoding}\''

    with engine.connect() as conn:
        connection_fairy = conn.connection
        try:
            with connection_fairy.cursor() as curs:
                curs.execute("SET DateStyle TO 'ISO, MDY'")
                curs.execute(f"SET TimeZone TO '{tz}'")

                with curs.copy(copy_cmd) as copy:
                    while True:
                        data = p.read(chunk_size)
                        if not data:
                            break
                        copy.write(data)
        finally:
            connection_fairy.commit()
            conn.close()

    return True

def wrds_update(
    table_name, schema,
    host=None,
    wrds_id=None,
    dbname=None,
    engine=None,
    force=False,
    fix_missing=False, fix_cr=False, drop=None, keep=None,
    obs=None, rename=None, where=None,
    alt_table_name=None,
    col_types=None, create_roles=True,
    encoding=None, sas_schema=None, sas_encoding=None,
    tz="UTC",
):
    """Update a PostgreSQL table using WRDS SAS data.

    Parameters
    ----------
    table_name: 
        Name of table (based on name of WRDS SAS file).
    
    schema: 
        Name of schema (normally the SAS library name).

    wrds_id: string [Optional]
        The WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    host: string [Optional]
        Host name for the PostgreSQL server.
        The default is to use the environment value `PGHOST`.

    dbname: string [Optional]
        Name for the PostgreSQL database.
        The default is to use the environment value `PGDATABASE`.

    engine: SQLAlchemy engine [Optional]
        Allows user to supply an existing database engine.

    force: Boolean [Optional]
        Forces update of file without checking status of WRDS SAS file.        
        Default is `False`.
        
    fix_missing: Boolean [Optional]
        Default is `False`.
        This converts special missing values to simple missing values.
        
    fix_cr: Boolean [Optional]
        Set to `True` when the SAS file contains unquoted carriage returns that 
        would otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards can 
        be used. See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards can
        be used. See examples below.
            
    obs: Integer [Optional]
        SAS code snippet indicating number of observations to import from SAS file.
        Setting this to modest value (e.g., `obs=1000`) can be useful for testing
        `wrds_update()` with large files.
        
    rename: string [Optional]
        SAS code snippet indicating variables to be renamed.
        (e.g., rename="fee=mgt_fee" renames `fee` to `mgt_fee`).
        
    where: string [Optional]
        SAS code snippet indicating observations to be retained.
        See examples below.
    
    alt_table_name: string [Optional]
        Basename of CSV file. Used when file should have different name from table_name.

    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to PostgreSQL or writing to Parquet files.
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by DuckDB.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by SAS's PROC EXPORT 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno':'integer', 'permco':'integer'`.

    create_roles: boolean
        Indicates whether database roles should be created for schema.
        Two roles are created.
        One role is for ownership (e.g., `crsp` for `crsp.dsf`) with write access.
        One role is read-onluy for access (e.g., `crsp_access`).
        This is only useful if you are running a shared server.
        Default is `True`.
        
    encoding: string  [Optional]
        Encoding to be used for text emitted by SAS.
    
    sas_schema: string [Optional]
        WRDS schema for the SAS data file. This can differ from the PostgreSQL schema in some cases.
        Data obtained from sas_schema is stored in schema.
        
    sas_encoding: string
        Encoding of the SAS data file.

    fpath: string [Optional]
        Path to local SAS file. Requires local SAS.

    Returns
    -------
    Boolean indicating function reached the end.
    This should mean that a parquet file was created.
    
    Examples
    ----------
    >>> wrds_update("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update("feed21_bankruptcy_notification", 
                        "audit", drop="match: closest: prior:")
    """
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
        
    if host is None:
        host = os.environ.get("PGHOST")
        
    if dbname is None:
        dbname = os.environ.get("PGDATABASE")

    if wrds_id is None:
        raise ValueError(
            "You must provide `wrds_id` or set the `WRDS_ID` environment variable."
        )

    if sas_schema is None:
        sas_schema = schema
    if alt_table_name is None:
        alt_table_name = table_name

    if engine is None:
        if not (host and dbname):
            raise ValueError("Specify `engine` or both `host` and `dbname`.")
        engine = create_engine(f"postgresql+psycopg://{host}/{dbname}")

    # 1. Get comments from PostgreSQL database
    comment = get_table_comment(alt_table_name, schema, engine)

    # 2. Get modified date from WRDS
    modified = get_modified_str(table_name, sas_schema, wrds_id, encoding=encoding)
    if not modified:
        return False

    # 3. If updated table available, get from WRDS
    if modified == comment and not force:
        print(f"{schema}.{alt_table_name} already up to date.")
        return False
    elif modified == "" and not force:
        print("WRDS flaked out!")
        return False
    else:
        if force:
            print("Forcing update based on user request.")
        else:
            print(f"Updated {schema}.{table_name} is available.")
            print("Getting from WRDS.")

        wrds_to_pg(
            table_name=table_name,
            schema=schema,
            engine=engine,
            wrds_id=wrds_id,
            fix_missing=fix_missing,
            fix_cr=fix_cr,
            drop=drop,
            keep=keep,
            obs=obs,
            rename=rename,
            where=where,
            alt_table_name=alt_table_name,
            encoding=encoding,
            col_types=col_types,
            create_roles=create_roles,
            sas_schema=sas_schema,
            sas_encoding=sas_encoding,
            tz=tz,
        )

        set_table_comment(alt_table_name, schema, modified, engine)

        if create_roles:
            if not role_exists(engine, schema):
                create_role(engine, schema)

            process_sql(
                f'ALTER TABLE "{schema}"."{alt_table_name}" OWNER TO {schema}',
                engine,
            )

            if not role_exists(engine, f"{schema}_access"):
                create_role(engine, f"{schema}_access")

            process_sql(
                f'GRANT SELECT ON "{schema}"."{alt_table_name}" TO {schema}_access',
                engine,
            )

        return True

def process_sql(sql, engine):

    connection = engine.connect()
    trans = connection.begin()

    try:
        res = connection.execute(text(sql))
        trans.commit()
    except:
        trans.rollback()
        raise

    return True

def run_file_sql(file, engine):
    f = open(file, 'r')
    sql = f.read()
    print("Running SQL in %s" % file)
    
    for i in sql.split(";"):
        j = i.strip()
        if j != "":
            print("\nRunning SQL: %s;" % j)
            process_sql(j, engine)
            
def make_engine(host=None, dbname=None, wrds_id=None):
    if not dbname:
        dbname = os.getenv("PGDATABASE")
    if not host:
        host = os.getenv("PGHOST", "localhost")
    if not wrds_id:
        wrds_id = os.getenv("WRDS_ID")
    
    engine = create_engine(f"postgresql+psycopg://{host}/{dbname}")
    return engine
  
def role_exists(engine, role):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM pg_roles WHERE rolname='%s'" % role))
        rs = [r[0] for r in res]
    
    return rs[0] > 0

def create_role(engine, role):
    process_sql("CREATE ROLE %s" % role, engine)
    return True

def get_wrds_url(wrds_id):
    host = "wrds-pgdata.wharton.upenn.edu"
    port = "9737"
    dbname = "wrds"
    return f"postgresql+psycopg://{wrds_id}@{host}:{port}/{dbname}"


def get_wrds_tables(schema, wrds_id=None, encoding="utf-8"):
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
    if wrds_id is None:
        raise ValueError("You must provide `wrds_id` or set the `WRDS_ID` environment variable.")

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

def wrds_update_pq(
    table_name,
    schema,
    wrds_id=None,
    data_dir=None,
    force=False,
    fix_missing=False,
    fix_cr=False,
    drop=None,
    keep=None,
    obs=None,
    rename=None,
    where=None,
    alt_table_name=None,
    col_types=None,
    encoding="utf-8",
    sas_schema=None,
    sas_encoding=None,
):
    """Update a local parquet version of a WRDS table.

    Parameters
    ----------
    table_name: 
        Name of table (based on name of WRDS SAS file).
    
    schema: 
        Name of schema (normally the SAS library name).

    wrds_id: string [Optional]
        The WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    data_dir: string [Optional]
        Root directory of CSV data repository. 
        The default is to use the environment value `DATA_DIR`.
    
    force: Boolean [Optional]
        Forces update of file without checking status of WRDS SAS file.        
        Default is `False`.
        
    fix_missing: Boolean [Optional]
        Default is `False`.
        This converts special missing values to simple missing values.
        
    fix_cr: Boolean [Optional]
        Set to `True` when the SAS file contains unquoted carriage returns 
        that would otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards 
        can be used. See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards 
        can be used. See examples below.
            
    obs: Integer [Optional]
        SAS code snippet indicating number of observations to import from 
        SAS file. Setting this to modest value (e.g., `obs=1000`) can be 
        useful for testing `wrds_update()` with large files.
        
    rename: string [Optional]
        SAS code snippet indicating variables to be renamed.
        (e.g., rename="fee=mgt_fee" renames `fee` to `mgt_fee`).
        
    where: string [Optional]
        SAS code snippet indicating observations to be retained.
        See examples below.
    
    alt_table_name: string [Optional]
        Basename of CSV file. Used when file should have different name 
        from table_name.

    col_types: Dict [Optional]
        Dictionary of PostgreSQL data types to be used when importing data to 
        PostgreSQL or writing to Parquet files. For Parquet files, conversion 
        from PostgreSQL to PyArrow types is handled by DuckDB. Only a subset of 
        columns needs to be supplied. Supplied types should be compatible with 
        data emitted by SAS's PROC EXPORT  (i.e., one can't "fix" arbitrary type
        issues using this argument). For example, 
        `col_types = {'permno':'integer', 'permco':'integer'`.
        
    encoding: string  [Optional]
        Encoding to be used for text emitted by SAS.
    
    sas_schema: string [Optional]
        WRDS schema for the SAS data file. This can differ from the PostgreSQL 
        schema in some cases. Data obtained from sas_schema is stored in schema.
        
    sas_encoding: string
        Encoding of the SAS data file.
    
    Returns
    -------
    Boolean indicating function reached the end.
    This should mean that a parquet file was created.
    
    Examples
    ----------
    >>> wrds_update_csv("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update_csv("feed21_bankruptcy_notification", 
                        "audit", drop="match: closest: prior:")
    """
    # --- resolve environment-backed defaults ---
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
    if wrds_id is None:
        raise ValueError("You must provide `wrds_id` or set the `WRDS_ID` environment variable.")

    if data_dir is None:
        data_dir = os.environ.get("DATA_DIR")
    if data_dir is None:
        raise ValueError("You must provide `data_dir` or set the `DATA_DIR` environment variable.")

    # --- normalize defaults ---
    if sas_schema is None:
        sas_schema = schema
    if alt_table_name is None:
        alt_table_name = table_name

    pq_file = get_pq_file(table_name=alt_table_name, schema=schema, data_dir=data_dir)

    modified = get_modified_str(
        table_name=table_name,
        sas_schema=sas_schema,
        wrds_id=wrds_id,
        encoding=encoding,
    )
    if modified is None:
        return False

    pq_modified = get_modified_pq(pq_file)

    if modified == pq_modified and not force:
        print(f"{schema}.{alt_table_name} already up to date.")
        return False

    if force:
        print("Forcing update based on user request.")
    else:
        print(f"Updated {schema}.{alt_table_name} is available.")
        print("Getting from WRDS.")

    print(f"Beginning file download at {get_now()} UTC.")
    print("Saving data to temporary CSV.")

    csv_file = tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False).name
    try:
        wrds_to_csv(
            table_name,
            schema,
            csv_file,
            wrds_id=wrds_id,
            fix_missing=fix_missing,
            fix_cr=fix_cr,
            drop=drop,
            keep=keep,
            obs=obs,
            rename=rename,
            encoding=encoding,
            where=where,
            sas_schema=sas_schema,
            sas_encoding=sas_encoding,
        )

        print("Converting temporary CSV to parquet.")
        make_table_data = get_table_sql(
            table_name=table_name,
            wrds_id=wrds_id,
            schema=sas_schema,
            drop=drop,
            rename=rename,
            keep=keep,
            col_types=col_types,
        )

        col_types_out = make_table_data["col_types"]
        names = make_table_data["names"]

        csv_to_pq_arrow_stream(csv_file, pq_file, names, col_types_out, modified)

    finally:
        # optional: clean up the temp csv; only do this if csv_to_pq doesn't need it afterward
        try:
            os.remove(csv_file)
        except OSError:
            pass

    print("Parquet file: " + str(pq_file))
    print(f"Completed creation of parquet file at {get_now()}.\n")
    return True

def wrds_csv_to_pq(table_name, schema, csv_file, pq_file, 
                   col_types=None,
                   wrds_id=None,
                   modified='',
                   row_group_size = 1048576):
    make_table_data = get_table_sql(table_name=table_name, wrds_id=wrds_id,
                                    schema=sas_schema, 
                                    drop=drop, rename=rename, keep=keep, 
                                    col_types=col_types)

    col_types = make_table_data["col_types"]
    names = make_table_data["names"]
    csv_to_pq_arrow_stream(csv_file, pq_file, names, col_types, 
                           modified=modified, 
                           row_group_size=row_group_size)

def wrds_to_csv(
    table_name,
    schema,
    csv_file,
    wrds_id=None,
    *,
    fix_missing=False,
    fix_cr=False,
    drop=None,
    keep=None,
    obs=None,
    rename=None,
    where=None,
    encoding="utf-8",
    sas_schema=None,
    sas_encoding=None,
):
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
    if wrds_id is None:
        raise ValueError("You must provide `wrds_id` or set the `WRDS_ID` environment variable.")

    if sas_schema is None:
        sas_schema = schema

    # get_wrds_process_stream should yield a *text* CSV stream
    with get_wrds_process_stream(
        table_name=table_name,
        schema=sas_schema,
        wrds_id=wrds_id,
        drop=drop,
        keep=keep,
        fix_cr=fix_cr,
        fix_missing=fix_missing,
        obs=obs,
        rename=rename,
        where=where,
        sas_encoding=sas_encoding,
        stream_encoding=encoding,
    ) as stream:
        # gzip expects bytes; wrap it in TextIOWrapper to write str safely
        with gzip.open(csv_file, mode="wt", encoding=encoding, newline="") as f:
            shutil.copyfileobj(stream, f)
        
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
    block_size=1 << 20,  # bytes per CSV read block (tune)
):
    with gzip.open(csv_file, "rb") as f:
        read_opts = pacsv.ReadOptions(
            use_threads=True,
            block_size=block_size,
            autogenerate_column_names=False,
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
                    # attach metadata once we know the final schema
                    schema = batch.schema.with_metadata(
                        {b"last_modified": modified.encode("utf-8")}
                    )
                    writer = pq.ParquetWriter(
                        pq_file,
                        schema=schema,
                        # You can add compression="zstd" or "snappy" if you want
                    )
                writer.write_batch(batch, row_group_size=row_group_size)
        finally:
            if writer is not None:
                writer.close()

def modified_encode(last_modified):
    date_time_str = last_modified.split("Last modified: ")[1]
    mtime = (datetime 
             .strptime(date_time_str, "%m/%d/%Y %H:%M:%S") 
             .replace(tzinfo=ZoneInfo("America/Chicago")) 
             .astimezone(timezone.utc) 
             .timestamp())
    return mtime

def modified_decode(mtime):
    """Decode mtime into last_modified string.

    Parameters
    ----------
    mtime:
        Modified time returned by operating system (epoch time)
    
    Returns
    -------
    last_modified: string
        Last modified information
    """
    utc_dt = datetime.fromtimestamp(mtime)
    last_modified = (utc_dt 
                     .astimezone(ZoneInfo("America/Chicago")) 
                     .strftime("Last modified: %m/%d/%Y %H:%M:%S"))
    return(last_modified)

def get_modified_csv(file_name):
    """Get last modified value for a local file using mtime.

    Parameters
    ----------
    file_name: string
        Name of file for which information is sought.
    
    Returns
    -------
    last_modified: string
        Last modified information
    """
    utc_dt=datetime.fromtimestamp(os.path.getmtime(file_name))
    last_modified = utc_dt \
                      .astimezone(ZoneInfo("America/Chicago")) \
                      .strftime("Last modified: %m/%d/%Y %H:%M:%S")
    return last_modified

def set_modified_csv(file_name, last_modified):
    """Set last modified value for a local file using mtime.

    Parameters
    ----------
    file_name: string
        Name of file to be modified
    
    last_modified: 
        String containing last modified information
    
    
    Returns
    -------
    result: boolean
        True if function succeeds.
    """
    mtimestamp = modified_encode(last_modified)
    current_time = time.time()  
    os.utime(file_name, times = (current_time, mtimestamp))    
    return True

def wrds_update_csv(
    table_name,
    schema,
    wrds_id=None,
    data_dir=None,
    force=False,
    fix_missing=False,
    fix_cr=False,
    drop=None,
    keep=None,
    obs=None,
    rename=None,
    where=None,
    alt_table_name=None,
    encoding="utf-8",
    sas_schema=None,
    sas_encoding=None,
):
    """Update a local gzipped CSV version of a WRDS table."""
    """Update a local gzipped CSV version of a WRDS table.

    Parameters
    ----------
    table_name: 
        Name of table (based on name of WRDS SAS file).
    
    schema: 
        Name of schema (normally the SAS library name).

    wrds_id: string [Optional]
        The WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    data_dir: string [Optional]
        Root directory of CSV data repository. 
        The default is to use the environment value `CSV_DIR`.
    
    force: Boolean [Optional]
        Forces update of file without checking status of WRDS SAS file.        
        Default is `False`.
        
    fix_missing: Boolean [Optional]
        Default is `False`.
        This converts special missing values to simple missing values.
        
    fix_cr: Boolean [Optional]
        Set to `True` when the SAS file contains unquoted carriage returns that
        would otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards can 
        be used. See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards can
        be used. See examples below.
            
    obs: Integer [Optional]
        SAS code snippet indicating number of observations to import from SAS 
        file. Setting this to modest value (e.g., `obs=1000`) can be useful 
        for testing `wrds_update()` with large files.
        
    rename: string [Optional]
        SAS code snippet indicating variables to be renamed.
        (e.g., rename="fee=mgt_fee" renames `fee` to `mgt_fee`).
        
    where: string [Optional]
        SAS code snippet indicating observations to be retained.
        See examples below.
    
    alt_table_name: string [Optional]
        Basename of CSV file. Used when file should have different name from 
        table_name.
    
    encoding: string  [Optional]
        Encoding to be used for text emitted by SAS.
    
    sas_schema: string [Optional]
        WRDS schema for the SAS data file. This can differ from the PostgreSQL 
        schema in some cases. Data obtained from sas_schema is stored in schema.
        
    sas_encoding: string
        Encoding of the SAS data file.
    
    Returns
    -------
    Boolean indicating function reached the end.
    This should mean that a CSV file was created.
    
    Examples
    ----------
    >>> wrds_update_csv("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update_csv("feed21_bankruptcy_notification", 
                        "audit", drop="match: closest: prior:")
    """
    # --- env-backed defaults ---
    if wrds_id is None:
        wrds_id = os.environ.get("WRDS_ID")
    if wrds_id is None:
        raise ValueError("You must provide `wrds_id` or set the `WRDS_ID` environment variable.")

    if data_dir is None:
        data_dir = os.environ.get("CSV_DIR")
    if data_dir is None:
        raise ValueError("You must provide `data_dir` or set the `CSV_DIR` environment variable.")

    # --- normalize defaults ---
    if alt_table_name is None:
        alt_table_name = table_name
    if sas_schema is None:
        sas_schema = schema

    schema_dir = Path(data_dir) / schema
    schema_dir.mkdir(parents=True, exist_ok=True)

    csv_file = (schema_dir / alt_table_name).with_suffix(".csv.gz")

    modified = get_modified_str(
        table_name=table_name,
        sas_schema=sas_schema,
        wrds_id=wrds_id,
        encoding=encoding,
    )
    if modified is None:
        return False

    csv_modified = get_modified_csv(csv_file) if csv_file.exists() else ""

    if modified == csv_modified and not force:
        print(f"{schema}.{alt_table_name} already up to date.\n")
        return False

    if force:
        print("Forcing update based on user request.")
    else:
        print(f"Updated {schema}.{alt_table_name} is available.")
        print("Getting from WRDS.")

    print(f"Beginning file download at {get_now()} UTC.")

    wrds_to_csv(
        table_name=table_name,
        schema=schema,
        csv_file=csv_file,
        wrds_id=wrds_id,
        fix_missing=fix_missing,
        fix_cr=fix_cr,
        drop=drop,
        keep=keep,
        obs=obs,
        rename=rename,
        where=where,
        encoding=encoding,
        sas_schema=sas_schema,
        sas_encoding=sas_encoding,
    )

    set_modified_csv(csv_file, modified)
    print(f"Completed file download at {get_now()} UTC.\n")
    return True
