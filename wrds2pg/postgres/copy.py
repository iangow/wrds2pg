from __future__ import annotations

import os

from sqlalchemy import inspect

from .._utils import get_now
from ..sas.stream import get_wrds_process_stream
from ..sas.metadata import get_table_metadata
from .ddl import process_sql, role_exists, create_role, create_table_sql

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

def wrds_to_pg(
    table_name,
    schema,
    engine,
    wrds_id=None,
    *,
    fpath=None,  # optional: local SAS library path (directory); if set, uses local SAS instead of WRDS
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
    """
    Stream a WRDS or local SAS table directly into PostgreSQL.

    This function creates (or replaces) a PostgreSQL table and populates it
    by streaming CSV output from SAS directly into PostgreSQL using
    `COPY FROM STDIN`. No intermediate files are written to disk.

    The SAS source may be either:
      - a WRDS-hosted dataset accessed via SSH (`wrds_id`), or
      - a local SAS library accessed via `fpath` (requires local SAS).

    Column names and PostgreSQL data types are inferred from
    `PROC CONTENTS` metadata, with optional user overrides via `col_types`.

    The function is designed for large tables and performs the import in a
    fully streaming manner.

    Parameters
    ----------
    table_name : str
        Name of the SAS dataset (WRDS table name or local SAS dataset name).
    schema : str
        Target PostgreSQL schema in which the table will be created.
    engine : sqlalchemy.Engine
        SQLAlchemy engine connected to the target PostgreSQL database.
    wrds_id : str, optional
        WRDS username used to authenticate via SSH. If not provided,
        the environment variable `WRDS_ID` is used.
        Exactly one of `wrds_id` or `fpath` must be supplied.
    fpath : str, optional
        Path to a local SAS library containing the dataset.
        Requires a local SAS installation.
        Exactly one of `wrds_id` or `fpath` must be supplied.
    fix_missing : bool, default False
        If True, converts special SAS missing values to standard missing values
        before export.
    fix_cr : bool, default False
        If True, removes unquoted carriage returns in character variables that
        would otherwise cause COPY failures.
    drop : str, optional
        SAS `drop=` specification indicating variables to exclude.
    keep : str, optional
        SAS `keep=` specification indicating variables to retain.
    obs : int, optional
        Maximum number of observations to import from the SAS table.
        Useful for testing.
    rename : str, optional
        SAS `rename=` specification for renaming variables.
    where : str, optional
        SAS `where` clause specifying rows to retain.
    alt_table_name : str, optional
        Name of the PostgreSQL table to create. Defaults to `table_name`.
    encoding : str, default "utf-8"
        Encoding used for text emitted by SAS.
    col_types : dict, optional
        Mapping from column name to PostgreSQL type. Supplied values override
        inferred types. Only a subset of columns needs to be specified.
    create_roles : bool, default True
        If True, creates schema ownership and access roles if they do not exist.
    sas_schema : str, optional
        SAS library containing the dataset. Defaults to `schema`.
    sas_encoding : str, optional
        Encoding of the SAS data file.
    tz : str, default "UTC"
        PostgreSQL time zone used during import.

    Returns
    -------
    bool
        True if the table was successfully created and populated.

    Notes
    -----
    - The target table is dropped and recreated on each run.
    - Data transfer is fully streamed; memory usage is independent of table size.
    - Table comments and modification metadata are handled by higher-level
      wrapper functions such as `wrds_update()`.

    See Also
    --------
    wrds_update : High-level user-facing wrapper.
    get_table_metadata : Retrieve column names and inferred types.
    wrds_process_to_pg : Low-level COPY FROM STDIN implementation.
    """
    # --- resolve mode / defaults ---
    if wrds_id is None and fpath is None:
        wrds_id = os.environ.get("WRDS_ID")

    if (wrds_id is None) == (fpath is None):
        raise ValueError(
            "Exactly one of `wrds_id` (WRDS mode) or `fpath` (local SAS mode) must be provided."
        )

    if alt_table_name is None:
        alt_table_name = table_name

    # SAS libref to use for the source table
    if sas_schema is None:
        sas_schema = schema if wrds_id is not None else "work"

    # --- ensure schema exists (and roles if desired) BEFORE creating table ---
    insp = inspect(engine)
    if schema not in insp.get_schema_names():
        process_sql(f'CREATE SCHEMA "{schema}"', engine)

        if create_roles:
            if not role_exists(engine, schema):
                create_role(engine, schema)
            process_sql(f'ALTER SCHEMA "{schema}" OWNER TO "{schema}"', engine)

            access_role = f"{schema}_access"
            if not role_exists(engine, access_role):
                create_role(engine, access_role)
            process_sql(f'GRANT USAGE ON SCHEMA "{schema}" TO "{access_role}"', engine)

    # --- drop existing target table ---
    process_sql(f'DROP TABLE IF EXISTS "{schema}"."{alt_table_name}" CASCADE', engine)

    # --- build CREATE TABLE from SAS metadata ---
    meta = get_table_metadata(
        table_name=table_name,
        wrds_id=wrds_id,
        fpath=fpath,
        drop=drop,
        keep=keep,
        rename=rename,
        sas_schema=sas_schema,
        encoding=encoding,
        col_types=col_types,
    )

    create_sql = create_table_sql(schema, alt_table_name, meta["names"], meta["col_types"])
    process_sql(create_sql, engine)

    # --- import data ---
    print(f"Beginning file import at {get_now()} UTC.")
    print(f"Importing data into {schema}.{alt_table_name}.")

    with get_wrds_process_stream(
        table_name=table_name,
        schema=sas_schema,      # SAS libref for source data
        wrds_id=wrds_id,
        fpath=fpath,
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

    # --- grants on the table (optional, but consistent with your earlier behavior) ---
    if create_roles:
        access_role = f"{schema}_access"
        process_sql(f'ALTER TABLE "{schema}"."{alt_table_name}" OWNER TO "{schema}"', engine)
        process_sql(f'GRANT SELECT ON "{schema}"."{alt_table_name}" TO "{access_role}"', engine)

    print(f"Completed file import at {get_now()} UTC.\n")
    return res
