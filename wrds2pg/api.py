from __future__ import annotations

import os
import tempfile
from pathlib import Path

from ._utils import get_now

# --- SAS / WRDS ---
from .sas.stream import get_process_stream, get_wrds_process_stream
from .sas.metadata import get_modified_str, get_table_metadata

# --- Postgres ---
from .postgres.ddl import (
    process_sql,
    get_table_comment,
    set_table_comment,
    role_exists,
    create_role,
    create_table_sql,
)
from .postgres.engine import make_engine
from .postgres.copy import wrds_to_pg, wrds_process_to_pg

# --- Files ---
from .files.csv import (
    wrds_to_csv,
    get_modified_csv,
    set_modified_csv,
)
from .files.paths import get_pq_file
from .files.parquet import (
    get_modified_pq,
    csv_to_pq_arrow_stream,
)

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
        For Parquet files, conversion from PostgreSQL to PyArrow types is handled by PyArrow.
        Only a subset of columns needs to be supplied.
        Supplied types should be compatible with data emitted by SAS's PROC EXPORT 
        (i.e., one can't "fix" arbitrary type issues using this argument).
        For example, `col_types = {'permno':'integer', 'permco':'integer'`.

    create_roles: boolean
        Indicates whether database roles should be created for schema.
        Two roles are created.
        One role is for ownership (e.g., `crsp` for `crsp.dsf`) with write access.
        One role is read-only for access (e.g., `crsp_access`).
        This is only useful if you are running a shared server.
        Default is `True`.
        
    encoding: string  [Optional]
        Encoding to be used for text emitted by SAS.
    
    sas_schema: string [Optional]
        WRDS schema for the SAS data file. This can differ from the PostgreSQL schema in some cases.
        Data obtained from sas_schema is stored in schema.
        
    sas_encoding: string
        Encoding of the SAS data file.

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
        engine = make_engine(host=host, dbname=dbname)

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
        from PostgreSQL to PyArrow types is handled by PyArrow. Only a subset of 
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
        meta = get_table_metadata(
            table_name=table_name,
            wrds_id=wrds_id,
            drop=drop,
            keep=keep,
            rename=rename,
            sas_schema=sas_schema,
            encoding=encoding,
            col_types=col_types,    # user overrides
        )
        
        names = meta["names"]
        col_types_out = meta["col_types"]
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
    """Update a local gzipped CSV version of a WRDS table.

    Parameters
    ----------
    table_name:
        Name of table (based on name of WRDS SAS file).

    schema:
        Name of schema (normally the SAS library name).

    wrds_id: string [Optional]
        The WRDS ID used to access WRDS SAS via SSH.
        Default is to use the environment value `WRDS_ID`.

    data_dir: string [Optional]
        Root directory of CSV data repository.
        Default is to use the environment value `CSV_DIR`.

    force: Boolean [Optional]
        Forces update of file without checking status of WRDS SAS file.
        Default is `False`.

    fix_missing: Boolean [Optional]
        Default is `False`.
        This converts special missing values to simple missing values.

    fix_cr: Boolean [Optional]
        Set to `True` when the SAS file contains unquoted carriage returns that
        would otherwise produce `BadCopyFileFormat`. Default is `False`.

    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.

    keep: string [Optional]
        SAS code snippet indicating variables to be retained.

    obs: Integer [Optional]
        Maximum number of observations to export. Useful for testing.

    rename: string [Optional]
        SAS code snippet indicating variables to be renamed.

    where: string [Optional]
        SAS code snippet indicating observations to be retained.

    alt_table_name: string [Optional]
        Basename of CSV file. Used when file should have different name from
        table_name.

    encoding: string [Optional]
        Encoding to be used for text emitted by SAS.

    sas_schema: string [Optional]
        WRDS schema (SAS library) for the table. Defaults to `schema`.
        Data obtained from `sas_schema` is stored under `schema`.

    sas_encoding: string
        Encoding of the SAS data file.

    Returns
    -------
    Boolean indicating whether the file was created or updated.

    Examples
    ----------
    >>> wrds_update_csv("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update_csv("feed21_bankruptcy_notification", "audit", drop="match: closest: prior:")
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
            
def sas_to_pandas(sas_code, wrds_id=None, fpath=None, encoding="utf-8"):
    """Run SAS code on WRDS or locally and return a pandas DataFrame.
    One of `wrds_id`, the environment
    variable `WRDS_ID`, or `fpath` must be set."""
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError(
            "sas_to_pandas() requires pandas. "
            "Install it with `pip install pandas`."
        ) from e

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

def run_file_sql(file, engine):
    f = open(file, 'r')
    sql = f.read()
    print("Running SQL in %s" % file)
    
    for i in sql.split(";"):
        j = i.strip()
        if j != "":
            print("\nRunning SQL: %s;" % j)
            process_sql(j, engine)

