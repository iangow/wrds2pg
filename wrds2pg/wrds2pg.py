# Run the SAS code on the WRDS server and get the result
import pandas as pd
from io import StringIO
import re, subprocess, os, paramiko
from time import gmtime, strftime
from sqlalchemy import create_engine, inspect
from sqlalchemy import text
import duckdb
from pathlib import Path
import shutil
import gzip
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
import time

from sqlalchemy.engine import reflection
from os import getenv

client = paramiko.SSHClient()
wrds_id = getenv("WRDS_ID")

import warnings
warnings.filterwarnings(action='ignore', module='.*paramiko.*')

def get_process(sas_code, wrds_id=wrds_id, fpath=None):
    """Update a local CSV version of a WRDS table.

    Parameters
    ----------
    sas_code: 
        SAS code to be run to yield output. 
                      
    wrds_id: string
        Optional WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    fpath: 
        Optional path to a local SAS file.
    
    Returns
    -------
    The STDOUT component of the process as a stream.
    """
    if client:
        client.close()

    if fpath:

        p=subprocess.Popen(['sas', '-stdio', '-noterminal'],
                           stdin=subprocess.PIPE,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True)
        p.stdin.write(sas_code)
        p.stdin.close()

        return p.stdout

    elif wrds_id:
        """Function runs SAS code on WRDS server and
        returns result as pipe on stdout."""
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.WarningPolicy())
        client.connect('wrds-cloud-sshkey.wharton.upenn.edu',
                       username=wrds_id, compress=False)
        command = "qsas -stdio -noterminal"
        stdin, stdout, stderr = client.exec_command(command)
        stdin.write(sas_code)
        stdin.close()

        channel = stdout.channel
        # indicate that we're not going to write to that channel anymore
        channel.shutdown_write()
        return stdout
  
def code_row(row):

    """A function to code PostgreSQL data types using output from SAS's PROC CONTENTS.
    Supported types that can be returned include FLOAT8, INT8, TEXT, TIMESTAMP, 
    TIME, and DATE.

    Parameters
    ----------
    row: A row from a Pandas data frame 

    Returns:
    -------
    type:
        The PostgreSQL type inferred for the row    
    """
    format_ = row['format']
    formatd = row['formatd']
    formatl = row['formatl']
    col_type = row['type']

    if col_type==2:
        return 'text'

    if not pd.isnull(format_):
        if re.search(r'datetime', format_, re.I):
            return 'timestamp'
        elif format_ =='TIME8.' or re.search(r'time', format_, re.I) or format_=='TOD':
            return "time"
        elif re.search(r'(date|yymmdd|mmddyy)', format_, re.I):
            return "date"

    if format_ == "BEST":
        return 'float8'
    if formatd != 0:
        return 'float8'
    if formatd == 0 and formatl != 0:
        return 'int8'
    if formatd == 0 and formatl == 0:
        return 'float8'
    else:
        return 'text'

def sas_to_pandas(sas_code, wrds_id=wrds_id, fpath=None, encoding="utf-8"):

    """Function that runs SAS code on WRDS or local server
    and returns a Pandas data frame.

     Parameters
    ----------
    sas_code: string
        SAS code to be run to yield output. 
                      
    wrds_id: string
        Optional WRDS ID to be use to access WRDS SAS. 
        Default is to use the environment value `WRDS_ID`
    
    fpath: 
        Optional path to a local SAS file.

    encoding: string
        Encoding to be used to decode output from SAS.
    
    Returns
    -------
    df: 
        A Pandas data frame
    """    
    p = get_process(sas_code=sas_code, 
                    wrds_id=wrds_id,
                    fpath=fpath)

    if fpath:
        df = pd.read_csv(StringIO(p.read()))
    elif wrds_id:
        df = pd.read_csv(StringIO(p.read().decode(encoding)))
        
    df.columns = map(str.lower, df.columns)
    p.close()

    return(df)

def make_sas_code(table_name, schema, wrds_id=wrds_id, fpath=None, 
                  rpath=None, drop=None, keep=None, rename=None, 
                  sas_schema=None):
    if not wrds_id:
        wrds_id = os.environ['WRDS_ID']

    if not sas_schema:
        sas_schema = schema
        
    if fpath:
        libname_stmt = "libname %s '%s';" % (sas_schema, fpath)
        
    elif rpath:
        libname_stmt = "libname %s '%s';" % (sas_schema, rpath)
    else:
        libname_stmt = ""

    sas_template = """
        options nonotes nosource;
        %s

        * Use PROC CONTENTS to extract the information desired.;
        proc contents data=%s.%s(%s %s obs=1 %s) out=schema noprint;
        run;

        proc sort data=schema;
            by varnum;
        run;

        * Now dump it out to a CSV file;
        proc export data=schema(keep=name format formatl 
                                      formatd length type)
            outfile=stdout dbms=csv;
        run;
    """

    if rename:
        rename_str = "rename=(" + rename + ") "
    else:
        rename_str = ""

    if drop:
        drop_str = "drop=" + drop 
    else:
        drop_str = ""
        
    if keep:
        keep_str = "keep=" + keep 
    else:
        keep_str = ""

    sas_code = sas_template % (libname_stmt, sas_schema, table_name, drop_str,
                               keep_str, rename_str)
    return sas_code
                             

def get_table_sql(table_name, schema, wrds_id=None, fpath=None, 
                  rpath=None, drop=None, keep=None, rename=None, return_sql=True, 
                  alt_table_name=None, col_types=None, sas_schema=None):
                      
    if not alt_table_name:
        alt_table_name = table_name
        
    sas_code = make_sas_code(table_name=table_name, 
                             schema = schema, wrds_id=wrds_id, fpath=fpath, 
                             rpath=rpath, drop=drop, keep=keep, rename=rename, 
                             sas_schema=sas_schema)
    
    # Run the SAS code on the WRDS server and get the result
    df = sas_to_pandas(sas_code, wrds_id, fpath)
    
    # Make all variable names lower case, get
    # inferred types, then set explicit types if given
    names = [name.lower() for name in df['name']]
    types = df.apply(code_row, axis=1)
    col_types_inferred = dict(zip(names, types))
    if col_types:
        for var in col_types.keys():
            col_types_inferred[var] = col_types[var]
    
    rows_str = ", ".join(['"' + name + '" ' + 
                          col_types_inferred[name] for name in names])
    
    make_table_sql = 'CREATE TABLE "' + schema + '"."' + alt_table_name + '" (' + \
                      rows_str + ')'
    
    if return_sql:
        return {"sql":make_table_sql, 
                "names":names,
                "col_types":col_types_inferred}
    else:
        df['name'] = names
        df['postgres_type'] = [col_types_inferred[name] for name in names]
        return df

def get_wrds_sas(table_name, schema, wrds_id=None, fpath=None, rpath=None,
                     drop=None, keep=None, fix_cr = False, 
                     fix_missing = False, obs=None, where=None,
                     rename=None, encoding=None, sas_encoding=None):
    
    make_table_data = get_table_sql(table_name=table_name, schema=schema, 
                                    wrds_id=wrds_id,
                                    fpath=fpath, rpath=rpath, 
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
    elif rpath:
        libname_stmt = "libname %s '%s';" % (schema, rpath)
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

            timestamps = [key for key in col_types if col_types[key]=='timestamp']
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
        
        sas_template = """
            options nosource nonotes;
            %s
            * Fix missing values;
            data %s;
                set %s.%s%s;
                %s
                %s
                %s
            run;

            proc datasets lib=work;
                modify %s; 
                    %s
                    %s
                    %s
            run;

            proc export data=%s(encoding="wlatin1") outfile=stdout dbms=csv;
            run;"""
        sas_code = sas_template % (libname_stmt, new_table, 
                                   schema, sas_table, sas_encoding_str,
                                   fix_cr_code, fix_missing_str, where_str,
                                   new_table,
                                   unformat_str, dates_str, timestamps_str,
                                   new_table)
                                   
                              
    else:

        sas_template = """
            options nosource nonotes;
            %s

            proc export data=%s.%s(%s encoding="wlatin1") outfile=stdout dbms=csv;
            run;"""

        sas_code = sas_template % (libname_stmt, schema, table_name, rename_str) 
    return sas_code        
    
def get_wrds_process(table_name, schema, wrds_id=None, fpath=None, rpath=None,
                     drop=None, keep=None, fix_cr = False, 
                     fix_missing = False, obs=None, rename=None, where=None,
                     encoding=None, sas_encoding=None):
    sas_code = get_wrds_sas(table_name=table_name, wrds_id=wrds_id,
                                    rpath=rpath, fpath=fpath, schema=schema, 
                                    drop=drop, rename=rename, keep=keep, 
                                    fix_cr=fix_cr, fix_missing=fix_missing, 
                                    obs=obs, where=where,
                                    encoding=encoding, sas_encoding=sas_encoding)
    
    p = get_process(sas_code, wrds_id=wrds_id, fpath=fpath)
    return(p)

def wrds_to_pandas(table_name, schema, wrds_id, rename=None, 
                   drop=None, obs=None, encoding=None, fpath=None, rpath=None,
                   col_types=None,
                   where=None, sas_schema=None):

    if not encoding:
        encoding = "utf-8"

    if not sas_schema:
        sas_schema = schema

    p = get_wrds_process(table_name, sas_schema, wrds_id, drop=drop, 
                         rename=rename, obs=obs, where=where, 
                         col_types=col_types,
                         fpath=fpath, rpath=rpath)
    df = pd.read_csv(StringIO(p.read().decode(encoding)))
    df.columns = map(str.lower, df.columns)
    p.close()

    return(df)

def get_modified_str(table_name, sas_schema, wrds_id=wrds_id, encoding=None, rpath=None):
    
    if not encoding:
        encoding = "utf-8"

    if not rpath:
        rpath = sas_schema
    
    sas_code = "proc contents data=" + rpath + "." + table_name + "(encoding='wlatin1');"

    p = get_process(sas_code, wrds_id)
    contents = p.readlines()
    modified = ""

    next_row = False
    for line in contents:
        if next_row:
            line = re.sub(r"^\s+(.*)\s+$", r"\1", line)
            line = re.sub(r"\s+$", "", line)
            if not re.findall(r"Protection", line):
                modified += " " + line.rstrip()
            next_row = False

        if re.match(r"Last Modified", line):
            modified = re.sub(r"^Last Modified\s+(.*?)\s{2,}.*$", r"Last modified: \1", line)
            modified = modified.rstrip()
            next_row = True

    return modified

def get_table_comment(table_name, schema, engine):

    if engine.dialect.has_table(engine.connect(), table_name, schema=schema):
        sql = """SELECT obj_description('"%s"."%s"'::regclass, 'pg_class')""" % (schema, table_name)
        with engine.connect() as conn:
            res = conn.execute(text(sql)).fetchone()[0]
        return(res)
    else:
        return ""
        
def set_table_comment(table_name, schema, comment, engine):

    connection = engine.connect()
    trans = connection.begin()
    sql = """
        COMMENT ON TABLE "%s"."%s" IS '%s'""" % (schema, table_name, comment)

    try:
        res = connection.execute(text(sql))
        trans.commit()
    except:
        trans.rollback()
        raise

    return True

def wrds_to_pg(table_name, schema, engine, wrds_id=None,
               fpath=None, rpath=None, fix_missing=False, fix_cr=False, 
               drop=None, obs=None, rename=None, keep=None, where=None,
               alt_table_name = None, encoding=None, col_types=None, create_roles=True,
               sas_schema=None, sas_encoding=None):

    if not alt_table_name:
        alt_table_name = table_name
    
    if not sas_schema:
        sas_schema = schema
        
    make_table_data = get_table_sql(table_name=table_name, wrds_id=wrds_id,
                                    rpath=rpath, fpath=fpath, schema=schema, 
                                    drop=drop, rename=rename, keep=keep,
                                    alt_table_name=alt_table_name, 
                                    col_types=col_types,
                                    sas_schema=sas_schema)

    process_sql('DROP TABLE IF EXISTS "' + schema + '"."' + alt_table_name + '" CASCADE', 
                engine)
        
    # Create schema (and associated role) if necessary
    insp = inspect(engine)
    if not schema in insp.get_schema_names():
        process_sql("CREATE SCHEMA " + schema, engine)
            
        if create_roles:
            if not role_exists(engine, schema):
                create_role(engine, schema)
            process_sql("ALTER SCHEMA " + schema + " OWNER TO " + schema, engine)
            if not role_exists(engine, "%s_access" % schema):
                create_role(engine, "%s_access" % schema)
            process_sql("GRANT USAGE ON SCHEMA " + schema + " TO " + 
                         schema + "_access", engine)
    process_sql(make_table_data["sql"], engine)
    
    now = strftime("%H:%M:%S", gmtime())
    print("Beginning file import at %s." % now)
    print("Importing data into %s.%s" % (schema, alt_table_name))
    p = get_wrds_process(table_name=table_name, fpath=fpath, rpath=rpath,
                                 schema=sas_schema, wrds_id=wrds_id,
                                 drop=drop, keep=keep, fix_cr=fix_cr, fix_missing=fix_missing, 
                                 obs=obs, rename=rename, where=where,
                                 sas_encoding=sas_encoding)

    res = wrds_process_to_pg(alt_table_name, schema, engine, p, encoding)
    now = strftime("%H:%M:%S", gmtime())
    print("Completed file import at %s." % now)

    return res

def wrds_process_to_pg(table_name, schema, engine, p, encoding=None):
    # The first line has the variable names ...
    
    if not encoding:
        encoding = "UTF8"
    
    var_names = p.readline().rstrip().lower().split(sep=",")
    
    # ... the rest is the data
    copy_cmd =  "COPY " + schema + "." + table_name + ' ("' + '", "'.join(var_names) + '")'
    copy_cmd += " FROM STDIN CSV ENCODING '%s'" % encoding
    
    with engine.connect() as conn:
        connection_fairy = conn.connection
        try:
            with connection_fairy.cursor() as curs:
                curs.execute("SET DateStyle TO 'ISO, MDY'")
                curs.copy_expert(copy_cmd, p)
                curs.close()
        finally:
            connection_fairy.commit()
            conn.close()
            p.close()
    return True

def wrds_update(table_name, schema, 
                host=os.getenv("PGHOST"),
                wrds_id=os.getenv("WRDS_ID"), 
                dbname=os.getenv("PGDATABASE"), 
                engine=None, 
                force=False, 
                fix_missing=False, fix_cr=False, drop=None, keep=None, 
                obs=None, rename=None, where=None,  
                alt_table_name=None, 
                col_types=None, create_roles=True,
                encoding=None, sas_schema=None, sas_encoding=None,
                rpath=None, fpath=None,):
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
        Set to `True` when the SAS file contains unquoted carriage returns that would
        otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
            
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

    rpath: string [Optional]
        Path to local SAS file. Requires local SAS.

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
          
    if not sas_schema:
        sas_schema = schema
        
    if not alt_table_name:
        alt_table_name = table_name
        
    if not engine:
        if not (host and dbname):
            print("Error: Missing connection variables. Please specify engine or (host, dbname).")
            quit()
        else:
            engine = create_engine("postgresql://" + host + "/" + dbname) 
    if wrds_id:
        # 1. Get comments from PostgreSQL database
        comment = get_table_comment(alt_table_name, schema, engine)
        
        # 2. Get modified date from WRDS
        modified = get_modified_str(table_name, sas_schema, wrds_id, 
                                    encoding=encoding, rpath=rpath)
    else:
        comment = 'Updated on ' + strftime("%Y-%m-%d %H:%M:%S", gmtime())
        modified = comment
        # 3. If updated table available, get from WRDS
    if modified == comment and not force and not fpath:
        print(schema + "." + alt_table_name + " already up to date")
        return False
    elif modified == "" and not force:
        print("WRDS flaked out!")
        return False
    else:
        if fpath:
            print("Importing local file.")
        elif force:
            print("Forcing update based on user request.")
        else:
            print("Updated %s.%s is available." % (schema, table_name))
            print("Getting from WRDS.\n")
        wrds_to_pg(table_name=table_name, schema=schema, engine=engine, 
                   wrds_id=wrds_id,
                   rpath=rpath, fpath=fpath, fix_missing=fix_missing, fix_cr=fix_cr,
                   drop=drop, keep=keep, obs=obs, rename=rename,
                   alt_table_name=alt_table_name,
                   encoding=encoding, col_types=col_types, create_roles=create_roles,
                   where=where, sas_schema=sas_schema, sas_encoding=sas_encoding)
        set_table_comment(alt_table_name, schema, modified, engine)
        
        if create_roles:
            if not role_exists(engine, schema):
                create_role(engine, schema)
            
            sql = r"""
                ALTER TABLE "%s"."%s" OWNER TO %s""" % (schema, alt_table_name, schema)
            with engine.connect() as conn:
                conn.execute(text(sql))

            if not role_exists(engine, "%s_access" % schema):
                create_role(engine, "%s_access" % schema)
                
            sql = r"""
                GRANT SELECT ON "%s"."%s"  TO %s_access""" % (schema, alt_table_name, schema)
            res = process_sql(sql, engine)
        else:
            res = True

        return res

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
        dbname = getenv("PGDATABASE")
    if not host:
        host = getenv("PGHOST", "localhost")
    if not wrds_id:
        wrds_id = getenv("WRDS_ID")
    
    engine = create_engine("postgresql://" + host + "/" + dbname)
    return engine
  
def role_exists(engine, role):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM pg_roles WHERE rolname='%s'" % role))
        rs = [r[0] for r in res]
    
    return rs[0] > 0

def create_role(engine, role):
    process_sql("CREATE ROLE %s" % role, engine)
    return True

def get_wrds_tables(schema, wrds_id=None):

    from sqlalchemy import MetaData

    if not wrds_id:
        wrds_id = getenv("WRDS_ID")

    wrds_engine = create_engine("postgresql://%s@wrds-pgdata.wharton.upenn.edu:9737/wrds" % wrds_id,
                                connect_args = {'sslmode':'require'})

    metadata = MetaData(wrds_engine, schema=schema)
    metadata.reflect(schema=schema, autoload=True)
  
    table_list = [key.name for key in metadata.tables.values()]
    wrds_engine.dispose()
    return table_list

def get_pq_file(table_name, schema, data_dir=os.getenv("DATA_DIR"), 
                sas_schema=None):
    data_dir = os.path.expanduser(data_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    if not sas_schema:
        sas_schema = schema

    schema_dir = Path(data_dir, schema)

    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)
    pq_file = Path(data_dir, schema, table_name).with_suffix('.parquet')
    return pq_file

def wrds_update_pq(table_name, schema, 
                   wrds_id=wrds_id, 
                   data_dir=os.getenv("DATA_DIR"),
                   force=False, 
                   fix_missing=False, 
                   fix_cr=False, drop=None, keep=None, 
                   obs=None, rename=None, 
                   where=None,
                   alt_table_name=None,
                   col_types=None,
                   encoding="utf-8", 
                   sas_schema=None, 
                   sas_encoding=None):
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
        Set to `True` when the SAS file contains unquoted carriage returns that would
        otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
            
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
    >>> wrds_update_csv("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update_csv("feed21_bankruptcy_notification", 
                        "audit", drop="match: closest: prior:")
    """
    if not sas_schema:
        sas_schema = schema
        
    if not alt_table_name:
        alt_table_name = table_name
    
    pq_file = get_pq_file(table_name=alt_table_name, schema=schema, 
                          data_dir=data_dir, sas_schema=sas_schema)
                
    modified = get_modified_str(table_name=table_name, 
                                sas_schema=sas_schema, wrds_id=wrds_id, 
                                encoding=encoding)
    
    pq_modified = get_modified_pq(pq_file)
        
    if modified == pq_modified and not force:
        print(schema + "." + alt_table_name + " already up to date")
        return False
    if force:
        print("Forcing update based on user request.")
    else:
        print("Updated %s.%s is available." % (schema, alt_table_name))
        print("Getting from WRDS.\n")
    now = strftime("%H:%M:%S", gmtime())
    print("Beginning file download at %s." % now)
    print("Saving data to temporary CSV.")
    csv_file = tempfile.NamedTemporaryFile(suffix = ".csv.gz").name
    wrds_to_csv(table_name, schema, csv_file, 
                wrds_id=wrds_id, 
                fix_missing=fix_missing, 
                fix_cr=fix_cr, 
                drop=drop, keep=keep, 
                obs=obs, rename=rename,
                encoding=encoding, 
                where=where,
                sas_schema=sas_schema, 
                sas_encoding=sas_encoding)
    print("Converting temporary CSV to parquet.")
    make_table_data = get_table_sql(table_name=table_name, wrds_id=wrds_id,
                                    schema=sas_schema, 
                                    drop=drop, rename=rename, keep=keep, 
                                    col_types=col_types)

    col_types = make_table_data["col_types"]
    names = make_table_data["names"]
    csv_to_pq(csv_file, pq_file, names, col_types, modified)
    print("Parquet file: " + str(pq_file))
    now = strftime("%H:%M:%S", gmtime())
    print("Completed creation of parquet file at %s." % now)
    return True

def wrds_csv_to_pq(table_name, schema, csv_file, pq_file, 
                   col_types=None,
                   wrds_id=os.getenv("WRDS_ID"),
                   modified='',
                   row_group_size = 1048576):
    make_table_data = get_table_sql(table_name=table_name, wrds_id=wrds_id,
                                    schema=sas_schema, 
                                    drop=drop, rename=rename, keep=keep, 
                                    col_types=col_types)

    col_types = make_table_data["col_types"]
    names = make_table_data["names"]
    csv_to_pq(csv_file, pq_file, names, col_types, 
              modified=modified, 
              row_group_size=row_group_size)

def wrds_to_csv(table_name, schema, csv_file, 
                wrds_id=os.getenv("WRDS_ID"), 
                fix_missing=False, fix_cr=False, drop=None, keep=None, 
                obs=None, rename=None, where=None,
                encoding="utf-8", 
                sas_schema=None, sas_encoding=None):
          
    if not sas_schema:
        sas_schema = schema
    p = get_wrds_process(table_name=table_name, 
                         schema=sas_schema, wrds_id=wrds_id,
                         drop=drop, keep=keep, fix_cr=fix_cr, 
                         fix_missing=fix_missing, 
                         obs=obs, rename=rename,
                         where=where,
                         encoding=encoding, sas_encoding=sas_encoding)
    with gzip.GzipFile(csv_file, mode='wb') as f:
        shutil.copyfileobj(p, f)
        f.close()
        
def get_modified_pq(file_name):
    
    if os.path.exists(file_name):
        md = pq.read_schema(file_name)
        schema_md = md.metadata
        if not schema_md:
            return ''
        if b'last_modified' in schema_md.keys():
            last_modified = schema_md[b'last_modified'].decode('utf-8')
        else:
            last_modified = ''
    else:
        last_modified = ''
    return last_modified

def csv_to_pq(csv_file, pq_file, names, col_types, modified,
              row_group_size = 1048576):
    with duckdb.connect() as con:
        df = con.from_csv_auto(csv_file,
                               compression = "gzip",
                               names = names,
                               header = True,
                               dtype = col_types)
        df_arrow = df.arrow()
        my_metadata = df_arrow.schema.with_metadata({b'last_modified': modified.encode()})
        to_write = df_arrow.cast(my_metadata)
        pq.write_table(to_write, pq_file, row_group_size = row_group_size)

def modified_encode(last_modified):
    date_time_str = last_modified.split("Last modified: ")[1]
    mtime = datetime \
            .strptime(date_time_str, "%m/%d/%Y %H:%M:%S") \
            .replace(tzinfo=ZoneInfo("America/Chicago")) \
            .astimezone(timezone.utc) \
            .timestamp()
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
    last_modified = utc_dt \
                      .astimezone(ZoneInfo("America/Chicago")) \
                      .strftime("Last modified: %m/%d/%Y %H:%M:%S")
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

def wrds_update_csv(table_name, schema,  
                    wrds_id=os.getenv("WRDS_ID"), 
                    data_dir=os.getenv("CSV_DIR"),
                    force=False, fix_missing=False, fix_cr=False,
                    drop=None, keep=None, obs=None, rename=None,
                    where=None, alt_table_name=None,
                    encoding=None,
                    sas_schema=None, sas_encoding=None):
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
        Set to `True` when the SAS file contains unquoted carriage returns that would
        otherwise produce `BadCopyFileFormat`.
        Default is `False`.
    
    drop: string [Optional]
        SAS code snippet indicating variables to be dropped.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
        
    keep: string [Optional]
        SAS code snippet indicating variables to be retained.
        Multiple variables should be separated by spaces and SAS wildcards can be used.
        See examples below.
            
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
    This should mean that a CSV file was created.
    
    Examples
    ----------
    >>> wrds_update_csv("dsi", "crsp", drop="usdval usdcnt")
    >>> wrds_update_csv("feed21_bankruptcy_notification", 
                        "audit", drop="match: closest: prior:")
    """

    if not alt_table_name:
        alt_table_name = table_name
    
    if not encoding:
        encoding = "utf-8"

    if not sas_schema:
        sas_schema = schema
        
    schema_dir = Path(data_dir, schema)
    
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir)
    
    csv_file = Path(data_dir, schema, alt_table_name).with_suffix('.csv.gz')
    modified = get_modified_str(table_name, sas_schema, wrds_id)
    
    if os.path.exists(csv_file):
        csv_modified = get_modified_csv(csv_file)
    else:
        csv_modified = ""
    if modified == csv_modified and not force:
        print(schema + "." + table_name + " already up to date")
        return False
    if force:
        print("Forcing update based on user request.")
    else:
        print("Updated %s.%s is available." % (schema, table_name))
        print("Getting from WRDS.\n")
    now = strftime("%H:%M:%S", gmtime())
    print("Beginning file download at %s." % now)
    wrds_to_csv(table_name=table_name, 
                schema=schema, 
                csv_file=csv_file,
                wrds_id=wrds_id, 
                fix_missing=fix_missing, 
                fix_cr=fix_cr,
                drop=drop, keep=keep,
                obs=obs, rename=rename,
                where=where,
                encoding=encoding,
                sas_schema=sas_schema, 
                sas_encoding=sas_encoding)
    set_modified_csv(csv_file, modified)
    now = strftime("%H:%M:%S", gmtime())
    print("Completed file download at %s." % now)
    return True
