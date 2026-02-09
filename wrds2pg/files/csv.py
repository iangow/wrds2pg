from __future__ import annotations

import gzip
import os
import shutil
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ..sas.stream import get_wrds_process_stream

_WRDS_TZ = ZoneInfo("America/Chicago")

def modified_encode(last_modified: str) -> float:
    prefix = "Last modified: "
    if not last_modified.startswith(prefix):
        raise ValueError(f"Unexpected modified string: {last_modified!r}")
    date_time_str = last_modified[len(prefix):]
    return (
        datetime.strptime(date_time_str, "%m/%d/%Y %H:%M:%S")
        .replace(tzinfo=_WRDS_TZ)
        .astimezone(timezone.utc)
        .timestamp()
    )

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
