from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def make_engine(
    host: str | None = None,
    dbname: str | None = None,
    port: int | None = None,
) -> Engine:
    """
    Create a SQLAlchemy Engine for a PostgreSQL database.

    Parameters
    ----------
    host : str, optional
        PostgreSQL host. Defaults to PGHOST.
    dbname : str, optional
        Database name. Defaults to PGDATABASE.
    port : int, optional
        PostgreSQL port. Defaults to PGPORT or 5432.

    Notes
    -----
    Authentication is handled by libpq/psycopg defaults
    (PGUSER, PGPASSWORD, .pgpass, Kerberos, etc.).

    The connection URL intentionally does not embed credentials.
    """
    host = host or os.environ.get("PGHOST")
    dbname = dbname or os.environ.get("PGDATABASE")
    port = port or int(os.environ.get("PGPORT", 5432))

    if not host or not dbname:
        raise ValueError(
            "Specify `host` and `dbname` or set PGHOST and PGDATABASE."
        )

    return create_engine(f"postgresql+psycopg://{host}:{port}/{dbname}")
