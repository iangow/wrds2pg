from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine import CursorResult

def get_table_comment(table_name: str, schema: str, engine: Engine) -> str:
    """Return the table comment from pg_class, or '' if none exists."""
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return ""

    sql = text(
        """
        SELECT obj_description(
            to_regclass(quote_ident(:schema) || '.' || quote_ident(:table)),
            'pg_class'
        )
        """
    )

    with engine.connect() as conn:
        return conn.execute(sql, {"schema": schema, "table": table_name}).scalar() or ""


def set_table_comment(table_name: str, schema: str, comment: str, engine: Engine) -> None:
    """
    Set table comment safely.

    Uses a parameter for the comment to avoid quoting issues.
    """
    sql = text(f'COMMENT ON TABLE "{schema}"."{table_name}" IS :comment')

    with engine.begin() as conn:
        conn.execute(sql, {"comment": comment})


def process_sql(sql: str, engine: Engine) -> CursorResult:
    """
    Execute SQL inside a transaction.

    Commits on success, rolls back on error.
    Intended for DDL / administrative SQL.
    """
    with engine.begin() as conn:
        return conn.execute(text(sql))


def role_exists(engine: Engine, role: str) -> bool:
    sql = text("SELECT 1 FROM pg_roles WHERE rolname = :role LIMIT 1")
    with engine.connect() as conn:
        return conn.execute(sql, {"role": role}).first() is not None


def create_table_sql(schema: str, table_name: str, names: list[str], col_types: dict[str, str]) -> str:
    cols = ", ".join([f'"{n}" {col_types[n]}' for n in names])
    return f'CREATE TABLE "{schema}"."{table_name}" ({cols})'


def create_role(engine: Engine, role: str) -> None:
    # role is an identifier; quote it
    process_sql(f'CREATE ROLE "{role}"', engine)
