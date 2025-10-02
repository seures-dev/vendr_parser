import os
from pathlib import Path


from .awriter import AWriter
from .postgre_writer import PostgresWriter
from .sqlite_writer import SqliteWriter

__all__ = [
    "AWriter",
    "PostgresWriter",
    "SqliteWriter",
]


def get_writer_batch():
    return int(os.environ.get("DB_WRITER_BATCH", "20"))


def get_db_dsn(use_env=True) -> str:
    """
    return DSN:
    - If env have DSN return it.
    - Else create sql lite database.
    """
    dsn = None
    if use_env:
        dsn = os.environ.get("DB_DSN", None)

    if dsn:
        return dsn

    db_dir = Path(os.getcwd()) / "db"
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / "vendr.db"
    return str(db_path)
