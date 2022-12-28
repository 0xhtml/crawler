"""Code for the database."""

import sqlalchemy
from sqlalchemy import Column, String, Table

ENGINE = sqlalchemy.create_engine("sqlite+pysqlite:///data.db")
METADATA = sqlalchemy.MetaData()

DOCUMENTS_TABLE = Table(
    "documents",
    METADATA,
    Column("url", String, primary_key=True),
    Column("content", String),
)

if __name__ == "__main__":
    METADATA.create_all(ENGINE)
