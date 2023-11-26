"""Code for the database."""

import sqlalchemy
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base, sessionmaker


class Document(declarative_base()):
    """Document in the database."""

    __tablename__ = "documents"

    url = Column(String, primary_key=True)
    content = Column(String)


ENGINE = sqlalchemy.create_engine("sqlite+pysqlite:///data.db")
Session = sessionmaker(bind=ENGINE)

if __name__ == "__main__":
    session = Session()
    Document.metadata.create_all(ENGINE)
    session.close()
