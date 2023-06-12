from sqlalchemy import Column, DateTime, Identity, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Fintransact(Base):  # type: ignore
    __tablename__ = "fintransacts"
    __table_args__ = {"schema": "fooproject"}
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True)
    last_transaction_date = Column(DateTime())
    description = Column(String(200))


class Ingestion(Base):  # type: ignore
    __tablename__ = "ingestions"
    __table_args__ = {"schema": "datalake"}
    id = Column(Integer, Identity(start=1, cycle=True), primary_key=True)
    last_transaction_date = Column(DateTime())
    ingestion_date = Column(DateTime(), server_default=func.current_timestamp())
