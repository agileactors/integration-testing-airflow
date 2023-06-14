import logging
from datetime import datetime
from typing import List, Optional

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection, Variable
from airflow.models.taskinstance import Context
from operators import ingest_utils
from operators.ingest_utils import MinioConfig
from operators.models import Fintransact, Ingestion
from sqlalchemy import create_engine, orm
from sqlalchemy.orm import Session


def create_session_from_connection(conn_id: str) -> Session:
    connections: List[Connection] = BaseHook.get_connections(conn_id)
    sqlalchemy_url = connections[0].get_uri()
    engine = create_engine(url=sqlalchemy_url)
    session_maker = orm.sessionmaker(bind=engine)
    return session_maker()


class IngestDataOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        finances_session = create_session_from_connection("finances_db")
        ingestion_session = create_session_from_connection("ingestions_db")

        connections: List[Connection] = BaseHook.get_connections("minio_store")
        endpoint_url = connections[0].extra_dejson.get("endpoint_url")

        minio_config = MinioConfig(
            minio_host=endpoint_url.replace("http://", "")
            if endpoint_url is not None
            else None,
            access_key=connections[0].login,
            secret_key=connections[0].password,
            bucket=str(Variable.get("minio_bucket")),
        )
        try:
            last_transaction_date: Optional[
                datetime
            ] = ingest_utils.retrieve_last_transaction_date(finances_session)

            if last_transaction_date is None:
                logging.info("No data")
                return

            top_ingestion: Optional[Ingestion] = ingest_utils.retrieve_top_ingestion(
                ingestion_session
            )

            fintrasacts: List[Fintransact] = ingest_utils.list_unprocessed_transactions(
                last_transaction_date, top_ingestion, finances_session
            )

            ingest_utils.dump_to_cloud_storage(
                minio_config, fintrasacts, ingestion_session, last_transaction_date
            )

        except Exception as ex:
            logging.info(
                "Connection could not be made due to the following error: \n", ex
            )
