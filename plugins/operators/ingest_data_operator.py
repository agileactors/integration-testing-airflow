import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, cast

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Connection, Variable
from airflow.models.taskinstance import Context
from minio import Minio
from operators.models import Fintransact, Ingestion
from sqlalchemy import create_engine, func, orm
from sqlalchemy.orm import Session


def create_session_from_connection(conn_id: str) -> Session:
    connections: List[Connection] = BaseHook.get_connections(conn_id)
    sqlalchemy_url = connections[0].get_uri()
    engine = create_engine(url=sqlalchemy_url)
    session_maker = orm.sessionmaker(bind=engine)
    return session_maker()


def retrieve_last_transaction_date(finances_session: Session) -> Optional[datetime]:
    result: Optional[Any] = finances_session.query(
        func.max(Fintransact.last_transaction_date)
    ).scalar()
    logging.info(f"Fintransacts said {result}")

    last_transaction_date: Optional[datetime] = cast(Optional[datetime], result)
    logging.info(last_transaction_date)
    return last_transaction_date


def retrieve_top_ingestion(ingestion_session: Session) -> Optional[Ingestion]:
    result = ingestion_session.query(func.max(Ingestion.ingestion_date)).scalar()
    logging.info(f"Ingestions said {result}")

    top_ingestion: Optional[Ingestion] = None

    if result:
        top_ingestion = (
            ingestion_session.query(Ingestion)
            .filter(Ingestion.ingestion_date == result)
            .scalar()
        )

    logging.info(top_ingestion)
    return top_ingestion


def list_unprocessed_transactions(
    last_transaction_date: datetime,
    top_ingestion: Optional[Ingestion],
    finances_session: Session,
) -> List[Fintransact]:
    fintrasacts: List[Fintransact] = []
    if not top_ingestion:
        fintrasacts = finances_session.query(Fintransact).all()
    else:
        if last_transaction_date > top_ingestion.last_transaction_date:
            fintrasacts = (
                finances_session.query(Fintransact)
                .filter(
                    Fintransact.last_transaction_date
                    > top_ingestion.last_transaction_date
                )
                .filter(Fintransact.last_transaction_date <= last_transaction_date)
                .all()
            )
        else:
            logging.info("Nothing new")

    return fintrasacts


class IngestDataOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        finances_session = create_session_from_connection("finances_db")
        ingestion_session = create_session_from_connection("ingestions_db")
        try:
            last_transaction_date: Optional[datetime] = retrieve_last_transaction_date(
                finances_session
            )

            if last_transaction_date is None:
                logging.info("No data")
                return

            top_ingestion: Optional[Ingestion] = retrieve_top_ingestion(
                ingestion_session
            )

            fintrasacts: List[Fintransact] = list_unprocessed_transactions(
                last_transaction_date, top_ingestion, finances_session
            )

            dump_to_cloud_storage(fintrasacts, ingestion_session, last_transaction_date)

        except Exception as ex:
            logging.info(
                "Connection could not be made due to the following error: \n", ex
            )


def save_dump(retrieved_data: List[Dict[str, Any]], last_ingestion_date: datetime):
    minio_bucket = str(Variable.get("minio_bucket"))
    connections: List[Connection] = BaseHook.get_connections("minio_store")
    endpoint_url = connections[0].extra_dejson.get("endpoint_url")

    if endpoint_url is None:
        logging.error("Minio host has not endpoint_url in extras")
    else:
        minio_host = endpoint_url.replace("http://", "")

        logging.info(f"Minio host {minio_host}")

        client = Minio(
            minio_host,
            secure=False,
            access_key=connections[0].login,
            secret_key=connections[0].password,
        )
        logging.info(f"Data is {retrieved_data}")
        byte_buf = bytes(str(retrieved_data), "utf-8")
        result = client.put_object(
            minio_bucket,
            f"ingestions/ts={last_ingestion_date}/mydump",
            io.BytesIO(byte_buf),
            len(byte_buf),
            content_type="text/plain; charset=utf-8",
        )
        logging.info(
            "created {0} object; etag: {1}, version-id: {2}".format(
                result.object_name,
                result.etag,
                result.version_id,
            ),
        )


def dump_to_cloud_storage(
    fintrasacts: List[Fintransact],
    ingestion_session: Session,
    last_transaction_date: datetime,
):
    if fintrasacts:
        ingestion = Ingestion(last_transaction_date=last_transaction_date)
        ingestion_session.add(ingestion)
        ingestion_session.flush()
        ingestion_session.refresh(ingestion)

        logging.info(f"Ingestion timestamp is {ingestion.ingestion_date}")
        retrieved_data: List[Dict[str, Any]] = [
            fintrasact.__dict__ for fintrasact in fintrasacts
        ]
        save_dump(retrieved_data, ingestion.ingestion_date)

        ingestion_session.commit()
