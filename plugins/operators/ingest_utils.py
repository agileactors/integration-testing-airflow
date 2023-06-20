import io
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, cast

from minio import Minio
from operators.models import Fintransact, Ingestion
from sqlalchemy import func
from sqlalchemy.orm import Session


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


@dataclass
class MinioConfig:
    minio_host: str
    access_key: str
    secret_key: str
    bucket: str


# https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def save_dump(
    minio_config: MinioConfig,
    retrieved_data: List[Dict[str, Any]],
    last_ingestion_date: datetime,
):
    logging.info(f"Minio host {minio_config.minio_host}")
    if minio_config.minio_host is not None:
        logging.error("Minio host has not endpoint_url in extras")

        client = Minio(
            minio_config.minio_host,
            secure=False,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
        )
        logging.info(f"Data is {retrieved_data}")

        str_data = json.dumps(
            retrieved_data, indent=4, sort_keys=True, default=json_serial
        )
        byte_buf = bytes(
            str_data,
            "utf-8",
        )
        result = client.put_object(
            minio_config.bucket,
            f"ingestions/ts={last_ingestion_date}/mydump.json",
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
    minio_config: MinioConfig,
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
        retrieved_data: List[Dict[str, Any]] = []
        for fintrasact in fintrasacts:
            temp: Dict[str, Any] = fintrasact.__dict__
            temp.pop("_sa_instance_state")
            retrieved_data.append(temp)
        save_dump(minio_config, retrieved_data, ingestion.ingestion_date)

        ingestion_session.commit()
