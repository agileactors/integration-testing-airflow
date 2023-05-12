import io
import logging
from datetime import datetime
from typing import List

from airflow.models import BaseOperator, Variable, Connection
from airflow.models.taskinstance import Context
from minio import Minio
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from airflow.hooks.base import BaseHook


class IngestDataOperator(BaseOperator):
    def __init__(self,*args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: Context):
        connections: List[Connection] = BaseHook.get_connections('mssql_db')
        sqlalchemy_url = connections[0].get_uri()

        logging.info(f"SQLALCHEMY URL IS {sqlalchemy_url}")

        engine = create_engine(
            url=sqlalchemy_url, connect_args={'autocommit': True}
        )

        do_something(engine)



def do_something(engine: Engine):

    try:
        res = engine.execute("SELECT max(last_transaction_date) from ncproject.fintransacts")
        latest_transactions = res.all()
        logging.info(latest_transactions)
        if latest_transactions:
            latest_transaction_date = latest_transactions[0][0]
        else:
            latest_transaction_date = None

        res2 = engine.execute("SELECT * from ncproject.ingestions where last_transaction_date in (select max(last_transaction_date) from ncproject.ingestions)")

        top_ingestions = res2.all()
        if top_ingestions:
            top_ingestion = top_ingestions[0]
        else:
            top_ingestion = None

        logging.info(latest_transaction_date)
        logging.info(top_ingestion)
        if top_ingestion is None:
            if latest_transaction_date is not None:
                last_transaction_date = latest_transaction_date.strftime("%Y-%m-%d %H:%M:%S")
                last_ingestion_date = str(datetime.timestamp(datetime.now()))


                res3 = engine.execute("SELECT * from ncproject.fintransacts")
                retrieved_data = [dict(row._mapping) for row in res3.all()]
                save_dump(retrieved_data, last_ingestion_date)

                insert_data(engine, last_transaction_date, last_ingestion_date, 1)
            else:
                logging.info("No data")
        else:
            if latest_transaction_date is not None and latest_transaction_date > top_ingestion[1]:
                last_transaction_date = latest_transaction_date.strftime("%Y-%m-%d %H:%M:%S")
                last_ingestion_date = str(datetime.timestamp(datetime.now()))

                retrieve_query = f"""
                    SELECT * from ncproject.fintransacts 
                    where last_transaction_date 
                    between convert(DATETIME, '{top_ingestion[1]}', 20) and convert(DATETIME, '{last_transaction_date}', 20)
                """

                logging.info(f"Retrieve query is {retrieve_query}")

                res3 = engine.execute(retrieve_query)
                retrieved_data = [dict(row._mapping) for row in res3.all()]

                save_dump(retrieved_data, last_ingestion_date)
                insert_data(engine, last_transaction_date, last_ingestion_date, top_ingestion[0] + 1)

            else:
                logging.info("Nothing new")



    except Exception as ex:
        logging.info("Connection could not be made due to the following error: \n", ex)



def save_dump(retrieved_data: str, last_ingestion_date: str):
    minio_bucket = str(Variable.get('minio_bucket'))
    connections: List[Connection] = BaseHook.get_connections('minio_store')

    logging.info(f"Minio host {connections[0].extra_dejson.get('endpoint_url').replace('http://','')}")
    client = Minio(
        connections[0].extra_dejson.get("endpoint_url").replace('http://',''),
        secure=False,
        access_key=connections[0].login,
        secret_key=connections[0].password,
    )
    logging.info(f"Data is {retrieved_data}")
    byte_buf = bytes(str(retrieved_data), 'utf-8')
    result = client.put_object(
        minio_bucket, f"ingestions/{last_ingestion_date}/mydump", io.BytesIO(byte_buf), len(byte_buf),
        content_type="text/plain; charset=utf-8",
    )
    logging.info(
        "created {0} object; etag: {1}, version-id: {2}".format(
            result.object_name, result.etag, result.version_id,
        ),
    )


def insert_data(engine: Engine, last_transaction_date: str, last_ingestion_date: str, next_one: int):
    ingestion_query = f"""
                    insert into ncproject.ingestions values ({next_one}, convert(DATETIME, '{last_transaction_date}', 20), '{last_ingestion_date}');
                    """
    logging.info(f"Ingestion query is {ingestion_query}")
    logging.info(f"Timestamp is {last_ingestion_date}")
    engine.execute(ingestion_query)