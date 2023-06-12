import logging
import time
from typing import Tuple

import pytest
from minio import Minio
from minio.deleteobjects import DeleteObject
from sqlalchemy import create_engine, orm
from sqlalchemy.orm import Session
from testcontainers.compose import DockerCompose

from plugins.operators.models import Fintransact, Ingestion
from tests.integration.db_connection import (setup_finances_db,
                                             setup_ingestions_db,
                                             teardown_finances_db,
                                             teardown_ingestions_db)


def cleanup_bucket(minio_client: Minio):
    delete_object_list = map(
        lambda x: DeleteObject(x.object_name),
        minio_client.list_objects("mybucket", "ingestions", recursive=True),
    )
    errors = minio_client.remove_objects("mybucket", delete_object_list)
    for error in errors:
        logging.error("error occurred when deleting object", error)


class FixtureMinio(object):
    def __init__(self):
        self.client = Minio(
            "127.0.0.1:9000",
            secure=False,
            access_key="minio_access_key",
            secret_key="minio_secret_key",
        )

    def __enter__(self):
        return self.client

    def __exit__(self, sometype, value, traceback):
        cleanup_bucket(self.client)


class FixtureDB(object):
    def __init__(self):
        ingestions_engine = create_engine(
            url="postgresql+psycopg2://ingestionuser:ingestion123@localhost:5432/ingestiondb"
        )
        session_maker = orm.sessionmaker(bind=ingestions_engine)
        self.ingestions_session = session_maker()
        finances_engine = create_engine(
            url="postgresql+psycopg2://dataengineer:dataengineer123@localhost:5432/financedb"
        )
        session_maker = orm.sessionmaker(bind=finances_engine)
        self.finances_session = session_maker()

    def __enter__(self) -> Tuple[Session, Session]:
        return (self.ingestions_session, self.finances_session)

    def __exit__(self, sometype, value, traceback):
        self.ingestions_session.query(Ingestion).delete()
        self.ingestions_session.commit()
        self.finances_session.query(Fintransact).delete()
        self.finances_session.commit()
        print("Done")


@pytest.fixture(scope="session", autouse=True)
def auto_resource(request):
    compose = DockerCompose(".", compose_file_name="docker-compose.yaml", pull=True)
    compose.start()
    compose.wait_for("http://localhost:8080/health")

    setup_finances_db()
    setup_ingestions_db()

    client: Minio = Minio(
        "127.0.0.1:9000",
        secure=False,
        access_key="minio_access_key",
        secret_key="minio_secret_key",
    )
    client.make_bucket("mybucket")

    # print("Blocking")
    # while True:
    #     time.sleep(10)

    def auto_resource_fin():
        cleanup_bucket(client)
        client.remove_bucket("mybucket")
        teardown_finances_db()
        teardown_ingestions_db()
        compose.stop()

    request.addfinalizer(auto_resource_fin)
