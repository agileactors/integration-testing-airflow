import logging
import os

import pytest
import requests
from minio import Minio
from minio.deleteobjects import DeleteObject
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from tests.integration.db_connection import get_connection
from tests.integration.airflow_api import AirflowAPI
from tests.integration.db_connection import setup_database, teardown_database


@pytest.fixture
def wait_for_airflow() -> requests.Session:
    api_url = f"http://localhost:8080/health"
    return assert_container_is_ready(api_url)


@pytest.fixture
def airflow_api():
    return AirflowAPI()


def assert_container_is_ready(readiness_check_url) -> requests.Session:
    request_session = requests.Session()
    retries = Retry(
        total=20,
        backoff_factor=0.2,
        status_forcelist=[404, 500, 502, 503, 504],
    )
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    assert request_session.get(readiness_check_url)
    return request_session


class TempComposeFile(object):
    def __init__(self):
        self.lines = []
        with open("docker-compose.yaml") as f:
            for line in f:
                if "AIRFLOW_VAR_MINIO_BUFFER" in line:
                    self.lines.append(
                        "    AIRFLOW_VAR_MINIO_BUFFER: 'integration-bucket'\n")
                elif "AIRFLOW_VAR_MSSQL_STORE" in line:
                    self.lines.append(
                        "    AIRFLOW_VAR_MSSQL_STORE: 'mssql+pyodbc://testnclogin:ncuser123!!@mssql:1433/testncintegration?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server'\n")
                else:
                    self.lines.append(line)
    def __enter__(self):
        with open('other_compose.yaml', 'w') as f:
            f.writelines(self.lines)
    def __exit__(self, sometype, value, traceback):
        pass
        os.unlink('other_compose.yaml')


class FixtureDataBase(object):
    def __init__(self):
        self.engine = get_connection()
        res = self.engine.execute("SELECT * FROM sys.databases WHERE name = N'testncintegration'")
        if res:
            res = res.all()

        if not res:
            self.engine.execute("CREATE DATABASE testncintegration")
    def __enter__(self):
        print('Setting up database')
        setup_database(self.engine)
    def __exit__(self, sometype, value, traceback):
        print('Tearing down database')
        teardown_database(self.engine)


class FixtureMinio(object):
    def __init__(self):
        self.client = Minio(
            "127.0.0.1:9000",
            secure=False,
            access_key="minio_access_key",
            secret_key="minio_secret_key",
        )
    def __enter__(self):
        print('Setting up buffer')
        if self.client.bucket_exists("integration-bucket"):
            self.delete_bucket()
        self.client.make_bucket("integration-bucket")
        return self.client

    def __exit__(self, sometype, value, traceback):
        print('Tearing down buffer')
        if self.client.bucket_exists("integration-bucket"):
            self.delete_bucket()


    def delete_bucket(self):
        delete_object_list = map(
            lambda x: DeleteObject(x.object_name),
            self.client.list_objects("integration-bucket", None, recursive=True),
        )
        errors = self.client.remove_objects("integration-bucket", delete_object_list)
        for error in errors:
            logging.error("error occurred when deleting object", error)

        self.client.remove_bucket("integration-bucket")