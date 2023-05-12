from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from tests.integration.conftest import FixtureDataBase, FixtureMinio
from tests.integration.dag_trigger import run_dag

SAMPLE_DAG_ID = "sample"

def test_ingestion_happens_succesfully():

   with FixtureDataBase():
       with FixtureMinio() as minio_fixture:
           ingestion_happens_succesfully(minio_fixture)


def ingestion_happens_succesfully(minio_fixture):
    engine: Engine = create_engine(
        url='mssql+pyodbc://testnclogin:ncuser123!!@127.0.0.1:1433/testncintegration?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server', connect_args={'autocommit': True}
    )

    engine.execute("""
    insert into  ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
    """)

    run_dag(SAMPLE_DAG_ID)

    res = engine.execute("""
            select * from ncproject.ingestions ;
            """)

    ingestion_date = res.all()[0][2]

    # we are testing that way because we may have spurious double runs
    assert len(list(minio_fixture.list_objects("integration-bucket", f"ingestions/{ingestion_date}", recursive=True))) == 1


def test_no_need_to_ingest():

   with FixtureDataBase():
       with FixtureMinio() as minio_fixture:
           no_need_to_ingest(minio_fixture)

def no_need_to_ingest(minio_fixture):
    engine: Engine = create_engine(
        url='mssql+pyodbc://testnclogin:ncuser123!!@127.0.0.1:1433/testncintegration?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server', connect_args={'autocommit': True}
    )

    engine.execute("""
        insert into ncproject.ingestions values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), '1682591431.352781');
        """)

    engine.execute("""
    insert into ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
    """)

    run_dag(SAMPLE_DAG_ID)

    assert len(list(minio_fixture.list_objects("integration-bucket", None, recursive=True))) == 0
