import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from plugins.operators.models import Fintransact, Ingestion
from tests.integration.conftest import FixtureDB, FixtureMinio
from tests.integration.dag_trigger import run_dag

SAMPLE_DAG_ID = "sample"


def retrieve_top_ingestion(ingestion_session: Session) -> Optional[Ingestion]:
    result = ingestion_session.query(func.max(Ingestion.ingestion_date)).scalar()

    top_ingestion: Optional[Ingestion] = None

    if result:
        top_ingestion = (
            ingestion_session.query(Ingestion)
            .filter(Ingestion.ingestion_date == result)
            .scalar()
        )

    return top_ingestion


def test_ingestion_happens_succesfully():
    with FixtureMinio() as minio_fixture:
        with FixtureDB() as (ingestions_session, finances_session):
            ingestion_happens_succesfully(
                minio_fixture, ingestions_session, finances_session
            )


def ingestion_happens_succesfully(
    minio_fixture, ingestions_session: Session, finances_session: Session
):
    assert (
        len(list(minio_fixture.list_objects("mybucket", "ingestions/", recursive=True)))
        == 0
    )

    last_transaction_date = datetime.datetime(
        year=1968, month=10, day=23, hour=12, minute=45, second=37
    )

    finstrasact = Fintransact(
        last_transaction_date=last_transaction_date, description="buy 1"
    )
    finances_session.add(finstrasact)
    finances_session.commit()

    # print("Blocking")
    # while True:
    #     time.sleep(10)

    run_dag(SAMPLE_DAG_ID)

    ingestion: Optional[Ingestion] = retrieve_top_ingestion(ingestions_session)

    assert ingestion is not None

    print(f"TS {ingestion.ingestion_date})")

    ingestion_date = ingestion.ingestion_date

    # we are testing that way because we may have spurious double runs
    assert (
        len(
            list(
                minio_fixture.list_objects(
                    "mybucket", f"ingestions/ts={ingestion_date}/", recursive=True
                )
            )
        )
        == 1
    )

    print("Files are")
    for obj in minio_fixture.list_objects(
        "mybucket", f"ingestions/ts={ingestion_date}/", recursive=True
    ):
        print(obj.object_name)


def test_no_need_to_ingest():
    with FixtureMinio() as minio_fixture:
        with FixtureDB() as (ingestions_session, finances_session):
            no_need_to_ingest(minio_fixture, ingestions_session, finances_session)


def no_need_to_ingest(minio_fixture, ingestions_session, finances_session):
    assert (
        len(list(minio_fixture.list_objects("mybucket", "ingestions/", recursive=True)))
        == 0
    )

    last_transaction_date = datetime.datetime(
        year=1968, month=10, day=23, hour=12, minute=45, second=37
    )

    finstrasact = Fintransact(
        last_transaction_date=last_transaction_date, description="buy 1"
    )
    finances_session.add(finstrasact)
    finances_session.commit()

    ingestion = Ingestion(last_transaction_date=last_transaction_date)
    ingestions_session.add(ingestion)
    ingestions_session.commit()

    # print("Blocking")
    # while True:
    #     time.sleep(10)

    run_dag(SAMPLE_DAG_ID)

    assert (
        len(list(minio_fixture.list_objects("mybucket", "ingestions/", recursive=True)))
        == 0
    )
