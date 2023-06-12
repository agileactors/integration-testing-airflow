import os

from sqlalchemy import create_engine
from sqlalchemy_utils import functions

from alembic import command  # type: ignore
from alembic.config import Config


def setup_ingestions_db():
    print("Migrating")
    db_url = "postgresql://postgres:example@localhost:5432/ingestiondb"
    functions.create_database(db_url)
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(alembic_cfg, "head")
    print("Migrated")


def teardown_ingestions_db():
    db_url = "postgresql://postgres:example@localhost:5432/ingestiondb"
    functions.drop_database(db_url)


def setup_finances_db():
    db_url = "postgresql://postgres:example@localhost:5432/financedb"
    functions.create_database(db_url)
    with open(f"{os.path.dirname(os.path.abspath(__file__))}/setup_database.sql") as f:
        setup_database_query: str = "".join(f.readlines())
        engine = create_engine(url=db_url)
        engine.execute(setup_database_query)


def teardown_finances_db():
    db_url = "postgresql://postgres:example@localhost:5432/financedb"

    with open(
        f"{os.path.dirname(os.path.abspath(__file__))}/teardown_database.sql"
    ) as f:
        teardown_database_query: str = "".join(f.readlines())
        engine = create_engine(url=db_url)
        engine.execute(teardown_database_query)

    functions.drop_database(db_url)
