"""ingestions database

Revision ID: 118a422f7dbb
Revises: 
Create Date: 2023-06-09 12:59:05.405868

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '118a422f7dbb'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        create schema if not exists datalake;
        create table if not exists datalake.ingestions(
             id Integer Primary Key Generated Always as Identity,
             last_transaction_date TIMESTAMP not Null,
             ingestion_date TIMESTAMP not Null DEFAULT CURRENT_TIMESTAMP
            );
        
        CREATE USER ingestionuser WITH PASSWORD 'ingestion123';
        GRANT ALL PRIVILEGES ON DATABASE ingestiondb TO ingestionuser;
        GRANT USAGE ON SCHEMA datalake TO ingestionuser ;
        GRANT ALL PRIVILEGES on datalake.ingestions TO ingestionuser;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE ALL PRIVILEGES on datalake.ingestions FROM ingestionuser;
        REVOKE USAGE ON SCHEMA datalake TO ingestionuser ;
        drop table if exists datalake.ingestions;
        drop schema if exists datalake;
        
        REVOKE ALL PRIVILEGES ON DATABASE ingestiondb FROM ingestionuser;
        drop user ingestionuser;
        """
    )
