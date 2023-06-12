create schema if not exists fooproject;
create table if not exists fooproject.fintransacts(
	 id Integer Primary Key Generated Always as Identity,
     last_transaction_date TIMESTAMP not Null,
     description Varchar(200)
	);

CREATE USER dataengineer WITH PASSWORD 'dataengineer123';
GRANT all PRIVILEGES ON DATABASE financedb TO dataengineer;
GRANT USAGE ON SCHEMA fooproject TO dataengineer ;
GRANT ALL PRIVILEGES on ALL TABLES IN schema fooproject TO dataengineer;