# Airflow integration testing


## Background

This code is for tutorial purposes of the Agile Actors Data Chapter. It continues in a more advanced manner from
[Basic integration Testing](https://github.com/fithisux/airflow-integration-testing) in the spirirt of
[Testing in Airflow Part 2 — Integration Tests and End-To-End Pipeline Tests](https://medium.com/@chandukavar/testing-in-airflow-part-2-integration-tests-and-end-to-end-pipeline-tests-af0555cd1a82)

In summary the project sets up an SQL Server where financial transactions are inserted. Airflow scans the table and when it finds new transactions, 
it ingests the transactions in a MinIO store (like S3). Then it marks in another table that the task has been accomplished. It is there for preventing double ingetsions.

## Integration testing

Tested with python 3.11.3.

First install dependencies

```bash
pip install -r requirements-dev.txt
```

and then build "good" image.

```bash
docker compose build
```


Now testing is as easy as executing

```bash
 rm -rf logs
 pytest -vvv -s --log-cli-level=DEBUG tests/integration/test_automatically_sample_dag.py
```

The integration tests (not complete) create a test MinIO buffer, a test database with test user, schema and table by executing a 
modified version of the `docker-compose.yaml`

## Manual testing

Here we run manually the steps leading to the integration tests. 
For maintaining state uncomment the sections in docker compose for volumes. 
Please comment them again before running the integration tests.

Start the deployment by running

```bash
docker-compose up
```

Now you need to create users, tables and buffer.

### Create buffer

Connect to [MinIO](http://127.0.0.1:9001) and create a buffer named `newcross`. This is the name that appears in airflow variable.
Credentials are in the `docker-compose.yaml`.


### Create users and tables.

Connect with DBeaver to the SQL Server with credentials

```python
user = 'sa'
password = 'newcross_123'
host = '127.0.0.1'
port = 1433
database = 'master'
```


Now for cleaning everything up (if you need it just run)

```sql
use ncintegration;
drop table IF EXISTS ncproject.fintransacts;
drop table IF EXISTS ncproject.ingestions;
drop user IF EXISTS ncuser;

IF SUSER_ID('nclogin') IS NOT NULL
	drop login nclogin;

drop schema IF EXISTS ncproject;

use master;
drop database IF EXISTS ncintegration;
```

To create the database used in the deployment execute

```sql
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'ncintegration')
    BEGIN
        CREATE DATABASE ncintegration;
    END
```

and to create the rest

```sql
use ncintegration;


IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'ncproject' )
    EXEC('CREATE SCHEMA ncproject');

IF SUSER_ID('nclogin') IS NULL
	create login nclogin with password='ncuser123!!';

IF DATABASE_PRINCIPAL_ID('ncuser') IS NULL
	create user ncuser for login nclogin with DEFAULT_SCHEMA = ncproject;

IF OBJECT_ID(N'ncproject.fintransacts', N'U') IS NULL
BEGIN
	create table ncproject.fintransacts(
	 id Int not Null,
	 last_transaction_date datetime,
	 description Varchar(200),
	 CONSTRAINT PK_FINTRANSACTS PRIMARY KEY (id)	
	)
END;

IF OBJECT_ID(N'ncproject.ingestions', N'U') IS NULL
BEGIN
	create table ncproject.ingestions(
	 id Int not Null,
	 last_transaction_date datetime,
	 ingestion_date Varchar(200) not Null,
	 CONSTRAINT PK_INGESTIONS PRIMARY KEY (id)	
	)
END;

Grant select on ncproject.fintransacts to ncuser;
Grant insert on ncproject.fintransacts to ncuser;
Grant update on ncproject.fintransacts to ncuser;
Grant delete on ncproject.fintransacts to ncuser;
Grant select on ncproject.ingestions to ncuser;
Grant insert on ncproject.ingestions to ncuser;
Grant update on ncproject.ingestions to ncuser;
Grant delete on ncproject.ingestions to ncuser;


delete from ncproject.fintransacts;
insert into  ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
insert into  ncproject.fintransacts values (2, convert(DATETIME, '1968-10-24 12:45:37', 20), 'buy 2');
insert into  ncproject.fintransacts values (3, convert(DATETIME, '1968-10-25 12:45:37', 20), 'buy 3');
insert into  ncproject.fintransacts values (4, convert(DATETIME, '1968-10-26 12:45:37', 20), 'buy 4');
insert into  ncproject.fintransacts values (5, convert(DATETIME, '1968-10-27 12:45:37', 20), 'buy 5');

```

### Run the dag

Visit the [airflow server](http://localhost:8080) and activate your dag. Extra info in official Airflow Documentation.
[Airflow Apache Project](https://airflow.apache.org/).

Have fun!
