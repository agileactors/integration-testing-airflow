# Airflow integration testing


## Background

This code is for tutorial purposes of the Agile Actors Data Chapter. It continues in a more advanced manner from
[Basic integration Testing](https://github.com/fithisux/airflow-integration-testing) in the spirirt of
[Testing in Airflow Part 2 — Integration Tests and End-To-End Pipeline Tests](https://medium.com/@chandukavar/testing-in-airflow-part-2-integration-tests-and-end-to-end-pipeline-tests-af0555cd1a82)

In summary the project sets up a PostgreSQL database where financial transactions are inserted. Airflow scans the table and when it finds new transactions, 
it ingests the transactions in a MinIO store (like S3). Then it marks in another PostgreSQL database that the task has been accomplished. It is there for preventing double ingestions.


For convenient developing in PyCharm setup interpreter paths for *plugins* and *dags*. Install also

```bash
pip install -r requirements-docker.txt
```

for autocompletion.

## General testing
First install dependencies

```bash
pip install -r requirements-dev.txt
```

and run

```bash
  tox
```

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
 docker compose down -v
 rm -rf tut-minio-data && mkdir tut-minio-data
 rm -rf logs
 pytest -vvv -s --log-cli-level=DEBUG tests/integration/test_ingestions.py
```

The integration tests (not complete) 
* create a test MinIO buffer
* create a test database with test user for financial transactions
* create a test database with test user for ingestions
* populate with test data, execute airflow DAG and run assertions

## Manual testing

Here we run manually the steps leading to the integration tests. 
For maintaining state uncomment the sections in docker compose for volumes. 
Please comment them again before running the integration tests.

Start the deployment by running

```bash
docker compose up
```

Now you need to create users, tables and buffer.

### Create buffer

Connect to [MinIO](http://127.0.0.1:9001) and create a buffer named `bucket`. This is the name that appears in airflow variable.
Credentials are in the `docker-compose.yaml`. Leave it open to see the ingestions.


### Create users and tables.

#### Financial transactions
Connect with DBeaver to the Postgresql with credentials in the corresponding `docker-compose.yaml` section.

```sql
CREATE DATABASE financedb;
```

(of course we could have inserted everything in docker compose like [here](https://levelup.gitconnected.com/creating-and-filling-a-postgres-db-with-docker-compose-e1607f6f882f))
Now you can run an SQL script in order to  [set up the financial database](tests/integration/setup_database.sql).

When you are finished

Run an SQL script in order to [tear down the financial database](tests/integration/teardown_database.sql).

Remove it by running

```sql
DROP DATABASE financedb;
```

#### Ingestions database

First we need a database

```sql
CREATE DATABASE ingestiondb;
```
which we can dispose if we do not need it

```sql
DROP DATABASE ingestiondb;
```

Here alembic covers you

```bash
alembic upgrade head
```

to nuke it when you're finished

```bash
alembic downgrade base
```

### Run the dag

Visit the [airflow server](http://localhost:8080) and activate your dag. Extra info in official Airflow Documentation.
[Airflow Apache Project](https://airflow.apache.org/).

Have fun!
