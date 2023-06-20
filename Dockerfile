FROM apache/airflow:2.6.2-python3.11
ADD requirements-docker.txt .
RUN pip install -r requirements-docker.txt