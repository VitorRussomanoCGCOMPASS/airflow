FROM apache/airflow:2.5.0-python3.10
USER root

RUN apt-get update
RUN apt-get -y install git

COPY requirements.txt /requirements.txt

USER airflow
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/include"

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# docker build . --tag extending_airflow:latest