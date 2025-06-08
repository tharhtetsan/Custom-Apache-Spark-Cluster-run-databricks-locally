FROM bitnami/spark:3.5.1-debian-12-r0

USER root

RUN apt-get update

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt
