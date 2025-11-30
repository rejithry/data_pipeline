FROM apache/superset
USER root

ENV SUPERSET_LOAD_EXAMPLES=yes
ENV SUPERSET_SECRET_KEY="some_random_base64_string"

USER root

RUN apt-get install libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
RUN apt update
RUN apt-get install -y build-essential

#!/bin/bash
RUN  apt update && apt install -y gcc libpq-dev python3-dev pkg-config
WORKDIR /app
RUN python3 -m venv .venv
RUN . .venv/bin/activate
RUN pip install psycopg2-binary

# Driver for Hive
USER superset

RUN flask fab create-admin \
               --username admin \
               --password admin \
               --firstname Superset \
               --lastname Admin \
               --email test@admin.com || true
RUN superset db upgrade
RUN superset superset init