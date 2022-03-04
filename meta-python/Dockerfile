FROM python:3.10-slim-bullseye

## ENV used in build and runtime
ENV TZ=${TZ:-'Europe/Berlin'}

ARG DEBIAN_FRONTEND="noninteractive"

## Update and install project requirements from requirements.txt (copy separately to entire src to aid docker layer cache reuse)
COPY support/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

## Bootstrap to allow secrets and utilise env variables correctly etc
COPY support/ /
## Copy project files over
COPY src /src

ENV LOG_CONF_PATH=${LOG_CONF_PATH:-'/config/logging.yaml'}
ENV APP_CONF_PATH=${APP_CONF_PATH:-'/config/config.yaml'}
ENV generator_sparql_endpoint=${generator_sparql_endpoint}

ENV I2B2DBHOST=${I2B2DBHOST:-i2b2.database}
ENV I2B2DBPORT=${I2B2DBPORT:-5432}
ENV I2B2DBNAME=${I2B2DBNAME:-i2b2}

ENV FLASK_APP=/src/listener.py
## TODO: --reload is a debug/test option? Remove for prod
CMD /bootstrap-meta.sh && flask run -h 0.0.0.0 --reload
