FROM python:3.10-slim

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PIP_NO_CACHE_DIR=TRUE
ENV PIP_DISABLE_PIP_VERSION_CHECK=TRUE

RUN mkdir -p /opt/program
WORKDIR /opt/program

COPY requirements.txt /opt/program/
RUN python3 -m pip install -r /opt/program/requirements.txt

COPY wikidata_events.py /opt/program/
