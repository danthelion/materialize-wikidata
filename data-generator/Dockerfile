FROM python:3.9-slim

COPY requirements.txt .

RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt

ADD wikidata_events.py .

CMD ["python", "-u", "./wikidata_events.py"]