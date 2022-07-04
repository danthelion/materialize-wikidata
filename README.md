# Kafka and data producer

Create Redpanda cluster and UI

```shell
docker-compose up -d redpanda provectus-ui-local
```

Create topic
```shell
docker-compose up -d create_topic
```

Start producer

```shell
docker-compose up -d python_producer
```

Validate -> http://localhost:9029/ui/clusters/local/topics/recentchange/messages


# Materialize

Start the database

```shell
docker-compose up -d materialized
```

# DBT

```shell
cd wikidata_analysis/
dbt run --profiles-dir .
cd ..
```


# website

```shell
docker-compose up -d backend
```
