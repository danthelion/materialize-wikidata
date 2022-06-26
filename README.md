# Producer

Create Redpanda cluster and topic

```shell
rpk container start -n 1
rpk topic create recentchanges --brokers 127.0.0.1:63248
```

Start producer

```shell
python data-generator/wikidata_events.py
```

Validate

```shell
rpk topic consume recentchanges --brokers 127.0.0.1:63248
```

# Materialize

Start the database

```shell
materialized --workers 1
```

Create Redpanda source

```sql
CREATE SOURCE recentchange
  FROM KAFKA BROKER 'localhost:63248' TOPIC 'recentchange'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
ENVELOPE NONE;
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW test3 AS
WITH jsonified_source AS (
    SELECT 
    (data ->> 'title') :: string as title,
    (data ->> '$schema') :: string as schema,
    (data ->> 'type') :: string as type,
    (data ->> 'bot') :: boolean as bot,
    (data ->> 'comment') :: string as comment,
    (data ->> 'id') :: integer as id,
    (data ->> 'length') :: jsonb as length,
    (data ->> 'log_action') :: string as log_action,
    (data ->> 'log_action_comment') :: string as log_action_comment,
    (data ->> 'log_id') :: string as log_id,
    (data ->> 'log_params') :: string as log_params,
    (data ->> 'log_type') :: string as log_type,
    (data ->> 'meta') :: jsonb as meta,
    (data ->> 'minor') :: boolean as minor,
    (data ->> 'namespace') :: integer as namespace,
    (data ->> 'parsedcomment') :: string as parsedcomment,
    (data ->> 'patrolled') :: boolean as patrolled,
    (data ->> 'revision') :: jsonb as revision,
    (data ->> 'server_name') :: string as server_name,
    (data ->> 'server_script_path') :: string as server_script_path,
    (data ->> 'server_url') :: string as server_url,
    (data ->> 'user') :: string as server_version,
    (data ->> 'timestamp') :: numeric as timestamp,
    (data ->> 'wiki') :: string as wiki
  FROM
    (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM public.recentchange)
)
  SELECT
   *
  FROM
    jsonified_source;
```

# Aggregate
```sql
CREATE OR REPLACE MATERIALIZED VIEW changes_by_server_5s AS
select server_name, count(id), to_timestamp(timestamp) ts from test3 
WHERE mz_logical_timestamp() >= timestamp * 1000
  AND mz_logical_timestamp() < timestamp * 1000 + 5000
group by server_name, timestamp order by count desc;
```