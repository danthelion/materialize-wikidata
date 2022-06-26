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
    (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM {{ ref('src_wikidata_events') }})
)

SELECT * FROM jsonified_source