select
       server_name,
       count(id)
from {{ ref('stg_event_changes') }}
WHERE mz_logical_timestamp() >= timestamp * 1000
  AND mz_logical_timestamp() < timestamp * 1000 + 5000
group by server_name
order by count desc