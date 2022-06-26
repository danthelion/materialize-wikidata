{% set source_name %}
    {{ mz_generate_name('src_wikidata_events') }}
{% endset %}

CREATE SOURCE {{ source_name }}
  FROM KAFKA BROKER 'localhost:63248' TOPIC 'recentchange'
  KEY FORMAT BYTES
  VALUE FORMAT BYTES
ENVELOPE NONE
