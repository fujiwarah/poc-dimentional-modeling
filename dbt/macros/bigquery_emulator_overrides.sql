{#
  BigQuery Emulator 用マクロオーバーライド。
  BigQuery Emulator は CREATE OR REPLACE TABLE をサポートしていないため、
  CREATE TABLE に置き換える。DROP は materialization 側で adapter.drop_relation()
  が呼ばれることで処理される（bq_emulator_patch.py のパッチ済み版）。
#}

{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {%- if language == 'sql' -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create table {{ relation }}
    as (
      {{ compiled_code }}
    );
  {%- endif -%}
{%- endmacro %}

{% macro bigquery__create_view_as(relation, sql) -%}
    create view {{ relation }}
    as {{ sql }};
{%- endmacro %}
