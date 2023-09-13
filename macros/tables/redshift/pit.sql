{%- macro redshift__pit(tracked_entity, hashkey, sat_names, ldts, ledts, sdts, snapshot_relation, dimension_key,snapshot_trigger_column=none, custom_rsrc=none, pit_type=none) -%}

{%- set hash = datavault4dbt.hash_method() -%}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'VARCHAR(64)') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{%- set rsrc = var('datavault4dbt.rsrc_alias', 'rsrc') -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{%- if datavault4dbt.is_something(pit_type) -%}
    {%- set quote = "'" -%}
    {%- set pit_type_quoted = quote + pit_type + quote -%}
    {%- set hashed_cols = [pit_type_quoted, datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix([sdts], 'snap')] -%}
{%- else -%}
    {%- set hashed_cols = [datavault4dbt.prefix([hashkey],'te'), datavault4dbt.prefix([sdts], 'snap')] -%}
{%- endif -%}

{{- datavault4dbt.prepend_generated_by() }}

WITH

{%- if is_incremental() %}

existing_dimension_keys AS (
    SELECT {{ dimension_key }}
    FROM {{ this }}
),

{%- endif %}

pit_records AS (
    SELECT
{%- if datavault4dbt.is_something(pit_type) %}
    '{{ datavault4dbt.as_constant(pit_type) }}' AS type,
{%- endif -%}
{%- if datavault4dbt.is_something(custom_rsrc) %}
    '{{ custom_rsrc }}' AS {{ rsrc }},
{%- endif %}
    {{ datavault4dbt.hash(
        columns=hashed_cols,
        alias=dimension_key,
        is_hashdiff=false
    ) }},
    te.{{ hashkey }},
    snap.{{ sdts }},
{%- for satellite in sat_names %}
    COALESCE({{ satellite }}.{{ hashkey }}, CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} AS {{ hash_dtype }})) AS hk_{{ satellite }},
    COALESCE({{ satellite }}.{{ ldts }}, {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }}) AS {{ ldts }}_{{ satellite }}
    {{- "," if not loop.last -}}
{%- endfor %}
    FROM {{ ref(tracked_entity) }} AS te
    FULL JOIN {{ ref(snapshot_relation) }} AS snap
    {%- if datavault4dbt.is_something(snapshot_trigger_column) %}
        ON snap.{{ snapshot_trigger_column }} = TRUE
    {%- else %}
        ON TRUE
    {%- endif -%}
{%- for satellite in sat_names -%}
    {%- set sat_columns = datavault4dbt.source_columns(ref(satellite)) -%}
    LEFT JOIN
{{- ' ' -}}
{%- if ledts|string|lower in sat_columns|map('lower') -%}
    {{ ref(satellite) }}
{%- else -%}
    (
        SELECT
            {{ hashkey }},
            {{ ldts }},
            COALESCE(
                LEAD({{ ldts }} - INTERVAL '1 MICROSECOND') OVER (PARTITION BY {{ hashkey }} ORDER BY {{ ldts }}),
                {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
            ) AS {{ ledts }}
        FROM {{ ref(satellite) }}
    )
{%- endif %} AS {{ satellite }}
        ON {{ satellite }}.{{ hashkey}} = te.{{ hashkey }}
        AND snap.{{ sdts }} BETWEEN {{ satellite }}.{{ ldts }} AND {{ satellite }}.{{ ledts }}
{%- endfor -%}
{%- if datavault4dbt.is_something(snapshot_trigger_column) %}
    WHERE snap.{{ snapshot_trigger_column }}
{%- endif %}
),

records_to_insert AS (
    SELECT DISTINCT *
    FROM pit_records
{%- if is_incremental() %}
    WHERE {{ dimension_key }} NOT IN (SELECT * FROM existing_dimension_keys)
{%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro %}
