{%- macro redshift__sat_v0(parent_hashkey, src_hashdiff, src_payload, src_ldts, src_rsrc, source_model) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{#- Set the name for the ordering column here until Redshift supports QUALIFY clauses -#}
{%- set rdshft_order_col = 'rdshft_order_col' -%}

{%- set ns=namespace(src_hashdiff="", hdiff_alias="") -%}

{%- if src_hashdiff is mapping and src_hashdiff is not none -%}
    {%- set ns.src_hashdiff = src_hashdiff["source_column"] -%}
    {%- set ns.hdiff_alias = src_hashdiff["alias"] -%}
{%- else -%}
    {%- set ns.src_hashdiff = src_hashdiff -%}
    {%- set ns.hdiff_alias = src_hashdiff -%}
{%- endif -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[src_rsrc, src_ldts, src_payload]) -%}

{%- set source_relation = ref(source_model) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

{#- Selecting all source data, that is newer than latest data in sat if incremental #}

source_data AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.src_hashdiff }} AS {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }}
    FROM {{ source_relation }}
{%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT MAX({{ src_ldts }})
        FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
{%- endif %}
),

{#- Get the latest record for each parent hashkey in existing sat, if incremental. -#}
{%- if is_incremental() %}

ordered_entries_in_sat AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }},
        ROW_NUMBER() OVER(PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }} DESC) AS {{ rdshft_order_col }}
    FROM {{ this }}
),

latest_entries_in_sat AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }}
    FROM ordered_entries_in_sat
    WHERE {{ rdshft_order_col }} = 1
),

{%- endif -%}

{#-
    Deduplicate source by comparing each hashdiff to the hashdiff of the previous record, for each hashkey.
    Additionally adding a row number based on that order, if incremental.
#}

duplicate_marked_source AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }},
        CASE
            WHEN {{ ns.hdiff_alias }} = LAG({{ ns.hdiff_alias }}) OVER (PARTITION BY {{ parent_hashkey|lower }} ORDER BY {{ src_ldts }})
                THEN TRUE
                ELSE FALSE
        END AS duplicate_marker
    FROM source_data
),

deduplicated_numbered_source AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }}
    {%- if is_incremental() %},
        ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }}) AS rn
    {%- endif %}
    FROM duplicate_marked_source
    WHERE duplicate_marker IS FALSE
),

{#-
    Select all records from the previous CTE. If incremental, compare the oldest incoming entry to
    the existing records in the satellite.
#}

records_to_insert AS (
    SELECT
        {{ parent_hashkey }},
        {{ ns.hdiff_alias }},
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }}
    FROM deduplicated_numbered_source
{%- if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM latest_entries_in_sat
        WHERE {{ datavault4dbt.multikey(parent_hashkey, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND {{ datavault4dbt.multikey(ns.hdiff_alias, prefix=['latest_entries_in_sat', 'deduplicated_numbered_source'], condition='=') }}
            AND deduplicated_numbered_source.rn = 1
    )
{%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro %}
