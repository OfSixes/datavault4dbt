{%- macro redshift__nh_sat(parent_hashkey, src_payload, src_ldts, src_rsrc, source_model, source_is_single_batch) -%}

{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{#- Set the name for the ordering column here until Redshift supports QUALIFY clauses -#}
{%- set rdshft_order_col = 'rdshft_order_col' -%}

{%- set source_cols = datavault4dbt.expand_column_list(columns=[parent_hashkey, src_ldts, src_rsrc, src_payload]) -%}
{%- set source_relation = ref(source_model) -%}

{{ datavault4dbt.prepend_generated_by() }}

WITH

source_data AS (
    {#- Selecting all source data, that is newer than latest data in sat if incremental #}
    SELECT
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }}
{%- if not source_is_single_batch -%},
        ROW_NUMBER() OVER (PARTITION BY {{ parent_hashkey }} ORDER BY {{ src_ldts }} ASC) AS {{ rdshft_order_col }}
{%- endif %}
    FROM {{ source_relation }}
{%- if is_incremental() %}
    WHERE {{ src_ldts }} > (
        SELECT MAX({{ src_ldts }})
        FROM {{ this }}
        WHERE {{ src_ldts }} != {{ datavault4dbt.string_to_timestamp(timestamp_format, end_of_all_times) }}
    )
{%- endif %}
),

{%- set source_data_cte = 'source_data' -%}

{%- if not source_is_single_batch %}

source_data_earliest AS (
    SELECT *
    FROM source_data
    WHERE {{ rdshft_order_col }} = 1
),

{%- set source_data_cte = 'source_data_earliest' -%}

{%- endif -%}

{%- if is_incremental() -%}

{#- Get distinct list of hashkeys inside the existing satellite, if incremental. #}

distinct_hashkeys AS (
    SELECT DISTINCT {{ parent_hashkey }}
    FROM {{ this }}
),

{%- endif -%}

{#- Select all records from the source. If incremental, insert only records, where the
    hashkey is not already in the existing satellite. #}

records_to_insert AS (
    SELECT
        {{ datavault4dbt.print_list(source_cols, 0) | indent(8) }}
    FROM {{ source_data_cte }}
{%- if is_incremental() %}
    WHERE {{ parent_hashkey }} NOT IN (SELECT * FROM distinct_hashkeys)
{%- endif %}
)

SELECT * FROM records_to_insert

{%- endmacro %}
