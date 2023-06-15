{%- macro redshift__stage(include_source_columns,
                ldts,
                rsrc,
                source_model,
                hashed_columns,
                derived_columns,
                sequence,
                prejoined_columns,
                missing_columns,
                multi_active_config) -%}

{%- if (source_model is none) and execute -%}

    {%- set error_message -%}
    Staging error: Missing source_model configuration. A source model name must be provided.
    e.g.
    [REF STYLE]
    source_model: model_name
    OR
    [SOURCES STYLE]
    source_model:
        source_name: source_table_name
    {%- endset -%}

    {{- exceptions.raise_compiler_error(error_message) -}}
{%- endif -%}

{{- log('source_model: ' ~ source_model, false ) -}}

{#- Check for source format or ref format and create relation object from source_model -#}
{%- if source_model is mapping and source_model is not none -%}

    {%- set source_name = source_model | first -%}
    {%- set source_table_name = source_model[source_name] -%}

    {%- set source_relation = source(source_name, source_table_name) -%}
    {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}

{%- elif source_model is not mapping and source_model is not none -%}

    {{- log('source_model is not mapping and not none: ' ~ source_model, false) -}}

    {%- set source_relation = ref(source_model) -%}
    {%- set all_source_columns = datavault4dbt.source_columns(source_relation=source_relation) -%}
{%- else -%}
    {%- set all_source_columns = [] -%}
{%- endif -%}

{{- log('source_relation: ' ~ source_relation, false) -}}

{#- Setting the column name for load date timestamp and record source to the alias coming from the attributes -#}
{%- set ldts_alias = var('datavault4dbt.ldts_alias', 'ldts') -%}
{%- set rsrc_alias = var('datavault4dbt.rsrc_alias', 'rsrc') -%}
{%- set copy_input_columns = var('datavault4dbt.copy_rsrc_ldts_input_columns', false) -%}
{%- set load_datetime_col_name = ldts_alias -%}
{%- set record_source_col_name = rsrc_alias -%}

{%- set ldts_rsrc_input_column_names = [] -%}
{%- if datavault4dbt.is_attribute(ldts) -%}
    {%- if not copy_input_columns -%}
        {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [ldts] -%}
    {%- else -%}
        {%- if ldts|lower == ldts_alias|lower -%}
            {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [ldts] -%}
        {%- endif -%}
    {%- endif %}
{%- endif -%}

{%- if datavault4dbt.is_attribute(rsrc) -%}
    {%- if not copy_input_columns -%}
        {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [rsrc] -%}
    {%- else -%}
        {%- if rsrc|lower == rsrc_alias|lower -%}
            {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [rsrc] -%}
        {%- endif -%}
    {%- endif -%}
{%- endif %}

{%- if datavault4dbt.is_something(sequence) -%}
    {%- set ldts_rsrc_input_column_names = ldts_rsrc_input_column_names + [sequence] -%}
{%- endif -%}

{%- set ldts = datavault4dbt.as_constant(ldts) -%}
{%- set rsrc = datavault4dbt.as_constant(rsrc) -%}

{#- Getting the column names for all additional columns -#}
{%- set derived_column_names = datavault4dbt.extract_column_names(derived_columns) -%}
{%- set hashed_column_names = datavault4dbt.extract_column_names(hashed_columns) -%}
{%- set prejoined_column_names = datavault4dbt.extract_column_names(prejoined_columns) -%}
{%- set missing_column_names = datavault4dbt.extract_column_names(missing_columns) -%}
{%- set exclude_column_names = hashed_column_names + prejoined_column_names + missing_column_names + ldts_rsrc_input_column_names -%}
{%- set source_and_derived_column_names = (all_source_columns + derived_column_names) | unique | list -%}
{%- set all_columns = adapter.get_columns_in_relation( source_relation ) -%}

{%- set columns_without_excluded_columns = [] -%}
{%- set final_columns_to_select = [] -%}

{%- if include_source_columns -%}
    {%- set source_columns_to_select = datavault4dbt.process_columns_to_select(all_source_columns, exclude_column_names) | list -%}

    {%- for column in all_columns -%}
        {%- if column.name|lower not in exclude_column_names|map('lower') -%}
            {%- do columns_without_excluded_columns.append(column) -%}
        {%- endif -%}
    {%- endfor -%}
{%- else -%}

    {#- Include from the source only the input columns needed -#}
    {#- Getting the input columns for the additional columns -#}
    {%- set derived_input_columns = datavault4dbt.extract_input_columns(derived_columns) -%}
    {%- set hashed_input_columns = datavault4dbt.expand_column_list(datavault4dbt.extract_input_columns(hashed_columns)) -%}
    {%- set hashed_input_columns = datavault4dbt.process_columns_to_select(hashed_input_columns, derived_column_names) -%} {#- Excluding the names of the derived columns. -#}
    {%- set hashed_input_columns = datavault4dbt.process_columns_to_select(hashed_input_columns, prejoined_column_names) -%} {#- Excluding the names of the prejoined columns. -#}
    {%- set hashed_input_columns = datavault4dbt.process_columns_to_select(hashed_input_columns, missing_column_names) -%} {#- Excluding the names of the missing columns. -#}
    {%- set prejoined_input_columns = datavault4dbt.extract_input_columns(prejoined_columns) -%}

    {%- if datavault4dbt.is_something(multi_active_config) -%}
        {%- if datavault4dbt.is_list(multi_active_config['multi_active_key']) -%}
            {%- set ma_keys = multi_active_config['multi_active_key'] -%}
        {%- else -%}
            {%- set ma_keys = [multi_active_config['multi_active_key']] -%}
        {%- endif -%}
        {%- set only_include_from_source = (derived_input_columns + hashed_input_columns + prejoined_input_columns + ma_keys) | unique | list -%}
    {%- else -%}
        {%- set only_include_from_source = (derived_input_columns + hashed_input_columns + prejoined_input_columns) | unique | list -%}
    {%- endif -%}

    {%- set source_columns_to_select = only_include_from_source -%}

{%- endif-%}

{%- set final_columns_to_select = final_columns_to_select + source_columns_to_select -%}
{%- set derived_columns_to_select = datavault4dbt.process_columns_to_select(source_and_derived_column_names, hashed_column_names) | unique | list -%}

{%- if datavault4dbt.is_something(derived_columns) -%}
    {#- Getting Data types for derived columns with detection from source relation -#}
    {%- set derived_columns_with_datatypes = datavault4dbt.derived_columns_datatypes(derived_columns, source_relation) -%}
    {%- set derived_columns_with_datatypes_DICT = fromjson(derived_columns_with_datatypes) -%}
{%- endif -%}
{#- Select hashing algorithm -#}

{#- Setting unknown and error keys with default values for the selected hash algorithm -#}
{%- set hash = datavault4dbt.hash_method() -%}
{{- log('hash_function: ' ~ hash, false) -}}
{%- set hash_dtype = var('datavault4dbt.hash_datatype', 'VARCHAR(64)') -%}
{%- set hash_default_values = fromjson(datavault4dbt.hash_default_values(hash_function=hash,hash_datatype=hash_dtype)) -%}
{%- set hash_alg = hash_default_values['hash_alg'] -%}
{%- set unknown_key = hash_default_values['unknown_key'] -%}
{%- set error_key = hash_default_values['error_key'] -%}

{#- Select timestamp and format variables -#}
{%- set beginning_of_all_times = datavault4dbt.beginning_of_all_times() -%}
{%- set end_of_all_times = datavault4dbt.end_of_all_times() -%}
{%- set timestamp_format = datavault4dbt.timestamp_format() -%}

{#- Setting the error/unknown value for the record source for the ghost records -#}
{%- set error_value_rsrc = var('datavault4dbt.default_error_rsrc', 'ERROR') -%}
{%- set unknown_value_rsrc = var('datavault4dbt.default_unknown_rsrc', 'SYSTEM') -%}

{#- Setting the rsrc default datatype and length -#}
{%- set rsrc_default_dtype = var('datavault4dbt.rsrc_default_dtype', 'VARCHAR(20000)') -%}

WITH
{#- Selecting everything that we need from the source relation. #}

source_data AS (
    SELECT
    {{- "\n        " ~ datavault4dbt.print_list(datavault4dbt.escape_column_names(all_source_columns)) | indent(8) if all_source_columns else " *" }}
    FROM {{ source_relation }}
{%- if is_incremental() %}
    WHERE {{ ldts }} > (
        SELECT MAX({{ load_datetime_col_name}})
        FROM {{ this }}
        WHERE {{ load_datetime_col_name}} != {{ datavault4dbt.string_to_timestamp(timestamp_format , end_of_all_times) }}
        )
{%- endif -%}
    {%- set last_cte = "source_data" %}
),

{%- set alias_columns = [load_datetime_col_name, record_source_col_name] -%}

{#- Selecting all columns from the source data, renaming load date and record source to global aliases #}

ldts_rsrc_data AS (
    SELECT
        {{ ldts }} AS {{ load_datetime_col_name}},
        CAST( {{ rsrc }} as {{ rsrc_default_dtype }} ) AS {{ record_source_col_name }}
        {%- if datavault4dbt.is_something(sequence) -%},
            {{ sequence | indent(8) }} AS edwSequence
            {%- set alias_columns = alias_columns + ['edwSequence'] -%}
        {%- endif -%}

        {%- if source_columns_to_select is not none and source_columns_to_select | length > 0 -%}
            {{- ",\n" -}}
            {{- datavault4dbt.print_list(datavault4dbt.escape_column_names(source_columns_to_select)) | indent(8, True) -}}
        {%- endif %}
    FROM {{ last_cte }}

    {%- set last_cte = "ldts_rsrc_data" -%}
    {%- set final_columns_to_select = alias_columns + final_columns_to_select %}
    {%- set final_columns_to_select = datavault4dbt.process_columns_to_select(final_columns_to_select, derived_column_names) | list -%}

    {%- set columns_without_excluded_columns_tmp = [] -%}
    {%- for column in columns_without_excluded_columns -%}
        {%- if column.name not in derived_column_names -%}
            {%- do columns_without_excluded_columns_tmp.append(column) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- set columns_without_excluded_columns = columns_without_excluded_columns_tmp | list %}
),

{%- if datavault4dbt.is_something(missing_columns) -%}

{#- Filling missing columns with NULL values for schema changes #}

missing_columns AS (
    SELECT
    {%- if final_columns_to_select | length > 0 %}
        {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select)) | indent(8) }}
    {%- endif -%}
    {%- for col, dtype in missing_columns.items() %},
        CAST(NULL AS {{ dtype }}) AS {{ col }}
    {%- endfor %}
    FROM {{ last_cte }}
    {%- set last_cte = "missing_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + missing_column_names %}
),
{%- endif -%}

{%- if datavault4dbt.is_something(prejoined_columns) -%}
{#- Prejoining Business Keys of other source objects for Link purposes #}

prejoined_columns AS (
    SELECT
    {%- if final_columns_to_select | length > 0 %}
        {{ datavault4dbt.print_list(datavault4dbt.prefix(columns=datavault4dbt.escape_column_names(final_columns_to_select), prefix_str='lcte').split(',')) | indent(8) -}}
    {%- endif -%}
    {%- for col, vals in prejoined_columns.items() -%},
        pj_{{loop.index}}.{{ vals['bk'] }} AS {{ col }}
    {%- endfor %}
    FROM {{ last_cte }} AS lcte

    {%- for col, vals in prejoined_columns.items() -%}
        {%- if 'src_name' in vals.keys() or 'src_table' in vals.keys() -%}
            {%- set relation = source(vals['src_name']|string, vals['src_table']) -%}
        {%- elif 'ref_model' in vals.keys() -%}
            {%- set relation = ref(vals['ref_model']) -%}
        {%- else -%}
            {%- set error_message -%}
                Prejoin error: Invalid target entity definition. Allowed are:
                e.g.
                [REF STYLE]
                  extracted_column_alias:
                  ref_model: model_name
                  bk: extracted_column_name
                  this_column_name: join_columns_in_this_model
                  ref_column_name: join_columns_in_ref_model
                OR
                [SOURCES STYLE]
                extracted_column_alias:
                  src_name: name_of_ref_source
                  src_table: name_of_ref_table
                  bk: extracted_column_name
                  this_column_name: join_columns_in_this_model
                  ref_column_name: join_columns_in_ref_model

                Got:
                {{ col }}: {{ vals }}
            {%- endset -%}

            {%- do exceptions.raise_compiler_error(error_message) -%}
        {%- endif -%}

        {#- This sets a default value for the operator that connects multiple joining conditions. Only when it is not set by user. -#}
        {%- if 'operator' not in vals.keys() -%}
            {%- set operator = 'AND' -%}
        {%- else -%}
            {%- set operator = vals['operator'] -%}
        {%- endif -%}

        {%- set prejoin_alias = 'pj_' + loop.index|string -%}

        LEFT JOIN {{ relation }} AS {{ prejoin_alias }}
            ON {{ datavault4dbt.multikey(columns=vals['this_column_name'], prefix=['lcte', prejoin_alias], condition='=', operator=operator, right_columns=vals['ref_column_name']) }}

    {%- endfor -%}

    {%- set last_cte = "prejoined_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + prejoined_column_names %}
),
{%- endif -%}

{%- if datavault4dbt.is_something(derived_columns) -%}
{%- set final_columns_to_select = datavault4dbt.process_columns_to_select(final_columns_to_select, derived_column_names) -%}
{#- Adding derived columns to the selection #}

derived_columns AS (
    SELECT
    {%- if final_columns_to_select | length > 0 %}
        {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select)) | indent(8) }},
    {%- endif %}
        {{ datavault4dbt.derive_columns(columns=derived_columns) | indent(8) }}
    FROM {{ last_cte }}

    {%- set last_cte = "derived_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + derived_column_names %}
),
{%- endif -%}

{%- if datavault4dbt.is_something(hashed_columns) and hashed_columns is mapping -%}

{#- Generating Hashed Columns (hashkeys and hashdiffs for Hubs/Links/Satellites) -#}

{%- set processed_hash_columns = datavault4dbt.process_hash_column_excludes(hashed_columns) %}

hashed_columns AS (
    SELECT
    {%- if datavault4dbt.is_something(multi_active_config) %}
        {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select), 0) | indent(8) }},
        {{ datavault4dbt.hash_columns(columns=processed_hash_columns, multi_active_key=multi_active_config['multi_active_key'], main_hashkey_column=multi_active_config['main_hashkey_column']) | indent(8) }}
    {%- else -%}
        {#- Hash calculation for single-active source data. #}
        {% if final_columns_to_select | length > 0 -%}
            {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select), 0) | indent(8) }},
        {% endif -%}
        {{ datavault4dbt.hash_columns(columns=processed_hash_columns) | indent(8) }}
    {%- endif %}
    FROM {{ last_cte }}

    {%- set last_cte = "hashed_columns" -%}
    {%- set final_columns_to_select = final_columns_to_select + hashed_column_names %}
),
{%- endif -%}

{% if not is_incremental() %}
{#- Creating Ghost Record for unknown case, based on datatype #}

unknown_values AS (
    SELECT
        {{ datavault4dbt.string_to_timestamp(timestamp_format, beginning_of_all_times) }} AS {{ load_datetime_col_name }},
        '{{ unknown_value_rsrc }}' AS {{ record_source_col_name }}

    {%- if columns_without_excluded_columns is defined and columns_without_excluded_columns| length > 0 -%}
        {#- Generating Ghost Records for all source columns, except the ldts, rsrc & edwSequence column -#}
        {%- for column in columns_without_excluded_columns %},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(missing_columns) -%}
        {#- Additionally generating ghost record for missing columns -#}
        {%- for col, dtype in missing_columns.items() %},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='unknown') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(prejoined_columns) -%}
        {#- Additionally generating ghost records for the prejoined attributes -#}
        {%- for col, vals in prejoined_columns.items() -%}
            {%- if 'src_name' in vals.keys() or 'src_table' in vals.keys() -%}
                {%- set relation = source(vals['src_name']|string, vals['src_table']) -%}
            {%- elif 'ref_model' in vals.keys() -%}
                {%- set relation = ref(vals['ref_model']) -%}
            {%- endif -%}
            {%- set pj_relation_columns = adapter.get_columns_in_relation( relation ) -%}
            {%- for column in pj_relation_columns -%}
                {%- if column.name|lower == vals['bk']|lower -%}
                    {{- ",\n        " -}}
                    {{ datavault4dbt.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='unknown', alias=col) }}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(derived_columns) -%}
        {# Additionally generating Ghost Records for Derived Columns #}
        {%- for column_name, properties in derived_columns_with_datatypes_DICT.items() %},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, col_size=properties.col_size, ghost_record_type='unknown') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(processed_hash_columns) -%}
        {%- for hash_column in processed_hash_columns %},
        CAST({{ datavault4dbt.as_constant(column_str=unknown_key) }} AS {{ hash_dtype }}) AS {{ hash_column }}
        {%- endfor -%}
    {%- endif %}
),

{#- Creating Ghost Record for error case, based on datatype #}

error_values AS (
    SELECT
        {{ datavault4dbt.string_to_timestamp(timestamp_format , end_of_all_times) }} AS {{ load_datetime_col_name }},
        '{{ error_value_rsrc }}' AS {{ record_source_col_name }}

    {%- if columns_without_excluded_columns is defined and columns_without_excluded_columns| length > 0 -%}
        {#- Generating Ghost Records for Source Columns -#}
        {%- for column in columns_without_excluded_columns %},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(missing_columns) -%}
        {#- Additionally generating ghost record for Missing columns -#}
        {%- for col, dtype in missing_columns.items() %},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=col, datatype=dtype, ghost_record_type='error') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(prejoined_columns) -%},
        {#- Additionally generating ghost records for the prejoined attributes -#}
        {%- for col, vals in prejoined_columns.items() -%}
            {%- if 'src_name' in vals.keys() or 'src_table' in vals.keys() -%}
                {%- set relation = source(vals['src_name']|string, vals['src_table']) -%}
            {%- elif 'ref_model' in vals.keys() -%}
                {%- set relation = ref(vals['ref_model']) -%}
            {%- endif -%}
            {%- set pj_relation_columns = adapter.get_columns_in_relation( relation ) -%}
            {%- for column in pj_relation_columns -%}
                {%- if column.name|lower == vals['bk']|lower -%}
                    {{- ",\n        " -}}
                    {{ datavault4dbt.ghost_record_per_datatype(column_name=column.name, datatype=column.dtype, ghost_record_type='error', alias=col) }}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(derived_columns) -%}
        {#- Additionally generating Ghost Records for Derived Columns -#}
        {%- for column_name, properties in derived_columns_with_datatypes_DICT.items() -%},
        {{ datavault4dbt.ghost_record_per_datatype(column_name=column_name, datatype=properties.datatype, col_size=properties.col_size, ghost_record_type='error') }}
        {%- endfor -%}
    {%- endif -%}

    {%- if datavault4dbt.is_something(processed_hash_columns) -%}
        {%- for hash_column in processed_hash_columns %},
        CAST({{ datavault4dbt.as_constant(column_str=error_key) }} AS {{ hash_dtype }}) AS {{ hash_column }}
        {%- endfor -%}
    {%- endif %}
),

{#- Combining all previous ghost record calculations to two rows with the same width as regular entries #}

ghost_records AS (
    SELECT *
    FROM unknown_values

    UNION ALL

    SELECT *
    FROM error_values
),

{%- endif -%}

{%- if not include_source_columns -%}
    {%- set final_columns_to_select = datavault4dbt.process_columns_to_select(columns_list=final_columns_to_select, exclude_columns_list=source_columns_to_select) -%}
{%- endif -%}

{#- Combining the two ghost records with the regular data #}

columns_to_select AS (
    SELECT
        {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select), 0) | indent(8) }}
    FROM {{ last_cte }}
{%- if not is_incremental() %}

    UNION ALL

    SELECT
        {{ datavault4dbt.print_list(datavault4dbt.escape_column_names(final_columns_to_select), 0) | indent(8) }}
    FROM ghost_records
{%- endif %}
)

SELECT * FROM columns_to_select

{%- endmacro %}