{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns",
        unique_key="int_id",
        incremental_strategy="delete+insert",
        merge_update_columns=["uuiid", "value", "dss_load_date"],
    )
}}

with
    source_data as (

        select
            uuid::string as uuid,
            int_id::int as int_id,
            value::int as value,
            dss_load_date::timestamp_tz as dss_load_date
        from {{ ref("TBL_PS_ID_DATA") }} ts
    )

select *
from source_data

{% if is_incremental() %}

-- this filter will only be applied on an incremental run!
where dss_load_date > (select max(dss_load_date) from {{ this }})

{% endif %}
