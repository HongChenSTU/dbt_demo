{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    source_data as (

        select
            uuid::string as uuid,
            int_id::int as int_id,
            value::int as value,
            dss_load_date::timestamp_tz as dss_load_date
        from {{ ref("TBL_NEW_DEMO_DATA") }} ts
    )

select *
from source_data

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where DSS_LOAD_DATE > (select max(DSS_LOAD_DATE) from {{ this }})

{% endif %}