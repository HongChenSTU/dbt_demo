{{ config(materialized="incremental", on_schema_change="append_new_columns") }}

with
    source_data as (

        select
            uuiid,
            int_id,
            value,
            dss_load_date
        from {{ ref("TBL_NEW_DEMO_DATA") }} ts
    )

select *
from source_data

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where DSS_LOAD_DATE > (select max(DSS_LOAD_DATE) from {{ this }})

{% endif %}