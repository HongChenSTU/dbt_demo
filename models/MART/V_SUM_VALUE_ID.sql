-- {{ config(materialized='view') }}
with
    source_data as (

        select int_id::int as int_id, sum(nvl(value, 0))::int as sum_value
        from {{ ref("TBL_PS_ID_DATA") }}
        group by int_id
    )

select *
from source_data 
