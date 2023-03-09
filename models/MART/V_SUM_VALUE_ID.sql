-- {{ config(materialized='view') }}
with
    source_data as (

        select int_id, sum(value) as sum_value
        from {{ ref("TBL_PS_ID_DATA") }}
        group by int_id
    )

select *
from source_data
