-- {{ config(materialized='table') }}
with
    source_data as (

        select

            uuid_string() as uuiid,
            abs(random(5)) as int_id,
            substr(abs(random()), 17) as value,
            convert_timezone('Europe/Berlin', current_timestamp(2)) as dss_load_date

        from table(generator(rowcount => 5))

    )

select *
from source_data
