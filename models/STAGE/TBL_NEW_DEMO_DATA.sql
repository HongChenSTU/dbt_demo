-- {{ config(materialized='table') }}
with
    source_data as (

        select

            uuid_string()::string as uuid,
            abs(random(5))::int as int_id,
            substr(abs(random()), 17)::int as value,
            convert_timezone('Europe/Berlin', current_timestamp(2))::timestamp_tz as dss_load_date

        from table(generator(rowcount => 5))

    )

select *
from source_data
