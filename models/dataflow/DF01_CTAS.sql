{{ config(materialized='table') }}

with source_data as (

    select *
    from snowflake_sample_data.tpcds_sf10tcl.store_sales
    limit 1

)

select *
from source_data