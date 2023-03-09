--{{ config(materialized='table') }}

with source_data as (

    select 

    SS_SOLD_DATE_SK
    ,SS_TICKET_NUMBER
    ,SS_QUANTITY
    ,ss_ext_sales_price
    ,current_timestamp() as DSS_LOAD_DATE
  
    from snowflake_sample_data.tpcds_sf10tcl.store_sales
    
    limit 100

)

select *
from source_data