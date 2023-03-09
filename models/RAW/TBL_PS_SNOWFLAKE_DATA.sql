{{ config(
        materialized='incremental',
        on_schema_change='append_new_columns'
    )

}}

with source_data as (

    select d_date
    ,sum(ss_quantity) as sum_ss_quantity
    ,sum(ss_ext_sales_price) as sum_ss_ext_sales_price
    ,DSS_LOAD_DATE
    from {{ ref('TBL_NEW_DEMO_DATA') }} ts
    join snowflake_sample_data.tpcds_sf10tcl.date_dim dd on ts.ss_sold_date_sk = dd.d_date_sk
    group by d_date, DSS_LOAD_DATE
    order by d_date desc
)

select *
from source_data