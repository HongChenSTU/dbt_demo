{%- macro create_task_t01_ctas()-%}

    {% set log_message = 'Executing create_task macro for task t01 ctas' %}
    {{ log(log_message, True) }}
   
    {{ log('Task to be created/recreated ...', True) }}
    
    {%- set sql -%}
        begin;
        create or replace task team_db.task_demo.t01_ctas
        warehouse = task_wh
        schedule = '15 minute'
        as
        create table tbl_sample if not exists as select * from snowflake_sample_data.tpcds_sf10tcl.store_sales limit 1;
        ;
        -- alter task team_db.task_demo.t01_ctas resume;
        commit;
    {%- endset -%} 
    {%- do run_query(sql) -%} 

    {% do log('Task has been successfully created.', info=True) %}


{%- endmacro -%}