{%- macro create_task_t02_insert_sample()-%}

    {% set log_message = 'Executing create_task macro for task t01 ctas' %}
    {{ log(log_message, True) }}
   
    {{ log('Task to be created/recreated ...', True) }}
    
    {%- set sql -%}
        begin;
        create or replace task team_db.task_demo.t02_insert_sample
        warehouse = task_wh
        after team_db.task_demo.t01_ctas
        as
        insert into tbl_sample (select * from (select * from snowflake_sample_data.tpcds_sf10tcl.store_sales sample block (0.5)))
        ;
        -- alter task team_db.task_demo.t01_ctas resume;
        commit;
    {%- endset -%} 
    {%- do run_query(sql) -%} 

    {% do log('Task has been successfully created.', info=True) %}


{%- endmacro -%}