{%- macro create_task(task_name, warehouse_name, scheduled_time, task_sql)-%}

    {% set log_message = 'Executing create_task macro for task -> ' ~ task_name ~ '...' %}
    {{ log(log_message, True) }}
   
    {{ log('Task to be created/recreated ...', True) }}
    
    {%- set sql -%}
        begin;
        create or replace task {{task_name}}
        warehouse={{warehouse_name}}
        schedule= {{scheduled_time}}
        {{task_sql}};
        alter task {{task_name}} resume;
        commit;
    {%- endset -%} 
    {%- do run_query(sql) -%} 

    {% do log('Task has been successfully created.', info=True) %}

{%- endmacro -%}