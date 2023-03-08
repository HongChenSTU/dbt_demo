{%- macro

 create_task_long(
    pipe_schema,
    pipe_name,
    task_fqn,
    warehouse_name,
    scheduled_time,
    stream_fqn,
    target_table_fqn,
    dml_action,
    dml_action_into_columns,
    sql
 )

-%}

{% if execute %}
    {% set log_message = 'Executing create_task macro for task -> ' ~ env['database'] ~ '.' ~ task_fqn ~ '...' %}
    {{ log(log_message, True) }}
    -- check if pipe exists
    {% set pipe_exists = object_exists(pipe_schema,'pipes',pipe_name,'') %}

    {% if pipe_exists %}
        -- pipe exists
        {{ log('Pipe exists.', True) }}
    
        {{alter_pipe_state(pipe_schema,pipe_name,'pause')}}

        {% set is_pipe_paused = is_pipe_paused(pipe_schema,pipe_name) %}
        
        {% if is_pipe_paused %}
            {{ log('Checking further pipe conditions - last_ingest_timestamp < current_timestamp ...', True) }}
            -- if pipe has no files to process

            {% set statement_last_ingest_timestamp   %}
                select '{{pipe_name}}' as pipe_name, cast(replace(parse_json(system$pipe_status('{{env['database']}}.{{pipe_schema}}.{{pipe_name}}')):lastIngestedTimestamp, '"') as timestamp) as LAST_INGESTED_TIMESTAMP;
            {% endset %}
           
            {% set last_ingested_timestamp = run_query(statement_last_ingest_timestamp) %}

            {% set last_ingested_timestamp_value = last_ingested_timestamp.columns[1].values()[0] %}

            {% set last_ingested_timestamp_smaller_than_current_timestamp = None %}

            {% if last_ingested_timestamp_value == None %}
                -- pipe has never processed anything before

                {% set last_ingested_timestamp_smaller_than_current_timestamp = True %}

            {% else %}
                -- last_ingested_timestamp < current_timestamp
                {% set current_timestamp_value = get_current_timestamp(False) %}

                {% set last_ingested_timestamp_smaller_than_current_timestamp = last_ingested_timestamp_value < current_timestamp_value %}
            {% endif %}            

            {% if last_ingested_timestamp_smaller_than_current_timestamp %}

            -- last_ingested_timestamp < current_timestamp
                {% if stream_fqn != '' and dml_action != '' and dml_action_into_columns != '' %}
                    {% set is_stream_empty = is_stream_empty(stream_fqn) %}

                    {% if is_stream_empty %}
                        {{ log('Task to be created/recreated ...', True) }}
                    -- last_ingested_timestamp < current_timestamp and stream is empty
                    
                        {% set tag = generate_env_parameters(env['database']) %}
                        {%- set sql -%}
                            begin;

                            create or replace task {{task_fqn}}
                            warehouse={{warehouse_name}}
                            schedule= {{scheduled_time}}
                            QUERY_TAG={{tag}}
                            when SYSTEM$STREAM_HAS_DATA('{{stream_fqn}}') as {{dml_action}} {{target_table_fqn}} {{dml_action_into_columns}} {{sql}};
                            alter task {{task_fqn}} resume;

                            commit;
                        {%- endset -%}

                        {%- do run_query(sql) -%} 
                        {% do log('Task has been successfully created.', info=True) %}

                    {% endif %}
                    
                {% else %}

                        {% set tag = generate_env_parameters(env['database']) %}
                        {%- set sql -%}
                            begin;

                            create or replace task {{task_fqn}}
                            warehouse={{warehouse_name}}
                            schedule= {{scheduled_time}}
                            QUERY_TAG={{tag}}
                            {{sql}};
                            alter task {{task_fqn}} resume;

                            commit;
                        {%- endset -%}

                        {%- do run_query(sql) -%} 
                        {% do log('Task has been successfully created.', info=True) %}

                {% endif %}

            {% endif %}

        {% endif %}

        {{alter_pipe_state(pipe_schema,pipe_name,'start')}}

    {% else %}
        -- pipe does not exist

        {% set is_stream_empty = is_stream_empty(stream_fqn) %}

            {% if is_stream_empty %}
                 -- pipe does not exist and stream is empty
                {{ log('Task to be created/recreated ...', True) }}
                {% set tag = generate_env_parameters(env['database']) %}
                {%- set sql -%}
                    begin;

                    create or replace task {{task_fqn}}
                    warehouse={{warehouse_name}}
                    schedule= {{scheduled_time}}
                    QUERY_TAG={{tag}}
                    when SYSTEM$STREAM_HAS_DATA('{{stream_fqn}}') as {{dml_action}} {{target_table_fqn}} {{dml_action_into_columns}} {{sql}};
                    alter task {{task_fqn}} resume;

                    commit;
                {%- endset -%} 
                {%- do run_query(sql) -%} 
                {% do log('Task has been successfully created.', info=True) %}
            
            {% endif %}

    {% endif%}

{% endif %}

{%- endmacro -%}