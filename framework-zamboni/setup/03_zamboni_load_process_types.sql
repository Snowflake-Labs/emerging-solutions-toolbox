USE ROLE ZAMBONI_ROLE;
USE WAREHOUSE XS_WH;
USE SCHEMA ZAMBONI_DB.ZAMBONI_METADATA;

--process_type
--clean first
DELETE FROM PROCESS_TYPES;
    


--create target_dynamic_table type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    1
    ,'target_dynamic_table'
    ,'a template that creates the target as a dynamic table'
    ,$$
        CREATE OR REPLACE DYNAMIC TABLE {{ target["object"] | sqlsafe }} 
        TARGET_LAG = {% if settings["downstream"] %}DOWNSTREAM{% else %}'{{ settings["target_lag"] | sqlsafe }} {{ settings["target_interval"] | sqlsafe }}'{% endif %}
        WAREHOUSE = {{ settings["warehouse"] | sqlsafe}}
        AS 
            SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                {{ columns[0] | sqlsafe}} 
            {% for column in columns[1:] %}
                ,{{ column | sqlsafe}}
            {% endfor %}
            FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
            {% if join %}
            {% for j in join %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON 
            {% for k in j["keys"] %}
                {{ k["attr_1"] | sqlsafe }} {{ k["operator"] | sqlsafe}} {{ k["attr_2"] | sqlsafe }} {{ k["condition"] | sqlsafe }}
            {% endfor %}
            {% endfor %}
            {% endif %}
            {% if where  %}
            WHERE 
            {% for w in where %}
                {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe }} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if group_by  %}
            GROUP BY 
                {{ group_by[0] | sqlsafe }}
            {% for gb in group_by[1:] %}
                ,{{ gb | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if having %}
            HAVING 
            {% for h in having %}
                {{ h["attr_1"] | sqlsafe }} {{ h["operator"]  | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_cols  %}
            ORDER BY 
                {{ order_by_cols[0] | sqlsafe }}
            {% for ob in order_by_cols[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_pos  %}
            ORDER BY {{ order_by_pos[0] | sqlsafe }}{% for ob in order_by_pos[1:] %},{{ ob | sqlsafe }}{% endfor %}{% endif %}
        ;
    $$
    ,'dynamic table'
    ,'create'
    ,SYSDATE()
    ,NULL
;

/*
--MANUEL's DYNAMIC TABLE (UNION) FROM MEDIA DATA FOUNDATION -- WILL USE AS A SEPARATE PROCESS TYPE
CREATE OR REPLACE DYNAMIC TABLE {{ target["object"] | sqlsafe }}
{% if target["columns"] %}
(
        {{ target["columns"][0]["name"] | sqlsafe }} {{ target["columns"][0]["type"] | sqlsafe }}
    {% for column in target["columns"][1:]%}
        ,{{ column["name"] | sqlsafe}} {{ column["type"] | sqlsafe}}
    {% endfor %}
)
{% endif %}
TARGET_LAG = '{{ settings["target_lag"] | sqlsafe }} {{ settings["target_interval"] | sqlsafe }}'
WAREHOUSE = '{{ settings["warehouse"] | sqlsafe}}'
COMMENT = '{"origin":"sf_sit","name":"Marketing Data Foundation","version":{"major":1, "minor":0},"attributes":""}'
AS 
    {% for definition in definitions %}
        SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
            {{ definition['columns'][0] | sqlsafe}} 
        {% for column in definition['columns'][1:] %}
            ,{{ column | sqlsafe}}
        {% endfor %}
        FROM {{ definition['source']["object"] | sqlsafe }} {{ definition['source']["alias"] | sqlsafe }}
        {% for j in definition['join'] %}
        {{ j["type"] | sqlsafe }} JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ j["on"] | sqlsafe }}
        {% endfor %}
        {% if definition['where']  %}
        WHERE 
        {% for w in definition['where'] %}
        {% if loop.index == loop.length  %}
            {{ w["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ w["clause"]  | sqlsafe }} {{ w["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        {% endif %}
        {% if definition['qualify']  %}
        QUALIFY
        {% for q in definition['qualify'] %}
        {% if loop.index == loop.length  %}
            {{ q["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ q["clause"]  | sqlsafe }} {{ q["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        {% endif %}
        {% if definition['group_by']  %}
        GROUP BY 
            {{ definition['group_by'][0] | sqlsafe }}
        {% for gb in definition['group_by'][1:] %}
            ,{{ gb | sqlsafe }}
        {% endfor %}
        {% endif %}
        {% if definition['order_by_cols']  %}
        ORDER BY 
            {{ definition['order_by_cols'][0] | sqlsafe }}
        {% for ob in definition['order_by_cols'][1:] %}
            ,{{ ob | sqlsafe }}
        {% endfor %}
        {% endif %}
        {% if definition['order_by_pos']  %}
        ORDER BY 
            {{ definition['order_by_pos'][0] | sqlsafe }}
        {% for ob in definition['order_by_pos'][1:] %}
            ,{{ ob | sqlsafe }}
        {% endfor %}
        {% endif %}
        {% if loop.index < loop.length  %}
            UNION ALL
        {% endif %}
    {% endfor %}
;

 */

--insert target_incremental_merge_insert type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT 
    2
    ,'target_incremental_merge_insert'
    ,'a template that update existing target records and inserts ones'
    ,$$
        MERGE INTO {{ target["object"] | sqlsafe }} t
        USING 
            (
                SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                    {{ columns[0] | sqlsafe}} 
                {% for column in columns[1:] %}
                    ,{{ column | sqlsafe}}
                {% endfor %}
                FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
                {% for j in join %}
                {% if loop.index == 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% if loop.index > 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% endfor %}
                {% if where  %}
                WHERE 
                {% for w in where %}
                {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe }} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if group_by  %}
                GROUP BY 
                    {{ group_by[0] | sqlsafe }}
                {% for gb in group_by[1:] %}
                    ,{{ gb | sqlsafe }}
                {% endfor %}
                {% endif %}
               {% if having %}
               HAVING 
               {% for h in having %}
               {{ h["attr_1"] | sqlsafe }} {{ h["operator"]  | sqlsafe }} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
               {% endfor %}
               {% endif %}
               {% if order_by_cols  %}
               ORDER BY 
                   {{ order_by_cols[0] | sqlsafe }}
               {% for ob in order_by_cols[1:] %}
                   ,{{ ob | sqlsafe }}
               {% endfor %}
               {% endif %}
               {% if order_by_pos  %}
               ORDER BY 
                   {{ order_by_pos[0] | sqlsafe }}
               {% for ob in order_by_pos[1:] %}
                   ,{{ ob | sqlsafe }}
               {% endfor %}
               {% endif %}
            ) s ON
        {% set ns = namespace(on = 0) %}
        {% for m in mapping %}
        {% if m["merge_on"].upper() == "Y"  %}
        {% set ns.on = ns.on + 1 %}
        {% if ns.on == 1  %}
            t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% else %}
            AND t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        WHEN MATCHED 
        {% if settings["when_matched"] %} AND
        (
        {% for wm in settings["when_matched"] %} 
        {% if loop.index == loop.length  %}
            {{ wm["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ wm["clause"]  | sqlsafe }} {{ wm["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        )
        {% endif %} 
        THEN UPDATE SET
        {% set ns = namespace(u = 0) %}
        {% for m in mapping %}
        {% if m["update"].upper() == "Y"  %}
        {% set ns.u = ns.u + 1 %}
        {% if ns.u == 1  %}
            t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% else %}
            ,t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        WHEN NOT MATCHED 
        {% if settings["when_not_matched"] %} AND
        (
        {% for wnm in settings["when_not_matched"] %} 
        {% if loop.index == loop.length  %}
            {{ wnm["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ wnm["clause"]  | sqlsafe }} {{ wnm["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        )
        {% endif %}
        THEN INSERT 
        (
        {% set ns = namespace(i = 0) %}
        {% for m in mapping %}
        {% if m["insert"].upper() == "Y"  %}
        {% set ns.i = ns.i + 1 %}
        {% if ns.i == 1  %}
            {{ m["target_attr"] | sqlsafe }}
        {% else %}
            ,{{ m["target_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        ) 
        VALUES (
        {% set ns = namespace(v = 0) %}
        {% for m in mapping %}
        {% if m["insert"].upper() == "Y"  %}
        {% set ns.v = ns.v + 1 %}
        {% if ns.v == 1  %}
            s.{{ m["source_attr"] | sqlsafe }}
        {% else %}
            ,s.{{ m["source_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        )
        ;
    $$
    ,'table'
    ,'merge_insert'
    ,SYSDATE()
    ,NULL
;

--insert target_standard_table type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    3
    ,'target_standard_table'
    ,'a template that creates the target as a standard table.  NOTE:  this should be used to create a target table that does not exist or to refresh the table'
    ,$$
        CREATE OR REPLACE TABLE {{ target["object"] | sqlsafe }} 
        AS
            SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                {{ columns[0] | sqlsafe}} 
            {% for column in columns[1:] %}
                ,{{ column | sqlsafe}}
            {% endfor %}
            FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
            {% for j in join %}
            {% if loop.index == 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% if loop.index > 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% endfor %}
            {% if where  %}
            WHERE 
            {% for w in where %}
            {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe}} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if group_by  %}
            GROUP BY 
                {{ group_by[0] | sqlsafe }}
            {% for gb in group_by[1:] %}
                ,{{ gb | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if having %}
            HAVING 
            {% for h in having %}
            {{ h["attr_1"] | sqlsafe }} {{ h["operator"] | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_cols  %}
            ORDER BY 
                {{ order_by_cols[0] | sqlsafe }}
            {% for ob in order_by_cols[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_pos  %}
            ORDER BY 
                {{ order_by_pos[0] | sqlsafe }}
            {% for ob in order_by_pos[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
        ;
    $$
    ,'table'
    ,'create'
    ,SYSDATE()
    ,NULL
;

--insert target_standard_view type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    4
    ,'target_standard_view'
    ,'a template that creates the target as a standard view'
    ,$$
        CREATE OR REPLACE {{ secure | sqlsafe}} VIEW {{ target["object"] | sqlsafe }} 
        AS 
            SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                {{ columns[0] | sqlsafe}} 
            {% for column in columns[1:] %}
                ,{{ column | sqlsafe}}
            {% endfor %}
            FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
            {% for j in join %}
            {% if loop.index == 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% if loop.index > 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% endfor %}
            {% if where  %}
            WHERE 
            {% for w in where %}
            {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe}} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if group_by  %}
            GROUP BY 
                {{ group_by[0] | sqlsafe }}
            {% for gb in group_by[1:] %}
                ,{{ gb | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if having %}
            HAVING 
            {% for h in having %}
            {{ h["attr_1"] | sqlsafe }} {{ h["operator"] | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_cols  %}
            ORDER BY 
                {{ order_by_cols[0] | sqlsafe }}
            {% for ob in order_by_cols[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_pos  %}
            ORDER BY 
                {{ order_by_pos[0] | sqlsafe }}
            {% for ob in order_by_pos[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
        ;
    $$
    ,'view'
    ,'create'
    ,SYSDATE()
    ,NULL
;

--insert target_materialized_view type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    5
    ,'target_materialized_view'
    ,'a template that creates the target as a materialized view'
    ,$$
        CREATE OR REPLACE {{ secure | sqlsafe}} MATERIALIZED VIEW {{ target["object"] | sqlsafe }} 
        AS 
            SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                {{ columns[0] | sqlsafe}} 
            {% for column in columns[1:] %}
                ,{{ column | sqlsafe}}
            {% endfor %}
            FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
            {% for j in join %}
            {% if loop.index == 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% if loop.index > 1  %}
            JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
            {% endif %}
            {% endfor %}
            {% if where  %}
            WHERE 
            {% for w in where %}
            {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe}} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if group_by  %}
            GROUP BY 
                {{ group_by[0] | sqlsafe }}
            {% for gb in group_by[1:] %}
                ,{{ gb | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if having %}
            HAVING 
            {% for h in having %}
            {{ h["attr_1"] | sqlsafe }} {{ h["operator"] | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_cols  %}
            ORDER BY 
                {{ order_by_cols[0] | sqlsafe }}
            {% for ob in order_by_cols[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
            {% if order_by_pos  %}
            ORDER BY 
                {{ order_by_pos[0] | sqlsafe }}
            {% for ob in order_by_pos[1:] %}
                ,{{ ob | sqlsafe }}
            {% endfor %}
            {% endif %}
        ;
    $$
    ,'materialized view'
    ,'create'
    ,SYSDATE()
    ,NULL
;




--insert target_incremental_merge_delete type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    6
    ,'target_incremental_merge_delete'
    ,'a template that deletes matching records and inserts new ones'
    ,$$
        MERGE INTO {{ target["object"] | sqlsafe }} t
        USING 
            (
                SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                    {{ columns[0] | sqlsafe}} 
                {% for column in columns[1:] %}
                    ,{{ column | sqlsafe}}
                {% endfor %}
                FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
                {% for j in join %}
                {% if loop.index == 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% if loop.index > 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% endfor %}
                {% if where  %}
                WHERE 
                {% for w in where %}
                {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe}} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if group_by  %}
                GROUP BY 
                    {{ group_by[0] | sqlsafe }}
                {% for gb in group_by[1:] %}
                    ,{{ gb | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if having %}
                HAVING 
                {% for h in having %}
                {{ h["attr_1"] | sqlsafe }} {{ h["operator"] | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if order_by_cols  %}
                ORDER BY 
                    {{ order_by_cols[0] | sqlsafe }}
                {% for ob in order_by_cols[1:] %}
                    ,{{ ob | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if order_by_pos  %}
                ORDER BY 
                    {{ order_by_pos[0] | sqlsafe }}
                {% for ob in order_by_pos[1:] %}
                    ,{{ ob | sqlsafe }}
                {% endfor %}
                {% endif %}
            ) s ON
        {% set ns = namespace(on = 0) %}
        {% for m in mapping %}
        {% if m["merge_on"].upper() == "Y"  %}
        {% set ns.on = ns.on + 1 %}
        {% if ns.on == 1  %}
            t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% else %}
            AND t.{{ m["target_attr"] | sqlsafe }} = s.{{ m["source_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        WHEN MATCHED 
        {% if settings["when_matched"] %} AND
        (
        {% for wm in settings["when_matched"] %} 
        {% if loop.index == loop.length  %}
            {{ wm["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ wm["clause"]  | sqlsafe }} {{ wm["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        )
        {% endif %} 
        THEN DELETE
        WHEN NOT MATCHED 
        {% if settings["when_not_matched"] %} AND
        (
        {% for wnm in settings["when_not_matched"] %} 
        {% if loop.index == loop.length  %}
            {{ wnm["clause"] | sqlsafe }}
        {% endif %}
        {% if loop.index < loop.length  %}
            {{ wnm["clause"]  | sqlsafe }} {{ wnm["condition"]  | sqlsafe }}
        {% endif %}
        {% endfor %}
        )
        {% endif %}
        THEN INSERT 
        (
        {% set ns = namespace(i = 0) %}
        {% for m in mapping %}
        {% if m["insert"].upper() == "Y"  %}
        {% set ns.i = ns.i + 1 %}
        {% if ns.i == 1  %}
            {{ m["target_attr"] | sqlsafe }}
        {% else %}
            ,{{ m["target_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        ) 
        VALUES (
        {% set ns = namespace(v = 0) %}
        {% for m in mapping %}
        {% if m["insert"].upper() == "Y"  %}
        {% set ns.v = ns.v + 1 %}
        {% if ns.v == 1  %}
            s.{{ m["source_attr"] | sqlsafe }}
        {% else %}
            ,s.{{ m["source_attr"] | sqlsafe }}
        {% endif %}
        {% endif %}
        {% endfor %}
        )
        ;
    $$
    ,'table'
    ,'merge_delete'
    ,SYSDATE()
    ,NULL
;

--insert target_file type
INSERT INTO PROCESS_TYPES (PROCESS_TYPE_ID, PROCESS_TYPE, DESCRIPTION, TEMPLATE, OBJECT_TYPE, OBJECT_ACTION, CREATED_TIMESTAMP, MODIFIED_TIMESTAMP)
SELECT
    7
    ,'target_file'
    ,'a template that creates the target file'
    ,$$
        COPY INTO {{ target | sqlsafe}} FROM 
            ( 
                SELECT {%if distinct%}DISTINCT{% endif %} {%if top%}{%if top != null %}TOP {{ top | sqlsafe }}{% endif %}{% endif %}
                    {{ columns[0] | sqlsafe}} 
                {% for column in columns[1:] %}
                    ,{{ column | sqlsafe}}
                {% endfor %}
                FROM {{ source["object"] | sqlsafe }} {{ source["alias"] | sqlsafe }}
                {% for j in join %}
                {% if loop.index == 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ source["alias"] | sqlsafe }}.{{ source["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% if loop.index > 1  %}
                JOIN {{ j["object"] | sqlsafe }} {{ j["alias"] | sqlsafe }} ON {{ loop.previtem["alias"] | sqlsafe }}.{{ loop.previtem["key"] | sqlsafe }} = {{ j["alias"] | sqlsafe }}.{{ j["key"] | sqlsafe }}
                {% endif %}
                {% endfor %}
                {% if where  %}
                WHERE 
                {% for w in where %}
                {{ w["attr_1"] | sqlsafe }} {{ w["operator"] | sqlsafe}} {{ w["attr_2"] | sqlsafe }} {{ w["condition"] | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if group_by  %}
                GROUP BY 
                    {{ group_by[0] | sqlsafe }}
                {% for gb in group_by[1:] %}
                    ,{{ gb | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if having %}
                HAVING 
                {% for h in having %}
                {{ h["attr_1"] | sqlsafe }} {{ h["operator"] | sqlsafe}} {{ h["attr_2"] | sqlsafe }} {{ h["condition"] | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if order_by_cols  %}
                ORDER BY 
                    {{ order_by_cols[0] | sqlsafe }}
                {% for ob in order_by_cols[1:] %}
                    ,{{ ob | sqlsafe }}
                {% endfor %}
                {% endif %}
                {% if order_by_pos  %}
                ORDER BY 
                    {{ order_by_pos[0] | sqlsafe }}
                {% for ob in order_by_pos[1:] %}
                    ,{{ ob | sqlsafe }}
                {% endfor %}
                {% endif %}
            )
        {% if  file_format  %}
        FILE_FORMAT = ( {{ file_format | sqlsafe }} )
        {% endif %}
        ;
    $$
    ,'file'
    ,'create'
    ,SYSDATE()
    ,NULL
;

SELECT * FROM ZAMBONI_DB.ZAMBONI_METADATA.PROCESS_TYPES;