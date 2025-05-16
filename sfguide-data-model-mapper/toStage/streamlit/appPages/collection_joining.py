from appPages.page import (BasePage, pd, st, time,
                           col, when_matched, when_not_matched, current_timestamp, parse_json, set_page)

if 'session' in st.session_state:
    session = st.session_state.session


def sanitize(value, action):
    sanitized = ''
    if action == 'upper':
        sanitized = value.upper()
    elif action == 'add_single_quotes':
        sanitized = "'{}'".format(value)

    return sanitized


def fetch_databases():
    session = st.session_state.session
    databases = session.sql("SHOW DATABASES").collect()
    database_name = [d["name"].lower() for d in databases]
    return database_name


def fetch_schemas(database_name):
    session = st.session_state.session
    schemas = session.sql(f"SHOW SCHEMAS IN {database_name}").collect()
    schema_name = [s["name"].lower() for s in schemas]
    schema_pd = pd.DataFrame(schema_name)
    schema_name = schema_pd[schema_pd[0] != 'information_schema']
    return schema_name


def fetch_tables(database_name, schema_name):
    session = st.session_state.session
    tables = session.sql(f"SHOW TABLES IN {database_name}.{schema_name}").collect()
    table_name = [t["name"].lower() for t in tables]
    return table_name


def fetch_views(database_name, schema_name):
    session = st.session_state.session
    views = session.sql(f"SHOW VIEWS IN {database_name}.{schema_name}").collect()
    view_name = [v["name"] for v in views]
    return view_name


def save_collection_name():
    session = st.session_state.session

    source_df = session.create_dataframe(
        [
            [
                st.session_state.collection_name,
                st.session_state.selected_target_collection,
                st.session_state.target_collection_version,
                st.session_state.collection_entity_name
            ]
        ],
        schema=["SOURCE_COLLECTION_NAME", "TARGET_COLLECTION_NAME", "VERSION", "TARGET_ENTITY_NAME"],
    ).with_column("LAST_UPDATED_TIMESTAMP", current_timestamp())

    if st.session_state["streamlit_mode"] == "NativeApp":
        target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION")
    else:
        target_df = session.table("MODELING.SOURCE_COLLECTION")

    try:
        target_df.merge(
            source_df,
            (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
            (target_df["TARGET_ENTITY_NAME"] == st.session_state.collection_entity_name),
            [
                when_matched().update(
                    {
                        "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                        "TARGET_COLLECTION_NAME": source_df["TARGET_COLLECTION_NAME"],
                        "VERSION": source_df["VERSION"],
                        "TARGET_ENTITY_NAME": source_df["TARGET_ENTITY_NAME"],
                        "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"],
                    }
                ),
                when_not_matched().insert(
                    {
                        "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                        "TARGET_COLLECTION_NAME": source_df["TARGET_COLLECTION_NAME"],
                        "VERSION": source_df["VERSION"],
                        "TARGET_ENTITY_NAME": source_df["TARGET_ENTITY_NAME"],
                        "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"],
                    }
                ),
            ],
        )
    except Exception as e:
        st.info(e)

    st.session_state.disable_collection_name = True


def save_entity(step):
    session = st.session_state.session
    # Put more error checking value checking later

    entity_value = st.session_state.entity_name_input

    if (st.session_state.selected_database is not None
            and st.session_state.selected_schema is not None
            and st.session_state.selected_table is not None):
        st.session_state.qualified_selected_table = (
                st.session_state.selected_database
                + "."
                + st.session_state.selected_schema
                + "."
                + st.session_state.selected_table)

    if 'collection_name_input' in st.session_state and len(st.session_state.collection_name_input) > 0:
        st.session_state.collection_name = st.session_state.collection_name_input

    if st.session_state.collection_name == "Please Enter Collection Name":
        st.warning("Please Input a Collection Name")

    else:
        if 'is_base' in st.session_state:
            if st.session_state.is_base:

                if st.session_state["streamlit_mode"] == "NativeApp":
                    update_entity_sql = ("DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY  \
                                         WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' \
                                            AND IS_BASE_ENTITY = True")
                else:
                    update_entity_sql = ("DELETE FROM MODELING.SOURCE_ENTITY  \
                                         WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' \
                                            AND IS_BASE_ENTITY = True")
                run_sql = session.sql(update_entity_sql)
                try:
                    run = run_sql.collect()
                except Exception as e:
                    st.info(e)

                boolean_flag = True
            else:
                boolean_flag = False

        save_collection_name()

        source_df = session.create_dataframe(
            [
                [
                    st.session_state.collection_name,
                    st.session_state.entity_name_input,
                    st.session_state.qualified_selected_table,
                    boolean_flag
                ]
            ],
            schema=[
                "SOURCE_COLLECTION_NAME",
                "SOURCE_ENTITY_NAME",
                "ENTITY_FULLY_QUALIFIED_SOURCE",
                "IS_BASE_ENTITY",
            ],
        ).with_column("LAST_UPDATED_TIMESTAMP", current_timestamp())

        if st.session_state["streamlit_mode"] == "NativeApp":
            target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY")
        else:
            target_df = session.table("MODELING.SOURCE_ENTITY")

        # # Use the MERGE statement to handle insert and update operations
        try:
            target_df.merge(
                source_df,
                (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == source_df["SOURCE_ENTITY_NAME"]) &
                (target_df["IS_BASE_ENTITY"] == boolean_flag),
                [
                    when_matched().update(
                        {
                            "SOURCE_ENTITY_NAME": source_df["SOURCE_ENTITY_NAME"],
                            "ENTITY_FULLY_QUALIFIED_SOURCE": source_df[
                                "ENTITY_FULLY_QUALIFIED_SOURCE"
                            ],
                            "IS_BASE_ENTITY": source_df["IS_BASE_ENTITY"],
                            "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"],
                        }
                    ),
                    when_not_matched().insert(
                        {
                            "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                            "SOURCE_ENTITY_NAME": source_df["SOURCE_ENTITY_NAME"],
                            "ENTITY_FULLY_QUALIFIED_SOURCE": source_df[
                                "ENTITY_FULLY_QUALIFIED_SOURCE"
                            ],
                            "IS_BASE_ENTITY": source_df["IS_BASE_ENTITY"],
                            "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"],
                        }
                    ),
                ],
            )
        except Exception as e:
            st.info(e)

        with st.spinner('Configuring Attributes...'):
            try:
                session.call("MODELING.GENERATE_ATTRIBUTES", st.session_state.collection_name,
                             st.session_state.entity_name_input)
            except Exception as e:
                st.info(e)

        st.success('Done Generating Attributes!')

    set_entity_list('')
    if 'current_relationship_index' in st.session_state:
        relationship_index_val = st.session_state.current_relationship_index
    else:
        relationship_index_val = -1
    update_manager_value('derived', 0, relationship_index_val)


def preview_click(add_condition_filter, page_change):
    session = st.session_state.session
    check_pd = pd.DataFrame(st.session_state.wizard_manager)
    default_values = check_pd.isin(['Add Derived Column/Literal Value', 'add_new', 'Please Select']).any().any()

    if default_values:
        st.error(
            'Collection Generation not executed: Please make sure to select valid values for all dropdown selections')
    else:
        # Need to put checks here for missing DF values
        with st.spinner('Generating Collection...'):

            if add_condition_filter:

                if st.session_state["streamlit_mode"] == "NativeApp":
                    condition_delete_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
                else:
                    condition_delete_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"

                run_condition = session.sql(condition_delete_sql)
                try:
                    run_condition.collect()
                except Exception as e:
                    st.info(e)

                tablename = 'filter_conditions_df' + str(int(time.time() * 1000.0))
                filter_conditions_df = session.write_pandas(st.session_state.filter_conditions, tablename,
                                                            auto_create_table=True)

                if st.session_state["streamlit_mode"] == "NativeApp":
                    filter_target_df = session.table(
                        st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION")
                else:
                    filter_target_df = session.table("MODELING.SOURCE_COLLECTION_FILTER_CONDITION")

                try:
                    filter_target_df.merge(
                        filter_conditions_df,
                        (filter_target_df["SOURCE_COLLECTION_NAME"] == st.session_state.collection_name) &
                        (filter_target_df["RIGHT_FILTER_EXPRESSION"] == filter_conditions_df[
                            "RIGHT_FILTER_EXPRESSION"]) &
                        (filter_target_df["LEFT_FILTER_EXPRESSION"] == filter_conditions_df["LEFT_FILTER_EXPRESSION"]) &
                        (filter_target_df["OPERATOR"] == filter_conditions_df["OPERATOR"])
                        ,
                        [

                            when_not_matched().insert(
                                {
                                    "SOURCE_COLLECTION_NAME": st.session_state.collection_name,
                                    "LEFT_FILTER_EXPRESSION": filter_conditions_df["LEFT_FILTER_EXPRESSION"],
                                    "OPERATOR": filter_conditions_df["OPERATOR"],
                                    "RIGHT_FILTER_EXPRESSION": filter_conditions_df["RIGHT_FILTER_EXPRESSION"],
                                    "LAST_UPDATED_TIMESTAMP": current_timestamp()
                                }
                            ),
                        ],
                    )
                except Exception as e:
                    st.info(e)

            wizard_pd = pd.DataFrame(st.session_state.wizard_manager)
            entity_names = wizard_pd.apply(
                lambda x: ', '.join(x.astype(str)) if x.dtype == 'int64' else ', '.join("\'" + x.astype(str) + "\'"))

            entity_names = entity_names['SOURCE_ENTITY_NAME']

            if st.session_state["streamlit_mode"] == "NativeApp":
                entity_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY WHERE SOURCE_ENTITY_NAME NOT IN (" + entity_names + " \
                        ) and SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
            else:
                entity_sql = "DELETE FROM MODELING.SOURCE_ENTITY WHERE SOURCE_ENTITY_NAME NOT IN (" + entity_names + " \
                        ) and SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"

            run = session.sql(entity_sql)
            try:
                run.collect()
            except Exception as e:
                st.info(e)

            sorted_df = wizard_pd.sort_values(by=['RELATIONSHIP_INDEX'], ascending=True)
            st.session_state.wizard_manager = sorted_df

            if st.session_state["streamlit_mode"] == "NativeApp":
                target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY")
            else:
                target_df = session.table("MODELING.SOURCE_ENTITY")

            unique_df = sorted_df[(sorted_df['RELATIONSHIP_INDEX'] != -1)]
            dropped_sorted_df = unique_df.drop(
                columns=['JOIN_FROM_ENTITY_ATTRIBUTE_NAME', 'ENTITY_FULLY_QUALIFIED_SOURCE', 'OPERATOR',
                         'JOIN_TO_ENTITY_ATTRIBUTE_NAME', 'SOURCE_INDEX',
                         'RELATIONSHIP_INDEX'])

            unique_sorted_df = dropped_sorted_df.drop_duplicates()

            tablename = 'sorted_df' + str(int(time.time() * 1000.0))

            try:
                source_df = session.write_pandas(unique_sorted_df, tablename, auto_create_table=True)
            except Exception as e:
                st.info(e)

            try:
                target_df.merge(
                    source_df,
                    (target_df["SOURCE_COLLECTION_NAME"] == st.session_state.collection_name) &
                    (target_df["SOURCE_ENTITY_NAME"] == source_df["SOURCE_ENTITY_NAME"]),
                    [
                        when_matched().update(
                            {
                                "JOIN_FROM_SOURCE_ENTITY_NAME": source_df["JOIN_FROM_SOURCE_ENTITY_NAME"],
                                "JOIN_TYPE": source_df["JOIN_TYPE"],
                                "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                            }
                        ),
                        when_not_matched().insert(
                            {
                                "JOIN_FROM_SOURCE_ENTITY_NAME": source_df["JOIN_FROM_SOURCE_ENTITY_NAME"],
                                "JOIN_TYPE": source_df["JOIN_TYPE"],
                                "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                            }
                        ),
                    ],
                )
            except Exception as e:
                st.info(e)

            try:
                source_df.drop_table()
            except Exception as e:
                st.info(e)

            if st.session_state["streamlit_mode"] == "NativeApp":
                target_join_table = st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION"
                entity_join_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
            else:
                target_join_table = "MODELING.SOURCE_ENTITY_JOIN_CONDITION"
                entity_join_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"

            run_join = session.sql(entity_join_sql)
            try:
                run_join.collect()
            except Exception as e:
                st.info(e)

            sorted_df_join = sorted_df.loc[sorted_df['RELATIONSHIP_INDEX'] != -1]

            timest = session.create_dataframe([1]).select(current_timestamp()).collect()
            current_time = timest[0]["CURRENT_TIMESTAMP()"]
            for index, row in sorted_df_join.iterrows():
                insert_statement = "INSERT INTO " + target_join_table + " VALUES ('" + st.session_state.collection_name + "', '" \
                                   + row['SOURCE_ENTITY_NAME'] + "', '" + row['JOIN_FROM_SOURCE_ENTITY_NAME'] + "', '" + \
                                   row['JOIN_FROM_ENTITY_ATTRIBUTE_NAME'] + "', '" + row['OPERATOR'] + "', '" + \
                                   row['JOIN_TO_ENTITY_ATTRIBUTE_NAME'] + "', '" + str(current_time) + "')"
                session.sql(insert_statement).collect()
            try:
                session.call("MODELING.GENERATE_COLLECTION_MODEL", st.session_state.collection_name)
            except Exception as e:
                st.info(e)

        if page_change:
            st.session_state.change_after_preview = True

        st.success('Done Generating Collection Model!')
        st.session_state.current_step = "preview"
        st.session_state.show_preview = True


def add_derived_attribute():
    session = st.session_state.session
    collection_name = st.session_state.collection_name

    insert_attribute_selections()

    derived_upper = sanitize(st.session_state.derived_attribute_name, 'upper')
    derived_upper_quotes = sanitize(derived_upper, 'add_single_quotes')
    # sanitize(upper_derived)


    if st.session_state.derivation_type == "EXPRESSION":

        source_df = (session.create_dataframe(
            [
                [
                    collection_name,
                    st.session_state.force_entity_name,
                    derived_upper_quotes, 
                    "null",
                    True,
                    st.session_state.expression_value_input,
                    "null"
                ]
            ],
            schema=["SOURCE_COLLECTION_NAME", "SOURCE_ENTITY_NAME", "SOURCE_ENTITY_ATTRIBUTE_NAME",
                    "SOURCE_ATTRIBUTE_PROPERTIES",
                    "INCLUDE_IN_ENTITY", "DERIVED_EXPRESSION", "AGGREGATION_FUNCTION"],
        ).with_column("SOURCE_ATTRIBUTE_PROPERTIES", parse_json(col("SOURCE_ATTRIBUTE_PROPERTIES")))
                     .with_column("LAST_UPDATED_TIMESTAMP", current_timestamp()))

        if st.session_state["streamlit_mode"] == "NativeApp":
            target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE")
        else:
            target_df = session.table("MODELING.SOURCE_ENTITY_ATTRIBUTE")

        try:
            target_df.merge(
                source_df,
                (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == source_df["SOURCE_ENTITY_NAME"]) &
                (target_df["SOURCE_ENTITY_ATTRIBUTE_NAME"] == source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"]),
                [
                    when_matched().update(
                        {
                            "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                            "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                            "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                            "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                    when_not_matched().insert(
                        {
                            "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                            "SOURCE_ENTITY_NAME": source_df["SOURCE_ENTITY_NAME"],
                            "SOURCE_ENTITY_ATTRIBUTE_NAME": source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"],
                            "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                            "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                            "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                            "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                ],
            )
        except Exception as e:
            st.info(e)

    else:
        if st.session_state.literal_value_input.isdigit():
            attribute_properties = '{ "data_type": "FIXED", "is_nullable": True,"precision": 38,"scale": 0 }'
            lit_value = st.session_state.literal_value_input
        else:
            attribute_properties = '{"byte_length": 16777216, "data_type": "TEXT","is_nullable": False, "length": 16777216}'
            lit_value = "'" + st.session_state.literal_value_input + "'"

        source_df = (session.create_dataframe(
            [
                [
                    collection_name,
                    st.session_state.force_entity_name,
                    derived_upper,
                    attribute_properties,
                    True,
                    lit_value,
                    "null"
                ]
            ],
            schema=["SOURCE_COLLECTION_NAME", "SOURCE_ENTITY_NAME", "SOURCE_ENTITY_ATTRIBUTE_NAME",
                    "SOURCE_ATTRIBUTE_PROPERTIES",
                    "INCLUDE_IN_ENTITY", "DERIVED_EXPRESSION", "AGGREGATION_FUNCTION"],
        ).with_column("SOURCE_ATTRIBUTE_PROPERTIES", parse_json(col("SOURCE_ATTRIBUTE_PROPERTIES")))
                     .with_column("LAST_UPDATED_TIMESTAMP", current_timestamp()))

        if st.session_state["streamlit_mode"] == "NativeApp":
            target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE")
        else:
            target_df = session.table("MODELING.SOURCE_ENTITY_ATTRIBUTE")

        try:
            target_df.merge(
                source_df,
                (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == source_df["SOURCE_ENTITY_NAME"]) &
                (target_df["SOURCE_ENTITY_ATTRIBUTE_NAME"] == source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"]),
                [
                    when_matched().update(
                        {
                            "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                            "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                            "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                            "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                    when_not_matched().insert(
                        {
                            "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                            "SOURCE_ENTITY_NAME": source_df["SOURCE_ENTITY_NAME"],
                            "SOURCE_ENTITY_ATTRIBUTE_NAME": source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"],
                            "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                            "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                            "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                            "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                ],
            )
        except Exception as e:
            st.info(e)

    st.session_state.expression_value_input = ''
    st.session_state.derived_attribute_name = ''
    st.session_state.add_derived = False


def insert_attribute_selections():
    session = st.session_state.session
    attribute_edited_df = st.session_state.attribute_edited
    collection_name = st.session_state.collection_name
    attribute_edited_pd = pd.DataFrame(attribute_edited_df)

    attr_data = []

    if st.session_state.is_base:
        for index, row in attribute_edited_pd.iterrows():
            attr_data.append(
                {
                    "SOURCE_COLLECTION_NAME": collection_name,
                    "SOURCE_ENTITY_NAME": st.session_state.force_entity_name,
                    "SOURCE_ENTITY_ATTRIBUTE_NAME": row['SOURCE ENTITY ATTRIBUTE NAME'],
                    "INCLUDE_IN_ENTITY": row['INCLUDE IN ENTITY']
                })
    else:
        for index, row in attribute_edited_pd.iterrows():
            attr_data.append(
                {
                    "SOURCE_COLLECTION_NAME": collection_name,
                    "SOURCE_ENTITY_NAME": st.session_state.force_entity_name,
                    "SOURCE_ENTITY_ATTRIBUTE_NAME": row['SOURCE ENTITY ATTRIBUTE NAME'],
                    "INCLUDE_IN_ENTITY": row['INCLUDE IN ENTITY'],
                    "AGGREGATION_FUNCTION": row['AGGREGATION FUNCTION']
                })

    try:
        initial_data_df = session.create_dataframe(attr_data)
    except Exception as e:
        st.info(e)

    if st.session_state.is_debug:
        if st.session_state.is_debug:
            st.write(st.session_state.is_base)
            st.dataframe(initial_data_df)

    if st.session_state["streamlit_mode"] == "NativeApp":
        target_df = session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE")
    else:
        target_df = session.table("MODELING.SOURCE_ENTITY_ATTRIBUTE")

    if st.session_state.is_base:
        try:
            target_df.merge(
                initial_data_df,
                (target_df["SOURCE_COLLECTION_NAME"] == initial_data_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_ATTRIBUTE_NAME"] == initial_data_df["SOURCE_ENTITY_ATTRIBUTE_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == initial_data_df["SOURCE_ENTITY_NAME"]),
                [
                    when_matched().update(
                        {
                            "INCLUDE_IN_ENTITY": initial_data_df["INCLUDE_IN_ENTITY"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                ],
            )
        except Exception as e:
            st.info(e)
    else:
        try:
            target_df.merge(
                initial_data_df,
                (target_df["SOURCE_COLLECTION_NAME"] == initial_data_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_ATTRIBUTE_NAME"] == initial_data_df["SOURCE_ENTITY_ATTRIBUTE_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == initial_data_df["SOURCE_ENTITY_NAME"]),
                [
                    when_matched().update(
                        {
                            "AGGREGATION_FUNCTION": initial_data_df['AGGREGATION_FUNCTION'],
                            "INCLUDE_IN_ENTITY": initial_data_df["INCLUDE_IN_ENTITY"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                ],
            )
        except Exception as e:
            st.info(e)


def add_relationship(source, source_index, relationship_index, relationship):
    if relationship_index == 0:
        join_from_source_entity_name = source
    else:
        join_from_source_entity_name = ''

    added_relationship_contents = {
        "SOURCE_INDEX": source_index,
        "RELATIONSHIP_INDEX": relationship_index,
        "SOURCE_ENTITY_NAME": '',
        "ENTITY_FULLY_QUALIFIED_SOURCE": '',
        "IS_BASE_ENTITY": False,
        "JOIN_FROM_SOURCE_ENTITY_NAME": join_from_source_entity_name,
        "JOIN_TYPE": 'INNER',
        "JOIN_FROM_ENTITY_ATTRIBUTE_NAME": '',
        "OPERATOR": '=',
        "JOIN_TO_ENTITY_ATTRIBUTE_NAME": ''
    }

    new_pd = pd.DataFrame(st.session_state.wizard_manager)

    added_relationship_pd = pd.DataFrame(added_relationship_contents, index=[0])

    wizard_contents = pd.concat(
        [added_relationship_pd, new_pd.loc[:]]
        , ignore_index=True).reset_index(drop=True)

    st.session_state.wizard_manager = wizard_contents


def add_filter_relationship(filter_index):
    collection_name = st.session_state.collection_name

    filter_contents = {
        "CONDITION_INDEX": filter_index,
        "SOURCE_COLLECTION_NAME": collection_name,
        "LEFT_FILTER_EXPRESSION": 'add_new',
        "OPERATOR": '=',
        "RIGHT_FILTER_EXPRESSION": ''
    }

    added_filter_pd = pd.DataFrame(filter_contents, index=[0])
    if 'filter_conditions' in st.session_state:
        if len(st.session_state.filter_conditions) > 0:
            added_filter_pd = pd.concat([st.session_state.filter_conditions, added_filter_pd]).reset_index(drop=True)
        else:
            added_filter_pd = pd.DataFrame(filter_contents, index=[0])

    st.session_state.filter_conditions = added_filter_pd

    st.session_state.add_filter = True


def remove_relationship():
    new_pd = pd.DataFrame(st.session_state.wizard_manager)

    last_relation_row = new_pd[['RELATIONSHIP_INDEX']].idxmax()
    last_relation_index = last_relation_row.iloc[0]

    new_pd.drop(last_relation_index, axis=0, inplace=True)

    st.session_state.wizard_manager = new_pd


def remove_filter_relationship():
    session = st.session_state.session
    new_pd = pd.DataFrame(st.session_state.filter_conditions)
    #st.write(new_pd)

    last_relation_row = new_pd[['CONDITION_INDEX']].idxmax()
    last_relation_index = last_relation_row.iloc[0]

    delete_row = new_pd[new_pd['CONDITION_INDEX'] == last_relation_row['CONDITION_INDEX']]

    #st.write(last_relation_row)

    if st.session_state["streamlit_mode"] == "NativeApp":
        filter_target_df = session.table(
            st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION")
    else:
        filter_target_df = session.table("MODELING.SOURCE_COLLECTION_FILTER_CONDITION")

    tablename = 'filter_conditions_df' + str(int(time.time() * 1000.0))
    delete_row = session.write_pandas(delete_row, tablename, auto_create_table=True)

    try:
        filter_target_df.merge(
            delete_row,
            (filter_target_df["SOURCE_COLLECTION_NAME"] == st.session_state.collection_name) &
            (filter_target_df["RIGHT_FILTER_EXPRESSION"] == delete_row["RIGHT_FILTER_EXPRESSION"]) &
            (filter_target_df["LEFT_FILTER_EXPRESSION"] == delete_row["LEFT_FILTER_EXPRESSION"]) &
            (filter_target_df["OPERATOR"] == delete_row["OPERATOR"])
            ,
            [

                when_matched().delete(),
            ],
        )
    except Exception as e:
        st.info(e)

    delete_row.drop_table()
    new_pd.drop(last_relation_index, axis=0, inplace=True)

    st.session_state.filter_conditions = new_pd
    session.call("MODELING.GENERATE_COLLECTION_MODEL", st.session_state.collection_name)
    #st.write(st.session_state.filter_conditions)


def update_manager_value(step, source_index, relationship_index):
    st.session_state.current_source_index = source_index
    st.session_state.current_relationship_index = relationship_index
    if relationship_index == -1:
        st.session_state.is_base = True
    else:
        st.session_state.is_base = False

    wizard_manager_pd = pd.DataFrame(st.session_state.wizard_manager)
    mask = wizard_manager_pd['SOURCE_INDEX'].eq(source_index) & wizard_manager_pd['RELATIONSHIP_INDEX'].eq(
        relationship_index)

    # Base table logic
    if step == "source_column":
        selected_entity = st.session_state["join_from_column_" + str(source_index) + str(relationship_index)]
        wizard_manager_pd.loc[mask, 'JOIN_FROM_ENTITY_ATTRIBUTE_NAME'] = selected_entity
    elif step == "base_source":
        mask_next = wizard_manager_pd['SOURCE_INDEX'].eq(source_index) & wizard_manager_pd['RELATIONSHIP_INDEX'].eq(
            relationship_index + 1)
        selected_entity = st.session_state["source_select_" + str(source_index)]
        selected_entity_base = st.session_state["source_select_" + str(source_index)]
        wizard_manager_pd.loc[mask, 'SOURCE_ENTITY_NAME'] = selected_entity
        wizard_manager_pd.loc[mask_next, 'JOIN_FROM_SOURCE_ENTITY_NAME'] = selected_entity_base
        if selected_entity == "Add New":
            st.session_state.current_step = "add"
            st.session_state.show_preview = True
    elif step == "base_join_source":
        selected_entity_base = st.session_state["source_select_" + str(source_index)]
        selected_entity = st.session_state["join_to_" + str(source_index) + str(relationship_index)]
        wizard_manager_pd.loc[mask, 'JOIN_FROM_SOURCE_ENTITY_NAME'] = selected_entity_base
        wizard_manager_pd.loc[mask, 'SOURCE_ENTITY_NAME'] = selected_entity
        if selected_entity == "Add New":
            st.session_state.current_step = "add"
            st.session_state.show_preview = True

    # Joins
    elif step == "join_type":
        selected_entity = st.session_state["join_type_" + str(source_index) + str(relationship_index)]

        wizard_manager_pd.loc[mask, 'JOIN_TYPE'] = selected_entity

        if selected_entity == 'AND':
            previous_row_pd = wizard_manager_pd[(wizard_manager_pd['SOURCE_INDEX'] == source_index) &
                                                (wizard_manager_pd[
                                                     'RELATIONSHIP_INDEX'] == relationship_index - 1)]
            previous_row_pd.reset_index(inplace=True)
            and_join_from = previous_row_pd.loc[0, "JOIN_FROM_SOURCE_ENTITY_NAME"]
            and_source_entity = previous_row_pd.loc[0, "SOURCE_ENTITY_NAME"]
            and_join_type = previous_row_pd.loc[0, "JOIN_TYPE"]
            wizard_manager_pd.loc[mask, 'JOIN_FROM_SOURCE_ENTITY_NAME'] = and_join_from
            wizard_manager_pd.loc[mask, 'SOURCE_ENTITY_NAME'] = and_source_entity
            wizard_manager_pd.loc[mask, 'JOIN_TYPE'] = and_join_type
        else:
            wizard_manager_pd.loc[mask, 'JOIN_TYPE'] = selected_entity

    elif step == "join_from":
        selected_entity = st.session_state["join_from_" + str(source_index) + str(relationship_index)]
        if selected_entity == "Add New":
            st.session_state.current_step = "add"
            st.session_state.show_preview = True
        else:
            wizard_manager_pd.loc[mask, 'JOIN_FROM_SOURCE_ENTITY_NAME'] = selected_entity
    elif step == "join_to":
        selected_entity = st.session_state["join_to_" + str(source_index) + str(relationship_index)]
        if selected_entity == "Add New":
            st.session_state.current_step = "add"
            st.session_state.show_preview = True
        else:
            wizard_manager_pd.loc[mask, 'SOURCE_ENTITY_NAME'] = selected_entity

    # Columns
    elif step == 'join_from_column':
        selected_entity = st.session_state["join_from_column_" + str(source_index) + str(relationship_index)]
        if selected_entity == "Add New Derived Column/Literal Value" and relationship_index == 0:
            st.session_state.force_entity_name = st.session_state["source_select_" + str(source_index)]
            st.session_state.current_step = "derived_join"
            st.session_state.is_base = True
            st.session_state.show_preview = True
        elif selected_entity == "Add New Derived Column/Literal Value":
            st.session_state.force_entity_name = st.session_state[
                "join_from_" + str(source_index) + str(relationship_index)]
            st.session_state.current_step = "derived_join"
            st.session_state.show_preview = True
        else:
            wizard_manager_pd.loc[mask, 'JOIN_FROM_ENTITY_ATTRIBUTE_NAME'] = selected_entity
    elif step == 'join_to_column':
        selected_entity = st.session_state["join_to_column_" + str(source_index) + str(relationship_index)]
        if selected_entity == "Add New Derived Column/Literal Value":
            st.session_state.force_entity_name = st.session_state[
                "join_to_" + str(source_index) + str(relationship_index)]
            st.session_state.current_step = "derived_join"
            st.session_state.is_base = True
            st.session_state.show_preview = True
        else:
            wizard_manager_pd.loc[mask, 'JOIN_TO_ENTITY_ATTRIBUTE_NAME'] = selected_entity

    # Work Area
    elif step == 'operation':
        selected_entity = st.session_state["operation_" + str(source_index) + str(relationship_index)]
        wizard_manager_pd.loc[mask, 'OPERATOR'] = selected_entity
    elif step == 'derived':
        st.session_state.force_entity_name = st.session_state.entity_name_input
        st.session_state.current_step = 'derived'
    elif step == 'done':
        st.session_state.show_preview = False
    elif step == 'done_attributes':
        insert_attribute_selections()
        wizard_manager_pd.loc[mask, 'SOURCE_ENTITY_NAME'] = st.session_state.force_entity_name
        st.session_state.add_derived = False
        st.session_state.show_preview = False

    st.session_state.wizard_manager = wizard_manager_pd


def update_filter_value(step, condition_index):
    filter_conditions_pd = pd.DataFrame(st.session_state.filter_conditions)

    mask = filter_conditions_pd['CONDITION_INDEX'].eq(condition_index)
    if step == "left_filter":
        selected_entity = st.session_state["left_filter" + str(condition_index)]
        filter_conditions_pd.loc[mask, 'LEFT_FILTER_EXPRESSION'] = selected_entity
    if step == "left_literal":
        first_attribute = st.session_state.dynamic_table_columns[0][0]
        filter_conditions_pd.loc[mask, 'LEFT_FILTER_EXPRESSION'] = first_attribute
    if step == "right_filter":
        selected_entity = st.session_state["right_filter" + str(condition_index)]
        filter_conditions_pd.loc[mask, 'RIGHT_FILTER_EXPRESSION'] = selected_entity
    if step == "right_literal":
        first_attribute = st.session_state.dynamic_table_columns[0][0]
        filter_conditions_pd.loc[mask, 'RIGHT_FILTER_EXPRESSION'] = first_attribute
    if step == "filter_operation":
        selected_entity = st.session_state["filter_operation" + str(condition_index)]
        filter_conditions_pd.loc[mask, 'OPERATOR'] = selected_entity

    st.session_state.filter_conditions = filter_conditions_pd


def set_collection_name():
    session = st.session_state.session

    if st.session_state["streamlit_mode"] == "NativeApp":
        collection_name = (
            session.table(st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION")
            .select(col("SOURCE_COLLECTION_NAME"))
            .filter(col("TARGET_COLLECTION_NAME") == st.session_state.selected_target_collection)
            .filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name).distinct()
        )
    else:
        collection_name = (
            session.table("MODELING.SOURCE_COLLECTION")
            .select(col("SOURCE_COLLECTION_NAME"))
            .filter(col("TARGET_COLLECTION_NAME") == st.session_state.selected_target_collection)
            .filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name).distinct()
        )

    collection_name_pd = collection_name.to_pandas()

    if len(collection_name_pd) > 0:
        st.session_state.collection_name = collection_name_pd.loc[0, "SOURCE_COLLECTION_NAME"]
    else:
        st.session_state.collection_name = ''


def set_entity_list(state):
    session = st.session_state.session

    if state == 'new':
        st.session_state.source_entity = pd.DataFrame({'SOURCE_ENTITY_NAME': 'Add New'}, index=[0])
    else:
        if st.session_state["streamlit_mode"] == "NativeApp":
            source_entity = (
                session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY")
                .select(col("SOURCE_ENTITY_NAME"))
                .filter(col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name)
                .distinct().to_pandas()
            )
        else:
            source_entity = (
                session.table("MODELING.SOURCE_ENTITY")
                .select(col("SOURCE_ENTITY_NAME"))
                .filter(col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name)
                .distinct().to_pandas()
            )

        blank_row = pd.DataFrame({'SOURCE_ENTITY_NAME': ''}, index=[0])
        add_new_row = pd.DataFrame({'SOURCE_ENTITY_NAME': 'Add New'}, index=[0])

        source_entity = pd.concat([blank_row, source_entity]).reset_index(drop=True)
        source_entity = pd.concat([source_entity, add_new_row], ignore_index=True)

        st.session_state.source_entity = source_entity


def set_selection_values(state):
    session = st.session_state.session
    collection_name = st.session_state.collection_name

    if state == 'initial':
        if len(collection_name) > 0:
            if st.session_state["streamlit_mode"] == "NativeApp":
                entity_sql = "SELECT A.SOURCE_ENTITY_NAME, \
                            A.ENTITY_FULLY_QUALIFIED_SOURCE, \
                            A.IS_BASE_ENTITY, \
                            B.JOIN_FROM_SOURCE_ENTITY_NAME, \
                            A.JOIN_TYPE, \
                            B.JOIN_FROM_ENTITY_ATTRIBUTE_NAME, \
                            B.OPERATOR, \
                            B.JOIN_TO_ENTITY_ATTRIBUTE_NAME \
                            FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY A \
                            LEFT JOIN " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION B \
                            ON A.SOURCE_ENTITY_NAME = B.SOURCE_ENTITY_NAME \
                            WHERE A.SOURCE_COLLECTION_NAME = '" + collection_name + "'"
            else:
                entity_sql = "SELECT A.SOURCE_ENTITY_NAME, \
                            A.ENTITY_FULLY_QUALIFIED_SOURCE, \
                            A.IS_BASE_ENTITY, \
                            B.JOIN_FROM_SOURCE_ENTITY_NAME, \
                            A.JOIN_TYPE, \
                            B.JOIN_FROM_ENTITY_ATTRIBUTE_NAME, \
                            B.OPERATOR, \
                            B.JOIN_TO_ENTITY_ATTRIBUTE_NAME \
                            FROM MODELING.SOURCE_ENTITY A \
                            LEFT JOIN MODELING.SOURCE_ENTITY_JOIN_CONDITION B \
                            ON A.SOURCE_ENTITY_NAME = B.SOURCE_ENTITY_NAME \
                            WHERE A.SOURCE_COLLECTION_NAME = '" + collection_name + "'"

            selections = session.sql(entity_sql)
            selections = selections.to_pandas()
            selections = selections.sort_values(by=['IS_BASE_ENTITY'], ascending=False)
            selections.reset_index(inplace=True, drop=True)

            # Get collection filter conditions
            if st.session_state["streamlit_mode"] == "NativeApp":
                filter_condition_pd = (
                    session.table(
                        st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION")
                    .filter(col("SOURCE_COLLECTION_NAME") == collection_name).to_pandas()
                )
            else:
                filter_condition_pd = (
                    session.table("MODELING.SOURCE_COLLECTION_FILTER_CONDITION")
                    .filter(col("SOURCE_COLLECTION_NAME") == collection_name).to_pandas()
                )

            filter_conditions = []

            if len(filter_condition_pd.columns) > 0 and 'filter_conditions' not in st.session_state:
                for index, row in filter_condition_pd.iterrows():
                    filter_conditions.append({
                        "CONDITION_INDEX": index,
                        "SOURCE_COLLECTION_NAME": row['SOURCE_COLLECTION_NAME'],
                        "LEFT_FILTER_EXPRESSION": row['LEFT_FILTER_EXPRESSION'],
                        "OPERATOR": row['OPERATOR'],
                        "RIGHT_FILTER_EXPRESSION": row['RIGHT_FILTER_EXPRESSION']
                    })

                st.session_state.filter_conditions = pd.DataFrame(filter_conditions)
            else:
                filter_contents = {
                    "CONDITION_INDEX": 0,
                    "SOURCE_COLLECTION_NAME": collection_name,
                    "LEFT_FILTER_EXPRESSION": '',
                    "OPERATOR": '',
                    "RIGHT_FILTER_EXPRESSION": ''
                }

                st.session_state.filter_conditions = pd.DataFrame(filter_contents, index=[0])
                if len(st.session_state.filter_conditions) > 0:
                    st.session_state.add_filter = True

            if len(selections) > 0:

                initial_data = []

                for index, row in selections.iterrows():
                    if row['IS_BASE_ENTITY']:
                        base_entity = True
                        relationship_index = -1
                    else:
                        base_entity = False
                        relationship_index = index - 1

                    initial_data.append(
                        {
                            "SOURCE_INDEX": 0,
                            "RELATIONSHIP_INDEX": relationship_index,
                            "SOURCE_ENTITY_NAME": row['SOURCE_ENTITY_NAME'],
                            "ENTITY_FULLY_QUALIFIED_SOURCE": row['ENTITY_FULLY_QUALIFIED_SOURCE'],
                            "IS_BASE_ENTITY": base_entity,
                            "JOIN_FROM_SOURCE_ENTITY_NAME": row['JOIN_FROM_SOURCE_ENTITY_NAME'],
                            "JOIN_TYPE": row['JOIN_TYPE'],
                            "JOIN_FROM_ENTITY_ATTRIBUTE_NAME": row['JOIN_FROM_ENTITY_ATTRIBUTE_NAME'],
                            "OPERATOR": row['OPERATOR'],
                            "JOIN_TO_ENTITY_ATTRIBUTE_NAME": row['JOIN_TO_ENTITY_ATTRIBUTE_NAME']
                        })

                initial_data_pd = pd.DataFrame(initial_data)

                # Drop out rows that weren't finished from last streamlit run
                initial_rows = initial_data_pd[(initial_data_pd['IS_BASE_ENTITY'] == False) &
                                               (initial_data_pd['JOIN_FROM_SOURCE_ENTITY_NAME'].isna())]

                initial_data_pd = initial_data_pd.drop(initial_rows.index)

                st.session_state.wizard_manager = initial_data_pd
            else:
                manager_contents = {
                    "SOURCE_INDEX": 0,
                    "RELATIONSHIP_INDEX": -1,
                    "SOURCE_ENTITY_NAME": '',
                    "ENTITY_FULLY_QUALIFIED_SOURCE": '',
                    "IS_BASE_ENTITY": True,
                    "JOIN_FROM_SOURCE_ENTITY_NAME": '',
                    "JOIN_TYPE": '',
                    "JOIN_FROM_ENTITY_ATTRIBUTE_NAME": '',
                    "OPERATOR": '',
                    "JOIN_TO_ENTITY_ATTRIBUTE_NAME": ''
                }

                st.session_state.wizard_manager = pd.DataFrame(manager_contents, index=[0])

    else:
        manager_contents = {
            "SOURCE_INDEX": 0,
            "RELATIONSHIP_INDEX": -1,
            "SOURCE_ENTITY_NAME": 'Add New',
            "ENTITY_FULLY_QUALIFIED_SOURCE": '',
            "IS_BASE_ENTITY": True,
            "JOIN_FROM_SOURCE_ENTITY_NAME": '',
            "JOIN_TYPE": '',
            "JOIN_FROM_ENTITY_ATTRIBUTE_NAME": '',
            "OPERATOR": '',
            "JOIN_TO_ENTITY_ATTRIBUTE_NAME": ''
        }

        st.session_state.wizard_manager = pd.DataFrame(manager_contents, index=[0])


def add_derived():
    st.session_state.add_derived = True


class CollectionJoining(BasePage):
    def __init__(self):
        self.name = "collection_joining"

    def print_page(self):
        #st.write(st.session_state)
        session = st.session_state.session

        # if st.session_state.current_step == "derived":
        #     st.button(
        #         label="Back",
        #         key="back" + str(self.name),
        #         help="Warning: Changes will be lost!",
        #         on_click=set_page,
        #         args=("overview",),
        #     )

        # elif st.session_state.current_step == "preview":
        #     st.button(
        #         label="Back",
        #         key="back" + str(self.name),
        #         help="Warning: Changes will be lost!",
        #         on_click=set_page,
        #         args=("overview",),
        #     )

        # else:
        #     st.button(
        #         label="Back",
        #         key="back" + str(self.name),
        #         help="Warning: Changes will be lost!",
        #         on_click=set_page,
        #         args=("overview",),
        #     )

        # Set debug flag to on or off this will show multiple df's if set to true
        st.session_state.is_debug = False

        st.header("Map Source Table")

        if 'mapping_state' in st.session_state:
            del st.session_state.mapping_state

        # Reinit session vars- had bug in v1.22 check if needed later version
        st.session_state.collection_entity_name = st.session_state.collection_entity_name

        if 'wizard_manager' not in st.session_state:
            set_collection_name()

            if len(st.session_state.collection_name) > 0:
                st.session_state.disable_collection_name = True
                set_selection_values('initial')
                set_entity_list('initial')
                st.session_state.current_step = 'initial'
                st.session_state.is_new = False
                st.session_state.show_preview = False

            else:
                set_selection_values('new')
                set_entity_list('new')
                st.session_state.is_new = True
                st.session_state.is_base = True
                st.session_state.current_step = "add"
                st.session_state.collection_name = 'Please Add a Collection Name'
                st.session_state.disable_collection_name = False
                st.session_state.show_preview = True
                st.session_state.current_source_index = 0
                st.session_state.current_relationship_index = -1

        # This will put the manager df on top for keeping track of joins
        if st.session_state.is_debug:
            if 'wizard_manager' in st.session_state:
                st.dataframe(st.session_state.wizard_manager)
            st.write(st.session_state)

        if st.session_state["streamlit_mode"] == "NativeApp":
            st.session_state.columns_df = (
                session.table(st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE")
                .select(col("SOURCE_ENTITY_ATTRIBUTE_NAME"), col("SOURCE_ENTITY_NAME"),
                        col("SOURCE_ATTRIBUTE_PROPERTIES"))
                .filter(col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name)
                .filter(col("INCLUDE_IN_ENTITY") == True)
            ).to_pandas()
        else:
            st.session_state.columns_df = (
                session.table("MODELING.SOURCE_ENTITY_ATTRIBUTE")
                .select(col("SOURCE_ENTITY_ATTRIBUTE_NAME"), col("SOURCE_ENTITY_NAME"),
                        col("SOURCE_ATTRIBUTE_PROPERTIES"))
                .filter(col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name)
                .filter(col("INCLUDE_IN_ENTITY") == True)
            ).to_pandas()

        if 'show_preview' in st.session_state:
            if st.session_state.show_preview:
                st.session_state.disable_flag = True
            else:
                st.session_state.disable_flag = False
        else:
            st.session_state.show_preview = False

        with st.expander("Work Area", expanded=st.session_state.show_preview):
            if 'current_step' in st.session_state:
                if st.session_state.current_step == 'initial' or st.session_state.current_step == 'done':
                    st.info("Please Continue With Your Selections")
                #if st.session_state.current_step == 'done':
                #    st.info("Please Continue With Your Selections")
                if st.session_state.current_step == 'add':
                    st.info("Choose your source table that has all of the attributes required for this target table")
                    st.session_state.add_derived = False

                    if st.session_state.help_check:
                        st.info('''
                                This is where you will identify entities for your Source Collection \n
                                ''')

                    #percent_complete2 = 0
                    #progress_text2 = "Step Completion " + str(percent_complete2) + "%"
                    #my_bar2 = st.progress(0, text=progress_text2)
                    #my_bar2.progress(percent_complete2, text=progress_text2)
                    #st.subheader('')

                    databases = fetch_databases()
                    st.session_state.selected_database = st.selectbox(
                        "Databases:", databases
                    )

                    # Based on selected database, fetch schemas and populate the dropdown
                    schemas = fetch_schemas(st.session_state.selected_database)
                    st.session_state.selected_schema = st.selectbox(
                        f"Schemas in {st.session_state.selected_database}:",
                        schemas
                    )

                    # Based on selected database and schema, fetch tables and populate the dropdown
                    tables = fetch_tables(
                        st.session_state.selected_database, st.session_state.selected_schema
                    )
                    st.session_state.selected_table = st.selectbox(
                        f"Tables in {st.session_state.selected_database}.{st.session_state.selected_schema}:",
                        tables
                    )

                    st.subheader('')

                    col1_ex, col2_ex, col3_ex, col4_ex, col5_ex, col6_ex = st.columns(
                        (3, 3, .25, 0.5, 1, 2)
                    )
                    with col1_ex:
                        collection_name = st.text_input(
                            "Collection Name",
                            key="collection_name_input",
                            placeholder=st.session_state.collection_name,
                            disabled=st.session_state.disable_collection_name
                        )
                    with col2_ex:
                        entity_name = st.text_input(
                            "Entity Name",
                            key="entity_name_input",
                            placeholder="Please Input Entity Name",
                        )
                    with col6_ex:
                        st.write("")
                        st.write("")
                        done_adding_button = st.button(
                            "Continue",
                            key="done",
                            on_click=save_entity,
                            type="primary",
                            args=("derived",)
                        )

                if st.session_state.current_step in ('derived', 'derived_join'):
                    if st.session_state.current_step == 'derived':
                        pass
                        #percent_complete2 = 50
                        #progress_text2 = "Step Completion " + str(percent_complete2) + "%"
                        #my_bar2 = st.progress(0, text=progress_text2)
                        #my_bar2.progress(percent_complete2, text=progress_text2)
                    else:
                        st.session_state.add_derived = True

                    st.write("#")

                    derivation_types = ["EXPRESSION", "LITERAL"]

                    if st.session_state["streamlit_mode"] == "NativeApp":
                        source_attribute_of_entity_df = (session.table(
                            st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE"
                        ).filter(
                            col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name).filter(
                            col("SOURCE_ENTITY_NAME") == st.session_state.force_entity_name)).distinct()
                        #  IS_BASE_ENTITY
                        source_attribute_of_entity_sql = "SELECT TOP 1 * FROM   \
                                            " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY \
                                            WHERE SOURCE_ENTITY_NAME = '" + st.session_state.force_entity_name + "'"
                    else:
                        source_attribute_of_entity_df = (session.table("MODELING.SOURCE_ENTITY_ATTRIBUTE")
                                                         .filter(
                            col("SOURCE_COLLECTION_NAME") == st.session_state.collection_name)
                                                         .filter(
                            col("SOURCE_ENTITY_NAME") == st.session_state.force_entity_name)).distinct()

                        source_attribute_of_entity_sql = "SELECT TOP 1 * FROM \
                                            MODELING.SOURCE_ENTITY \
                                            WHERE SOURCE_ENTITY_NAME = '" + st.session_state.force_entity_name + "'"

                    source_attribute_of_entity_base_df = session.sql(source_attribute_of_entity_sql).collect()
                    source_attribute_of_entity_pd = pd.DataFrame(source_attribute_of_entity_base_df)

                    source_entity_base_value = source_attribute_of_entity_pd.loc[0, "IS_BASE_ENTITY"]
                    source_entity_qualified_table_name = source_attribute_of_entity_pd.loc[
                        0, "ENTITY_FULLY_QUALIFIED_SOURCE"]
                    preview_table_df = session.table(source_entity_qualified_table_name)

                    st.session_state.source_attribute_of_entity_df = source_attribute_of_entity_df

                    source_attribute_filtered = source_attribute_of_entity_df.select(
                        source_attribute_of_entity_df.SOURCE_ENTITY_ATTRIBUTE_NAME,
                        source_attribute_of_entity_df.SOURCE_ATTRIBUTE_PROPERTIES[
                            "data_type"
                        ],
                        source_attribute_of_entity_df.INCLUDE_IN_ENTITY,
                        source_attribute_of_entity_df.AGGREGATION_FUNCTION,
                        source_attribute_of_entity_df.DERIVED_EXPRESSION
                    ).to_pandas()

                    source_attribute_filtered.columns = ["SOURCE ENTITY ATTRIBUTE NAME", "DATA TYPE",
                                                         "INCLUDE IN ENTITY", "AGGREGATION FUNCTION",
                                                         "DERIVED EXPRESSION"]
                    # Remove Quotes in column
                    source_attribute_filtered['DATA TYPE'] = source_attribute_filtered['DATA TYPE'].str.replace(r'"',
                                                                                                                '')

                    agg_data = []
                    agg_function = ''
                    for index, row in source_attribute_filtered.iterrows():
                        if row['AGGREGATION FUNCTION'] is None:
                            if row['DATA TYPE'] == 'TEXT':
                                agg_function = 'LISTAGG'
                            elif row['DATA TYPE'] == 'FIXED':
                                agg_function = 'SUM'
                            elif row['DATA TYPE'] == 'BOOLEAN':
                                agg_function = 'LISTAGG'
                            elif row['DATA TYPE'] == 'DATE':
                                agg_function = 'LISTAGG'
                            elif row['DATA TYPE'] == 'TIMESTAMP_NTZ':
                                agg_function = 'MAX'
                            elif row['DATA TYPE'] == 'TIMESTAMP_TZ':
                                agg_function = 'MAX'
                            else:
                                agg_function = 'MAX'

                        agg_data.append(
                            {
                                "SOURCE ENTITY ATTRIBUTE NAME": row['SOURCE ENTITY ATTRIBUTE NAME'],
                                "DATA TYPE": row['DATA TYPE'],
                                "INCLUDE IN ENTITY": row['INCLUDE IN ENTITY'],
                                "AGGREGATION FUNCTION": agg_function,
                                "DERIVED EXPRESSION": row['DERIVED EXPRESSION']
                            })

                    agg_data_pd = pd.DataFrame(agg_data)

                    source_attribute_list = source_attribute_of_entity_df.select(
                        source_attribute_of_entity_df['SOURCE_ENTITY_ATTRIBUTE_NAME']
                    ).to_pandas()

                    entity_col1, entity_col2 = st.columns((1, 1))
                    with entity_col1:
                        st.subheader("Attribute List: " + st.session_state.force_entity_name)
                        if source_entity_base_value:

                            source_attribute_no_agg = source_attribute_filtered[
                                ["SOURCE ENTITY ATTRIBUTE NAME", "DATA TYPE", "INCLUDE IN ENTITY",
                                 "DERIVED EXPRESSION"]]

                            st.session_state.attribute_edited = st.data_editor(source_attribute_no_agg)
                        else:
                            st.session_state.attribute_edited = st.data_editor(agg_data_pd)

                    with entity_col2:
                        if st.session_state.help_check:
                            st.info('''
                                    To the left is the entity you are currently configuring \n
                                    You are able to use the 'INCLUDE IN ENTITY' column to check or un-check
                                    columns if you choose. \n 
                                    - This allows functionality to hide/un-hide columns inside the join wizard. \n 
                                    You may also decide to add a derived column or literal value as a column selection
                                    by click the 'Add Derived Column/Literal Value' button below the left entity table.\n
                                    - When configuring a derived column, please click the 'floppy disk' save button.\n
                                    Once saved, you should see your new derived column show up in the entity table above it. \n
                                    You man add however many derived columns you would like \n
                                    Once done, hit the 'done' button at the bottom right to return to join wizard below
                                      ''')
                        else:
                            st.subheader("Preview Existing Table Records: ")
                            st.dataframe(preview_table_df, hide_index=True)

                    bttn1, filler1, filler2, bttn2 = st.columns((3, 4, 4, 1.5))
                    with bttn1:
                        if not st.session_state.add_derived:
                            add_derived_column = st.button(
                                "Add Derived Column/Literal Value",
                                key="add_derived_column",
                                on_click=add_derived,
                                disabled=True,
                                help="Currently disabled in this version of DMM"
                            )

                    with bttn2:
                        if 'current_source_index' in st.session_state:
                            source_index_val = st.session_state.current_source_index
                        else:
                            source_index_val = 0
                        if 'current_relationship_index' in st.session_state:
                            relationship_index_val = st.session_state.current_relationship_index
                        else:
                            relationship_index_val = -1
                        done_adding_button = st.button(
                            "Continue",
                            key="outside_done",
                            on_click=update_manager_value,
                            type="primary",
                            args=("done_attributes", source_index_val, relationship_index_val)
                        )

                    if 'add_derived' in st.session_state:
                        if st.session_state.add_derived:
                            derive_col1, derive_col2, derive_col3, derive_col4, derive_col5 = st.columns(
                                (0.5, 0.6, 0.5, 0.2, 0.3))
                            with derive_col1:
                                derived_attribute_name = st.text_input(
                                    "Attribute Name",
                                    key="derived_attribute_name",
                                    placeholder="Please Enter Attribute Name"
                                )
                                st.text("")

                            with derive_col2:
                                derived_type = st.selectbox(
                                    "Derivation Type",
                                    derivation_types,
                                    key="derivation_type"
                                )

                            if st.session_state.derivation_type == "LITERAL":
                                with derive_col3:
                                    literal_input = st.text_input(
                                        "Value",
                                        key="literal_value_input"
                                    )
                                with derive_col4:
                                    # st.title('')
                                    st.text('')
                                    add_derived_button = st.button(
                                        ":floppy_disk:",
                                        key="add_derived_attribute",
                                        on_click=add_derived_attribute,
                                        help="Save Derived",
                                    )
                            if st.session_state.derivation_type == "EXPRESSION":
                                with derive_col3:
                                    # st.title('')
                                    st.text('')
                                    add_derived_button = st.button(
                                        ":floppy_disk:",
                                        key="add_derived_attribute",
                                        on_click=add_derived_attribute,
                                        help="Save Derived",
                                    )
                                derive_input, derive_input2, derive_input3, derive_input4, derive_input5 = st.columns(
                                    (2, 0.6, 0.5, 0.2, 0.3))
                                with derive_input:
                                    expression_input = st.text_input(
                                        "Expression Value",
                                        key="expression_value_input"
                                    )
                                st.markdown(
                                    "To learn more about functions and expressions available, please refer to the "
                                    "[Documentation](https://docs.snowflake.com/en/sql-reference/functions)"
                                )
                        done_col1, done_col2, done_col3 = st.columns((4, 2.5, 1.9))
                        with done_col3:
                            st.write("#")
                            if 'current_source_index' in st.session_state:
                                source_index_val = st.session_state.current_source_index
                            else:
                                source_index_val = 0
                            if 'current_relationship_index' in st.session_state:
                                relationship_index_val = st.session_state.current_relationship_index
                            else:
                                relationship_index_val = -1

                if st.session_state.current_step == 'preview':
                    if st.session_state["streamlit_mode"] == "NativeApp":
                        qualified_table_name = st.session_state.native_database_name + ".MODELED." + st.session_state.collection_name
                    else:
                        qualified_table_name = "MAPPING." + st.session_state.collection_name
                    try:
                        dynamic_table = session.table(qualified_table_name).to_pandas()
                    except Exception as e:
                        st.info(e)

                    dynamic_table_columns = pd.DataFrame(dynamic_table.columns)

                    add_new_row = pd.DataFrame({0: 'Please Select'}, index=[0])

                    dynamic_table_columns = pd.concat([add_new_row, dynamic_table_columns]).reset_index(drop=True)
                    st.session_state.dynamic_table_columns = dynamic_table_columns

                    if st.session_state.help_check:
                        st.info('''
                                Your generated collection from your defined join is below \n
                                You may use the add filter button to add a collection filter to further filter results. \n
                                To add multiple filters, click the green 'Add Relationship' button. \n
                                After you are done defining a filter, please click the 'Save' Icon
                                Once finished click Continue to Mapping to go ahead and start mapping your Source to Target attributes
                                  ''')
                    st.write('#')
                    st.dataframe(dynamic_table,hide_index=True)

                    if 'filter_conditions' in st.session_state:
                        filter_conditions_pd = pd.DataFrame(st.session_state.filter_conditions)
                        if len(filter_conditions_pd) > 0:
                            button_disable_flag = True
                        else:
                            button_disable_flag = False
                    else:
                        filter_conditions_pd = pd.DataFrame()
                        button_disable_flag = False

                    add_filter_column = st.button(
                        "Add Filter",
                        key="add_filter_column",
                        on_click=add_filter_relationship,
                        args=(0,),
                        disabled=button_disable_flag
                    )

                    if 'add_filter' in st.session_state:
                        st.header('#')
                        # st.dataframe(st.session_state.filter_conditions)
                        if 'filter_conditions' in st.session_state:
                            filter_conditions_pd = pd.DataFrame(st.session_state.filter_conditions)
                        else:
                            filter_conditions_pd = pd.DataFrame()

                        operations = ["=", ">=", "<=", "!="]
                        operations_pd = pd.DataFrame(operations)

                        if st.session_state.add_filter:
                            #if len(st.session_state.filter_conditions) > 0:
                            if len(filter_conditions_pd) > 0:
                                condition_counter = range(len(st.session_state.filter_conditions))
                            else:
                                condition_counter = pd.DataFrame()

                            for i in condition_counter:
                                condition_row = filter_conditions_pd[(filter_conditions_pd['CONDITION_INDEX'] == i)]
                                condition_row.reset_index(inplace=True)

                                if len(condition_row) > 0:  # and 'left_filter' + str(i) not in st.session_state:
                                    left_filter_expression_value = condition_row.loc[0, "LEFT_FILTER_EXPRESSION"]
                                    if left_filter_expression_value != 'add_new':
                                        if left_filter_expression_value in dynamic_table_columns[0].values:
                                            left_filter_expression_index = dynamic_table_columns.loc[
                                                dynamic_table_columns[0] == left_filter_expression_value].index[0]

                                        else:
                                            left_filter_expression_index = 0
                                            st.session_state["left_literal" + str(i)] = True
                                            st.session_state["left_filter" + str(i)] = left_filter_expression_value

                                        operator_value = condition_row.loc[0, "OPERATOR"]
                                        operatoration_index = \
                                            operations_pd.loc[operations_pd[0] == operator_value].index[0]

                                        right_filter_expression_value = condition_row.loc[0, "RIGHT_FILTER_EXPRESSION"]

                                        if right_filter_expression_value in dynamic_table_columns[0].values:
                                            right_filter_expression_index = dynamic_table_columns.loc[
                                                dynamic_table_columns[0] == right_filter_expression_value].index[0]
                                        else:
                                            right_filter_expression_index = 0
                                            st.session_state["right_literal" + str(i)] = True
                                            st.session_state["right_filter" + str(i)] = right_filter_expression_value

                                    else:
                                        left_filter_expression_index = 0
                                        operatoration_index = 0
                                        right_filter_expression_index = 0

                                add_col1, add_col2, add_col3, add_col4, add_col5, add_col6 = st.columns(
                                    (3, 1, 3, .5, .5, .5))

                                st.divider()

                                with add_col1:

                                    if 'left_literal' + str(i) in st.session_state and st.session_state[
                                        'left_literal' + str(i)]:
                                        left_filter = st.text_input(
                                            "Left Filter",
                                            on_change=update_filter_value,
                                            key="left_filter" + str(i),
                                            args=("left_filter", i,)
                                        )
                                    else:
                                        left_filter = st.selectbox(
                                            'Left Filter Column',
                                            dynamic_table_columns,
                                            on_change=update_filter_value,
                                            index=int(left_filter_expression_index),
                                            key="left_filter" + str(i),
                                            args=("left_filter", i,)
                                        )
                                    left_literal_check = st.checkbox('Value or Expression',
                                                                     key='left_literal' + str(i),
                                                                     on_change=update_filter_value,
                                                                     args=("left_literal", i,))

                                with add_col2:
                                    filter_operation = st.selectbox(
                                        "Operation",
                                        operations_pd,
                                        on_change=update_filter_value,
                                        index=int(operatoration_index),
                                        key="filter_operation" + str(i),
                                        args=("filter_operation", i,)
                                    )
                                with add_col3:
                                    if 'right_literal' + str(i) in st.session_state and st.session_state[
                                        'right_literal' + str(i)]:

                                        right_filter = st.text_input(
                                            "Right Filter",
                                            on_change=update_filter_value,
                                            key="right_filter" + str(i),
                                            args=("right_filter", i,)
                                        )
                                    else:
                                        right_filter = st.selectbox(
                                            'Right Filter Column',
                                            dynamic_table_columns,
                                            index=int(right_filter_expression_index),
                                            on_change=update_filter_value,
                                            key="right_filter" + str(i),
                                            args=("right_filter", i,)
                                        )
                                    right_literal_check = st.checkbox('Value or Expression',
                                                                      on_change=update_filter_value,
                                                                      key='right_literal' + str(i),
                                                                      args=("right_literal", i,))

                                if i == len(st.session_state.filter_conditions) - 1:
                                    with add_col4:
                                        st.text('')
                                        st.text('')

                                        remove_relationship_button = st.button(
                                            ":x:",
                                            key="remove_condition" + str(i),
                                            on_click=remove_filter_relationship,
                                            help="Remove",
                                        )
                                    with add_col5:
                                        st.text('')
                                        st.text('')
                                        add_condition_button = st.button(
                                            ":sparkle:",
                                            key="add_condition" + str(i),
                                            on_click=add_filter_relationship,
                                            args=[i + 1, ],
                                            help="Add Condition",
                                        )
                                    with add_col6:
                                        st.text('')
                                        st.text('')
                                        preview_button = st.button(
                                            ":floppy_disk:",
                                            key="preview" + str(i),
                                            on_click=preview_click,
                                            help="Preview",
                                            args=[True, False]
                                        )

                    done_col1, done_col2, done_col3 = st.columns((5, .75, 1))
                    if 'change_after_preview' in st.session_state:
                        if st.session_state.change_after_preview:
                            with done_col3:
                                st.write("#")
                                done_adding_button = st.button(
                                    "Continue",
                                    key="outside_done",
                                    on_click=set_page,
                                    type="primary",
                                    args=("collection_mapping",),
                                )
                    else:
                        with done_col2:
                            st.write("#")
                            done_adding_button = st.button(
                                "Close",
                                key="outside_done",
                                on_click=update_manager_value,
                                type="primary",
                                args=("done", -1, -1),
                            )
                            with done_col3:
                                st.write("#")
                                done_to_mapping = st.button(
                                    "Continue",
                                    key="outside_to_mapping",
                                    on_click=set_page,
                                    type="primary",
                                    args=("collection_mapping",),
                                )

        st.write('#')

        wizard_counter = len(st.session_state.wizard_manager)
        wizard_manager_pd = pd.DataFrame(st.session_state.wizard_manager)
        source_entity_pd = pd.DataFrame(st.session_state.source_entity)

        join_types = ["INNER", "LEFT", "OUTER", "AND"]
        join_types_pd = pd.DataFrame(join_types)
        operations = ["=", ">=", "<=", "!="]
        operations_pd = pd.DataFrame(operations)
        if st.session_state.help_check:
            st.info('''
                     Below is where you will configure your joins \n
                     Step 1: Select your base table, you may stop if that is where all your source attributes are defined for target mapping\n

                     Step 2: If you want to define a join on the base table, click the add relationship 'green' icon to the right of 
                     the defined base table dropdown. \n
                     You will then be able to 'Join To' another table. If your table is not yet inside the source collection to choose from,
                     please select 'Add New' in the dropdown. \n
                     When selecting 'Add New', you will then be brought to the add entity wizard. Once done, you will be able to choose your entity in the dropdown
                     and continue your selections. \n
                     You are also able to add a new derived column or literal value by choosing the 'Add Derived Column/Literal Value'. \n
                     When selecting 'Add Derived Column/Literal Value' you will be brought to the entity attribute screen to add your derived conifurations

                     Note: by clicking the preview 'eye' icon, or the 'save and continue' button. \n
                     You would then be brought to a view of the constructed join/entity with an option to add a collection filter or proceed onto mapping your source collection to your selected 
                      Target Collection\n
                       ''')

        for i in range(1):  # change this to filtered source df if multiple sources functionality added
            base_source_index = wizard_manager_pd[(wizard_manager_pd['IS_BASE_ENTITY'] == True)][['SOURCE_ENTITY_NAME']]
            base_source_index.reset_index(inplace=True, drop=True)
            base_source_value = base_source_index.loc[0, "SOURCE_ENTITY_NAME"]

            if len(base_source_index) > 0:
                if st.session_state.is_debug:
                    st.dataframe(source_entity_pd)
                base_source_index = \
                    source_entity_pd.loc[source_entity_pd['SOURCE_ENTITY_NAME'] == base_source_value].index[0]
            else:
                base_source_index = 0

            source_select = st.selectbox(
                "Base Table",
                st.session_state.source_entity,
                disabled=st.session_state.disable_flag,
                index=int(base_source_index),
                on_change=update_manager_value,
                key="source_select_" + str(i),
                args=("base_source", i, -1)
            )

            src_col1, src_col2, src_col3, src_col4, src_col5, cont_col = st.columns(
                (2, 1.5, 3, 3, 3, 1.5), gap="small")
            if wizard_counter == 1:
                with src_col1:
                    add_source_button = st.button(
                        ":heavy_plus_sign: Add Relationship",
                        key="add_source" + str(i),
                        on_click=add_relationship,
                        disabled=st.session_state.disable_flag,
                        args=[source_select, i, 0, "source"],
                        help="Add Relationship",
                    )
                with src_col2:
                    preview_button = st.button(
                        "Preview",
                        key="preview_filter" + str(i),
                        on_click=preview_click,
                        disabled=st.session_state.disable_flag,
                        help="Preview",
                        args=[False, False]
                    )
                with cont_col:
                    done_adding_button = st.button(
                        "Continue",
                        key="done" + str(i),
                        on_click=preview_click,
                        disabled=st.session_state.disable_flag,
                        type="primary",
                        args=(False, True)
                    )

            if 'columns_df' in st.session_state:
                columns_df = st.session_state.columns_df
            else:
                columns_df = pd.DataFrame()

            for idx in range(wizard_counter - 1):
                # Grab Session State Row
                wizard_manager_row = wizard_manager_pd[(wizard_manager_pd['SOURCE_INDEX'] == i) &
                                                       (wizard_manager_pd['RELATIONSHIP_INDEX'] == idx)]

                if len(wizard_manager_row) > 0:

                    wizard_manager_row.reset_index(inplace=True)
                    join_type_wiz_value = wizard_manager_row.loc[0, "JOIN_TYPE"]

                    if join_type_wiz_value in ("OR", "AND"):
                        wizard_manager_row = wizard_manager_pd[(wizard_manager_pd['SOURCE_INDEX'] == i) &
                                                               (wizard_manager_pd[
                                                                    'RELATIONSHIP_INDEX'] == idx - 1)]
                    elif idx > 0:
                        st.divider()
                if len(wizard_manager_row) > 0:
                    # Reset Index
                    wizard_manager_row.reset_index(inplace=True)

                    # Grab row values

                    join_from_entity_wiz_value = wizard_manager_row.loc[0, "JOIN_FROM_SOURCE_ENTITY_NAME"]

                    join_to_entity_wiz_value = wizard_manager_row.loc[0, "SOURCE_ENTITY_NAME"]
                    join_from_entity_attr_wiz_value = wizard_manager_row.loc[0, "JOIN_FROM_ENTITY_ATTRIBUTE_NAME"]
                    join_to_entity_attr_wiz_value = wizard_manager_row.loc[0, "JOIN_TO_ENTITY_ATTRIBUTE_NAME"]
                    join_type_wiz_value = wizard_manager_row.loc[0, "JOIN_TYPE"]

                    if join_type_wiz_value is None:
                        join_type_wiz_value = 'INNER'
                    operator_wiz_value = wizard_manager_row.loc[0, "OPERATOR"]
                    if operator_wiz_value is None:
                        operator_wiz_value = '='

                    # Get index for rows
                    join_from_index = source_entity_pd.loc[
                        source_entity_pd['SOURCE_ENTITY_NAME'] == join_from_entity_wiz_value].index[0]

                    join_to_index = \
                        source_entity_pd.loc[
                            source_entity_pd['SOURCE_ENTITY_NAME'] == join_to_entity_wiz_value].index[0]

                    join_type_index = join_types_pd.loc[join_types_pd[0] == join_type_wiz_value].index[0]
                    operatoration_index = operations_pd.loc[operations_pd[0] == operator_wiz_value].index[0]

                    # Grab attribute from columns
                    from_columns_df = columns_df[(columns_df['SOURCE_ENTITY_NAME'] == join_from_entity_wiz_value)][
                        ['SOURCE_ENTITY_ATTRIBUTE_NAME']]

                    add_new_row = pd.DataFrame({'SOURCE_ENTITY_ATTRIBUTE_NAME': 'Please Select'}, index=[0])
                    add_new_column = pd.DataFrame(
                        {'SOURCE_ENTITY_ATTRIBUTE_NAME': 'Add New Derived Column/Literal Value'}, index=[0])

                    from_columns_df = from_columns_df.sort_values(by=['SOURCE_ENTITY_ATTRIBUTE_NAME'], ascending=True)
                    from_columns_df = pd.concat([add_new_column, from_columns_df]).reset_index(drop=True)
                    from_columns_df = pd.concat([add_new_row, from_columns_df]).reset_index(drop=True)

                    # Reset index of from columns
                    from_columns_df.reset_index(inplace=True, drop=True)
                    from_table_switch = from_columns_df.isin([join_from_entity_attr_wiz_value.upper()]).any().any()
                    if not from_table_switch:
                        from_columns_index = 0
                    elif len(from_columns_df) > 0 and len(
                            join_from_entity_attr_wiz_value) > 0 and join_from_entity_attr_wiz_value != 'Add New Derived Column/Literal Value' \
                            and join_from_entity_wiz_value != 'Add New':

                        from_columns_index = from_columns_df.loc[from_columns_df[
                                                                     'SOURCE_ENTITY_ATTRIBUTE_NAME'] == join_from_entity_attr_wiz_value.upper()].index[
                            0]
                    else:
                        from_columns_index = 0
                    # Grab attribute to columns
                    to_columns_df = columns_df[(columns_df['SOURCE_ENTITY_NAME'] == join_to_entity_wiz_value)][
                        ['SOURCE_ENTITY_ATTRIBUTE_NAME']]

                    add_new_row = pd.DataFrame({'SOURCE_ENTITY_ATTRIBUTE_NAME': 'Please Select'}, index=[0])
                    add_new_column = pd.DataFrame(
                        {'SOURCE_ENTITY_ATTRIBUTE_NAME': 'Add New Derived Column/Literal Value'}, index=[0])

                    to_columns_df = to_columns_df.sort_values(by=['SOURCE_ENTITY_ATTRIBUTE_NAME'], ascending=True)
                    to_columns_df = pd.concat([add_new_column, to_columns_df]).reset_index(drop=True)
                    to_columns_df = pd.concat([add_new_row, to_columns_df]).reset_index(drop=True)

                    to_table_switch = to_columns_df.isin([join_to_entity_attr_wiz_value.upper()]).any().any()

                    if not to_table_switch:
                        to_columns_index = 0
                    elif (len(to_columns_df) > 0 and len(join_to_entity_attr_wiz_value) > 0
                          and join_to_entity_attr_wiz_value != 'Add New Derived Column/Literal Value'):
                        to_columns_index = to_columns_df.loc[to_columns_df[
                                                                 'SOURCE_ENTITY_ATTRIBUTE_NAME'] == join_to_entity_attr_wiz_value.upper()].index[
                            0]
                    else:
                        to_columns_index = 0

                else:
                    join_from_index = 0
                    join_to_index = 0
                    join_type_index = 0
                    operatoration_index = 0
                    from_columns_index = 0
                    to_columns_index = 0
                    from_columns_df = ''
                    to_columns_df = ''

                (
                    sub_col1,
                    sub_col2,
                    sub_col3,
                    sub_col4,
                    sub_col5,
                    sub_col6,
                    sub_col7,
                ) = st.columns((.75, .75, .75, 0.11, 0.11, 0.11, 0.11))
                with (sub_col1):
                    if idx == 0:
                        join_type = st.selectbox(
                            "Join Type",
                            ["INNER", "LEFT", "OUTER"],
                            index=int(join_type_index),
                            key="join_type_" + str(i) + str(idx),
                            on_change=update_manager_value,
                            disabled=st.session_state.disable_flag,
                            args=("join_type", i, idx)
                        )
                    else:
                        join_type = st.selectbox("Join/Column Condition",
                                                 ["INNER", "LEFT", "OUTER", "AND"],
                                                 index=int(join_type_index),
                                                 key="join_type_" + str(i) + str(idx),
                                                 on_change=update_manager_value,
                                                 disabled=st.session_state.disable_flag,
                                                 args=("join_type", i, idx))

                        if "join_type_" + str(i) + str(idx) in st.session_state:
                            current_join_column_condition = st.session_state["join_type_" + str(i) + str(idx)]
                            if current_join_column_condition in ("INNER", "LEFT", "OUTER"):
                                join_from = st.selectbox(
                                    "Join from",
                                    st.session_state.source_entity,
                                    disabled=st.session_state.disable_flag,
                                    on_change=update_manager_value,
                                    index=int(join_from_index),
                                    key="join_from_" + str(i) + str(idx),
                                    args=("join_from", i, idx)
                                )
                    join_from_column = st.selectbox(
                        "Join From Column",
                        from_columns_df,
                        on_change=update_manager_value,
                        disabled=st.session_state.disable_flag,
                        index=int(from_columns_index),
                        args=("join_from_column", i, idx),
                        key="join_from_column_" + str(i) + str(idx)
                    )

                with sub_col2:
                    if idx == 0:
                        join_to = st.selectbox(
                            "Join To",
                            st.session_state.source_entity,
                            disabled=st.session_state.disable_flag,
                            on_change=update_manager_value,
                            index=int(join_to_index),
                            key="join_to_" + str(i) + str(idx),
                            args=("base_join_source", i, idx)
                        )
                        operation = st.selectbox(
                            "Operation",
                            ["=", ">=", "<=", "!="],
                            on_change=update_manager_value,
                            disabled=st.session_state.disable_flag,
                            index=int(operatoration_index),
                            args=("operation", i, idx),
                            key="operation_" + str(i) + str(idx))

                    elif current_join_column_condition in ("INNER", "LEFT", "RIGHT"):
                        st.subheader('')
                        st.text('')
                        st.text('')

                        join_to = st.selectbox(
                            "Join To",
                            st.session_state.source_entity,
                            disabled=st.session_state.disable_flag,
                            index=int(join_to_index),
                            on_change=update_manager_value,
                            key="join_to_" + str(i) + str(idx),
                            args=("join_to", i, idx)
                        )

                        operation = st.selectbox("Operation",
                                                 ["=", ">=", "<=", "!="],
                                                 on_change=update_manager_value,
                                                 disabled=st.session_state.disable_flag,
                                                 index=int(operatoration_index),
                                                 args=("operation", i, idx),
                                                 key="operation_" + str(i) + str(idx))
                    else:
                        st.text('')
                        st.subheader('')
                        st.subheader('')
                        st.subheader('')

                        operation = st.selectbox("Operation",
                                                 ["=", ">=", "<=", "!="],
                                                 on_change=update_manager_value,
                                                 disabled=st.session_state.disable_flag,
                                                 index=int(operatoration_index),
                                                 args=("operation", i, idx),
                                                 key="operation_" + str(i) + str(idx))
                with sub_col3:
                    if idx == 0:
                        st.subheader('')
                        st.text('')
                        st.text('')

                    elif current_join_column_condition in ("INNER", "LEFT", "RIGHT"):
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.subheader('')
                        st.text('')
                        st.text('')
                    else:
                        st.text('')
                        st.subheader('')
                        st.subheader('')
                        st.subheader('')
                    join_to_column = st.selectbox(
                        "Join to Column",
                        to_columns_df,
                        on_change=update_manager_value,
                        disabled=st.session_state.disable_flag,
                        index=int(to_columns_index),
                        args=("join_to_column", i, idx),
                        key="join_to_column_" + str(i) + str(idx)
                    )

                with sub_col4:
                    if idx == 0:
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')
                    elif current_join_column_condition in ("OR", "AND"):
                        st.write("####")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")

                    else:
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')

                    if idx == wizard_counter - 2:
                        remove_relationship_button = st.button(
                            ":x:",
                            key=str("remove" + str(i) + str(idx)) + "_",
                            disabled=st.session_state.disable_flag,
                            on_click=remove_relationship,
                            help="Remove",
                        )

                with sub_col5:
                    if idx == 0:
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')
                    elif current_join_column_condition in ("OR", "AND"):
                        st.write("####")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")
                    else:
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')

                    if idx == wizard_counter - 2:
                        add_relationship_button = st.button(
                            ":sparkle:",
                            key=str("add_" + str(i) + str(idx)),
                            on_click=add_relationship,
                            disabled=st.session_state.disable_flag,
                            args=[source_select, i, idx + 1, "join"],
                            help="Add Relationship",
                        )
                with sub_col6:
                    if idx == 0:
                        st.session_state.divider = True
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')
                    elif current_join_column_condition in ("OR", "AND"):
                        st.session_state.divider = False
                        st.write("####")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")
                        st.subheader("")

                    else:
                        st.session_state.divider = True

                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.subheader('')
                        st.text('')
                        st.text('')
                        st.text('')
                        st.text('')

                    if idx == wizard_counter - 2:
                        preview_button = st.button(
                            ":eye:",
                            key=str("preview" + str(i) + str(idx)) + "_",
                            on_click=preview_click,
                            disabled=st.session_state.disable_flag,
                            help="Preview",
                            args=[False, False]
                        )

    def print_sidebar(self):
        super().print_sidebar()
