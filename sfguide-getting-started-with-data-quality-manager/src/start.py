import json

from src.Page import BasePage, set_page
import streamlit as st
import pandas as pd


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


def table_direct(database, schema, table_name):
    session = st.session_state.session
    # st.session_state.qualified_selected_table = (
    #             st.session_state.selected_database
    #             + "."
    #             + st.session_state.selected_schema
    #             + "."
    #             + st.session_state.selected_table)
    st.session_state.selected_table = table_name
    table_sql = ("select  \
                 table_catalog as DB, \
                 table_schema as SCHEMA, \
                 table_name, \
                 column_name, \
                 data_type, \
                 is_identity, \
                 control_report_id, \
                 case when cr.active_flg = True then True else False end::boolean as active_flag, \
                from information_schema.columns c \
                left join QC_TESTING.QC.control_report cr \
                on upper(cr.object_var:database_name::varchar) = c.table_catalog \
                and upper(cr.object_var:schema_name::varchar) = c.table_schema \
                and upper(cr.object_var:table_name::varchar) = c.table_name \
                and upper(cr.object_var:c_name::varchar) = c.column_name \
                where c.table_catalog = '" + database.upper() + "' \
                and c.table_schema = '" + schema.upper() + "' \
                and c.table_name = '" + table_name.upper() + "'")
    # st.write(table_sql)

    st.session_state.selected_database = database.upper()
    st.session_state.selected_schema = schema.upper()
    st.session_state.selected_table = table_name.upper()
    table_result = session.sql(table_sql).to_pandas()

    st.session_state.selected_df = table_result

    set_page('column_select')

def get_table_attributes():
    session = st.session_state.session
    if (st.session_state.selected_database is not None
            and st.session_state.selected_schema is not None
            and st.session_state.selected_table is not None):
        st.session_state.qualified_selected_table = (
                st.session_state.selected_database
                + "."
                + st.session_state.selected_schema
                + "."
                + st.session_state.selected_table)

    # get_columns_sql = "SELECT column_name, data_type, is_identity FROM \
    #                   "+ st.session_state.selected_database +".information_schema.columns \
    #                   where table_catalog = '"+ st.session_state.selected_database.upper() +"' \
    #                   and table_schema = '"+ st.session_state.selected_schema.upper() +"'"
    #
    # df = session.sql(get_columns_sql).to_pandas()



    # TO-DO Query control table to set is_active or selected
    table_sql = ("select  \
                 table_catalog as DB, \
                 table_schema as SCHEMA, \
                 table_name, \
                 column_name, \
                 data_type, \
                 is_identity, \
                 cr.control_report_id, \
                 case when cr.active_flg = True then True else False end::boolean as active_flag, \
                from information_schema.columns c \
                left join QC_TESTING.QC.control_report cr \
                on upper(cr.object_var:database_name::varchar) = c.table_catalog \
                and upper(cr.object_var:schema_name::varchar) = c.table_schema \
                and upper(cr.object_var:table_name::varchar) = c.table_name \
                and upper(cr.object_var:c_name::varchar) = c.column_name \
                where c.table_catalog = '"+ st.session_state.selected_database.upper() +"' \
                and c.table_schema = '"+ st.session_state.selected_schema.upper() +"' \
                and c.table_name = '"+ st.session_state.selected_table.upper() +"'")
    table_result = session.sql(table_sql).to_pandas()
    # st.write(table_result)
    # st.write(type(table_result))

    # Took this out and did it with a query instead above
    # This is to add a boolean column to the dataframe
    # attr_data = []
    # for index, row in df.iterrows():
    #     attr_data.append(
    #         {
    #             "COLUMN NAME": row['COLUMN_NAME'],
    #             "DATA TYPE": row['DATA_TYPE'],
    #             "IS IDENTITY": row['IS_IDENTITY'],
    #             "SELECTED": False,
    #             "IS_ACTIVE": False
    #         })

    st.session_state.selected_df = table_result

    set_page('column_select')


class StartPage(BasePage):
    def __init__(self):
        self.name = "start"

    def print_page(self):
        session = st.session_state.session

        if 'initialized_select_page' in st.session_state:
            del st.session_state.initialized_select_page

        st.progress(50, text='**Step 1 of 2**') # Example of boldness
        st.write('#')
        st.info("Please make your selections")

        col1, col2 = st.columns((4, 4))
        with col1:
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

            # This makes space between elements
            st.write('#')
            st.write('#')

        with col2:

            table_list = session.table("QC_TESTING.QC.control_report").to_pandas()

            object_column = table_list['OBJECT_VAR']

            table_list = []
            for row in object_column:
                the_row = json.loads(row)
                table_object_new = {
                    "database_name": the_row['database_name'],
                    "schema_name": the_row['schema_name'],
                    "table_name": the_row['table_name']
                }
                table_list.append(table_object_new)

            unique_table_list = pd.Series(table_list).drop_duplicates().tolist()
            unique_table_list_pd = pd.DataFrame(unique_table_list)
            # st.dataframe(unique_table_list)

        col1, col2, col3, col4 = st.columns((2, 3, 2, .85))
        with col4:
            done_adding_button = st.button(
                "Next Step",
                key="done",
                on_click=get_table_attributes,
                type="primary"
            )

        cols = st.columns((2, 2, 2, 1))
        fields = ['Database', 'Schema', 'Table Name', 'Actions']

        for column, field in zip(cols, fields):
            column.write("**" + field + "**")

        for idx, row in unique_table_list_pd.iterrows():
            col1, col2, col3, col4 = st.columns(
                (2, 2, 2, 1))
            with col1:
                st.write(str(row["database_name"]))
            with col2:
                st.write(str(row["schema_name"]))
            with col3:
                st.write(str(row["table_name"]))
            with col4:
                st.button(label=":arrow_right:", help="Go Directly to " + str(row["table_name"]),
                          key="edit" + str(idx),
                          on_click=table_direct,
                          args=(row["database_name"],row["schema_name"], row["table_name"]))

    def print_sidebar(self):
        super().print_sidebar()
