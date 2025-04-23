import json

from src.Page import BasePage, set_page
import streamlit as st
import pandas as pd

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
    st.write(table_sql)

    st.session_state.selected_database = database.upper()
    st.session_state.selected_schema = schema.upper()
    st.session_state.selected_table = table_name.upper()
    table_result = session.sql(table_sql).to_pandas()

    st.session_state.selected_df = table_result

    set_page('column_select')

class ReportPage(BasePage):
    def __init__(self):
        self.name = "report_package"

    def print_page(self):
        session = st.session_state.session
        st.title("Report Package")
        st.write("#")
        cols = st.columns((2, 2, 2, 1))
        fields = ['Database', 'Schema', 'Table Name', 'Actions']

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

