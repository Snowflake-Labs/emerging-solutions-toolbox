from src.Page import BasePage, set_page
import streamlit as st
import pandas as pd
import numpy as np
import time
from snowflake.snowpark.functions import col, when_matched, when_not_matched


def refresh_table():
    session = st.session_state.session
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
                left join control_report cr \
                on upper(cr.object_var:database_name::varchar) = c.table_catalog \
                and upper(cr.object_var:schema_name::varchar) = c.table_schema \
                and upper(cr.object_var:table_name::varchar) = c.table_name \
                and upper(cr.object_var:c_name::varchar) = c.column_name \
                where c.table_catalog = '"+ st.session_state.selected_database.upper() +"' \
                and c.table_schema = '"+ st.session_state.selected_schema.upper() +"' \
                and c.table_name = '"+ st.session_state.selected_table.upper() +"'")
    # st.write(table_sql)
    st.session_state.edit_table = session.sql(table_sql).to_pandas()


def save_table_attributes():
    session = st.session_state.session

    # st.write(st.session_state.resultset)
    st.session_state.resultset_pd = pd.DataFrame(st.session_state.edited)

    new_df_list = []
    for index, row in st.session_state.resultset_pd.iterrows():

        new_df_list.append(
            {
                "CONTROL_REPORT_ID": row['CONTROL_REPORT_ID'],
                "OBJECT_VAR": {"c_datatype": row['DATA_TYPE'].lower(), "c_name": row['COLUMN_NAME'].lower(), "database_name": row['DB'].lower(), "schema_name": row['SCHEMA'].lower(), "table_name": row['TABLE_NAME'].lower()},
                "ACTIVE_FLAG": row['ACTIVE_FLAG']

            }

        )

    source_df = session.create_dataframe(new_df_list)

    source_df_filtered = source_df[(source_df['CONTROL_REPORT_ID'] != "nan" ) |
                                   (source_df[
                                        'ACTIVE_FLAG'] == True)]

    target_table = session.table("control_report")

    target_table.merge(
        source_df_filtered,
        (target_table["CONTROL_REPORT_ID"] == source_df_filtered["CONTROL_REPORT_ID"]),
        [
            when_matched().update(
                {
                    "OBJECT_VAR": source_df_filtered["OBJECT_VAR"],
                    "ACTIVE_FLG": source_df_filtered["ACTIVE_FLAG"],
                }
            ),
            when_not_matched().insert(
                {
                    "OBJECT_VAR": source_df_filtered["OBJECT_VAR"],
                    "ACTIVE_FLG": source_df_filtered["ACTIVE_FLAG"],
                }
            ),
        ],
    )


   #  ## Success message and revert to homepage after save
    refresh_table()
    st.info('Save Successful')
   #  #st.experimental_rerun()
   #  # with st.spinner('Redirecting back to Home in 5 Seconds'):
   #  #    time.sleep(5)
   #  #    set_page('start')



def initialize():
    # Do this once on first page load
    st.session_state.edit_table = st.session_state.selected_df
    st.session_state.initialized_select_page = True


class ColumnPage(BasePage):
    def __init__(self):
        self.name = "column_select"

    def print_page(self):
        session = st.session_state.session

        if 'initialized_select_page' not in st.session_state:
            initialize()

        st.progress(100, text='**Step 2 of 2**') # Example of boldness
        st.write('#')
        if 'selected_df' in st.session_state:
            st.subheader(st.session_state.selected_table + ': Metadata')
            # edit_table = st.session_state.selected_df
            st.session_state.edited = st.data_editor(st.session_state.edit_table, use_container_width=True,
                                                     disabled="CHECK_FLAG", key="resultset")

            # This is for the multiselect option
            # selected_pd = pd.DataFrame(st.session_state.selected_df)
            # options = selected_pd['COLUMN_NAME']
            # multi = st.multiselect('**Please Select Your Columns**', options, key='column_multi')
            # st.write(multi)


            st.write('#')
            st.write('#')

            col1, col2, col3, col4 = st.columns((2, 3, 2, .85))
            with col1:
                back_button = st.button(
                    "Back",
                    key="back",
                    on_click=set_page,
                    args=("start",),
                    type="primary"
                )
            with col4:
                save_button = st.button(
                    "Save",
                    key="save",
                    on_click=save_table_attributes,
                    type="primary"
                )

            # Commented out
            # st.header("Debug Area  😵‍💫")
            # st.write(st.session_state)
            # st.write("Current Control Table")
            # st.dataframe(session.table("control_report"),use_container_width=True)

    def print_sidebar(self):
        super().print_sidebar()