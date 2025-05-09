import streamlit as st
import re
from appPages.page import BasePage, col, set_page, pd
import ast


def sql_insert_from_df(source, target):
    cols = ', '.join(source.columns.to_list())
    vals = []

    for index, r in source.iterrows():
        row = []
        for x in r:
            row.append(f"'{str(x)}'")

        row_str = ', '.join(row)
        vals.append(row_str)

    f_values = []
    for v in vals:
        f_values.append(f'({v})')

    # Handle inputting NULL values
    f_values = ', '.join(f_values)
    f_values = re.sub(r"('None')", "NULL", f_values)

    sql = f"insert into {target} ({cols}) values {f_values};"

    return sql


def json_insert_from_df(source, target):
    f_values = []
    for index, row in source.iterrows():
        f_values.append((f"INSERT INTO {target} (" + str(
            ', '.join(
                source.columns)) + f") SELECT '{row['TARGET_COLLECTION_NAME']}', '{row['VERSION']}', '{row['TARGET_ENTITY_NAME']}', '{row['TARGET_ENTITY_ATTRIBUTE_NAME']}' , PARSE_JSON( ' {row['TARGET_ATTRIBUTE_PROPERTIES']} '); \n"))

    f_values = ' '.join(f_values)
    f_values = re.sub(r"('None')", "NULL", f_values)
    parse_sql = f"{f_values}"
    return parse_sql


class ConsumerAppSetup(BasePage):
    def __init__(self):
        self.name = "consumer_app_setup"

    def print_page(self):
        session = st.session_state.session

        target_collection_pd = (
            session.table(st.session_state.native_database_name + ".ADMIN.TARGET_COLLECTION")
            .select(col("TARGET_COLLECTION_NAME"), col("VERSION"))
            .distinct()
            .to_pandas()
        )

        target_entity_pd = (
            session.table(st.session_state.native_database_name + ".ADMIN.TARGET_ENTITY")
            .select(col("TARGET_COLLECTION_NAME"), col("VERSION"), col("TARGET_ENTITY_NAME"))
            .distinct()
            .to_pandas()
        )

        target_entity_attribute_pd = (
            session.table(st.session_state.native_database_name + ".ADMIN.TARGET_ENTITY_ATTRIBUTE")
            .select(col("TARGET_COLLECTION_NAME"), col("VERSION"), col("TARGET_ENTITY_NAME"),
                    col("TARGET_ENTITY_ATTRIBUTE_NAME"), col("TARGET_ATTRIBUTE_PROPERTIES"))
            .filter(col("INCLUDE_IN_ENTITY") == 'TRUE')
            .distinct()
            .to_pandas()
        )

        insert_collection_statement = sql_insert_from_df(target_collection_pd,
                                                         'DATA_MODEL_MAPPER_APP_PACKAGE.ADMIN.TARGET_COLLECTION')

        insert_entity_statement = sql_insert_from_df(target_entity_pd,
                                                     'DATA_MODEL_MAPPER_APP_PACKAGE.ADMIN.TARGET_ENTITY')

        insert_entity_attribute_statement = json_insert_from_df(target_entity_attribute_pd,
                                                                  'DATA_MODEL_MAPPER_APP_PACKAGE.ADMIN.TARGET_ENTITY_ATTRIBUTE')

        st.header("Consumer App Setup")
        st.markdown("")
        st.markdown("**Please run through the following steps to use your own target data inside the data model mapper consumer app:**")
        st.markdown("")
        with st.container(border=True):
            st.markdown("**Step 1**")
            st.write("Replace the cell inside the data_model_mapper notebook that is named Target_Sample_Data with the contents below")

            st.code(
                insert_collection_statement + '\n\n' + insert_entity_statement + '\n\n' + insert_entity_attribute_statement,
                language="sql", wrap_lines=False)

        with st.container(border=True):
            st.markdown("**Step 2**")
            st.write(f'''Once you have replaced the section of sample data with the target data above, you can proceed with installing the data
                     model mapper application on the consumer account using your modified data_model_mapper notebook''')

    def print_sidebar(self):
        super().print_sidebar()
