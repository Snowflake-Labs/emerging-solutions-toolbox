import streamlit as st
import pandas as pd
from appPages.page import BasePage, col, set_page


def get_collection_name():
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
        st.session_state.delete_enable = False
    else:
        st.session_state.delete_enable = True
        st.session_state.collection_name = "N/A"


def drop_view_and_dynamic():
    session = st.session_state.session

    # get current database (app name)

    if st.session_state["streamlit_mode"] == "NativeApp":
        application_name = session.sql("""select current_database()""").collect()[0][0]

        source_collection_pd = (
            session.table("data_model_mapper_share_db.configuration.source_collection")
            .filter(col("source_collection_name") == st.session_state.collection_name)).distinct().to_pandas()

        source_collection_pd.reset_index(inplace=True, drop=True)

        if len(source_collection_pd) > 0:
            application_name = session.sql("""select current_database()""").collect()[0][0]
            # target collection
            target_collection_name = source_collection_pd.loc[0, "TARGET_COLLECTION_NAME"]

            # version
            version = source_collection_pd.loc[0, "VERSION"]

            # target entity
            target_entity_name = source_collection_pd.loc[0, "TARGET_ENTITY_NAME"]

            view_name = 'DATA_MODEL_MAPPER_SHARE_DB.MAPPED.' + target_collection_name + '__' + version + '__' + target_entity_name

            # Drop View Name
            drop_view_sql = f"""DROP VIEW IF EXISTS {view_name} """

            try:
                session.sql(drop_view_sql).collect()
            except Exception as e:
                st.info(e)
            # Drop Dynamic Table

            dynamic_table_name = 'DATA_MODEL_MAPPER_SHARE_DB.MODELED.' + st.session_state.collection_name
            drop_dynamic_sql = f"""DROP DYNAMIC TABLE IF EXISTS {dynamic_table_name} """

            try:
                session.sql(drop_dynamic_sql).collect()
            except Exception as e:
                st.info(e)

        else:
            st.write("No Views to delete")


def remove_definitions():
    session = st.session_state.session

    # Drop views and dynamic table tied to collection
    drop_view_and_dynamic()

    if st.session_state["streamlit_mode"] == "NativeApp":
        collection_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        update_entity_sql = ("DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY  \
                                     WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' ")
        condition_delete_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        filter_condition_delete_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_join_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_attribute_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_target_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_TO_TARGET_MAPPING WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_filter_condition_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_entity_condition_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
    else:
        collection_sql = "DELETE FROM MODELING.SOURCE_COLLECTION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        update_entity_sql = ("DELETE FROM MODELING.SOURCE_ENTITY  \
                                     WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' ")
        condition_delete_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        filter_condition_delete_sql = "DELETE FROM MODELING.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_join_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_attribute_sql = "DELETE FROM MODELING.SOURCE_ENTITY_ATTRIBUTE WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_target_sql = "DELETE FROM MODELING.SOURCE_TO_TARGET_MAPPING WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_filter_condition_sql = "DELETE FROM MODELING.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_entity_condition_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"

    # Add Try Catches Error Handling
    try:
        collection_run = session.sql(collection_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        update_run = session.sql(update_entity_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        condition_run = session.sql(condition_delete_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        filter_condition_run = session.sql(filter_condition_delete_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        entity_run = session.sql(entity_join_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        entity_attribute_run = session.sql(entity_attribute_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_target_run = session.sql(source_target_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_filter_condition_run = session.sql(source_filter_condition_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_entity_condition_run = session.sql(source_entity_condition_sql).collect()
    except Exception as e:
        st.info(e)

    st.success("Collection data successfully deleted")

def update_dynamic_refresh(selected_refresh_rate):
    session = st.session_state.session
    view_name = f'{st.session_state.selected_target_collection}__v1__{st.session_state.collection_entity_name}'
    dyn_table_nm = st.session_state.collection_name.upper()
    fq_dynamic_table_name = f"DATA_MODEL_MAPPER_SHARE_DB.MODELED.{dyn_table_nm}"
    update_refresh_mins = f"""ALTER DYNAMIC TABLE {fq_dynamic_table_name} SET TARGET_LAG = '{selected_refresh_rate} minutes'; """
    session.sql(update_refresh_mins).collect()
    
    show_dynamic_tables_sql = f"""show dynamic tables in DATA_MODEL_MAPPER_SHARE_DB.MODELED;"""
    show_dynamic_tables = session.sql(show_dynamic_tables_sql).collect()
    
    if len(show_dynamic_tables) > 0:
        dynamic_tables_df = pd.DataFrame(show_dynamic_tables)

        for index, table in dynamic_tables_df.iterrows():
            if table['name'] == dyn_table_nm:
                lag_time = table['target_lag']
                if lag_time == '1 day':
                    lag_time = '24 hours'
                    create_val_task = f"""call data_model_mapper_app.validation.create_validation_task('{view_name}', '{lag_time}');"""
                    session.sql(create_val_task).collect()
                else:
                    create_val_task = f"""call data_model_mapper_app.validation.create_validation_task('{view_name}', '{lag_time}');"""
                    session.sql(create_val_task).collect()
                

class EntityConfiguration(BasePage):
    def __init__(self):
        self.name = "entity_config"

    def print_page(self):
        st.title("Collection Configuration")
        get_collection_name()

        st.write("")
        if 'collection_name' in st.session_state:
            st.write("Collection data for: " + '\n' + '**' + st.session_state.collection_name + '**')

        tab1, tab2 = st.tabs(["Delete Collection", "Dynamic Refresh Schedule"])

        with tab1:
            if 'collection_name' in st.session_state:
                if st.session_state.collection_name == "N/A":
                    st.write("No collection data exists for deletion")
                else:
                    st.write("(This will delete all metadata, views, dynamic tables tied to the collection)")
                    st.button(
                        "DELETE ALL",
                        key="delete",
                        type="primary",
                        disabled=st.session_state.delete_enable,
                        on_click=remove_definitions,
                    )
    
        with tab2:
            st.write("")
            selected_refresh_rate = st.slider("Select Dynamic Table Refresh (in Minutes):", 0, 1440, 1440)
            st.button(
                "Save",
                key="save_dynamic",
                disabled= True if st.session_state.collection_name == "N/A" else False,
                on_click=update_dynamic_refresh,
                args=(selected_refresh_rate,),
                type="primary"
            )

    def print_sidebar(self):
        super().print_sidebar()
