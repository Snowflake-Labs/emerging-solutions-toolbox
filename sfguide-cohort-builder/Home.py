import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import os

def initialize_session_state():
    st.session_state.card_bg_color = "#eceff1"
    st.session_state.header_bg_color = "#37474f"
    
    if 'build_cohort_selected_tab' not in st.session_state:
        st.session_state.build_cohort_selected_tab = 1
    
    if 'selected_database' not in st.session_state:
        st.session_state.selected_database = ''
    if 'selected_schema' not in st.session_state:
        st.session_state.selected_schema = ''
    if 'selected_table' not in st.session_state:
        st.session_state.selected_table = ''
    if 'prev_selected_database' not in st.session_state:
        st.session_state.prev_selected_database = ''
    if 'prev_selected_schema' not in st.session_state:
        st.session_state.prev_selected_schema = ''
    if 'prev_selected_table' not in st.session_state:
        st.session_state.prev_selected_table = ''
    if 'table_data' not in st.session_state:
        st.session_state.table_data = None
    if 'metadata_raw' not in st.session_state:
        st.session_state.metadata_raw = None
    if 'dataset' not in st.session_state:
        st.session_state.dataset = None
    if 'row_count' not in st.session_state:
        st.session_state.row_count = None
    if 'table_size_mb' not in st.session_state:
        st.session_state.table_size_mb = None
    if 'dynamic_row_count' not in st.session_state:
        st.session_state.dynamic_row_count = None
    if 'save_changes_pressed' not in st.session_state:
        st.session_state.save_changes_pressed = False
    if 'llm_data_dict' not in st.session_state:
        st.session_state.llm_data_dict = False
    if 'is_cohort_saved' not in st.session_state:
        st.session_state.is_cohort_saved = False
    if 'expander_open' not in st.session_state:
        st.session_state.expander_open = True
    if 'process_metadata_clicked' not in st.session_state:
        st.session_state.process_metadata_clicked = False
    if 'metadata' not in st.session_state:
        st.session_state.metadata = None
    if 'primary_filters_df' not in st.session_state:
        st.session_state.primary_filters_df = None
    if 'secondary_filters_df' not in st.session_state:
        st.session_state.secondary_filters_df = None
    if 'filter_values' not in st.session_state:
        st.session_state.filter_values = {}
    if 'primary_where_clause' not in st.session_state:
        st.session_state.primary_where_clause = ''
    if 'final_where_clause' not in st.session_state:
        st.session_state.final_where_clause = ''
    if 'text_filter_conditions' not in st.session_state:
        st.session_state.text_filter_conditions = {}
    if 'secondary_filter_values' not in st.session_state:
        st.session_state.secondary_filter_values = {}
    if 'cohort_row_count' not in st.session_state:
        st.session_state.cohort_row_count = None
    if 'base_filter_query' not in st.session_state:
        st.session_state.base_filter_query = ''
    if 'selected_table_full' not in st.session_state:
        st.session_state.selected_table_full = ''
    if 'save_changes_pressed' not in st.session_state:
        st.session_state.save_changes_pressed = ''
    if 'cohort_query' not in st.session_state:
        st.session_state.cohort_query = ''
    if 'final_query' not in st.session_state:
        st.session_state.final_query = ''
    if 'cohort_name' not in st.session_state:
        st.session_state.cohort_name = ''
    if 'preview_dataset' not in st.session_state:
        st.session_state.preview_dataset = pd.DataFrame()

class CohortBuilder:
    def __init__(self):
        # Set the page config in the constructor to ensure it is called only once
        st.set_page_config(
            page_title="Home page",
            page_icon="🏠",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def run(self):
        initialize_session_state()
        self.home()

    def connect_to_snowflake(self):
        if 'session' in st.session_state:
            return st.session_state.session
        try:
            session = Session.builder.getOrCreate()
        except Exception as e1:
            try:
                session = Session.builder.configs(st.secrets.connections.snowflake).create()
            except Exception as e2:
                st.error(f"Failed to connect to Snowflake. Initial error: {e1}. Secondary error: {e2}")
                return None
        st.session_state.session = session
        return session

    def home(self):
        with st.spinner('Connecting to Snowflake...'):
            session = self.connect_to_snowflake()
            if session:
                st.success("Connected to Snowflake successfully!")
                self.introduction()
            else:
                st.error("Failed to connect to Snowflake.")

    def introduction(self):

        # Title
        st.title("Welcome to the Cohort Management App! 💻")

        # Introduction
        st.markdown("""
        ## How to Use This App 🧑‍💻

        This app is designed to help you manage cohorts efficiently. You can build new cohorts, view existing ones, and schedule them as needed. Here’s a quick guide to get you started:

        1. **Build Cohort:** Create new cohorts by defining their parameters.
        2. **Existing Cohorts:** View and manage all existing cohorts.
        3. **Schedule Cohorts:** Set up schedules for your cohorts based on various criteria.

        ### Navigation 🗺️
        Use the sidebar on the left to navigate between different pages of the app.
        """)

        # Summary of Each Page
        st.markdown("""
        ## Page Summaries 📄

        ### 1. Build Cohort
        On this page, you can follow these four steps to create and manage cohorts:

        1. **Select Dataset:**
        - Choose the database, schema, and table from which to build your cohort.
        - Retrieve and display table data for further processing.

        2. **Data Dictionary:**
        - Process and display metadata related to your selected dataset.
        - Use metadata to understand and refine your dataset for cohort building.

        3. **Build Cohort:**
        - Apply filters and criteria to create specific cohorts.
        - Utilize metadata and table data to define your cohorts accurately.

        4. **Schedule Cohort:**
        - **Storage Options:** Choose how to store the cohort data:
            - **Dynamic Table:** Continuously updates with new data.
            - **Snapshot Table:** Captures data at specific intervals.
            - **One-Time Table:** Captures data at a single point in time.
        - **Dynamic Table Options:** Define parameters like lag time and refresh mode.
        - **Snapshot Table Options:** Set the cadence (daily, weekly, monthly) for updates.
        - **One-Time Table:** Creates a static table with current data.

        ### 2. Existing Cohorts
        This section allows you to:
        - View a comprehensive list of all your cohorts
        - Edit and manage cohort details

        It's your go-to place for maintaining and updating the cohorts you've created.

        ### 3. Schedule Cohorts
        Here, you can:
        - Schedule your cohorts based on predefined criteria
        - Ensure timely and well-organized cohort activities

        This page helps you keep your cohorts on track with minimal effort.
        """)

        # Footer
        st.markdown("""
        ---
        Happy Cohorting! 🚀
        """)

if __name__ == "__main__":
    app = CohortBuilder()
    app.run()
