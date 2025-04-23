import streamlit as st
from appPages.page import BasePage, col, set_page


class TargetAdministration(BasePage):
    def __init__(self):
        self.name = "target_admin"

    def print_page(self):
        session = st.session_state.session

        st.write("Begin to list existing collections for selection and/or add new button")
        st.write("Similar to collection_list.py, but we want to output the collection list as expanders vs. the entities")

    def print_sidebar(self):
        super().print_sidebar()