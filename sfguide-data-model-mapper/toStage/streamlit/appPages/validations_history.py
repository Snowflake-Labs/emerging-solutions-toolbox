import streamlit as st
import pandas as pd
from appPages.page import BasePage, col, set_page

class ValidationsHistoryPage(BasePage):
    def __init__(self):
        self.name = "validations_history"

    def print_page(self):
        session = st.session_state.session
        st.title("Validations History")
        view_all_validations = f"""select * from data_model_mapper_share_db.validated.validation_log; """
        all_validations_df = pd.DataFrame(session.sql(view_all_validations).collect())
        st.dataframe(all_validations_df)

    def print_sidebar(self):
        super().print_sidebar()