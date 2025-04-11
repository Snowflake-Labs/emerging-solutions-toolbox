import streamlit as st
from appPages.page import BasePage, col, set_page


#This page is not currently being leveraged in the framework but you could use it as a landing page if wanted/needed. This is a welcome page for the consumer.

class OverviewPage(BasePage):
    def __init__(self):
        self.name = "overview"

    def print_page(self):
        session = st.session_state.session
        if 'help_check' not in st.session_state:
            st.session_state.help_check = False

        collection_names_pd = (
            session.table("ADMIN.SUBSCRIPTION")
            .select(col("CUSTOMER_NAME"), col("TARGET_COLLECTION_NAME"))
            .distinct()
            .to_pandas()
        )

        st.title("Welcome " + collection_names_pd.loc[0, "CUSTOMER_NAME"] + " !")

        st.subheader("What would you like to do?")

        participant_access_col, template_management_col = st.columns(2)

        with participant_access_col:
            st.write("I would like to map available collections")
            st.button(
                "View Collections",
                on_click=set_page,
                type="primary",
                args=("collection_list",),
            )

    def print_sidebar(self):
        super().print_sidebar()
