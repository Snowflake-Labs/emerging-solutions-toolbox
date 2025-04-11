import streamlit as st
import streamlit.elements.arrow
from snowflake.snowpark.context import get_active_session
from abc import ABC, abstractmethod
import io
import os
import re
from snowflake.snowpark.functions import col, when_matched, when_not_matched, current_timestamp, call_function, \
    parse_json, upper

from snowflake.snowpark.functions import col

# Check snowflake connection type
try:
    import snowflake.permissions as permissions

    session = get_active_session()

    st.session_state["streamlit_mode"] = "NativeApp"
except:
    try:
        session = get_active_session()

        st.session_state["streamlit_mode"] = "SiS"
    except:
        import snowflake_conn as sfc

        session = sfc.init_snowpark_session('account_1')

        st.session_state["streamlit_mode"] = "OSS"

# Wide mode
st.set_page_config(layout="wide")

# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "Overview"

# Set user organization name
if "organization_name" not in st.session_state:
    st.session_state.organization_name = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0]


# Sets the page based on page name
def set_page(page: str):
    st.session_state.page = page


# Default sidebar used for every page
def set_default_sidebar():
    with st.sidebar:
        st.title("Dr. Bernard Data Mapping App")
        st.markdown("")
        st.markdown(
            "This application facilitates mapping raw data to target data models and sharing back to app provider")
        st.markdown("")
        st.markdown("To learn more about Snowflake's Native Apps Framework, feel free to check out the "
                    "[Documentation](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about)")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.button(label="Return Home", help="Warning: Changes will be lost!", on_click=set_page, args=('Overview',))


class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def print_page(self):
        pass

    @abstractmethod
    def print_sidebar(self):
        pass


class OverviewPage(Page):
    def __init__(self):
        self.name = "Overview"

    def print_page(self):
        st.title("Welcome!")
        st.header("Dr. Bernard Data Mapping")

        st.subheader("PLACEHOLDER FOR INTERNAL ADMINISTRATOR APP")

    def print_sidebar(self):
        set_default_sidebar()


pages = [OverviewPage()]


def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page()
            page.print_sidebar()


main()
