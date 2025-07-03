import streamlit as st
from snowflake.snowpark.context import get_active_session
from appPages.start import StartPage
from appPages.getting_started import GettingStartedPage


def set_session():
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

            session = sfc.init_snowpark_session("account")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "start"


pages = [StartPage(), GettingStartedPage()]


def main():
    st.set_page_config(layout="wide")
    if 'session' not in st.session_state:
        st.session_state.session = set_session()

    for page in pages:
        if page.name == st.session_state.page:

            page.print_sidebar()
            page.print_page()


main()
