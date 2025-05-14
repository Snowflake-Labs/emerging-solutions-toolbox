from page import st
from voc import VOC
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session


def set_session():
    try:
        session = get_active_session()

        st.session_state["streamlit_mode"] = "NativeApp"
    except Exception:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except Exception:
            session = Session.builder.create()

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "voc_page"


pages = [VOC()]


def main():
    st.set_page_config(
        page_title="Voice of the Customer",
        page_icon=":speech_balloon:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    if "session" not in st.session_state:
        st.session_state.session = set_session()
    for page in pages:
        if page.name == st.session_state.page:
            st.session_state.layout = "wide"
            page.print_sidebar()
            page.print_page()


main()
