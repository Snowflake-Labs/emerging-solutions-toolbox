from page import st
from voc import VOC
from snowflake.snowpark.context import get_active_session


def set_session():
    try:
        session = get_active_session()

        st.session_state["streamlit_mode"] = "NativeApp"
    except Exception:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except Exception:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account_1")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "voc_page"


pages = [VOC()]


def main():
    st.set_page_config(layout="wide")
    if "session" not in st.session_state:
        st.session_state.session = set_session()
    for page in pages:
        if page.name == st.session_state.page:
            st.session_state.layout = "wide"
            page.print_sidebar()
            page.print_page()


main()
