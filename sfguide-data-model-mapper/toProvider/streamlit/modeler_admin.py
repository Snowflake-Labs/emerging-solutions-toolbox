from appPages.page import st
from appPages.target_creation import TargetCreation
from appPages.target_administration import TargetAdministration
from appPages.target_entity_selection import TargetEntitySelection
from appPages.consumer_app_setup import ConsumerAppSetup
from snowflake.snowpark.context import get_active_session


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

            session = sfc.init_snowpark_session("account_1")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "target_admin"

    #Set table database location session variables
    #This should happen on first load only
    st.session_state.native_database_name = "DATA_MODEL_MAPPER_ADMIN_APP"

pages = [
    TargetCreation(),
    TargetAdministration(),
    TargetEntitySelection(),
    ConsumerAppSetup()
]


def main():
    st.set_page_config(layout="wide")
    if 'session' not in st.session_state:
        st.session_state.session = set_session()
    for page in pages:
        if page.name == st.session_state.page:
            st.session_state.layout = "wide"
            page.print_sidebar()
            page.print_page()


main()
