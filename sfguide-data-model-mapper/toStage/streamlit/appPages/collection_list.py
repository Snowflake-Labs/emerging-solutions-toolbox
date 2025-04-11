import streamlit as st
from snowflake.snowpark.context import get_active_session
from appPages.page import BasePage, set_page, col, Image, base64, pd


def select_collection(collection_entity_name, target_collection_name, edit):
    session = st.session_state.session

    st.session_state.selected_target_collection = target_collection_name
    st.session_state.collection_entity_name = collection_entity_name

    target_version_pd = (
        session.table("ADMIN.TARGET_COLLECTION")
        .select(col("VERSION"))
        .distinct()
        .to_pandas()
    )

    target_version_pd.reset_index(inplace=True)
    target_collection_version = target_version_pd.loc[0, "VERSION"]

    st.session_state.target_collection_version = target_collection_version
    if edit:
        set_page("entity_config")
    else:
        set_page("collection_joining")


class CollectionList(BasePage):
    def __init__(self):
        self.name = "collection_list"

    def print_page(self):
        session = st.session_state.session
        if 'help_check' not in st.session_state:
            st.session_state.help_check = False
        st.header("Target Collections")

        # Clear out selections if previously configuring other
        if 'wizard_manager' in st.session_state:
            del st.session_state.wizard_manager
        if 'current_step' in st.session_state:
            del st.session_state.current_step
        if 'filter_conditions' in st.session_state:
            del st.session_state.filter_conditions

        collection_names_pd = (
            session.table("ADMIN.SUBSCRIPTION")
            .select(col("CUSTOMER_NAME"), col("TARGET_COLLECTION_NAME"))
            .distinct()
            .to_pandas()
        )

        collection_entity_list_pd = (
            session.table("ADMIN.TARGET_ENTITY")
            .sort(col("TARGET_COLLECTION_NAME").asc())
            .select(col("TARGET_COLLECTION_NAME"), col("TARGET_ENTITY_NAME"))
            .distinct()
            .to_pandas()
        )

        if st.session_state.help_check:
            st.info('''
                    This is a list of all your Target Collections \n
                    Your Target Collection contains a list of Target Entities(Tables) 
                    You will be defining your own Source Collection that matches these Targets
                    
                     Step 1: You will choose a Target entity below listed inside a Target Collection to begin mapping \n
                      ''')

        for i in range(len(collection_entity_list_pd)):
            if i == 0:
                st.subheader(collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"])

            else:
                if (collection_entity_list_pd.loc[i - 1, "TARGET_COLLECTION_NAME"] !=
                        collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"]):
                    st.subheader(collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"])

            target_collection_name = collection_entity_list_pd.loc[i, "TARGET_COLLECTION_NAME"]

            with st.expander("", expanded=True):
            
                collection_entity_name = collection_entity_list_pd.loc[i, "TARGET_ENTITY_NAME"]

                col1_ex, col2_ex, col3_ex, col4_ex, col5_ex = st.columns(
                    (0.1, 1, 2, 1, 1)
                )

                with col1_ex:
                    col1_ex.empty()

                with col2_ex:
                    if st.session_state.streamlit_mode != "OSS":
                        if i == 0 or i == 1:
                            image_name = "Images/collection.png"
                        else:
                            image_name = "Images/collection2.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=90)
                    else:
                        if i == 0 or i == 1:
                            dataimage = Image.open("toStage/streamlit/Images/collection.png")
                        else:
                            dataimage = Image.open("toStage/streamlit/Images/collection2.png")

                        st.image(dataimage, width=90)

                with col3_ex:
                    st.subheader(collection_entity_name)

                with col4_ex:
                        #percent_complete = 0
                        #progress_text = ("Mapping Completion " + str(percent_complete) + "%")
                        #my_bar = st.progress(0, text=progress_text)
                        #percent_complete = 0
                        #progress_text = ("Mapping Completion " + str(percent_complete) + "%")
                        #my_bar = st.progress(0, text=progress_text)

                        #my_bar.progress(percent_complete, text=progress_text)
                        #my_bar.progress(percent_complete, text=progress_text)
                    st.button(
                        "Configure",
                        key="configure" + str(i),
                        use_container_width=True,
                        on_click=select_collection,
                        args=(collection_entity_name, target_collection_name, True),
                    )
                with col5_ex:
                    st.button(
                        "Select",
                        key=i,
                        use_container_width=True,
                        on_click=select_collection,
                        type="primary",
                        args=(collection_entity_name, target_collection_name, False),
                    )

    def print_sidebar(self):
        super().print_sidebar()
