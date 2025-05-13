from abc import ABC, abstractmethod
import streamlit as st
import base64
import pandas as pd
import time
from PIL import Image
from snowflake.snowpark.functions import col, when_matched, when_not_matched, current_timestamp, parse_json

if 'session' in st.session_state:
    session = st.session_state.session


# Sets the page based on page name
def set_page(page: str):
    if "editor" in st.session_state:
        del st.session_state.editor

    st.session_state.page = page


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


class BasePage(Page):
    def __init__(self):
        pass

    def print_page(self):
        pass

    # Repeatable element: sidebar buttons that navigate to linked pages
    def print_sidebar(self):
        session = st.session_state.session

        if st.session_state.page in ("collection_joining", "collection_mapping"):
            with st.sidebar:
                side_col1, side_col2 = st.columns((0.5, 2.5))
                with side_col1:
                    if st.session_state.streamlit_mode != "OSS":
                        image_name = "Images/snow.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=200)
                    else:
                        dataimage = Image.open("toStage/streamlit/Images/snow.png")
                        st.image(dataimage, width=200)

                with side_col2:
                    st.write('#')
                st.header("Data Model Mapper")

                css = """
                    <style>
                        .st-key-initial_setup_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                        .st-key-target_collect_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                        # .st-key-grants_bttn button {
                        # #color: #ffffff;
                        # padding: 0px;
                        # margin: 0px;
                        # min-height: .5px;
                        # border-width: 0px;
                        # font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        # background-color:transparent;
                        # # border-radius:10px;
                        # # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        # border-color:transparent
                        # }
                        .st-key-validations_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                    </style>"""
                st.html(css)

                st.button(
                    label="Initial Setup",
                    key="initial_setup_bttn",
                    on_click=set_page,
                    args=("initial_setup",),
                )

                st.button(
                    label="Target Collections",
                    key="target_collect_bttn",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=("collection_list",),
                )

                st.button(
                    label="Validations History",
                    key="validations_bttn",
                    on_click=set_page,
                    args=("validations_history",),
                )

                st.write("")
                help_check = st.checkbox('Enable Verbose Instructions',
                                         key='help_check')
                st.subheader('')
                with side_col2:
                    st.write("#")

                if 'collection_entity_name' in st.session_state:
                    st.write("Target Collection: " + '**' + st.session_state.selected_target_collection + '**')

                st.session_state.attribute_of_entity_df = session.table(
                    "ADMIN.TARGET_ENTITY_ATTRIBUTE"
                ).filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name)

                data_type_pd = st.session_state.attribute_of_entity_df.select(
                    st.session_state.attribute_of_entity_df.TARGET_ENTITY_ATTRIBUTE_NAME,
                    st.session_state.attribute_of_entity_df.TARGET_ATTRIBUTE_PROPERTIES["data_type"],
                ).to_pandas()

                # Rename Columns for better readability
                data_type_pd.columns = ["ATTRIBUTE NAME", "DATA TYPE"]
                data_type_pd['DATA TYPE'] = data_type_pd['DATA TYPE'].str.replace(r'"', '')

                st.write("Target Entity: " + '**' + st.session_state.collection_entity_name + '**')

                st.dataframe(data_type_pd,hide_index=True)

                st.session_state.data_type_pd = data_type_pd

        else:
            with st.sidebar:

                side_col1, side_col2 = st.columns((0.5, 2.5))
                with side_col1:

                    if st.session_state.streamlit_mode != "OSS":
                        image_name = "Images/snow.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=200)
                    else:
                        dataimage = Image.open("toStage/streamlit/Images/snow.png")
                        st.image(dataimage, width=200)
                st.header("Data Model Mapper")
                st.subheader("")
                css = """
                    <style>
                        .st-key-initial_setup_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                        .st-key-target_collect_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                        # .st-key-grants_bttn button {
                        # #color: #ffffff;
                        # padding: 0px;
                        # margin: 0px;
                        # min-height: .5px;
                        # border-width: 0px;
                        # font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        # background-color:transparent;
                        # # border-radius:10px;
                        # # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        # border-color:transparent
                        # }
                        .st-key-validations_bttn button {
                        #color: #ffffff;
                        padding: 0px;
                        margin: 0px;
                        min-height: .5px;
                        border-width: 0px;
                        font-family: Inter, Lato, Roboto, Arial, sans-serif;
                        background-color:transparent;
                        # border-radius:10px;
                        # box-shadow: 3px 3px 3px 1px rgba(64, 64, 64, .25);
                        border-color:transparent
                        }
                    </style>"""
                st.html(css)

                st.button(
                    label="Initial Setup",
                    key="initial_setup_bttn",
                    on_click=set_page,
                    args=("initial_setup",),
                )

                st.button(
                    label="Target Collections",
                    key="target_collect_bttn",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=("collection_list",),
                )

                st.button(
                    label="Validations History",
                    key="validations_bttn",
                    on_click=set_page,
                    args=("validations_history",),
                )

                help_check = st.checkbox('Enable Verbose Instructions',
                                         key='help_check')
                st.subheader('')
                with side_col2:
                    st.write("#")
