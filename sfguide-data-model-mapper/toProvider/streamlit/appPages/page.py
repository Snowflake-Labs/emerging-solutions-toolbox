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

        with st.sidebar:

            side_col1, side_col2 = st.columns((0.5, 2.5))
            with side_col1:
                #
                # if st.session_state.streamlit_mode != "OSS":
                #     image_name = "Images/snow.png"
                #     mime_type = image_name.split(".")[-1:][0].lower()
                #     with open(image_name, "rb") as f:
                #         content_bytes = f.read()
                #         content_b64encoded = base64.b64encode(
                #             content_bytes
                #         ).decode()
                #         image_name_string = (
                #             f"data:image/{mime_type};base64,{content_b64encoded}"
                #         )
                #         st.image(image_name_string, width=500)
                if st.session_state.streamlit_mode != "OSS":
                    dataimage = Image.open("Images/snow.png")
                    st.logo(dataimage, size="large")
                else:
                    dataimage = Image.open("toStage/streamlit/Images/snow.png")
                    st.logo(dataimage, size="large")
            st.header("Data Model Mapper")
            st.subheader("")
            css = """
                <style>
                    .st-key-app_setup_bttn button {
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
                    .st-key-target_admin_bttn button {
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
                label="Administration",
                key="target_admin_bttn",
                on_click=set_page,
                args=("target_admin",),
            )

            st.button(
                label="Consumer App Setup",
                key="app_setup_bttn",
                on_click=set_page,
                args=("consumer_app_setup",),
            )


            st.subheader('')
            with side_col2:
                st.write("#")
