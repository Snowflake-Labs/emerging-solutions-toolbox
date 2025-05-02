from abc import ABC, abstractmethod
import streamlit as st
import base64
from PIL import Image
import pandas as pd

if "session" in st.session_state:
    session = st.session_state.session


# Sets the page based on page name
def set_page(page: str):
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
        with st.sidebar:
            side_col1, side_col2 = st.columns((0.5, 2.5))
            with side_col1:
                if st.session_state.streamlit_mode != "OSS":
                    image_name = "snow.png"
                    mime_type = image_name.split(".")[-1:][0].lower()
                    with open(image_name, "rb") as f:
                        content_bytes = f.read()
                        content_b64encoded = base64.b64encode(content_bytes).decode()
                        image_name_string = (
                            f"data:image/{mime_type};base64,{content_b64encoded}"
                        )
                        # st.image(image_name_string, width=400)
                        st.logo(image_name_string, size="large")
                else:
                    dataimage = Image.open("snow.png")
                    st.logo(dataimage, size="large")
            st.header("Voice of the Customer")
            st.subheader("")
            css = """
                <style>
                    .st-key-voc_bttn button {
                    padding: 0px;
                    margin: 0px;
                    min-height: .5px;
                    border-width: 0px;
                    font-family: Inter, Lato, Roboto, Arial, sans-serif;
                    background-color:transparent;
                    border-color:transparent
                    }
                    .st-key-previous_runs_bttn button {
                    padding: 0px;
                    margin: 0px;
                    min-height: .5px;
                    border-width: 0px;
                    font-family: Inter, Lato, Roboto, Arial, sans-serif;
                    background-color:transparent;
                    border-color:transparent
                    }
                </style>"""
            st.html(css)

            st.button(
                label="Home",
                key="voc_bttn",
                on_click=set_page,
                args=("voc_page",),
            )

            # st.button(
            #     label="Previous Runs/Analysis",
            #     key="previous_runs_bttn",
            #     on_click=set_page,
            #     args=("previous_runs_page",),
            # )

            st.subheader("")
            with side_col2:
                st.write("#")
