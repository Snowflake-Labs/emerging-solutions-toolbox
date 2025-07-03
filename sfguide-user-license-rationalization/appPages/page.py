from abc import ABC, abstractmethod
import streamlit as st
import base64
from PIL import Image


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
            if st.session_state.streamlit_mode != "OSS":
                image_name = "img/snow.png"
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
                dataimage = Image.open("img/snow.png")
                st.image(dataimage, width=250)
            # st.title(":rotating_light: SaaS License Optimization")
            st.markdown("")
            st.markdown("")
            st.button(label="Overview", help="", on_click=set_page, args=('start',), use_container_width=True)
            st.button(label="Getting Started", help="", on_click=set_page, args=('getting_started',), use_container_width=True)
