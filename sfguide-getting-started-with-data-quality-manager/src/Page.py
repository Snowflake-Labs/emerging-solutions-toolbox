from abc import ABC, abstractmethod
import streamlit as st
import base64
from PIL import Image



# Sets the page based on page name
def set_page(page: str):

    st.session_state.current_page = page


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
            pass