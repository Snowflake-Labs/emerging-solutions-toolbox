from appPages.page import BasePage, set_page
import streamlit as st
import pandas as pd
from PIL import Image
import re


def insert_img(img_file: str, caption:str):
    dataimage = Image.open("img/feature_engineering.png")
    st.image(dataimage, caption=caption)


class StartPage(BasePage):
    def __init__(self):
        self.name = "start"

    def print_page(self):
        session = st.session_state.session

        st.header(":rotating_light: SaaS License Optimization")

        st.divider()

        with open("appPages/About.md") as markdown_content:
            for line in markdown_content.readlines():
                if line.startswith('insert_img'):
                    res = re.search(r'insert_img\((.*),(.*)\)', line)
                    insert_img(res.group(1), res.group(2))
                else:
                    st.write(line)

    def print_sidebar(self):
        super().print_sidebar()
