import streamlit as st
import time
import random
import json
import sys
import os
import math
import pandas as pd
import matplotlib.pyplot as plt
import snowflake.snowpark.functions as F
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from snowflake.snowpark import Row, Session
from snowflake.snowpark import types as T
from datetime import datetime, timedelta
from ast import literal_eval
from src.Page import Page


class paginator:

	def __init__(self,name,list,interval,callback):
		self.name = name
		self.list = list
		self.callback = callback
		self.interval = interval
		if self.name+"_paginator" not in st.session_state or st.session_state[self.name+"_paginator"]["start"] == st.session_state[self.name+"_paginator"]["end"]:
			st.session_state[self.name+"_paginator"]={"start":0,"end":self.interval}
		self.paginator = st.session_state[self.name+"_paginator"]
		self.print(callback)
	
	def page_up(self):
		interval = self.interval
		st.session_state[self.name+"_paginator"]={"start":st.session_state[self.name+"_paginator"]["start"]+interval,"end":st.session_state[self.name+"_paginator"]["end"]+interval}

	def page_down(self):
		interval = self.interval
		st.session_state[self.name+"_paginator"]={"start":st.session_state[self.name+"_paginator"]["start"]-interval,"end":st.session_state[self.name+"_paginator"]["end"]-interval}


	def print(self,the_callback):

		for list_item in self.list[self.paginator["start"]:self.paginator["end"]]:
			the_callback(list_item)

		total_pages = int(math.ceil(len(self.list)/self.interval))
		current_page = int(st.session_state[self.name+"_paginator"]["end"]/self.interval)
		
		if len(self.list) > self.interval:
			space,btn1,text,btn2,space = st.columns([20,2,2,2,20])
			text.write(f"Page ***{current_page}/{total_pages}***")
			if self.paginator["start"] > 0:
				btn1.button("Prev",on_click=self.page_down, key=self.name+"_prev_button")

			if self.paginator["end"] < len(self.list):
				btn2.button("Next",on_click=self.page_up, key=self.name+"_next_button")

