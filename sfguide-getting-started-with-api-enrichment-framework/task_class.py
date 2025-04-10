import streamlit as st
import json

class Scheduled_Task:

	def remove_task(self,task_id, task_name):
		session = st.session_state.session
		session.sql(f"delete from DATA.SCHEDULED_TASKS WHERE task_id = '{task_id}'").collect()
		session.sql(f"drop task DATA.{task_name}_ENRICHMENT_TASK").collect()
	
	def __init__(self, task):
		self.task = task
		self.print()

	def print(self):
		task = self.task
		specs = json.loads(task["JOB_SPECS"])
		with st.expander("TASK: "+specs["task_name"]):
			st.write("###")
			col1, col2, __ = st.columns((2,2,10))
			col1.write("Target Table:")
			col2.write(specs["name"])
			st.divider()
			col1, col2, __ = st.columns((2,2,10))
			col1.write("Output Table:")
			col2.write(specs["output_table"])
			st.divider()
			col1, col2, __ = st.columns((2,2,10))
			col1.write("Mapping:")
			col2.json(specs["mapping"])
			st.divider()
			col1, col2, __ = st.columns((2,2,10))
			col1.write("Warehouse Size:")
			col2.write(specs["wh_size"])
			st.divider()
			col1, col2, __ = st.columns((2,2,10))
			col1.write("Schedule:")
			col2.write(specs["schedule"])
			st.divider()
			st.button("Delete", on_click=self.remove_task, args=(task["TASK_ID"],specs["task_name"]), key='delete'+str(task["TASK_ID"]))

