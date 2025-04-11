import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def manage_consumer_controls(app_code, consumer_params:dict):

  session = get_active_session()

  #get controls to update and consumers
  consumer_controls = consumer_params["consumer_controls"]
  consumer_list = ','.join(["'{}'".format(value) for value in consumer_params["master_consumer_list"]])

  for key, value in consumer_controls.items():
    update_controls = f"UPDATE P_{app_code}_ACF_DB.METADATA.METADATA SET value = '{value}' WHERE key = '{key}' AND consumer_name IN ({consumer_list})"
    session.sql(update_controls).collect()

  return "CONSUMER(S) SUCCESSFULLY ONBOARDED"