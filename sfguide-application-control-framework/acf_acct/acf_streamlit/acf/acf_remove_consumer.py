import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def remove_consumer(app_code, consumer_params:dict):

  session = get_active_session()

  #get controls to update and consumers
  consumer_list = consumer_params["master_consumer_list"]

  for c in consumer_list:
    consumer_acct = c[0]
    consumer_name = c[1]

    remove_consumer = f"CALL P_{app_code}_ACF_DB.CONSUMER_MGMT.REMOVE_CONSUMER('{consumer_acct}', '{consumer_name}')"
    session.sql(remove_consumer).collect()


  return "CONSUMER(S) SUCCESSFULLY REMOVED"