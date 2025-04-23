import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def re_enable_consumer(app_code, consumer_params:dict):

  session = get_active_session()

  #get consumers and comments
  consumer_list = ','.join(["'{}'".format(value) for value in consumer_params["master_consumer_list"]])
  comments = consumer_params["comments"]

  update_enabled_flag = f"UPDATE P_{app_code}_ACF_DB.METADATA.METADATA SET value = 'Y' WHERE key = 'enabled' AND consumer_name IN ({consumer_list})"
  session.sql(update_enabled_flag).collect()

  update_auto_enabled_flag = f"UPDATE P_{app_code}_ACF_DB.METADATA.METADATA SET value = 'Y' WHERE key = 'auto_enable' AND consumer_name IN ({consumer_list})"
  session.sql(update_auto_enabled_flag).collect()  

  update_comments = f"UPDATE P_{app_code}_ACF_DB.METADATA.METADATA SET value = $${comments}$$ WHERE key = 'comments' AND consumer_name IN ({consumer_list})"
  session.sql(update_comments).collect()   


  return "CONSUMER(S) SUCCESSFULLY RE-ENABLED"