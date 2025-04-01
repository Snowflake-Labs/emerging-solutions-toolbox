import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def remove_acf(app_code):

  session = get_active_session()

  remove = f"CALL P_{app_code}_ACF_DB.UTIL.REMOVE_ACF()"
  session.sql(remove).collect()

  return f"ACF for app: {app_code} SUCCESSFULLY REMOVED"