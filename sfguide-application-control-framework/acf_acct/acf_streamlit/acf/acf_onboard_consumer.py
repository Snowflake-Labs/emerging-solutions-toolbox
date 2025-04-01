import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def onboard_consumer(app_code, consumer_params:dict):

  session = get_active_session()

  for cons in consumer_params:
    cons_params = consumer_params[cons]

    consumer_acct = cons_params["consumer_account"]
    consumer_name = cons_params["consumer_name"]
    override_controls = str(cons_params["control_overrides"]).replace("'", r"\"") #replace single quotes with doubles in controls dictionary

    #onboard consumer
    onboard = f"CALL P_{app_code}_ACF_DB.CONSUMER_MGMT.ONBOARD_CONSUMER('{consumer_acct}', '{consumer_name}', '{override_controls}')"
    session.sql(onboard).collect()


  return "CONSUMER(S) SUCCESSFULLY ONBOARDED"