import streamlit as st
from snowflake.snowpark.context import get_active_session

def drop_app_package(app_code, app_pkg_name):

  session = get_active_session()

  #var to store command statement to insert into commands table
  cmd_stmt = f"DROP APPLICATION PACKAGE {app_pkg_name}"

  #insert into commands table
  session.sql(f"""INSERT INTO P_{app_code}_ACF_DB.STREAMLIT.COMMANDS VALUES
                  ('{cmd_stmt}', SYSDATE())""").collect()

  return f"APPLICATION PACKAGE VERSION {app_pkg_name.upper()} WAS SUCCESSFULLY DROPPED"