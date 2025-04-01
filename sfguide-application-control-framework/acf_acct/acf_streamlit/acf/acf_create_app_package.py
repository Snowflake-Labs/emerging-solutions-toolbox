import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def create_app_package(app_code, app_pkg_name, source_table_list):

  session = get_active_session()
  
  #create app package
  create_app_pkg = f"CREATE APPLICATION PACKAGE IF NOT EXISTS {app_pkg_name}"
  
  #grant reference usage to app db
  grant_ref_usage_to_pkg = f"GRANT REFERENCE_USAGE ON DATABASE P_{app_code}_ACF_DB TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  
  #create application package schemas
  create_sch_versions = f"CREATE OR REPLACE SCHEMA {app_pkg_name}.VERSIONS"
  create_sch_events = f"CREATE OR REPLACE SCHEMA {app_pkg_name}.EVENTS"
  create_sch_metadata = f"CREATE OR REPLACE SCHEMA {app_pkg_name}.METADATA"
  create_sch_trust_center = f"CREATE OR REPLACE SCHEMA {app_pkg_name}.TRUST_CENTER"
  create_sch_data = f"CREATE OR REPLACE SCHEMA {app_pkg_name}.DATA"
  
  #create events_master_v view
  create_consumer_events_master_v = """CREATE OR REPLACE SECURE VIEW """+app_pkg_name+""".EVENTS.EVENTS_MASTER_V COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"events_master_v","type":"secure_view"}}'
  AS SELECT * FROM P_"""+app_code +"""_ACF_DB.EVENTS.EVENTS_MASTER"""
  
  #create metadata_v view
  create_metadata_v = """CREATE OR REPLACE SECURE VIEW """+app_pkg_name+""".METADATA.METADATA_V CHANGE_TRACKING=TRUE COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"metadata_v","type":"secure_view"}}'
  AS SELECT * FROM P_""" +app_code+"""_ACF_DB.METADATA.METADATA"""
  
  #create rules_dictionary_v view
  create_rules_dictionary_v = """CREATE OR REPLACE SECURE VIEW """+app_pkg_name+""".METADATA.RULES_DICTIONARY_V COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"rules_dictionary_v","type":"secure_view"}}'
  AS SELECT * FROM P_""" +app_code+"""_ACF_DB.METADATA.RULES_DICTIONARY"""
  
  #create scanners_v view
  create_scanners_v = """CREATE OR REPLACE SECURE VIEW """+app_pkg_name+""".TRUST_CENTER.SCANNERS_V COMMENT = '{"origin":"sf_sit","name":"acf","version":{"major":1, "minor":7},"attributes":{"env":"native_app","component":"scanners_v","type":"secure_view"}}'
  AS SELECT * FROM P_""" +app_code+"""_ACF_DB.TRUST_CENTER.SCANNERS"""

  #grant privileges on objects to app package
  grant_usage_sch_events = f"GRANT USAGE ON SCHEMA {app_pkg_name}.EVENTS TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_usage_sch_metadata = f"GRANT USAGE ON SCHEMA {app_pkg_name}.METADATA TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_usage_sch_trust_center = f"GRANT USAGE ON SCHEMA {app_pkg_name}.TRUST_CENTER TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_usage_sch_data = f"GRANT USAGE ON SCHEMA {app_pkg_name}.DATA TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  
  grant_select_events_master_v = f"GRANT SELECT ON VIEW {app_pkg_name}.EVENTS.EVENTS_MASTER_V TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_select_metadata_v = f"GRANT SELECT ON VIEW {app_pkg_name}.METADATA.METADATA_V TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_select_rules_dictionary_v = f"GRANT SELECT ON VIEW {app_pkg_name}.METADATA.RULES_DICTIONARY_V TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"
  grant_select_scanners_v = f"GRANT SELECT ON VIEW {app_pkg_name}.TRUST_CENTER.SCANNERS_V TO SHARE IN APPLICATION PACKAGE {app_pkg_name}"

  #add statements to list
  sql_statements = [create_app_pkg
                    ,grant_ref_usage_to_pkg
                    ,create_sch_versions,create_sch_events,create_sch_metadata,create_sch_trust_center,create_sch_data
                    ,create_consumer_events_master_v,create_metadata_v,create_rules_dictionary_v,create_scanners_v
                    ,grant_usage_sch_events,grant_usage_sch_metadata,grant_usage_sch_trust_center,grant_usage_sch_data
                    ,grant_select_events_master_v,grant_select_metadata_v,grant_select_rules_dictionary_v,grant_select_scanners_v]
  
  #execute each sql statement
  for stmt in sql_statements:
      stmt.replace("'", r"\'")
      session.sql(stmt).collect()

  #create secure views and add to application package
  if len(source_table_list) > 0:
    source_table_list_str = ','.join(source_table_list)
    session.sql(f"CALL P_{app_code}_ACF_DB.UTIL.APP_PKG_SOURCE_VIEWS(TO_ARRAY('{source_table_list_str}'), TO_ARRAY('{app_pkg_name}'), 'GRANT')").collect()
 
  return "APP PACKAGE CREATED SUCCESSFULLY"