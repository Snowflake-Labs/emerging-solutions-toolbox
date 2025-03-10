# Import python packages
import streamlit as st

from snowflake.snowpark.context import get_active_session
from snowflake.connector.pandas_tools import pd_writer
import snowflake.snowpark.functions as F
from abc import ABC, abstractmethod
import base64
import datetime
import json
import pandas as pd
import plotly.figure_factory as ff
import plotly.express as px
import plotly.graph_objects as go
import random
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import time
import utils.utils as u

if "page" not in st.session_state:
    st.session_state.page = "home"

if st.session_state.page in ["query_comparison"]:
    st.set_page_config(layout="wide")
else:
    st.set_page_config(layout="centered")

if "ev_check" not in st.session_state:
    st.session_state.ev_check = False
    
if "evs" not in st.session_state:
    st.session_state.evs = pd.DataFrame()

if "select_ev_idx" not in st.session_state:
    st.session_state.select_ev_idx = 0

if "ev_created" not in st.session_state:
    st.session_state.ev_created = False

if "flag_disable_ev_fields" not in st.session_state:
    st.session_state.flag_disable_ev_fields = False

if "flag_disable_btn_create_ev" not in st.session_state:
    st.session_state.flag_disable_btn_create_ev = True

if "query_op_stats_check" not in st.session_state:
    st.session_state.query_op_stats_check = False

if "convert_tbl_list" not in st.session_state:
    st.session_state.convert_tbl_list = []

if "convert_tbl_select_list" not in st.session_state:
    st.session_state.convert_tbl_select_list = []

if "converted_tbl_list" not in st.session_state:
    st.session_state.converted_tbl_list = []


def clear_c2i_session_vars():
    #global c2i session vars
    if "select_ev_idx" in st.session_state:
        del st.session_state.select_ev_idx
    
    if "ev_created" in st.session_state:
        del st.session_state.ev_created
    
    if "flag_disable_ev_fields" in st.session_state:
        del st.session_state.flag_disable_ev_fields
    
    if "flag_disable_btn_create_ev" in st.session_state:
        del st.session_state.flag_disable_btn_create_ev
    
    if "query_op_stats_check" in st.session_state:
        del st.session_state.query_op_stats_check
    
    if "convert_tbl_list" in st.session_state:
        del st.session_state.convert_tbl_list
    
    if "convert_tbl_select_list" in st.session_state:
        del st.session_state.convert_tbl_select_list

    if "converted_tbl_list" in st.session_state:
        del st.session_state.converted_tbl_list


    #table wizard session vars
    if "current_step" in st.session_state:
        del st.session_state.current_step
        
    if "disable_step_2" in st.session_state:
        del st.session_state.disable_step_2
    
    if "disable_step_3" in st.session_state:
        del st.session_state.disable_step_3

    if "disable_step_4" in st.session_state:
        del st.session_state.disable_step_4

    if "cloud" in st.session_state:
        del st.session_state.cloud

    if "ev_name" in st.session_state:
        del st.session_state.ev_name

    if "ev_storage_provider" in st.session_state:
        del st.session_state.ev_storage_provider

    if "base_url" in st.session_state:
        del st.session_state.base_url

    if "use_privatelink_endpoint_idx" in st.session_state:
        del st.session_state.use_privatelink_endpoint_idx

    if "aws_storage_access_point_arn" in st.session_state:
        del st.session_state.aws_storage_access_point_arn

    if "aws_role_arn" in st.session_state:
        del st.session_state.aws_role_arn

    if "aws_external_id" in st.session_state:
        del st.session_state.aws_external_id

    if "aws_encryption_type_idx" in st.session_state:
        del st.session_state.aws_encryption_type_idx

    if "kms_key_id" in st.session_state:
        del st.session_state.kms_key_id

    if "azure_tenant_id" in st.session_state:
        del st.session_state.azure_tenant_id

    if "gcs_encryption_type_idx" in st.session_state:
        del st.session_state.gcs_encryption_type_idx

    if "db_idx" in st.session_state:
        del st.session_state.db_idx

    if "schema_idx" in st.session_state:
        del st.session_state.schema_idx
        
    if "table_idx" in st.session_state:
        del st.session_state.table_idx

    if "table" in st.session_state:
        del st.session_state.table

    if "table_selected"  in st.session_state:
        del st.session_state.table_selected


    #query wizard session vars
    if "queries" in st.session_state:
        del st.session_state.queries

    if "sb_query_option_idx" in st.session_state:
        del st.session_state.sb_query_option_idx

    if "sb_query_history_range_idx" in st.session_state:
        del st.session_state.sb_query_history_range_idx

    if "enter_query_id" in st.session_state:
        del st.session_state.enter_query_id

    if "choose_query_id" in st.session_state:
        del st.session_state.choose_query_id

    if "df_query_history_id" in st.session_state:
        del st.session_state.df_query_history_id

    if "query_history_range" in st.session_state:
        del st.session_state.query_history_range

    if "df_query_history_range" in st.session_state:
        del st.session_state.df_query_history_range

    if "selected_query_id" in st.session_state:
        del st.session_state.selected_query_id

    if "selected_query" in st.session_state:
        del st.session_state.selected_query

    if "converted_query_id" in st.session_state:
        del st.session_state.converted_query_id

    if "converted_query" in st.session_state:
        del st.session_state.converted_query

    if "flag_render_select_ev" in st.session_state:
        del st.session_state.flag_render_select_ev

    if "sb_select_convert_idx" in st.session_state:
        del st.session_state.sb_select_convert_idx

    #clear all cached data
    st.cache_data.clear()

#setting custom width for larger st.dialogs
st.markdown(
    """
<style>
div[data-testid="stDialog"] div[role="dialog"]:has(.large-dialog) {
    width: 85%;
}
</style>
""",
    unsafe_allow_html=True,
)


@st.dialog("Prerequisites")
def render_prereqs():
    current_region = ""
    sf_ext_vol_url = "https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume"

    try:
        current_region = pd.DataFrame(session.sql("SELECT CURRENT_REGION()").collect()).iloc[0,0]
    except:
        st.rerun()

    if current_region.lower().startswith('aws_'):
        storage_name = "an S3 bucket"
        sf_ext_vol_url = "https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3"

    if current_region.lower().startswith('azure_'):
        storage_name = "an Azure container"
        sf_ext_vol_url = 'https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-azure'

    if current_region.lower().startswith('gcp_'):
        storage_name = "a Google Cloud Storage bucket"
        sf_ext_vol_url = "https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-gcs"

    prereqs = f"""
        - At a minimum, granted the ```CREATE TABLE``` privilege on the schema that contains the desired table to convert.
        - Granted either the ```ACCOUNTADMIN``` role or a role with the ```CREATE EXTERNAL VOLUME``` privilege.
        - **Either**: 
            - an existing Snowflake EXTERNAL VOLUME registered to {storage_name} in the ```{current_region}``` region
            - {storage_name} in the ```{current_region}``` region (if an existing EXTERNAL VOLUME does not exist). 
                - Convert2Iceberg can be used to create an EXTERNAL VOLUME. 
                - Visit {sf_ext_vol_url} for instructions for ```{current_region.split('_')[0]}```.
        """
    st.markdown(prereqs)


def extract_table_names(sql):
    """Extract table names from an SQL query."""

    parsed = sqlparse.parse(sql)[0]
    
    tables = []
    for token in parsed.tokens:
        if isinstance(token, Identifier):
            st.write(token.flatten())
            table = str(token).split(" ")[0]
            tables.append(table)

    return tables

    

def render_create_ev():
    current_region= ""
    cloud = ""
    btn_create_ev = False
    flag_create_ev = False
    txt_ev_name = ""
    flag_disable_btn_create_ev = True
    ev_created = False

    try:
        current_region = pd.DataFrame(session.sql("SELECT CURRENT_REGION()").collect()).iloc[0,0]
    except:
        st.rerun()

    if current_region.lower().startswith("aws_"):
        cloud = "AWS"

    if current_region.lower().startswith('azure_'):
        cloud = "AZURE"

    if current_region.lower().startswith('gcp_'):
        cloud = "GCP"

    st.subheader("**Create an External Volume**")
    st.caption(":red[⚠︎ NOTE:  The current role should be either ACCOUNTADMIN or have been granted the CREATE EXTERNAL VOLUME privilege.]")
    st.write("")

    #set cloud environment based on region
    st.text_input("Cloud Provider:", cloud, disabled = True, key = "txt_cloud")

    if st.session_state.txt_cloud == "AWS":
        #external volume name
        txt_ev_name = st.text_input('External Volume Name:'
                                    , key = "txt_ev_name"
                                    )

        #storage provider
        aws_storage_provider = "S3GOV" if "gov" in current_region.lower() else "S3"
        txt_ev_storage_provider = st.text_input("Storage Provider:"
                                                , aws_storage_provider
                                                , disabled = True
                                                )

        #AWS role ARN
        txt_aws_role_arn = st.text_input("AWS Role ARN:"
                                         , help = "i.e.: arn:aws:iam::123456789012:role/myrole"
                                         , key = "txt_aws_role_arn"
                                         )

        #AWS base URL
        txt_base_url = st.text_input("Base URL:"
                                     , help = "i.e.: s3://<my_bucket>/"
                                     , key = "txt_base_url"
                                     )

        #AWS storage access point ARN (if applicable)
        aws_storage_access_point_arn_field = ""
        if txt_base_url.endswith("-s3alias"):
            txt_aws_storage_access_point_arn = st.text_input("AWS Storage Access Point ARN:"
                                                             , help = "required only when you specify an S3 access point alias for your storage STORAGE_BASE_URL."
                                                             , key = "txt_aws_storage_access_point_arn"
                                                             )
            if txt_aws_storage_access_point_arn == "":
                st.error(f"The AWS base URL is aliased, which requires an AWS Storage Access Point ARN. Please provide.", icon="🚨")
            aws_storage_access_point_arn_field = f"STORAGE_AWS_ACCESS_POINT_ARN = '{txt_aws_storage_access_point_arn}'"            
        
        #AWS exernal ID
        txt_aws_external_id = st.text_input("AWS External ID:"
                                            , help = "(optional) i.e.: my_iceberg_table_external_id"
                                            , key = "txt_aws_external_id"
                                            )
        aws_external_id_field = "" if txt_aws_external_id == "" else f"STORAGE_AWS_EXTERNAL_ID = '{txt_aws_external_id}'"

        #encryption (optional)
        encryption_type_list = ["AWS_SSE_S3", "AWS_SSE_KMS", "NONE"]
        sb_encryption_type = st.selectbox("Encryption Type:"
                                          , encryption_type_list
                                          , index = 2
                                          , key = "sb_encryption_type"
                                          )
        kms_key_id_field = ""
        if sb_encryption_type == "AWS_SSE_KMS":
            txt_kms_key_id = st.text_input('KMS Key ID:'
                                           , help = "(optional) specifies the ID for the AWS KMS-managed key used to encrypt files written to the bucket. If no value is provided, your default KMS key is used to encrypt files for writing data."
                                           , key = "txt_kms_key_id"
                                           )
            if txt_kms_key_id == "":
                st.error(f"{sb_encryption_type} requires a KMS Key ID. Please provide.", icon="🚨")
            else:
                kms_key_id_field = f" KMS_KEY_ID = '{txt_kms_key_id}'"
        encryption_type_field = "" if sb_encryption_type == "NONE" else f"ENCRYPTION=(TYPE = '{sb_encryption_type}'{kms_key_id_field})"

        #use privatelink (optional)
        use_privatelink_endpoint_list = ["TRUE", "FALSE"]
        sb_use_privatelink_endpoint = st.selectbox("Use Privatelink Endpoint:"
                                                   , use_privatelink_endpoint_list
                                                   , index = 1
                                                   , key = "sb_use_privatelink_endpoint"
                                                   )
        use_privatelink_endpoint_field = "" if sb_use_privatelink_endpoint == "FALSE" else f"USE_PRIVATELINK_ENDPOINT = TRUE"

        #enable create button if required fields are not empty
        if all(v is not '' for v in [txt_ev_name, txt_aws_role_arn, txt_base_url]):
            flag_disable_btn_create_ev = False

        #disable create button if required fields for optional parameters are empty
        if any(v is '' for v in [txt_ev_name, txt_aws_role_arn, txt_base_url]):
            flag_disable_btn_create_ev = True
        
        if (txt_base_url.endswith("-s3alias") and txt_aws_storage_access_point_arn == "") or (sb_encryption_type == "AWS_SSE_KMS" and kms_key_id_field == ""):
            flag_disable_btn_create_ev = True
        
        #create external volume
        col1, col2, col3 = st.columns([3.5,3.5,0.975])

        with col1:
            st.write("")
            st.write("")
            st.button("Home", type="secondary", on_click=set_page, args=["home"])

        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", type="primary", disabled=flag_disable_btn_create_ev)

        if btn_create_ev:
            flag_create_ev = True

        if flag_create_ev:
             with st.spinner("Updating..."):
                 session.sql(f"""CREATE OR REPLACE EXTERNAL VOLUME {txt_ev_name}
                                   STORAGE_LOCATIONS =
                                      (
                                         (
                                            NAME = '{txt_ev_name}'
                                            STORAGE_PROVIDER = '{aws_storage_provider}'
                                            STORAGE_AWS_ROLE_ARN = '{txt_aws_role_arn}'
                                            STORAGE_BASE_URL = '{txt_base_url}'
                                            {aws_storage_access_point_arn_field}
                                            {aws_external_id_field}
                                            {encryption_type_field}
                                            {use_privatelink_endpoint_field}
                                         )
                                      )""").collect()
                 ev_created = True
                 st.session_state.ev_check = False

    
    if st.session_state.txt_cloud == "AZURE":
        #external volume name
        txt_ev_name = st.text_input("External Volume Name:"
                                    , key = "txt_ev_name"
                                    )

        #storage provide: AZURE
        txt_ev_storage_provider = st.text_input("Storage Provider:", "AZURE", disabled=True)

        #Azure tenant ID
        txt_azure_tenant_id = st.text_input("Azure Tenant ID:"
                                            , help= "To find your tenant ID, log into the Azure portal and select Azure Active Directory » Properties."
                                            , key = "txt_azure_tenant_id"
                                            )
        
        #Azure base URL
        txt_base_url = st.text_input("Base URL:"
                                     , help = "i.e.: azure://..."
                                     , key = "txt_base_url"
                                     )

        #use privatelink (optional)
        use_privatelink_endpoint_list = ["TRUE", "FALSE"]
        sb_use_privatelink_endpoint = st.selectbox("Use Privatelink Endpoint:"
                                                   , use_privatelink_endpoint_list
                                                   , index=1
                                                   , key="sb_use_privatelink_endpoint"
                                                   )
        use_privatelink_endpoint_field = "" if sb_use_privatelink_endpoint == "FALSE" else f"USE_PRIVATELINK_ENDPOINT = TRUE"

        #enable create button if required fields are not empty
        if all(v is not "" for v in [txt_ev_name, txt_azure_tenant_id, txt_base_url]):
            flag_disable_btn_create_ev = False

        #create external volume
        col1, col2, col3 = st.columns([3.5,3.5,0.975])

        with col1:
            st.write("")
            st.write("")
            st.button("Home", type="secondary", on_click=set_page, args=["home"])

        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", type="primary", disabled=flag_disable_btn_create_ev)

        if btn_create_ev:
            flag_create_ev = True

        if flag_create_ev:
             with st.spinner("Updating..."):
                 session.sql(f"""CREATE OR REPLACE EXTERNAL VOLUME {txt_ev_name}
                                   STORAGE_LOCATIONS =
                                      (
                                         (
                                            NAME = '{txt_ev_name}'
                                            STORAGE_PROVIDER = '{txt_ev_storage_provider}'
                                            AZURE_TENANT_ID = '{txt_azure_tenant_id}'
                                            STORAGE_BASE_URL = '{txt_base_url}'
                                            {use_privatelink_endpoint_field}
                                         )
                                      )""").collect()
                 ev_created = True
                 st.session_state.ev_check = False


    if st.session_state.txt_cloud == "GCP":
        #external volume name
        txt_ev_name = st.text_input("External Volume Name:"
                                    , key = "txt_ev_name"
                                    )

        #storage provider: GCS
        txt_ev_storage_provider = st.text_input("Storage Provider:", "GCS", disabled = True)
        st.session_state.ev_storage_provider = txt_ev_storage_provider
        
        txt_base_url = st.text_input("Base URL:"
                                     , help = "i.e.: gcs://<bucket>/<path>/"
                                     , key = "txt_base_url"
                                     )

        #encryption (optional)
        encryption_type_list = ["GCS_SSE_KMS", "NONE"]
        sb_encryption_type = st.selectbox("Encryption Type:"
                                          , encryption_type_list
                                          , index = 1
                                          , key = "sb_encryption_type"
                                          )
        kms_key_id_field = ""
        if sb_encryption_type == "GCS_SSE_KMS":
            txt_kms_key_id = st.text_input("KMS Key ID:"
                                           , help = "(optional) specifies the ID for the GCS KMS-managed key used to encrypt files written to the bucket. If no value is provided, your default KMS key is used to encrypt files for writing data."
                                           , key = "txt_kms_key_id"
                                           )
            if txt_kms_key_id == "":
                st.error(f"{sb_encryption_type} requires a KMS Key ID. Please provide.", icon="🚨")
            else:
                kms_key_id_field = f"ENCRYPTION=(TYPE = '{sb_encryption_type}' KMS_KEY_ID = '{txt_kms_key_id}')"
        encryption_type_field = "" if sb_encryption_type == "NONE" else kms_key_id_field

        #enable create button if required fields are not empty
        if all(v is not "" for v in [txt_ev_name, txt_base_url]):
            flag_disable_btn_create_ev = False

        #disable create button if required fields for optional parameters are empty
        if sb_encryption_type == "GCS_SSE_KMS" and kms_key_id_field == "":
            flag_disable_btn_create_ev = True

        #create external volume
        col1, col2, col3 = st.columns([3.5,3.5,0.975])

        with col1:
            st.write("")
            st.write("")
            st.button("Home", type="secondary", on_click=set_page, args=["home"])
    
        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", type="primary", disabled=flag_disable_btn_create_ev)

        if btn_create_ev:
            flag_create_ev = True

        if flag_create_ev:
             with st.spinner("Updating..."):
                 session.sql(f"""CREATE OR REPLACE EXTERNAL VOLUME {txt_ev_name}
                                   STORAGE_LOCATIONS =
                                      (
                                         (
                                            NAME = '{txt_ev_name}'
                                            STORAGE_PROVIDER = '{txt_ev_storage_provider}'
                                            STORAGE_BASE_URL = '{txt_base_url}'
                                            {encryption_type_field}
                                         )
                                      )""").collect()
                 ev_created = True
                 st.session_state.ev_check = False
                 

    if ev_created:
        st.write("")
        st.success(f"External Volume: **{txt_ev_name}** successfully created. 🎉")
        

def input_callback(session_key, input_key):
    st.session_state[session_key] = st.session_state[input_key]

    if input_key.lower() == "txt_enter_query_id":
        if st.session_state.selected_query_id == "" or (st.session_state.enter_query_id == st.session_state.selected_query_id):
            st.session_state.disable_step_2 = False
        else:
            st.session_state.disable_step_2 = True


def selectbox_callback(wizard, val, idx, list):
    if st.session_state[val] in list:
        st.session_state[idx] = list.index(st.session_state[val])
    else:
       st.session_state[idx] = 0 

    #enable steps
    if st.session_state.current_step == 1:
        if wizard.lower() == "table":
            if st.session_state.sb_select_ev not in ["Choose...","Create New..."]:
                st.session_state.ev_name = st.session_state.sb_select_ev
                st.session_state.ev_created = True
            st.session_state.disable_step_2 = False
            
    if st.session_state.current_step == 2:
        if wizard.lower() == "table":
            if val == "sb_table" and st.session_state.sb_table != "Choose...":
                st.session_state.table_selected = True
            st.session_state.disable_step_3 = False
            

#@st.cache_data(show_spinner=False)
def run_nonsis_cmd(cmd, generate_results_flag):
    df_cmd_results = pd.DataFrame("Dataframe not generated", index=[0], columns=["Status"])
    
    #set time parameters
    timeout = 120000 #2 mins in milliseconds
    cmd_start_time = int(time.time() * 1000) #current time in ms
    cmd_end_time = int(time.time() * 1000)

    #insert command into nonsis cmd mgr
    cmd_hash = ''
    if generate_results_flag:
        cmd_hash = pd.DataFrame(session.sql(f"""SELECT "SIS_CMD_MGR_<MY_ROLE>".UTIL.TABLE_UUID($${cmd}$$)""").collect()).iloc[0,0] #hash of the command
    
    start_timestamp = pd.DataFrame(session.sql(f"SELECT SYSDATE()").collect()).iloc[0,0]
    
    session.sql(f"""INSERT INTO "SIS_CMD_MGR_<MY_ROLE>".CMD_MGR.COMMANDS(app_name, cmd, cmd_hash, status, start_timestamp, completed_timestamp, generate_results_table, results_table, notes)
                    SELECT 
                        'CONVERT2ICEBERG'
                        ,$${cmd}$$
                        ,'{cmd_hash}'
                        ,'PROCESSING'
                        ,'{start_timestamp}'
                        ,NULL
                        ,{generate_results_flag}
                        ,NULL
                        ,NULL
                    ;""").collect()

    run_id = pd.DataFrame(session.sql(f"""SELECT MAX(run_id) 
                                FROM "SIS_CMD_MGR_<MY_ROLE>".CMD_MGR.COMMANDS 
                                WHERE app_name = 'CONVERT2ICEBERG'
                                AND cmd_hash = '{cmd_hash}'""").collect()).iloc[0,0]
    status = "PROCESSING"
    results_table = ""
    notes = ""
    
    #poll nonsis cmd mgr commands table until command status is complete
    while True:
        df_cmd_status = pd.DataFrame(session.sql(f"""SELECT status, results_table, notes FROM "SIS_CMD_MGR_<MY_ROLE>".CMD_MGR.COMMANDS WHERE app_name = 'CONVERT2ICEBERG' AND run_id = '{run_id}'""").collect())
        
        status = df_cmd_status.iloc[0,0]
        results_table = df_cmd_status.iloc[0,1]
        notes = df_cmd_status.iloc[0,2]
        
        cmd_end_time = int(time.time() * 1000)
        if (status != "PROCESSING") or ((cmd_end_time - cmd_start_time) >= timeout) or status.lower() == "error":
            break

    if cmd_end_time - cmd_start_time >= timeout:
        st.error("ERROR:  Command timed out")

    if status.lower() == "error":
        st.error(f"ERROR: {notes}")

    if generate_results_flag and status.lower() == "complete":
        df_cmd_results = pd.DataFrame(session.sql(f"""SELECT * FROM "SIS_CMD_MGR_<MY_ROLE>".RESULTS."{results_table}";""").collect())
    
    return df_cmd_results


def render_select_ev():
    if not st.session_state.ev_check:
        #call run_nonsis_cmd to get external volumes --SiS cannot execute this show command
        st.session_state.evs = run_nonsis_cmd("SHOW EXTERNAL VOLUMES", True)
        st.session_state.ev_check = True
            
    select_ev_list = []
    
    if not st.session_state.evs.empty :
        select_ev_list = ["Choose..."] + st.session_state.evs["name"].values.tolist()
    else:
        select_ev_list = ["Choose..."]

    sb_select_ev = st.selectbox("Select External Volume:"
                                , select_ev_list
                                , index = st.session_state.select_ev_idx
                                , key = "sb_select_ev"
                                , on_change = selectbox_callback
                                , args = ("table", "sb_select_ev", "select_ev_idx", select_ev_list)
                                )
    st.write("")

    if sb_select_ev != "Choose...":
        st.success(f"External Volume: **{sb_select_ev}** selected. 🎉")

    return sb_select_ev
        

def set_form_step(action,step=None):
    if action == "Next":
        st.session_state.current_step = st.session_state.current_step + 1
    if action == "Back":
        st.session_state.current_step = st.session_state.current_step - 1
    if action == "Jump":
        st.session_state.current_step = step

def get_query_id_from_qh():
    if st.session_state["query_history_id"].selection["rows"]:
        #get index of the selected row
        df_selected_query_idx = st.session_state["query_history_id"].selection["rows"][0]
    
        #set selected query ID
        st.session_state.selected_query_id = st.session_state.df_query_history_range.iloc[df_selected_query_idx,0]
        st.session_state.disable_step_2 = False


def render_query_stats(query_id):
    df_query_history_id = pd.DataFrame(session.sql(f"""SELECT
                                                            --query details
                                                            QUERY_ID
                                                            ,QUERY_TEXT
                                                            ,QUERY_TYPE
                                                            --execution details
                                                            ,WAREHOUSE_NAME
                                                            ,WAREHOUSE_SIZE
                                                            ,WAREHOUSE_TYPE
                                                            ,EXECUTION_STATUS
                                                            ,START_TIME
                                                            ,END_TIME
                                                            ,TOTAL_ELAPSED_TIME TOTAL_ELAPSED_TIME_MS
                                                            --bytes
                                                            ,BYTES_SCANNED
                                                            ,BYTES_WRITTEN_TO_RESULT
                                                            ,PARTITIONS_SCANNED
                                                            ,PARTITIONS_TOTAL
                                                            --execution breakdown
                                                            ,COMPILATION_TIME
                                                            ,EXECUTION_TIME
                                                            ,QUEUED_PROVISIONING_TIME
                                                            ,QUEUED_REPAIR_TIME
                                                            ,QUEUED_OVERLOAD_TIME
                                                            ,TRANSACTION_BLOCKED_TIME
                                                        FROM 
                                                            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                                                        WHERE
                                                            ROLE_NAME = CURRENT_ROLE() AND
                                                            QUERY_ID = '{query_id}' AND
                                                            LOWER(EXECUTION_STATUS) = 'success';""").collect())

    if not df_query_history_id.empty:
        #query details transposed
        df_query_details = df_query_history_id[["QUERY_ID","QUERY_TYPE","QUERY_TEXT"]].copy().transpose()
    
        #execution details
        df_execution_details = df_query_history_id[["WAREHOUSE_NAME"
                                                    ,"WAREHOUSE_SIZE"
                                                    ,"WAREHOUSE_TYPE"
                                                    ,"EXECUTION_STATUS"
                                                    ,"START_TIME"
                                                    ,"END_TIME"
                                                    ,"TOTAL_ELAPSED_TIME_MS"]].copy().transpose()
    
        #execution breakdown
        df_execution_breakdown_details = df_query_history_id[["COMPILATION_TIME"
                                                              ,"EXECUTION_TIME"
                                                              ,"QUEUED_PROVISIONING_TIME"
                                                              ,"QUEUED_REPAIR_TIME"
                                                              ,"QUEUED_OVERLOAD_TIME"
                                                              ,"TRANSACTION_BLOCKED_TIME"]].copy().transpose()
    
        #bytes details
        df_bytes_details = df_query_history_id[["BYTES_SCANNED"
                                                ,"BYTES_WRITTEN_TO_RESULT"
                                                ,"PARTITIONS_SCANNED"
                                                ,"PARTITIONS_TOTAL"]].copy().transpose()
    
        st.write("")
        with st.container(border=True, key=query_id):
            st.subheader("Query Details")
            st.write("")
            st.markdown(df_query_details.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 1).hide(axis = 1).to_html(), unsafe_allow_html = True)
    
            #st.write('#')
            st.write("")
            st.write("")
    
            st.subheader('Query Execution')
            st.write("")
            st.markdown(df_execution_details.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 1).hide(axis = 1).to_html(), unsafe_allow_html = True)
            st.write("")
                
            col1, col2, col3 = st.columns([0.95,2.5,0.1])
            
            with col2:
                if not df_execution_breakdown_details.empty:
                    ex_bd_pie = px.pie(df_execution_breakdown_details, values=0, names=["COMPILATION_TIME"
                                                                                        ,"EXECUTION_TIME"
                                                                                        ,"QUEUED_PROVISIONING_TIME"
                                                                                        ,"QUEUED_REPAIR_TIME"
                                                                                        ,"QUEUED_OVERLOAD_TIME"
                                                                                        ,"TRANSACTION_BLOCKED_TIME"])
                                            
                    ex_bd_pie.update_layout(autosize=True
                                            ,width=500
                                            ,height=400
                                            ,title_x=0.10
                                            ,title_y=0.95
                                            ,title_text="Execution Breakdown"
                                            ,title_font=dict(size=20)
                                            ,legend=dict(
                                                yanchor="bottom",
                                                y=-0.35,
                                                xanchor="left",
                                                x=0.8
                                            ))
                    ex_bd_pie.update_traces(textposition="inside", textinfo="percent+label")
                    st.plotly_chart(ex_bd_pie, theme="streamlit", use_container_width=False)
    
            st.write("")
            st.subheader("Bytes Details")
            st.write("")
            st.markdown(df_bytes_details.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 1).hide(axis = 1).to_html(), unsafe_allow_html = True)
    else:
        st.markdown("<h3 style='text-align: center; color: black;'>Stats Currently Unvailable</h3>", unsafe_allow_html=True)
        
    #return the ACCOUNT USAGE dataframe for the query ID
    return df_query_history_id


@st.dialog("Query Comparison")
def query_stats_compare(org_query_id, converted_query_id):
    st.html("<span class='large-dialog'></span>")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("<h2 style='text-align: center; color: black;'>Original Query</h2>", unsafe_allow_html=True)
        render_query_stats(org_query_id)

    with col2:
        st.markdown("<h2 style='text-align: center; color: black;'>Query w/updated Iceberg Tables</h2>", unsafe_allow_html=True)
        render_query_stats(converted_query_id)



def render_create_iceberg_table(ev_name, table):
    base_location_field = ""
    disable_create_it_btn = True
    flag_create_it = False

    col1, col2 = st.columns([1,4])
    with col1:
        st.write("")
        st.write("")
        st.write("**External Volume:**")
    with col2:
        st.text_input(''
                       , ev_name
                       , key = "selected_external_volume"
                       , disabled=True
                       )

    col1, col2 = st.columns([1,4])
    with col1:
        st.write("")
        st.write("")
        st.write("**Table Name:**")
    with col2:
        st.text_input(''
                       , table
                       , key = "selected_table"
                       , disabled=True
                       )

    col1, col2 = st.columns([1,4])
    with col1:
        st.write("")
        st.write("")
        st.write("**Base Location:**")
    with col2:
        base_location_path = st.text_input(""
                                           , key = "base_location_path"
                                           , help = "(optional) The path to a directory where Snowflake can write data and metadata files for the table.\nSpecify a relative path from the table’s EXTERNAL_VOLUME location. i.e: my/relative/path/from/external_volume"
                                           )

        if base_location_path:
            disable_create_it_btn = False
            base_location_field = f"""\n  BASE_LOCATION = '{base_location_path}'"""

    #get table columns to check types: 
    #all date/time columns must either have a precision of 6 or cast to it
    #cast arrays and variants to varchar
    
    table_cols = ""
    session.sql(f"DESCRIBE TABLE {table}").collect()

    df_table_cols = pd.DataFrame(session.sql(f"""SELECT "name", "type" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))""").collect())

    for index, row in df_table_cols.iterrows():
        col = str(row["name"])
        type = str(row["type"])

        #set the precision to 6 for all date types that do not have it
        date_pattern = r"(DATE|DATETIME|TIME|TIMESTAMP_LTZ|TIMESTAMP_NTZ|TIMESTAMP_TZ)\([0-5,7-9]\)"
        date_results = re.findall(date_pattern, type)
        
        if date_results:
            res_type = str(date_results[0])
            col = f"{col}::{res_type}(6) AS {col}"

        #cast variants as varchar
        if type.lower() in ["array","variant"]:
            col = f"{col}::VARCHAR AS {col}"

        if index == 0:
            table_cols += f"\t{col}\n"
        elif index > 0 and index < len(df_table_cols) - 1:
            table_cols += f"\t,{col}\n"
        else:
            table_cols += f"\t,{col}"

    st.write("")
    st.write("")
    show_iceberg_ddl = st.checkbox("Show DDL")
    
    #DDL syntax
    create_iceberg_ddl = f"""
    CREATE OR REPLACE ICEBERG TABLE {table}_IT
  EXTERNAL_VOLUME = '{ev_name}'
  CATALOG = 'SNOWFLAKE'{base_location_field}
  COMMENT = '{{"origin":"sf_sit","name":"convert2iceberg","version":{{"major":1, "minor":0}},"attributes":{{"env":"convert2iceberg","component":"{table}_IT","type":"table_iceberg_snowflake"}}}}'
  AS 
  SELECT 
  {table_cols}
  FROM {table}"""

    #beautify DDL
    create_iceberg_ddl_pretty = sqlparse.format(create_iceberg_ddl, reindent=False, keyword_case="upper")

    if show_iceberg_ddl:
        st.code(create_iceberg_ddl_pretty)


    #create external volume
    col1, col2, col3 = st.columns([3.5,3.5,0.975])

    with col3:
        btn_create_it = st.button("Create", type="primary", disabled=disable_create_it_btn)

    if btn_create_it:
        flag_create_it = True

    if flag_create_it:
        with st.spinner("Creating..."):
             session.sql(create_iceberg_ddl).collect()
        st.success(f"Iceberg table: **{table}_IT** successfully created 🎉")

        return True
    

    
def render_choose_table_wizard_view():
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
        
    if "disable_step_2" not in st.session_state:
        st.session_state.disable_step_2 = True
    
    if "disable_step_3" not in st.session_state:
        st.session_state.disable_step_3 = True

    if "cloud" not in st.session_state:
        st.session_state.cloud = ""

    if "ev_name" not in st.session_state:
        st.session_state.ev_name = ""

    if "ev_storage_provider" not in st.session_state:
        st.session_state.ev_storage_provider = ""

    if "base_url" not in st.session_state:
        st.session_state.base_url = ""

    if "use_privatelink_endpoint_idx" not in st.session_state:
        st.session_state.use_privatelink_endpoint_idx = 1

    if "aws_storage_access_point_arn" not in st.session_state:
        st.session_state.aws_storage_access_point_arn = ""

    if "aws_role_arn" not in st.session_state:
        st.session_state.aws_role_arn = ""

    if "aws_external_id" not in st.session_state:
        st.session_state.aws_external_id = ""

    if "aws_encryption_type_idx" not in st.session_state:
        st.session_state.aws_encryption_type_idx = 2

    if "kms_key_id" not in st.session_state:
        st.session_state.kms_key_id = ""

    if "azure_tenant_id" not in st.session_state:
        st.session_state.azure_tenant_id = ""

    if "gcs_encryption_type_idx" not in st.session_state:
        st.session_state.gcs_encryption_type_idx = 1

    if "db_idx" not in st.session_state:
        st.session_state.db_idx = 0

    if "schema_idx" not in st.session_state:
        st.session_state.schema_idx = 0
        
    if "table_idx" not in st.session_state:
        st.session_state.table_idx = 0

    if "table" not in st.session_state:
        st.session_state.table = ""

    if "table_selected" not in st.session_state:
        st.session_state.table_selected = False


    st.markdown("<h2 style='text-align: center; color: black;'>Convert Existing Table</h2>", unsafe_allow_html=True)    

    ###### Top Navigation ######
    btn_ev_type = "primary" if st.session_state.current_step == 1 else "secondary"
    btn_tbl_type = "primary" if st.session_state.current_step == 2 else "secondary"
    btn_ib_tbl_type = "primary" if st.session_state.current_step == 3 else "secondary"

    st.write("")
    st.write("")  
    step_cols = st.columns([0.65, .55, .55, .55, 0.5])
    step_cols[1].button("STEP 1", on_click=set_form_step, args=["Jump", 1], type=btn_ev_type, disabled=False)
    step_cols[2].button("STEP 2", on_click=set_form_step, args=["Jump", 2], type=btn_tbl_type, disabled=st.session_state.disable_step_2)        
    step_cols[3].button("STEP 3", on_click=set_form_step, args=["Jump", 3], type=btn_ib_tbl_type, disabled=st.session_state.disable_step_3)
    st.write("")
    st.write("")                   
        
    ###### Step 1: Select External Volume ######
    if st.session_state.current_step == 1:
        st.subheader("**STEP 1: Please Choose an existing External Volume**")
        st.caption(":red[⚠︎ NOTE:  If an External Volume does not exist, use the app's **CREATE EXTERNAL VOLUME** tool to create one.]")

        #select EV
        with st.spinner("Fetching External Volumes..."):
            st.session_state.ev_name = render_select_ev()
            
    
    ###### Step 2: Select Table ######
    if st.session_state.current_step == 2:                     
        st.subheader("**STEP 2: Select Table**")
        st.write("")
        
        dbs = pd.DataFrame(session.sql("SHOW DATABASES").collect())
        
        if not dbs.empty:
            sb_db = st.selectbox("Select Database:"
                                 , dbs["name"]
                                 , index = st.session_state.db_idx
                                 , key = "sb_db"
                                 , on_change = selectbox_callback                            
                                 , args = ("table", "sb_db", "db_idx", dbs["name"].values.tolist())
                                 )
            schemas = pd.DataFrame(session.sql(f"""SHOW SCHEMAS IN DATABASE {sb_db}""").collect())
            
        if not schemas.empty:            
            sb_schema = st.selectbox("Select Schema:"
                                  , schemas["name"]
                                  , index = st.session_state.schema_idx
                                  , key = "sb_schema"
                                  , on_change = selectbox_callback
                                  , args = ("table", "sb_schema", "schema_idx", schemas["name"].values.tolist())
                                  )
            session.sql(f"""SHOW OBJECTS IN SCHEMA {sb_db}.{sb_schema}""").collect()
            tables = pd.DataFrame(session.sql(f"""SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE LOWER(\"kind\") = 'table'""").collect())
        
        if not tables.empty:
            table_list = ["Choose..."] + tables["name"].values.tolist()
            sb_table = st.selectbox(f"Select Table:"
                                  , table_list
                                  , index = st.session_state.table_idx
                                  , key = "sb_table"
                                  , on_change = selectbox_callback
                                  , args = ("table", "sb_table", "table_idx", table_list)
                                  )
            st.write("")

            if st.session_state.table_selected:
                st.session_state.table = f"{sb_db}.{sb_schema}.{sb_table}"
                st.success(f"Table: **{st.session_state.table}** selected. 🎉")
        

    ###### Step 3: Create Iceberg Table ######            
    if st.session_state.current_step == 3:
        st.subheader("**STEP 3: Create Iceberg Table**")
        st.write("Verify the details below to create an iceberg table")

        #call render_create_iceberg_table
        ib_table_created = render_create_iceberg_table(st.session_state.ev_name, st.session_state.table)

        #write query conversion to table
        if ib_table_created:
            session.sql(f"""INSERT INTO "CONVERT2ICEBERG_<MY_ROLE>".LOGGING.CONVERSION_LOG(id
                                                                        ,conversion_type
                                                                        ,original_tables
                                                                        ,converted_tables
                                                                        ,timestamp)
                            SELECT
                                    UUID_STRING()
                                    ,'table'
                                    ,TO_ARRAY('{st.session_state.table}')
                                    ,TO_ARRAY('{st.session_state.table}_IT')
                                    ,SYSDATE()""").collect()
              

    ###### Bottom Navigation ###### 
    st.divider()
    disable_back_button = True if st.session_state.current_step == 1 else False
    disable_next_button = True if st.session_state.current_step == 3 or (st.session_state.current_step == 1 and st.session_state.disable_step_2) or (st.session_state.current_step == 2 and st.session_state.disable_step_3) else False

    form_footer_cols = st.columns([14,1.875,1.875])

    form_footer_cols[0].button("Home", type="secondary", on_click=set_page, args=["home"])
    form_footer_cols[1].button("Back", type="secondary", on_click=set_form_step, args=["Back"], disabled=disable_back_button)
    form_footer_cols[2].button("Next", type="primary", on_click=set_form_step, args=["Next"], disabled=disable_next_button)


def render_choose_query_wizard_view():
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
        
    if "disable_step_2" not in st.session_state:
        st.session_state.disable_step_2 = True
    
    if "disable_step_3" not in st.session_state:
        st.session_state.disable_step_3 = True

    if "disable_step_4" not in st.session_state:
        st.session_state.disable_step_4 = True
        
    if "queries" not in st.session_state:
        st.session_state.queries = pd.DataFrame()

    if "sb_query_option_idx" not in st.session_state:
        st.session_state.sb_query_option_idx = 0

    if "sb_query_history_range_idx" not in st.session_state:
        st.session_state.sb_query_history_range_idx = 0

    if "enter_query_id" not in st.session_state:
        st.session_state.enter_query_id = ""

    if "query_id_check" not in st.session_state:
        st.session_state.query_id_check = False

    if "choose_query_id" not in st.session_state:
        st.session_state.choose_query_id = ""

    if "df_query_history_id" not in st.session_state:
        st.session_state.df_query_history_id = pd.DataFrame()

    if "query_history_range" not in st.session_state:
        st.session_state.query_history_range = "Choose a Date Range"

    if "df_query_history_range" not in st.session_state:
        st.session_state.df_query_history_range = pd.DataFrame()

    if "selected_query_id" not in st.session_state:
        st.session_state.selected_query_id = ""

    if "selected_query" not in st.session_state:
        st.session_state.selected_query = ""

    if "converted_query_id" not in st.session_state:
        st.session_state.converted_query_id = ""

    if "flag_render_select_ev" not in st.session_state:
        st.session_state.flag_render_select_ev = False

    if "sb_select_convert_idx" not in st.session_state:
        st.session_state.sb_select_convert_idx = 0

    df_query_history_id = None
            
    st.markdown("<h2 style='text-align: center; color: black;'>Convert Table(s) from Query</h2>", unsafe_allow_html=True)
        
    ###### Top Navigation ######
    btn_choose_query_id_type = "primary" if st.session_state.current_step == 1 else "secondary"
    btn_query_stats_type = "primary" if st.session_state.current_step == 2 else "secondary"
    btn_convert_table_type = "primary" if st.session_state.current_step == 3 else "secondary"
    btn_view_qry_type = "primary" if st.session_state.current_step == 4 else "secondary"

    st.write("")
    st.write("")  
    step_cols = st.columns([0.5, .55, .55, .55, .55, 0.5])
    step_cols[1].button("STEP 1", on_click=set_form_step, args=["Jump", 1], type=btn_choose_query_id_type, disabled=False)
        
    step_cols[2].button("STEP 2", on_click=set_form_step, args=["Jump", 2], type=btn_query_stats_type, disabled=st.session_state.disable_step_2)        
    step_cols[3].button("STEP 3", on_click=set_form_step, args=["Jump", 3], type=btn_convert_table_type, disabled=st.session_state.disable_step_3) 
    step_cols[4].button("STEP 4", on_click=set_form_step, args=["Jump", 4], type=btn_view_qry_type, disabled=st.session_state.disable_step_4)  
    
    st.write("") 
    st.write("") 
        
    ###### Step 1: Select Choose Query ######
    if st.session_state.current_step == 1:
        disable_show_query_stats = True
        
        st.subheader("**STEP 1: Provide a Query ID or Choose a Query**")
        st.write("")
        query_option_list = ["Choose...", "Enter Query ID", "Choose Query"]

        sb_query_option = st.selectbox("Either provide a Query ID or select a query from QUERY_HISTORY"
                                    , query_option_list
                                    , index = st.session_state.sb_query_option_idx
                                    , key = "sb_query_option"
                                    , on_change = selectbox_callback
                                    , args = ("query", "sb_query_option", "sb_query_option_idx", query_option_list)
                                    )
        st.write("")

        if sb_query_option == "Enter Query ID":            
            txt_enter_query_id = st.text_input("Enter a Query ID:"
                                           , st.session_state.enter_query_id
                                           , key = "txt_enter_query_id"
                                           , on_change = input_callback
                                           , args = ("enter_query_id","txt_enter_query_id")
                                           )
            st.write("")

            if txt_enter_query_id:
                df_check_query_id = pd.DataFrame(session.sql(f"""SELECT 
                                                                    QUERY_ID
                                                                    ,END_TIME 
                                                                FROM
                                                                    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                                                                WHERE
                                                                    ROLE_NAME = CURRENT_ROLE()
                                                                    AND QUERY_ID = '{st.session_state.enter_query_id}'
                                                                    AND LOWER(EXECUTION_STATUS) = 'success';""").collect())
                

                if not df_check_query_id.empty:
                    check_query_id = df_check_query_id.iloc[0,0]
                    check_query_end_time = df_check_query_id.iloc[0,1].replace(tzinfo=datetime.timezone.utc)

                    current_time = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc)
                    query_time_diff = current_time - check_query_end_time
                    
                    if query_time_diff.days > 14:
                        st.warning(f"The query belonging to the Query ID entered was executed **{query_time_diff.days}** days ago, which is greater than the max 14 days allowed. Please enter another Query ID.", icon="⚠️")
                    else: 
                        st.session_state.selected_query_id = st.session_state.enter_query_id
                        disable_show_query_stats = False
                else:
                    st.warning(f"Invalid Query ID. Ensure that the query was successfully executed by this role. If so, please check back later or enter another Query ID.", icon="⚠️")

        if sb_query_option == "Choose Query":
            date_time_part = ""
            increment = ""
            
            query_history_range_list = ["Choose a Date Range", "Last day", "Last 7 days", "Last 14 days"]
            sb_query_history_range = st.selectbox("Select Query History Date Range:"
                                                    , query_history_range_list
                                                    , index=st.session_state.sb_query_history_range_idx
                                                    , key="sb_query_history_range"
                                                    , on_change=selectbox_callback
                                                    , args=("query", "sb_query_history_range", "sb_query_history_range_idx", query_history_range_list))
                                    
            st.write("")
            
            if sb_query_history_range == "Last day":
                date_time_part = "hours"
                increment = "24"
            elif sb_query_history_range == "Last 7 days":
                date_time_part = "days"
                increment = "7"
            elif sb_query_history_range == "Last 14 days":
                date_time_part = "days"
                increment = "14"

            if sb_query_history_range != "Choose a Date Range":
                with st.spinner("Fetching..."):
                    st.session_state.df_query_history_range = u.query_cache(session, f"""SELECT
                                                                                           --query details
                                                                                            QUERY_ID
                                                                                            ,QUERY_TEXT
                                                                                            ,QUERY_TYPE
                                                                                            --execution details
                                                                                            ,WAREHOUSE_NAME
                                                                                            ,WAREHOUSE_SIZE
                                                                                            ,WAREHOUSE_TYPE
                                                                                            ,EXECUTION_STATUS
                                                                                            ,START_TIME
                                                                                            ,END_TIME
                                                                                            ,TOTAL_ELAPSED_TIME TOTAL_ELAPSED_TIME_MS
                                                                                        FROM 
                                                                                            SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                                                                                        WHERE
                                                                                            ROLE_NAME = CURRENT_ROLE()
                                                                                            AND (QUERY_TEXT ILIKE 'select %from%' 
                                                                                                OR QUERY_TEXT ILIKE 'with%select %from%')
                                                                                            AND (CONTAINS(LOWER(QUERY_TEXT), LOWER('"sis_cmd_mgr_<MY_ROLE>".')) 
                                                                                                OR CONTAINS(LOWER(QUERY_TEXT),'cmd_mgr.')) = FALSE  
                                                                                            AND LOWER(QUERY_TYPE) = 'select'
                                                                                            AND LOWER(EXECUTION_STATUS) = 'success'
                                                                                            AND IS_CLIENT_GENERATED_STATEMENT = FALSE
                                                                                            AND WAREHOUSE_NAME IS NOT NULL
                                                                                            AND END_TIME > DATEADD({date_time_part}, -{increment}, CURRENT_TIMESTAMP())
                                                                                        ORDER BY
                                                                                            END_TIME DESC""")

                if not st.session_state.df_query_history_range.empty:
                    st.markdown("Choose a query from below:")
                    df_query_history_id = st.dataframe(
                        st.session_state.df_query_history_range
                        ,key = "query_history_id"
                        ,on_select = get_query_id_from_qh
                        ,hide_index = True
                        ,selection_mode = ["single-row"]
                    )
    
                    if df_query_history_id.selection["rows"]:
                        disable_show_query_stats = False
        
        if (sb_query_option == "Enter Query ID" and st.session_state.selected_query_id and st.session_state.selected_query_id == st.session_state.enter_query_id) or (sb_query_option == "Choose Query" and st.session_state.selected_query_id and not st.session_state["df_query_history_range"].empty):
            st.write("")
            st.success(f"Current Query ID: **{st.session_state.selected_query_id}**.")
            st.write("")

            
    ###### Step 2: Query Stats ######
    if st.session_state.current_step == 2:                     
        st.subheader("**STEP 2: Query Stats**")
        st.write("")
        if st.session_state.selected_query_id:
            #render query stats and get underlying dataframe
            df_query_history = render_query_stats(st.session_state.selected_query_id)

            if not df_query_history.empty:
                #get selected query from dataframe
                st.session_state.selected_query = df_query_history.iloc[0,1]
    
                #enable Step 3
                st.session_state.disable_step_3 = False
            else:
                st.write("#")
                st.error(f"Query ID: **{st.session_state.selected_query_id}** does not exist, not yet available in ACCOUNT_USAGE or this role does not have privileges to the query. Please return to STEP 1 and select another query.", icon="🚨")
    
                #disable Step 3
                st.session_state.disable_step_3 = True
        

    ###### Step 3: Create Iceberg Table ######            
    if st.session_state.current_step == 3:
        ib_table_created = False

        if "converted_query" not in st.session_state:
            st.session_state.converted_query = st.session_state.selected_query
        
        st.subheader("**STEP 3: Create Iceberg Table**")
        st.write("")
        if not st.session_state.query_op_stats_check:
            #get query table(s) based on query ID            
            df_query_operator_stats = u.query_cache(session, f"""SELECT
                                                                    OPERATOR_ATTRIBUTES:table_name::varchar TARGET_TABLE
                                                                FROM TABLE(GET_QUERY_OPERATOR_STATS('{st.session_state.selected_query_id}')) 
                                                                WHERE LOWER(OPERATOR_TYPE) = 'tablescan'""")

            for index, row in df_query_operator_stats.iterrows():
                tbl = str(row["TARGET_TABLE"])

                if tbl not in st.session_state.convert_tbl_select_list:
                    st.session_state.convert_tbl_select_list.append(tbl)

        st.write("")
        st.write("")
        if st.session_state.convert_tbl_select_list:
            st.write(f"The following tables from the Query ID **{st.session_state.selected_query_id}** can be converted to Iceberg tables:")
            
            for tbl in st.session_state.convert_tbl_select_list:
                st.markdown(f"- :blue[**{tbl}**]")
    
            sb_select_convert_list = ["Choose..."]+st.session_state.convert_tbl_select_list
    
            st.write("")
            sb_select_convert_tbl = st.selectbox("Select Table:"
                                        , sb_select_convert_list
                                        , index=st.session_state.sb_select_convert_idx
                                        , key="sb_select_convert_tbl"
                                        , on_change=selectbox_callback
                                        , args=("query", "sb_select_convert_tbl", "sb_select_convert_idx", sb_select_convert_list)
                                        )
    
            if sb_select_convert_tbl != "Choose...":
                col1, col2, col3 = st.columns([2,1,2])
    
                with col2:
                    btn_create_iceberg = st.button("Convert", type="primary")
        
                    if btn_create_iceberg:
                        st.session_state.flag_render_select_ev = True
    
                if st.session_state.flag_render_select_ev:    
                    #select EV
                    with st.spinner("Getting External Volumes..."):
                        ev_name = render_select_ev()
    
                    st.write("")
    
                    ib_table_created = render_create_iceberg_table(ev_name, sb_select_convert_tbl)
    
                    if ib_table_created:
                        #replace original table in query with iceberg table
                        if f"{sb_select_convert_tbl}_IT" not in st.session_state.converted_query:
                            st.session_state.converted_query = st.session_state.converted_query.replace(sb_select_convert_tbl, f"{sb_select_convert_tbl}_IT")
        
                        #add table to converted list
                        if f"{sb_select_convert_tbl}_IT" not in st.session_state.converted_tbl_list:
                            st.session_state.converted_tbl_list.append(f"{sb_select_convert_tbl}_IT")
        
                        st.session_state.disable_step_4 = False
        else:
            st.error(f"There are no eligible tables from Query ID: **{st.session_state.selected_query_id}** Please return to STEP 1 and select another query.", icon="🚨")

    ###### Step 4: View/Run Query with new Iceberg Tables ######
    if st.session_state.current_step == 4:
        flag_execute_query = False
        query_executed = False
        
        st.subheader("**STEP 4: View/Run Query with new Iceberg Tables**")
        st.write("")
        st.write("The table(s) in the original query have been replaced with the Iceberg table(s) created in the Step 3. Click **Run Query** below to execute the query.")
        st.write("")
        st.code(st.session_state.converted_query)

        #create external volume
        col1, col2, col3 = st.columns([3.5,3.5,0.975])

        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Run", type="primary")

        if btn_create_ev:
            flag_execute_query = True

        if flag_execute_query:
            with st.spinner("Updating..."):
                session.sql(st.session_state.converted_query).collect()

            #get last query ID
            last_query_id = pd.DataFrame(session.sql("SELECT LAST_QUERY_ID()").collect()).iloc[0,0]

            #write query conversion to table
            session.sql(f"""INSERT INTO "CONVERT2ICEBERG_<MY_ROLE>".LOGGING.CONVERSION_LOG(id
                                                                            ,conversion_type
                                                                            ,original_tables
                                                                            ,original_query
                                                                            ,original_query_id
                                                                            ,converted_tables
                                                                            ,converted_query
                                                                            ,converted_query_id
                                                                            ,timestamp)
                            SELECT
                                    UUID_STRING()
                                    ,'query'
                                    ,{st.session_state.convert_tbl_select_list}
                                    ,$${st.session_state.selected_query}$$
                                    ,'{st.session_state.selected_query_id}'
                                    ,{st.session_state.converted_tbl_list}
                                    ,$${st.session_state.converted_query}$$
                                    ,'{last_query_id}'
                                    ,SYSDATE()""").collect()

            query_executed = True
                 

        if query_executed:
            st.write("")
            st.success(f"""Query: **{last_query_id}** successfully executed. 🎉 
                        \nQuery execution statistics can be viewed and compared to the original query by checking the **Query Comparison** page of this app.""")

                 

    ###### Bottom Navigation ###### 
    st.divider()
    disable_back_button = True if st.session_state.current_step == 1 else False
    disable_next_button = True if st.session_state.current_step == 4 or (st.session_state.current_step == 1 and st.session_state.disable_step_2) or (st.session_state.current_step == 2 and st.session_state.disable_step_3) or (st.session_state.current_step == 3 and st.session_state.disable_step_4) else False

    form_footer_cols = st.columns([14,1.875,1.875])

    form_footer_cols[0].button("Home", type="secondary", on_click=set_page, args=["home"])
    form_footer_cols[1].button("Back", type="secondary", on_click=set_form_step, args=["Back"], disabled=disable_back_button)
    form_footer_cols[2].button("Next", type="primary", on_click=set_form_step, args=["Next"], disabled=disable_next_button)

def render_query_comparison_view():
    st.markdown("<h2 style='text-align: center; color: black;'>Query Comparisons</h2>", unsafe_allow_html=True)
    st.write("")
    st.write("The table below contains the Iceberg tables created either by selecting an existing table or query.")
    st.write("If queries were used to convert tables and the revised query was executed in this app (via **Step 4** of the Choose Query page), the execution stats of old and new queries can be can be viewed and compared by clicking **Compare** below.")
    st.write("#")

    #create table header
    col1, col2, col3, col4, col5, col6, col7 = st.columns([0.75,1,3,3,3,3,1])

    col1.markdown('<span style="font-size: 14px;">**Date**</span>', unsafe_allow_html=True)
    col2.markdown('<span style="font-size: 14px;">**Conversion Type**</span>', unsafe_allow_html=True)
    col3.markdown('<span style="font-size: 14px;">**Original Tables**</span>', unsafe_allow_html=True)
    col4.markdown('<span style="font-size: 14px;">**Original Query**</span>', unsafe_allow_html=True)
    col5.markdown('<span style="font-size: 14px;">**Converted Tables**</span>', unsafe_allow_html=True)
    col6.markdown('<span style="font-size: 14px;">**Converted Query**</span>', unsafe_allow_html=True)
    col7.markdown('<span style="font-size: 14px;">**Compare**</span>', unsafe_allow_html=True)

    df_conversion_log = pd.DataFrame(session.sql(f"""SELECT
                                                        ID
                                                        ,TO_DATE(TIMESTAMP) AS DATE
                                                        ,CONVERSION_TYPE
                                                        ,ORIGINAL_TABLES
                                                        ,ORIGINAL_QUERY
                                                        ,ORIGINAL_QUERY_ID
                                                        ,CONVERTED_TABLES
                                                        ,CONVERTED_QUERY
                                                        ,CONVERTED_QUERY_ID
                                                    FROM "CONVERT2ICEBERG_<MY_ROLE>".LOGGING.CONVERSION_LOG ORDER BY TIMESTAMP DESC""").collect())

    for index, row in df_conversion_log.iterrows():
        id = str(row["ID"])
        date = row["DATE"]
        type = str(row["CONVERSION_TYPE"])
        org_tables = json.loads(row["ORIGINAL_TABLES"])
        org_query = str(row["ORIGINAL_QUERY"])
        org_query_id = str(row["ORIGINAL_QUERY_ID"])
        converted_tables = json.loads(row["CONVERTED_TABLES"])
        converted_query = str(row["CONVERTED_QUERY"])
        converted_query_id = str(row["CONVERTED_QUERY_ID"])
        
        col1, col2, col3, col4, col5, col6, col7 = st.columns([0.75,1,3,3,3,3,1])

        col1.markdown(f'<span style="font-size: 12px;">{date}</span>', unsafe_allow_html=True)
        col2.markdown(f'<span style="font-size: 12px;">{type}</span>', unsafe_allow_html=True)
        for tbl in org_tables:
            col3.markdown(f'<li style="font-size: 12px;">{tbl}</li>', unsafe_allow_html=True)
        col4.markdown(f'<span style="font-size: 12px;">{org_query}</span>', unsafe_allow_html=True)
        for tbl in converted_tables:
            col5.markdown(f'<li style="font-size: 12px;">{tbl}</li>', unsafe_allow_html=True)
        col6.markdown(f'<span style="font-size: 12px;">{converted_query}</span>', unsafe_allow_html=True)

        #add convert button if type is query
        if type.lower() == "query":
            if col7.button("Compare", type="secondary", key=f"btn_{id}"):
                query_stats_compare(org_query_id, converted_query_id)


    st.write("#")

    #home button
    col1, col2, col3 = st.columns([3.5,3.5,0.975])

    with col1:
        st.write("")
        st.write("")
        st.button("Home", type="secondary", on_click=set_page, args=["home"])



def set_page(page: str):
    st.session_state.page = page


class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass
    
    @abstractmethod
    def print_page(self):
        pass


class BasePage(Page):
    def __init__(self):
        pass
    
    def print_page(self):
        u.render_image("img/snowflake-logo-color-rgb@2x.png")
        
        st.markdown("<h1 style='text-align: center; color: black;'>CONVERT2ICEBERG</h1>", unsafe_allow_html=True)
        st.write("")
        st.write("Convert2Iceberg converts a Snowflake table (FDN-based) to a Snowflake-managed Iceberg table, either by selecting the table by name or choosing a query that contains table(s) to convert. In addition, this app also allows the user compare query execution statistics between a query before and after the conversion to Iceberg.")
        st.divider()


class home(BasePage):
    def __init__(self):
        self.name="home"
        
    def print_page(self):
        super().print_page()

        #clear session_state when at home screen
        clear_c2i_session_vars()
        
        #st.write("")        
        col1, col2, col3 = st.columns([1,1,1])

        with col2:
            st.write("")
            if st.button("Prerequisites", type="primary"):
                render_prereqs()

        st.divider()

        col1, col2 = st.columns(2, gap="small")
        with col1:
           st.markdown("<h3 style='text-align: center; color: black;'>External Volume</h3>", unsafe_allow_html=True)
           ev_col1, ev_col2, ev_col3 = st.columns([0.25,0.75,0.25], gap="small")
           with ev_col2:
               u.render_image_menu("img/ra_volume.png")
           st.markdown("""
                        Create a new External Volume to use for your Snowflake-managed Iceberg table.
                        """)
           st.write("")

           ev_col1, ev_col2, ev_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with ev_col2:
            st.button("Create Volume", type="primary", on_click=set_page,args=("create_ev",), key="btn_create_ev")

        with col2:
           st.markdown("<h3 style='text-align: center; color: black;'>Choose Table</h3>", unsafe_allow_html=True)
           ct_col1, ct_col2, ct_col3 = st.columns([0.25,0.75,0.25], gap="small")
            
           with ct_col2:  
               u.render_image_menu("img/ra_table.png")
           st.markdown("""
                        Create a new Iceberg table from an existing table
                        """)
           st.write("")

           ct_col1, ct_col2, ct_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with ct_col2:
                st.button("Choose Table", type="primary", on_click=set_page,args=("choose_table",), key="btn_choose_table") 

        st.write("")
        st.write("")
        st.write("")
        st.write("")
        
        col1, col2 = st.columns(2, gap="small")
        with col1:
           st.markdown("<h3 style='text-align: center; color: black;'>Choose Query</h3>", unsafe_allow_html=True)
           cq_col1, cq_col2, cq_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with cq_col2:  
               u.render_image_menu("img/sql.png")
           st.markdown("""
                        Create one or more Iceberg tables from tables in a query. 
                        """)
           st.write("")

           cq_col1, cq_col2, cq_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with cq_col2: 
               st.button("Choose Query", type="primary", on_click=set_page,args=("choose_query",), key="btn_choose_query") 
        
        with col2:
           st.markdown("<h3 style='text-align: center; color: black;'>Query Comparison</h3>", unsafe_allow_html=True)
           qc_col1, qc_col2, qc_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with qc_col2:  
               u.render_image_menu("img/analytics.png")
           st.markdown("""
                        Compare a query's performance before and after converting table(s) to Iceberg. 
                        """)
           st.write("") 

           qc_col1, qc_col2, qc_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with qc_col2:
                st.button("Query Comparison", type="primary", on_click=set_page,args=("query_comparison",), key="btn_query_comparison")
        



########################################################################### Create External Volum

class create_ev_page(BasePage):
    def __init__(self):
        self.name="create_ev"
        
    def print_page(self):
        super().print_page()

        #render create new External Volume wizard
        render_create_ev()

########################################################################### Choose Table

class choose_table_page(BasePage):
    def __init__(self):
        self.name="choose_table"
        
    def print_page(self):
        super().print_page()

        #render create new Iceberg table wizard
        render_choose_table_wizard_view()

########################################################################### Choose Query

class choose_query_page(BasePage):
    def __init__(self):
        self.name="choose_query"
        
    def print_page(self):
        super().print_page()

        #render Choose Query wizard
        render_choose_query_wizard_view()

########################################################################### Query Comparison

class query_comparison_page(BasePage):
    def __init__(self):
        self.name="query_comparison"
        
    def print_page(self):
        super().print_page()

        #render Query Comparison view
        render_query_comparison_view()

############################################################################## Main ####################################################################################################

pages = [home(), create_ev_page(), choose_table_page(), choose_query_page(), query_comparison_page()]

session = get_active_session()

def main():
    for page in pages:
        if page.name == st.session_state.page:
            if page.name == "query_comparison":
                st.session_state.layout="wide"
            else:
                st.session_state.layout="centered"
            
            page.print_page();

main()