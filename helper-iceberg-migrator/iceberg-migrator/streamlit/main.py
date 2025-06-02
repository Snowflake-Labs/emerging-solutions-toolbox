# Import python packages
import streamlit as st

from snowflake.snowpark.context import get_active_session
from abc import ABC, abstractmethod
import base64
import datetime
import json
import pandas as pd
import time
import utils.utils as u

if "page" not in st.session_state:
    st.session_state.page = "home"

if st.session_state.page in ["migration_log"]:
    st.set_page_config(layout="wide")
else:
    st.set_page_config(layout="centered")

if "ev_created" not in st.session_state:
    st.session_state.ev_created = False

if "flag_disable_btn_create_ev" not in st.session_state:
    st.session_state.flag_disable_btn_create_ev = True


def clear_c2i_session_vars():
    #global c2i session vars 
    if "ev_created" in st.session_state:
        del st.session_state.ev_created
    
    if "flag_disable_btn_create_ev" in st.session_state:
        del st.session_state.flag_disable_btn_create_ev

    #migrate snowflake tables wizard session vars
    if "current_step" in st.session_state:
        del st.session_state.current_step
        
    if "disable_step_2" in st.session_state:
        del st.session_state.disable_step_2
    
    if "disable_step_3" in st.session_state:
        del st.session_state.disable_step_3

    if "snowflake_ev_check" in st.session_state:
        del st.session_state.snowflake_ev_check

    if "snowflake_evs" in st.session_state:
        del st.session_state.snowflake_evs

    if "snowflake_ev_idx" in st.session_state:
        del st.session_state.snowflake_ev_idx

    if "snowflake_ev_name" in st.session_state:
        del st.session_state.snowflake_ev_name

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

    if "snowflake_src_db_idx" in st.session_state:
        del st.session_state.snowflake_src_db_idx
        
    if "snowflake_src_db" in st.session_state:
        del st.session_state.snowflake_src_db

    if "disable_schemas_ms" in st.session_state:
        del st.session_state.disable_schemas_ms

    if "schema_val" in st.session_state:
        del st.session_state.schema_val

    if "schema_list" in st.session_state:
        del st.session_state.schema_list
        
    if "snowflake_src_schemas" in st.session_state:
        del st.session_state.snowflake_src_schemas
        
    if "snowflake_target_db_sch_value" in st.session_state:
        del st.session_state.snowflake_target_db_sch_value
        
    if "snowflake_target_db_idx" in st.session_state:
        del st.session_state.snowflake_target_db_idx
        
    if "snowflake_target_db" in st.session_state:
        del st.session_state.snowflake_target_db
        
    if "snowflake_target_sch_idx" in st.session_state:
        del st.session_state.snowflake_target_sch_idx
        
    if "snowflake_target_sch" in st.session_state:
        del st.session_state.snowflake_target_sch

    #migrate delta tables wizard session vars
    if "current_step" in st.session_state:
        del st.session_state.current_step
        
    if "disable_step_2" in st.session_state:
        del st.session_state.disable_step_2
    
    if "disable_step_3" in st.session_state:
        del st.session_state.disable_step_3
        
    if "delta_ev_check" in st.session_state:
        del st.session_state.delta_ev_check
        
    if "delta_evs" in st.session_state:
        del st.session_state.delta_evs
        
    if "delta_ev_idx" in st.session_state:
        del st.session_state.delta_ev_idx

    if "delta_ev_name" in st.session_state:
        del st.session_state.delta_ev_name

    if "delta_ci_check" in st.session_state:
        del st.session_state.delta_ci_check
      
    if "delta_c_ints" in st.session_state:
        del st.session_state.delta_c_ints
        
    if "delta_ci_idx" in st.session_state:
        del st.session_state.delta_ci_idx
    
    if "delta_ci_name" in st.session_state:
        del st.session_state.delta_ci_name

    if "stages_created" in st.session_state:
        del st.session_state.stages_created

    if "ev_location" in st.session_state:
        del st.session_state.ev_location
        
    if "cb_all_delta_tables_value" in st.session_state:
        del st.session_state.cb_all_delta_tables_value

    if "master_delta_table_list" in st.session_state:
        del st.session_state.master_delta_table_list

    if "selected_delta_tables_list" in st.session_state:
        del st.session_state.selected_delta_tables_list
        
    if "df_show_delta_tables" in st.session_state:
        del st.session_state.df_show_delta_tables

    if "disable_ms_delta_tables" in st.session_state:
        del st.session_state.disable_ms_delta_tables
        
    if "delta_db_idx" in st.session_state:
        del st.session_state.delta_db_idx
        
    if "delta_target_db" in st.session_state:
        del st.session_state.delta_target_db
        
    if "delta_sch_idx" in st.session_state:
        del st.session_state.delta_sch_idx
        
    if "delta_target_sch" in st.session_state:
        del st.session_state.delta_target_sch

    #clear all cached data
    st.cache_data.clear()
    
    
def set_form_step(action,step=None):
    if action == "Next":
        st.session_state.current_step = st.session_state.current_step + 1
    if action == "Back":
        st.session_state.current_step = st.session_state.current_step - 1
    if action == "Jump":
        st.session_state.current_step = step


#setting custom width for larger st.dialogs
st.markdown(
    """
<style>
div[data-testid="stDialog"] div[role="dialog"]:has(.large-dialog) {
    width: 85%;
}

div[data-testid="stDialog"] div[role="dialog"]:has(.medium-dialog) {
    width: 65%;
}

div[data-testid="stDialog"] div[role="dialog"]:has(.small-dialog) {
    width: 45%;
}
</style>
""",
    unsafe_allow_html=True,
)


def create_pairs(input_list):
    pairs = []
    for i in range(0, len(input_list), 2):
        if i + 1 < len(input_list):
            pairs.append((input_list[i], input_list[i + 1]))
        else:
            pairs.append((input_list[i], None))
    return pairs


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
    if wizard.lower() in ("snowflake_table", "delta_table"):
        if st.session_state.current_step == 2:
            st.session_state.disable_step_3 = False


def multiselect_callback(wizard, val, pair_idx, sch_idx):
    if wizard.lower() in ("snowflake_table", "delta_table"):
        if val.lower() == "ms_snowflake_src_schemas":
            st.session_state.schema_list = st.session_state[val]

        if val.startswith("ms_snowflake_tables_"):
            st.session_state[f"ms_snowflake_tables_{pair_idx}_{sch_idx}_list"] = st.session_state[val]
            
        if val.lower() == "ms_delta_tables":
            st.session_state.selected_delta_tables_list = st.session_state[val]


def checkbox_callback(wizard, val, ms_val, ms_flag, pair_idx, sch_idx):
    if wizard.lower() in ("snowflake_table", "delta_table"):
        if ms_val.lower() == "ms_snowflake_src_schemas":
            if st.session_state[ms_val]:
                st.session_state.schema_list = []
                        
            if st.session_state[val]:
                st.session_state.schema_val = True
                st.session_state[ms_flag] = True
            else:
                st.session_state.schema_val = False
                st.session_state[ms_flag] = False

        if ms_val.lower() == "ms_delta_tables":
            if st.session_state[ms_val]:
                st.session_state.selected_delta_tables_list = []

            if st.session_state[val]:
                st.session_state.cb_all_delta_tables_value = True
                st.session_state[ms_flag] = True
            else:
                st.session_state.cb_all_delta_tables_value = False
                st.session_state[ms_flag] = False
                
            if st.session_state[val]:
                st.session_state.disable_step_2 = False

            if not st.session_state[val]:
                if not st.session_state.master_delta_table_list:
                    st.session_state.disable_step_2 = True
                    st.rerun()
                    
        if val.lower() == "snowflake_target_db_sch":
            if st.session_state[val]:
                st.session_state.snowflake_target_db_sch_value = True
            else:
                st.session_state.snowflake_target_db_sch_value = False
                st.session_state.snowflake_target_db = None
                st.session_state.snowflake_target_sch = None
            
        if val.startswith("cb_all_tables_"):
            if st.session_state[val]:
                st.session_state[f"cb_all_tables_{pair_idx}_{sch_idx}_value"] = True
            else:
                st.session_state[f"cb_all_tables_{pair_idx}_{sch_idx}_value"] = False
            
            if st.session_state[val]:
                st.session_state.disable_step_2 = False

            if not st.session_state[val]:
                if not st.session_state.master_table_list:
                    st.session_state.disable_step_2 = True
                    st.rerun()


def highlight_log_status(val):
    match val:
        case "QUEUED":
            color = "#FFF8D5"
            weight = "bold"
        case "RUNNING":
            color = "#BEDDF1"
            weight = "bold"
        case "FAILED":
            color =  "#F1BEB5"
            weight = "bold"
        case "COMPLETE":
            color = "#D1FEB8"
            weight = "bold"
        case _:
            color = "#FFFFFF"
            weight = "normal"
    
    return f"background-color: {color}; font-weight: {weight}"

@st.fragment(run_every="10s")
def migration_log_check():
    log_check_stmt = """WITH log_cte AS (
                            select 
                                mt.table_instance_id table_id
                                ,mtl.run_id
                                ,mt.table_type
                                ,mt.target_table_catalog
                                ,mt.target_table_schema
                                ,mt.table_name
                                ,mtl.state_code
                                ,TO_VARCHAR(mtl.log_time, 'YYYY-MM-DD HH24:MI:SS') log_time
                                ,mtl.log_message
                                ,ROW_NUMBER() OVER (PARTITION BY mt.table_instance_id, run_id ORDER BY log_time asc) AS row_number
                            from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log mtl
                            inner join ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table mt
                               on mt.table_instance_id = mtl.table_instance_id
                            --where log_time >= (select max(start_time) from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_run_log)
                            order by log_time desc
                        )
                        
                        SELECT
                            * exclude(row_number)
                        FROM log_cte
                        WHERE row_number = (SELECT MAX(row_number) FROM log_cte WHERE table_id = log_cte.table_id AND run_id = log_cte.run_id GROUP BY ALL)
                        ;"""

    #set df for first log check for this run
    df_run_status = pd.DataFrame(session.sql(log_check_stmt).collect())

    st.write("")
    st.write("")
    st.markdown("<h4 style='text-align: left; color: black;'>Migration Log</h4>", unsafe_allow_html=True)
    st.markdown(df_run_status.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px'),('background-color','#D3D3D3')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 0).hide(axis = 0).applymap(highlight_log_status).to_html(), unsafe_allow_html = True)

def manual_migration_log_check():
    df_log_check = pd.DataFrame(session.sql(f"""WITH log_cte AS (
                                                        select 
                                                            mt.table_instance_id table_id
                                                            ,mtl.run_id
                                                            ,mt.table_type
                                                            ,mt.target_table_catalog
                                                            ,mt.target_table_schema
                                                            ,mt.table_name
                                                            ,mt.insert_date
                                                            ,mtl.state_code
                                                            ,TO_VARCHAR(mtl.log_time, 'YYYY-MM-DD HH24:MI:SS') log_time
                                                            ,mtl.log_message
                                                            ,ROW_NUMBER() OVER (PARTITION BY mt.table_instance_id, run_id ORDER BY log_time asc) AS row_number
                                                        from ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table_log mtl
                                                        inner join ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.migration_table mt
                                                           on mt.table_instance_id = mtl.table_instance_id
                                                        order by log_time desc
                                                    )
                                                    
                                                    SELECT
                                                        * exclude(row_number)
                                                    FROM log_cte
                                                    WHERE row_number = (SELECT MAX(row_number) FROM log_cte WHERE table_id = log_cte.table_id AND run_id = log_cte.run_id GROUP BY ALL);
                                                    """).collect())

    return df_log_check

def run_sis_cmd(cmd, generate_results_flag):
    df_cmd_results = pd.DataFrame("Dataframe not generated", index=[0], columns=["Status"])
    
    #set time parameters
    timeout = 120000 #2 mins in milliseconds
    cmd_start_time = int(time.time() * 1000) #current time in ms
    cmd_end_time = int(time.time() * 1000)

    #insert command into sis cmd mgr
    cmd_hash = ''
    if generate_results_flag:
        cmd_hash = pd.DataFrame(session.sql(f"""SELECT "SIS_CMD_MGR_ACCOUNTADMIN".UTIL.TABLE_UUID($${cmd}$$)""").collect()).iloc[0,0] #hash of the command
    
    start_timestamp = pd.DataFrame(session.sql(f"SELECT SYSDATE()").collect()).iloc[0,0]
    
    session.sql(f"""INSERT INTO "SIS_CMD_MGR_ACCOUNTADMIN".CMD_MGR.COMMANDS(app_name, cmd, cmd_hash, status, start_timestamp, completed_timestamp, generate_results_table, results_table, notes)
                    SELECT 
                        'ICEBERGMIGRATOR'
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
                                FROM "SIS_CMD_MGR_ACCOUNTADMIN".CMD_MGR.COMMANDS 
                                WHERE app_name = 'ICEBERGMIGRATOR'
                                AND cmd_hash = '{cmd_hash}'""").collect()).iloc[0,0]
    status = "PROCESSING"
    results_table = ""
    notes = ""
    
    #poll sis cmd mgr commands table until command status is complete
    while True:
        df_cmd_status = pd.DataFrame(session.sql(f"""SELECT status, results_table, notes FROM "SIS_CMD_MGR_ACCOUNTADMIN".CMD_MGR.COMMANDS WHERE app_name = 'ICEBERGMIGRATOR' AND run_id = '{run_id}'""").collect())
        
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
        df_cmd_results = pd.DataFrame(session.sql(f"""SELECT * FROM "SIS_CMD_MGR_ACCOUNTADMIN".RESULTS."{results_table}";""").collect())
    
    return df_cmd_results

    
@st.dialog("Snowflake Prerequisites")
def render_sf_prereqs():
    st.html("<span class='small-dialog'></span>")
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
        - Migrating Snowflake Tables:
            - **External:**
                - {storage_name} in the ```{current_region}``` region to write the iceberg files  
            - **Snowflake:**
                - Granted either the ```ACCOUNTADMIN``` role or a role with the ```CREATE EXTERNAL VOLUME``` privilege.
                - An existing Snowflake EXTERNAL VOLUME registered to {storage_name} in the ```{current_region}``` region
                    - This tool can be used to create the EXTERNAL VOLUME. 
                    - Visit {sf_ext_vol_url} for instructions for ```{current_region.split('_')[0]}```. 
                - Schema to install the metadata tables, views and procedures 
                - Warehouse that will be used for migration to iceberg 
            - **Role Permissions:**
                - Ability to query and update all the object in the tool schema 
                - Ability to query all the views in the tool schema 
                - Ability to create and execute procedures in the tool schema
                - Ability to use the warehouse for processing
                - Ability to create, execute, monitor and drop tasks in the tool schema 
                - Usage of the external volume
                - Ability to create and drop objects in the databases and schemas that contain table to be modified 
                - Ability to create objects in the target databases and schemas (if not replacing existing Snowflake tables)
                - Ability to set permissions and change ownership of the newly created iceberg tables               
        """
    st.markdown(prereqs)


@st.dialog("Delta Prerequisites")
def render_delta_prereqs():
    st.html("<span class='small-dialog'></span>")
    sf_ext_vol_url = "https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume"
    
    prereqs = f"""
        - Delta Lake Integration:
            - **External:**
                - Table Delta files stored in cloud storage (S3, AZURE, or GCS)
            - **Snowflake:**
                - Granted either the ```ACCOUNTADMIN``` role or a role with the ```CREATE EXTERNAL VOLUME``` privilege..
                - An existing Snowflake EXTERNAL VOLUME
                    - This tool can be used to create an EXTERNAL VOLUME. 
                    - Visit {sf_ext_vol_url} for instructions for the cloud storage where the Delta files reside. 
                    - :red[⚠︎ **NOTE:**] the External Volume's ```STORAGE_BASE_URL``` must contain directories for each table to migrate. Each table directory must contain that table's Delta files
                - Granted either the ```ACCOUNTADMIN``` role or a role with the ```CREATE INTEGRATION``` privilege.
                - Granted either the ```ACCOUNTADMIN``` role or a role with the ```CREATE STORAGE INTEGRATION``` privilege.
            - **Role Permissions:**
                - Ability to query and update all the object in the tool schema 
                - Ability to query all the views in the tool schema 
                - Ability to create and execute procedures in the tool schema
                - Ability to use the warehouse for processing
                - Ability to create, execute, monitor and drop tasks in the tool schema 
                - Ability to create and drop stages in the staging schema 
                - Ability to create and drop the Storage Integration
                - Usage of the External Volume
                - Usage of Storage Integration
                - Ability to create and drop objects in the databases and schemas that contain table to be modified 
                - Ability to create Iceberg Tables in the target databases and schemas                
        """
    st.markdown(prereqs)

  
def render_create_ev():
    current_region= ""
    btn_create_ev = False
    flag_create_ev = False
    txt_ev_name = ""
    flag_disable_btn_create_ev = True
    ev_created = False


    st.subheader("**Create an External Volume**")
    st.caption(":red[⚠︎ NOTE:  The current role should be either ACCOUNTADMIN or have been granted the CREATE EXTERNAL VOLUME privilege.]")
    st.write("")

    cloud_list = ["AWS", "AZURE", "GCS"]
    st.selectbox("Cloud Provider:"
                          , cloud_list
                          , key = "sb_cloud"
                          )

    if st.session_state.sb_cloud == "AWS":
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
            st.button("Home", key="create_ev_home", type="secondary", on_click=set_page, args=["home"])

        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", key="create_ev_btn", type="primary", disabled=flag_disable_btn_create_ev)

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
    
    if st.session_state.sb_cloud == "AZURE":
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
            st.button("Home", key="create_ev_home", type="secondary", on_click=set_page, args=["home"])

        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", key="create_ev_btn", type="primary", disabled=flag_disable_btn_create_ev)

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

    if st.session_state.sb_cloud == "GCS":
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
            st.button("Home", key="create_ev_home", type="secondary", on_click=set_page, args=["home"])
    
        with col3:
            st.write("")
            st.write("")
            btn_create_ev = st.button("Create", key="create_ev_btn", type="primary", disabled=flag_disable_btn_create_ev)

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

    if ev_created:
        st.write("")
        st.success(f"External Volume: **{txt_ev_name}** successfully created. 🎉")


def render_create_ci():
    btn_create_ci = False
    flag_create_ci = False
    flag_disable_btn_create_ci = True
    ci_created = False

    st.subheader("**Create a Catalog Integration (DELTA)**")
    st.caption(":red[⚠︎ NOTE:  The current role should be either ACCOUNTADMIN or have been granted the `CREATE INTEGRATION` privilege.]")
    st.write("")

    #catalog integration name
    txt_ci_name = st.text_input('Catalog Integration Name:'
                                , key = "txt_ci_name"
                                )

    if txt_ci_name != '':
        flag_disable_btn_create_ci = False
        
    st.write("#")

    #create catalog integration
    col1, col2, col3 = st.columns([3.5,3.5,0.975])

    with col1:
        st.write("")
        st.write("")
        st.button("Home", key="create_ci_home", type="secondary", on_click=set_page, args=["home"])

    with col3:
        st.write("")
        st.write("")
        btn_create_ci = st.button("Create", key="create_ci_btn", type="primary", disabled=flag_disable_btn_create_ci)

    if btn_create_ci:
        flag_create_ci = True

    if flag_create_ci:
         with st.spinner("Updating..."):
             session.sql(f"""CREATE OR REPLACE CATALOG INTEGRATION {txt_ci_name}
                                CATALOG_SOURCE = OBJECT_STORE
                                TABLE_FORMAT = DELTA
                                ENABLED = TRUE;""").collect()
             ci_created = True             

    if ci_created:
        st.write("")
        st.success(f"Catalog Integration: **{txt_ci_name}** successfully created. 🎉")


def render_choose_snowflake_tables_wizard_view():
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
        
    if "disable_step_2" not in st.session_state:
        st.session_state.disable_step_2 = True
    
    if "disable_step_3" not in st.session_state:
        st.session_state.disable_step_3 = True

    if "snowflake_ev_check" not in st.session_state:
        st.session_state.snowflake_ev_check = False

    if "snowflake_evs" not in st.session_state:
        st.session_state.snowflake_evs = pd.DataFrame()

    if "snowflake_ev_idx" not in st.session_state:
        st.session_state.snowflake_ev_idx = 0

    if "snowflake_ev_name" not in st.session_state:
        st.session_state.snowflake_ev_name = ""

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

    if "snowflake_src_db_idx" not in st.session_state:
        st.session_state.snowflake_src_db_idx = 0
        
    if "snowflake_src_db" not in st.session_state:
        st.session_state.snowflake_src_db = ""

    if "disable_schemas_ms" not in st.session_state:
        st.session_state.disable_schemas_ms = False

    if "schema_val" not in st.session_state:
        st.session_state.schema_val = False

    if "schema_list" not in st.session_state:
        st.session_state.schema_list = []
        
    if "snowflake_src_schemas" not in st.session_state:
        st.session_state.snowflake_src_schemas = []
        
    if "snowflake_target_db_sch_value" not in st.session_state:
        st.session_state.snowflake_target_db_sch_value = False
        
    if "snowflake_target_db_idx" not in st.session_state:
        st.session_state.snowflake_target_db_idx = 0
        
    if "snowflake_target_db" not in st.session_state:
        st.session_state.snowflake_target_db = None
        
    if "snowflake_target_sch_idx" not in st.session_state:
        st.session_state.snowflake_target_sch_idx = 0
        
    if "snowflake_target_sch" not in st.session_state:
        st.session_state.snowflake_target_sch = None

    master_table_list = []


    st.markdown("<h2 style='text-align: center; color: black;'>Migrate Snowflake Tables</h2>", unsafe_allow_html=True)    

    ###### Top Navigation ######
    btn_ev_type = "primary" if st.session_state.current_step == 1 else "secondary"
    btn_tbl_type = "primary" if st.session_state.current_step == 2 else "secondary"
    btn_ib_tbl_type = "primary" if st.session_state.current_step == 3 else "secondary"

    if st.session_state.current_step == 2:
        st.session_state.disable_step_3 = False

    st.write("")
    st.write("")  
    step_cols = st.columns([0.65, .55, .55, .55, 0.5])
    step_cols[1].button("STEP 1", key="nav_step1", on_click=set_form_step, args=["Jump", 1], type=btn_ev_type, disabled=False)
    step_cols[2].button("STEP 2", key="nav_step2", on_click=set_form_step, args=["Jump", 2], type=btn_tbl_type, disabled=st.session_state.disable_step_2)        
    step_cols[3].button("STEP 3", key="nav_step3", on_click=set_form_step, args=["Jump", 3], type=btn_ib_tbl_type, disabled=st.session_state.disable_step_3)
    st.write("")
    st.write("")                       
    
    ###### Step 1: Select Tables ######
    if st.session_state.current_step == 1:                     
        st.subheader("**STEP 1: Select Tables**")
        st.write("")

        #select EV
        st.markdown("<h6 style='text-align: left; color: black;'>Please choose the External Volume</h6>", unsafe_allow_html=True)
        st.caption(":red[⚠︎ NOTE:  If an External Volume does not exist, use the app's **CREATE EXTERNAL VOLUME** tool to create one.]")
        st.write("")
        
        with st.spinner("Fetching External Volumes..."):
            if not st.session_state.snowflake_ev_check:
                #call run_sis_cmd to get external volumes --SiS cannot execute this show command
                st.session_state.snowflake_evs = run_sis_cmd("SHOW EXTERNAL VOLUMES", True)
                st.session_state.snowflake_ev_check = True
                    
            select_ev_list = []
            
            if not st.session_state.snowflake_evs.empty :
                select_ev_list = ["Choose..."] + st.session_state.snowflake_evs["name"].values.tolist()
            else:
                select_ev_list = ["Choose..."]
        
            st.session_state.snowflake_ev_name = st.selectbox("Select External Volume:"
                                                            , select_ev_list
                                                            , index = st.session_state.snowflake_ev_idx
                                                            , key = "sb_snowflake_ev"
                                                            , on_change = selectbox_callback
                                                            , args = ("snowflake_table", "sb_snowflake_ev", "snowflake_ev_idx", select_ev_list)
                                                            )
        st.write("")
        st.write("")
        
        if st.session_state.snowflake_ev_name != "Choose...":
            dbs = pd.DataFrame(session.sql("SHOW DATABASES").collect())
            schemas = None
            tables = None
            
            if not dbs.empty:
                st.session_state.snowflake_src_db = st.selectbox("Select Source Database:"
                                                                    , dbs["name"]
                                                                    , index = st.session_state.snowflake_src_db_idx
                                                                    , key = "sb_snowflake_src_db"
                                                                    , on_change = selectbox_callback                            
                                                                    , args = ("snowflake_table", "sb_snowflake_src_db", "snowflake_src_db_idx", dbs["name"].values.tolist())
                                                                    )
                schemas = pd.DataFrame(session.sql(f"""SHOW SCHEMAS IN DATABASE {st.session_state.snowflake_src_db}  WITH PRIVILEGES OWNERSHIP, USAGE""").collect())
                
            if not schemas.empty:
                st.session_state.snowflake_src_schemas = st.multiselect("Select Source Schema(s):"
                                                                        , schemas["name"].values.tolist()
                                                                        , default = st.session_state.schema_list
                                                                        , key = "ms_snowflake_src_schemas"
                                                                        , disabled = st.session_state.disable_schemas_ms
                                                                        , on_change= multiselect_callback
                                                                        , args = ("snowflake_table", f"ms_snowflake_src_schemas", None, None)
                                                                        )
                cb_all_schemas = st.checkbox("All Schemas"
                                            , key = "cb_all_schemas"
                                            , value = st.session_state.schema_val
                                            , on_change = checkbox_callback
                                            , args = ("snowflake_table", "cb_all_schemas", "ms_snowflake_src_schemas", "disable_schemas_ms", None, None))

                if cb_all_schemas:
                    schemas = pd.DataFrame(session.sql(f"""SHOW SCHEMAS IN DATABASE {st.session_state.snowflake_src_db}  WITH PRIVILEGES OWNERSHIP, USAGE""").collect())
                    st.session_state.snowflake_src_schemas = pd.DataFrame(session.sql(f"""SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))""").collect())['name'].values.tolist()

            st.divider()
            st.markdown("<h4 style='text-align: left; color: black;'>Schemas</h4>", unsafe_allow_html=True)

            #create a list of schema pairs (tuples)
            sch_pair = create_pairs(st.session_state.snowflake_src_schemas)
            
            for p_index, pair in enumerate(sch_pair):
                p_idx = p_index + 1

                #create two columns per pair
                col1, col2 = st.columns(2)
                
                for s_index, sch in enumerate(pair):
                    if sch is not None:
                        table_list = []
                        s_idx = s_index + 1
                        
                        if f"ms_snowflake_tables_{p_idx}_{s_idx}_list" not in st.session_state:
                            st.session_state[f"ms_snowflake_tables_{p_idx}_{s_idx}_list"] = []
                            
                        if f"disable_tables_ms_{p_idx}_{s_idx}" not in st.session_state:
                            st.session_state[f"disable_tables_ms_{p_idx}_{s_idx}"] = False

                        if f"cb_all_tables_{p_idx}_{s_idx}_value" not in st.session_state:
                            st.session_state[f"cb_all_tables_{p_idx}_{s_idx}_value"] = False
                                
                        session.sql(f"""SHOW OBJECTS IN SCHEMA {st.session_state.snowflake_src_db}.{sch}""").collect()
                        tables = pd.DataFrame(session.sql(f"""SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE LOWER(\"kind\") = 'table'""").collect())
        
                        if s_idx == 1:
                            ms_tables_1 = []
                            cb_all_tables_1_flag = True
                            
                            with col1:
                                with st.container(border=True):
                                    st.markdown(f"**Schema:** {sch}")
                                    
                                    if not tables.empty:
                                        table_list = tables["name"].values.tolist()
                                        cb_all_tables_1_flag = False
                                        
                                    ms_tables_1 = st.multiselect("Select Table(s):"
                                        , table_list
                                        , default = st.session_state[f"ms_snowflake_tables_{p_idx}_{s_idx}_list"] 
                                        , key = f"ms_snowflake_tables_{p_idx}_{s_idx}"
                                        , disabled = st.session_state[f"disable_tables_ms_{p_idx}_{s_idx}"]
                                        , on_change= multiselect_callback
                                        , args = ("snowflake_table", f"ms_snowflake_tables_{p_idx}_{s_idx}", p_idx, s_idx)
                                        )
        
                                    cb_all_tables_1 = st.checkbox("All Tables"
                                            , value = st.session_state[f"cb_all_tables_{p_idx}_{s_idx}_value"] 
                                            , key = f"cb_all_tables_{p_idx}_{s_idx}"
                                            , disabled = cb_all_tables_1_flag
                                            , on_change = checkbox_callback
                                            , args = ("snowflake_table", f"cb_all_tables_{p_idx}_{s_idx}", "ms_tables_1", f"disable_tables_ms_{p_idx}_{s_idx}", p_idx, s_idx)
                                    )
                                    
                                    if cb_all_tables_1:
                                        ms_tables_1 = table_list

                                    
                                    for tbl in ms_tables_1:
                                        if {"Database": f"{st.session_state.snowflake_src_db}", "Schema": f"{sch}", "Table": f"{tbl}"} not in master_table_list:
                                            #master_table_list.append({"Database": f"{st.session_state.snowflake_src_db}", "Schema": f"{sch}", "Table": f"{tbl}"})
                                            master_table_list.append({"Source Database": f"{st.session_state.snowflake_src_db}", "Source Schema": f"{sch}", "Table": f"{tbl}", "Target Database": "", "Target Schema": ""})
                                            
                        if s_idx == 2:
                            ms_tables_2 = []
                            cb_all_tables_2_flag = True
                            
                            with col2:
                                with st.container(border=True):
                                    st.markdown(f"**Schema:** {sch}")
                                    
                                    if not tables.empty:
                                        table_list = tables["name"].values.tolist()
                                        cb_all_tables_2_flag = False
                                        
                                    ms_tables_2 = st.multiselect("Select Table(s):"
                                            , table_list
                                            , default = st.session_state[f"ms_snowflake_tables_{p_idx}_{s_idx}_list"] 
                                            , key = f"ms_snowflake_tables_{p_idx}_{s_idx}"
                                            , disabled = st.session_state[f"disable_tables_ms_{p_idx}_{s_idx}"]
                                            , on_change= multiselect_callback
                                            , args = ("snowflake_table", f"ms_snowflake_tables_{p_idx}_{s_idx}",p_idx, s_idx )
                                        )
        
                                    cb_all_tables_2 = st.checkbox("All Tables"
                                            , value = st.session_state[f"cb_all_tables_{p_idx}_{s_idx}_value"] 
                                            , key = f"cb_all_tables_{p_idx}_{s_idx}"
                                            , disabled = cb_all_tables_2_flag
                                            , on_change = checkbox_callback
                                            , args = ("snowflake_table", f"cb_all_tables_{p_idx}_{s_idx}", "ms_tables_2", f"disable_tables_ms_{p_idx}_{s_idx}", p_idx, s_idx)
                                    )

                                    if cb_all_tables_2:
                                        ms_tables_2 = table_list

                                    for tbl in ms_tables_2:
                                        if {"Database": f"{st.session_state.snowflake_src_db}", "Schema": f"{sch}", "Table": f"{tbl}"} not in master_table_list:
                                            master_table_list.append({"Source Database": f"{st.session_state.snowflake_src_db}", "Source Schema": f"{sch}", "Table": f"{tbl}", "Target Database": "", "Target Schema": ""})
                                            
            if master_table_list:
                cb_target_db_sch = st.checkbox("Choose Target Database/Schema"
                                                , value = st.session_state.snowflake_target_db_sch_value 
                                                , key = f"snowflake_target_db_sch"
                                                , on_change = checkbox_callback
                                                , args = ("snowflake_table", f"snowflake_target_db_sch", "", "", None, None)
                                            )
                
                #choose target db and sch once tables are selected
                if cb_target_db_sch:
                    st.write("")
                    st.write("")
                    st.markdown("<h6 style='text-align: left; color: black;'>Please choose the Target Database and Schema</h6>", unsafe_allow_html=True)
                    dbs = pd.DataFrame(session.sql("SHOW DATABASES").collect())
                    schemas = None
                    
                    if not dbs.empty:
                        st.session_state.snowflake_target_db = st.selectbox("Select Target Database:"
                                                                            , dbs["name"]
                                                                            , index = st.session_state.snowflake_target_db_idx
                                                                            , key = "sb_snowflake_target_db"
                                                                            , on_change = selectbox_callback                            
                                                                            , args = ("snowflake_table", "sb_snowflake_target_db", "snowflake_target_db_idx", dbs["name"].values.tolist())
                                                                            )
                        schemas = pd.DataFrame(session.sql(f"""SHOW SCHEMAS IN DATABASE {st.session_state.sb_snowflake_target_db}  WITH PRIVILEGES OWNERSHIP, USAGE""").collect())
                        
                    if not schemas.empty:
                        st.session_state.snowflake_target_sch = st.selectbox("Select Target Schema:"
                                                                            , schemas["name"].values.tolist()
                                                                            , index = st.session_state.snowflake_target_sch_idx
                                                                            , key = "sb_snowflake_target_sch"
                                                                            , on_change= selectbox_callback
                                                                            , args = ("snowflake_table", "sb_snowflake_target_sch", "snowflake_target_sch_idx", schemas["name"].values.tolist())
                                                                            )
                    st.write("")
                    st.write("")
                    
                    #update target db and sch
                    for t in master_table_list:
                        t.update((k, f"{st.session_state.snowflake_target_db}") for k, v in t.items() if k == "Target Database")
                        t.update((k, f"{st.session_state.snowflake_target_sch}") for k, v in t.items() if k == "Target Schema")
                

        st.divider()        
        st.markdown("<h4 style='text-align: left; color: black;'>Selected Tables</h4>", unsafe_allow_html=True)

        st.session_state.master_table_list = master_table_list

        if st.session_state.master_table_list:
            df_master_table_list = pd.DataFrame(st.session_state.master_table_list)
            u.paginate_data(df_master_table_list)
            st.session_state.disable_step_2 = False
        else:
            st.session_state.disable_step_2 = True

    ###### Step 2: Confirm Settings ######            
    if st.session_state.current_step == 2:
        #update SNOWFLAKE_TOOL_CONFIG with selected EV and CI from Step 1
        session.sql(f"""UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET
                            tool_value = '{st.session_state.snowflake_ev_name}'
                        WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = 'external_volume'""").collect()
        
        st.subheader("**STEP 2: Verify/Update Settings**")
        st.write("Verify the settings below. Update as needed.")

        update_config_flag = False
        config_updates = {}
    
        df_configuration_settings = pd.DataFrame(session.sql(f"""SELECT
                                                                    tool_parameter
                                                                    ,tool_value
                                                                FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE LOWER(tool_name) = 'iceberg_migrator'""").collect())
    
        for index, row in df_configuration_settings.iterrows():
            parameter = str(row["TOOL_PARAMETER"])
            value = row["TOOL_VALUE"]
    
            if value == "":
                prefix = "<No Value Defined>"  
            else:
                prefix = "Current: "
    
            col1, col2 = st.columns(2, gap="small")
                        
            with col1:
                st.text_input("Parameter:", parameter, key=f"parameter_{index}", disabled=True)
            with col2:
                updated_value = value
                st.text_input("Value:", key=f"value_{index}", placeholder=f"{prefix}{value}")
                
                if st.session_state[f"value_{index}"] != "":
                    updated_value = st.session_state[f"value_{index}"]
                    
                config_updates.update({parameter:updated_value})
    
        st.write("")
        st.write("")
    
        #update button
        col1, col2, col3 = st.columns([3.5,3.25,1])
    
        with col3:
            st.write("")
            st.write("")
            btn_update_config = st.button("Update", type="primary", key=f"btn_update_config")
    
            if btn_update_config:
                update_config_flag = True
    
        if update_config_flag:
            with st.spinner("Updating..."):
                for key, value in config_updates.items():
                    session.sql(f"UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET tool_value = '{value}' WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = ('{key}');").collect()
            
            st.success(f"Settings updated successfully 🎉")

    ###### Step 3: Confirm Settings ######            
    if st.session_state.current_step == 3:
        convert_tables_flag = False
        show_log_flag = False
        
        st.subheader("**STEP 3: Convert Tables**")
        st.write("Confirm the selected tables and migration settings.")
        st.write("")
        st.markdown("<h4 style='text-align: left; color: black;'>Selected Tables</h4>", unsafe_allow_html=True)
        st.write("")

        if st.session_state.master_table_list:
            df_master_table_list = pd.DataFrame(st.session_state.master_table_list)
            u.paginate_data(df_master_table_list)
            
        st.write("")
        st.markdown("<h4 style='text-align: left; color: black;'>Migration Settings</h4>", unsafe_allow_html=True)
        st.write("")

        df_configuration_settings = pd.DataFrame(session.sql(f"""SELECT
                                                                    tool_parameter
                                                                    ,tool_value
                                                                FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE LOWER(tool_name) = 'iceberg_migrator'""").collect())

        st.markdown(df_configuration_settings.style.format(precision=1, subset=list(df_configuration_settings.columns)).set_properties(**{'background-color': '#D3D3D3', 'font-weight': 'bold'}, subset=['TOOL_PARAMETER']).hide(axis = 0).hide(axis = 1).to_html(), unsafe_allow_html = True)
        st.write("#")
    
        #update button
        col1, col2, col3 = st.columns([3.5,3.25,1.05])
    
        with col3:
            btn_convert_tables = st.button("Convert", type="primary", key=f"btn_convert_tables")
    
            if btn_convert_tables:
                convert_tables_flag = True
    
        if convert_tables_flag:            
            with st.spinner("Running..."):
                ins_stmt = 'INSERT INTO ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.MIGRATION_TABLE(table_type, table_catalog, table_schema, table_name, target_table_catalog, target_table_schema) VALUES\n'
                
                #set target database/schema to null if empty/null
                target_db = f"'{st.session_state.snowflake_target_db}'" if st.session_state.snowflake_target_db else 'NULL'
                target_sch = f"'{st.session_state.snowflake_target_sch}'" if st.session_state.snowflake_target_sch else 'NULL'
                
                for index, row in enumerate(st.session_state.master_table_list):
                    if index == 0:
                        ins_stmt += f"('snowflake', '{row['Source Database']}', '{row['Source Schema']}', '{row['Table']}', {target_db}, {target_sch})\n"
                    else:
                        ins_stmt += f",('snowflake', '{row['Source Database']}', '{row['Source Schema']}', '{row['Table']}', {target_db}, {target_sch})\n"

                ins_stmt = ins_stmt.rstrip("\n")+";"
                
                #insert tables into migration table
                session.sql(ins_stmt).collect()

                #call ICEBERG_MIGRATION_DISPATCHER proc
                session.sql(f"CALL ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.ICEBERG_MIGRATION_DISPATCHER()").collect()

            st.success(f"Iceberg migration has started. Check the **Migration Log** page of the app for updates.")
        

    ###### Bottom Navigation ###### 
    st.divider()
    disable_back_button = True if st.session_state.current_step == 1 else False
    disable_next_button = True if st.session_state.current_step == 3 or (st.session_state.current_step == 1 and st.session_state.disable_step_2) or (st.session_state.current_step == 2 and st.session_state.disable_step_3) else False

    form_footer_cols = st.columns([14,1.875,1.875])

    form_footer_cols[0].button("Home", key="footer_home", type="secondary", on_click=set_page, args=["home"])
    form_footer_cols[1].button("Back", key="footer_back", type="secondary", on_click=set_form_step, args=["Back"], disabled=disable_back_button)
    form_footer_cols[2].button("Next", key="footer_next", type="primary", on_click=set_form_step, args=["Next"], disabled=disable_next_button)


def render_choose_delta_tables_wizard_view():
    if "current_step" not in st.session_state:
        st.session_state.current_step = 1
        
    if "disable_step_2" not in st.session_state:
        st.session_state.disable_step_2 = True
    
    if "disable_step_3" not in st.session_state:
        st.session_state.disable_step_3 = True
        
    if "delta_ev_check" not in st.session_state:
        st.session_state.delta_ev_check = False
        
    if "delta_evs" not in st.session_state:
        st.session_state.delta_evs = pd.DataFrame()
        
    if "delta_ev_idx" not in st.session_state:
        st.session_state.delta_ev_idx = 0

    if "delta_ev_name" not in st.session_state:
        st.session_state.delta_ev_name = ""

    if "delta_ci_check" not in st.session_state:
        st.session_state.delta_ci_check = False
      
    if "delta_c_ints" not in st.session_state:
        st.session_state.delta_c_ints = pd.DataFrame()
        
    if "delta_ci_idx" not in st.session_state:
        st.session_state.delta_ci_idx = 0
    
    if "delta_ci_name" not in st.session_state:
        st.session_state.delta_ci_name = ""

    if "stages_created" not in st.session_state:
        st.session_state.stages_created = []

    if "ev_location" not in st.session_state:
        st.session_state.ev_location = ""
        
    if "cb_all_delta_tables_value" not in st.session_state:
        st.session_state.cb_all_delta_tables_value = False

    if "master_delta_table_list" not in st.session_state:
        st.session_state.master_delta_table_list = []

    if "selected_delta_tables_list" not in st.session_state:
        st.session_state.selected_delta_tables_list = []
        
    if "df_show_delta_tables" not in st.session_state:
        st.session_state.df_show_delta_tables = pd.DataFrame()

    if "disable_ms_delta_tables" not in st.session_state:
        st.session_state.disable_ms_delta_tables = False
        
    if "delta_db_idx" not in st.session_state:
        st.session_state.delta_db_idx = 0
        
    if "delta_target_db" not in st.session_state:
        st.session_state.delta_target_db = ""
        
    if "delta_sch_idx" not in st.session_state:
        st.session_state.delta_sch_idx = 0
        
    if "delta_target_sch" not in st.session_state:
        st.session_state.delta_target_sch = ""

    st.markdown("<h2 style='text-align: center; color: black;'>Migrate Delta Tables</h2>", unsafe_allow_html=True)    

    ###### Top Navigation ######
    btn_ev_type = "primary" if st.session_state.current_step == 1 else "secondary"
    btn_tbl_type = "primary" if st.session_state.current_step == 2 else "secondary"
    btn_ib_tbl_type = "primary" if st.session_state.current_step == 3 else "secondary"

    if st.session_state.current_step == 2:
        st.session_state.disable_step_3 = False

    st.write("")
    st.write("")  
    step_cols = st.columns([0.65, .55, .55, .55, 0.5])
    step_cols[1].button("STEP 1", key="nav_step1", on_click=set_form_step, args=["Jump", 1], type=btn_ev_type, disabled=False)
    step_cols[2].button("STEP 2", key="nav_step2", on_click=set_form_step, args=["Jump", 2], type=btn_tbl_type, disabled=st.session_state.disable_step_2)        
    step_cols[3].button("STEP 3", key="nav_step3", on_click=set_form_step, args=["Jump", 3], type=btn_ib_tbl_type, disabled=st.session_state.disable_step_3)
    st.write("")
    st.write("")                       
    
    ###### Step 1: Select Tables ######
    if st.session_state.current_step == 1:                     
        st.subheader("**STEP 1: Select Tables**")
        st.write("")
        
        #select EV
        st.markdown("<h6 style='text-align: left; color: black;'>Please choose the External Volume</h6>", unsafe_allow_html=True)
        st.caption(":red[⚠︎ NOTE:  If an External Volume does not exist, use the app's **CREATE EXTERNAL VOLUME** tool to create one.]")
        st.write("")
        
        with st.spinner("Fetching External Volumes..."):
            if not st.session_state.delta_ev_check:
                #call run_sis_cmd to get external volumes --SiS cannot execute this show command
                st.session_state.delta_evs = run_sis_cmd("SHOW EXTERNAL VOLUMES", True)
                st.session_state.delta_ev_check = True
                    
            select_ev_list = []
            
            if not st.session_state.delta_evs.empty :
                select_ev_list = ["Choose..."] + st.session_state.delta_evs["name"].values.tolist()
            else:
                select_ev_list = ["Choose..."]
        
            st.session_state.delta_ev_name = st.selectbox("Select External Volume:"
                                                            , select_ev_list
                                                            , index = st.session_state.delta_ev_idx
                                                            , key = "sb_delta_ev"
                                                            , on_change = selectbox_callback
                                                            , args = ("delta_table", "sb_delta_ev", "delta_ev_idx", select_ev_list)
                                                            )
        st.write("")
        st.write("")
        
        #select CI
        st.markdown("<h6 style='text-align: left; color: black;'>Please choose the Catalog Integration</h6>", unsafe_allow_html=True)
        st.caption(":red[⚠︎ NOTE:  If a Catalog Integration does not exist, use the app's **CREATE CATALOG INTEGRATION** tool to create one.]")
        st.write("")
        
        with st.spinner("Fetching Catalog Integrations..."):
            if not st.session_state.delta_ci_check:
                #call run_sis_cmd to get catalog integration --SiS cannot execute this show command
                st.session_state.delta_c_ints = run_sis_cmd("SHOW CATALOG INTEGRATIONS", True)
                st.session_state.delta_ci_check = True
                    
            select_ci_list = []
            
            if not st.session_state.delta_c_ints.empty :
                select_ci_list = ["Choose..."] + st.session_state.delta_c_ints["name"].values.tolist()
            else:
                select_ci_list = ["Choose..."]
        
            st.session_state.delta_ci_name = st.selectbox("Select Catalog Integration:"
                                                            , select_ci_list
                                                            , index = st.session_state.delta_ci_idx
                                                            , key = "sb_delta_ci"
                                                            , on_change = selectbox_callback                            
                                                            , args = ("delta_table", "sb_delta_ci", "delta_ci_idx", select_ci_list)
                                                            )
        st.write("")
        st.write("")
        
        #create SI and stage to get table list
        if st.session_state.sb_delta_ev != "Choose" and st.session_state.sb_delta_ci != "Choose...":
            #get cloud
            cloud = ""
            storage_base_url = ""
            credential_str = ""
            
            master_delta_table_list = []
            df_all_delta_tables = pd.DataFrame()
            
            with st.spinner("Fetching Delta Tables..."):
                #describe external volume to get necessary details
                if st.session_state.sb_delta_ev not in st.session_state.stages_created:
                    desc_ev = run_sis_cmd(f"DESCRIBE EXTERNAL VOLUME {st.session_state.sb_delta_ev}", True)
                    
                    if not desc_ev.empty:
                        property_value = pd.DataFrame(session.sql(f"""SELECT "property_value" 
                                                                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) 
                                                                WHERE LOWER("property") = 'storage_location_1'""").collect()).iloc[0,0]
            
                        if property_value:
                            property_value_json = json.loads(str(property_value))
                            cloud = property_value_json["STORAGE_PROVIDER"]
                            storage_base_url = property_value_json["STORAGE_BASE_URL"]
            
                            if cloud.lower() == "s3":
                                storage_aws_role_arn = property_value_json["STORAGE_AWS_ROLE_ARN"];
                                credential_str = f"STORAGE_AWS_ROLE_ARN = '{storage_aws_role_arn}'"
            
                            if cloud.lower() == "azure":
                                azure_tenant_id = property_value_json["AZURE_TENANT_ID"];
                                credential_str = f"AZURE_TENANT_ID = '{azure_tenant_id}'"
                        
                        #create storage location, if it doesn't exist
                        session.sql(f"""CREATE STORAGE INTEGRATION IF NOT EXISTS SI_{st.session_state.sb_delta_ev}
                                                        TYPE = EXTERNAL_STAGE
                                                        STORAGE_PROVIDER = '{cloud}'
                                                        ENABLED = TRUE
                                                        {credential_str}
                                                        STORAGE_ALLOWED_LOCATIONS = ('{storage_base_url}')""").collect()
                                
                        #create stage
                        session.sql(f"""CREATE STAGE IF NOT EXISTS ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_{st.session_state.sb_delta_ev}
                                                        URL = '{storage_base_url}'
                                                        STORAGE_INTEGRATION = SI_{st.session_state.sb_delta_ev}
                                                        DIRECTORY = (
                                                            ENABLE = true
                                                        );""").collect()
                
                        #add forward slash if the location does not have it:
                        st.session_state.ev_location = storage_base_url;
                
                        if not st.session_state.ev_location.endswith("/"):
                            st.session_state.ev_location = f"{storage_base_url}/"
                            
                        if st.session_state.sb_delta_ev not in st.session_state.stages_created:
                            st.session_state.stages_created.append(st.session_state.sb_delta_ev)

                
                session.sql(f"""LIST @ICEBERG_MIGRATOR_DB.ICEBERG_STAGING.STAGE_{st.session_state.sb_delta_ev}""").collect()
                df_all_delta_tables = pd.DataFrame(session.sql(f"""SELECT DISTINCT '{st.session_state.ev_location}' AS location, REGEXP_SUBSTR("name", '{st.session_state.ev_location}([A-Za-z0-9_-]*)/', 1,1,'e') AS tbls
                                                FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE tbls IS NOT NULL""").collect())                        
    
            if not df_all_delta_tables.empty:
                ms_delta_tables = st.multiselect("Select Table(s):"
                                  , df_all_delta_tables["TBLS"].values.tolist()
                                  , default = st.session_state.selected_delta_tables_list
                                  , key = "ms_delta_tables"
                                  , disabled = st.session_state.disable_ms_delta_tables
                                  , on_change= multiselect_callback
                                  , args = ("delta_table", f"ms_delta_tables", None, None)
                                  )

                cb_all_delta_tables = st.checkbox("All Tables"
                                            , value = st.session_state.cb_all_delta_tables_value
                                            , key = "cb_all_delta_tables"
                                            , on_change = checkbox_callback
                                            , args = ("delta_table", "cb_all_delta_tables", "ms_delta_tables", "disable_ms_delta_tables", None, None)
                                )

                if cb_all_delta_tables:
                    st.session_state.master_delta_table_list = [{"Location": f"{st.session_state.ev_location}", "Table": ["*"]}]
                    st.session_state.df_show_delta_tables = df_all_delta_tables
                else:
                    for tbl in ms_delta_tables:
                        if {"Location": f"{st.session_state.ev_location}", "Table": f"{tbl}"} not in master_delta_table_list:
                            master_delta_table_list.append({"Location": f"{st.session_state.ev_location}", "Table": f"{tbl}"})
                        
                    st.session_state.master_delta_table_list = master_delta_table_list
                    st.session_state.df_show_delta_tables = pd.DataFrame(st.session_state.master_delta_table_list)
                    
            #choose target db and sch once tables are selected
            if st.session_state.master_delta_table_list:
                st.write("")
                st.write("")
                st.markdown("<h6 style='text-align: left; color: black;'>Please choose the Target Database and Schema</h6>", unsafe_allow_html=True)
                dbs = pd.DataFrame(session.sql("SHOW DATABASES").collect())
                schemas = None
                
                if not dbs.empty:
                    st.session_state.delta_target_db = st.selectbox("Select Target Database:"
                                                                        , dbs["name"]
                                                                        , index = st.session_state.delta_db_idx
                                                                        , key = "sb_delta_target_db"
                                                                        , on_change = selectbox_callback                            
                                                                        , args = ("delta_table", "sb_delta_target_db", "delta_db_idx", dbs["name"].values.tolist())
                                                                        )
                    schemas = pd.DataFrame(session.sql(f"""SHOW SCHEMAS IN DATABASE {st.session_state.sb_delta_target_db}  WITH PRIVILEGES OWNERSHIP, USAGE""").collect())
                    
                if not schemas.empty:
                    st.session_state.delta_target_sch = st.selectbox("Select Target Schema:"
                                                                        , schemas["name"].values.tolist()
                                                                        , index = st.session_state.delta_sch_idx
                                                                        , key = "sb_delta_target_sch"
                                                                        , on_change= selectbox_callback
                                                                        , args = ("delta_table", "sb_delta_target_sch", "delta_sch_idx", schemas["name"].values.tolist())
                                                                        )
                st.write("")
                st.write("")
    
            st.divider()        
            st.markdown("<h4 style='text-align: left; color: black;'>Selected Delta Tables</h4>", unsafe_allow_html=True)
    
            if st.session_state.master_delta_table_list:
                u.paginate_data(st.session_state.df_show_delta_tables)
                st.session_state.disable_step_2 = False
            else:
                st.session_state.disable_step_2 = True

    

    ###### Step 2: Confirm Settings ######            
    if st.session_state.current_step == 2:
        #update SNOWFLAKE_TOOL_CONFIG with selected EV and CI from Step 1
        session.sql(f"""UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET
                            tool_value = '{st.session_state.delta_ev_name}'
                        WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = 'external_volume'""").collect()
        
        session.sql(f"""UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET
                            tool_value = '{st.session_state.delta_ci_name}'
                        WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = 'delta_catalog_integration'""").collect()
        
        st.subheader("**STEP 2: Verify/Update Settings**")
        st.write("Verify the settings below. Update as needed.")
        
        update_config_flag = False
        config_updates = {}
    
        df_configuration_settings = pd.DataFrame(session.sql(f"""SELECT
                                                                    tool_parameter
                                                                    ,tool_value
                                                                FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE LOWER(tool_name) = 'iceberg_migrator'""").collect())
    
        for index, row in df_configuration_settings.iterrows():
            parameter = str(row["TOOL_PARAMETER"])
            value = row["TOOL_VALUE"]
    
            if value == "":
                prefix = "<No Value Defined>"  
            else:
                prefix = "Current: "
    
            col1, col2 = st.columns(2, gap="small")
                        
            with col1:
                st.text_input("Parameter:", parameter, key=f"parameter_{index}", disabled=True)
            with col2:
                updated_value = value
                st.text_input("Value:", key=f"value_{index}", placeholder=f"{prefix}{value}")
                
                if st.session_state[f"value_{index}"] != "":
                    updated_value = st.session_state[f"value_{index}"]
                    
                config_updates.update({parameter:updated_value})
    
        st.write("")
        st.write("")
    
        #update button
        col1, col2, col3 = st.columns([3.5,3.25,1])
    
        with col3:
            st.write("")
            st.write("")
            btn_update_config = st.button("Update", type="primary", key=f"btn_update_config")
    
            if btn_update_config:
                update_config_flag = True
    
        if update_config_flag:
            with st.spinner("Updating..."):
                for key, value in config_updates.items():
                    session.sql(f"UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET tool_value = '{value}' WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = ('{key}');").collect()
            
            st.success(f"Settings updated successfully 🎉")

    ###### Step 3: Confirm Settings ######            
    if st.session_state.current_step == 3:
        convert_tables_flag = False
        show_log_flag = False
        
        st.subheader("**STEP 3: Convert Tables**")
        st.write("Confirm the selected tables and migration settings.")
        st.write("")
        st.markdown("<h4 style='text-align: left; color: black;'>Selected Tables</h4>", unsafe_allow_html=True)
        st.write("")

        if st.session_state.master_delta_table_list:
            df_master_delta_table_list = pd.DataFrame(st.session_state.df_show_delta_tables)
            u.paginate_data(df_master_delta_table_list)
            
        st.write("")
        st.markdown("<h4 style='text-align: left; color: black;'>Migration Settings</h4>", unsafe_allow_html=True)
        st.write("")

        df_configuration_settings = pd.DataFrame(session.sql(f"""SELECT
                                                                    tool_parameter
                                                                    ,tool_value
                                                                FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE LOWER(tool_name) = 'iceberg_migrator'""").collect())

        st.markdown(df_configuration_settings.style.format(precision=1, subset=list(df_configuration_settings.columns)).set_properties(**{'background-color': '#D3D3D3', 'font-weight': 'bold'}, subset=['TOOL_PARAMETER']).hide(axis = 0).hide(axis = 1).to_html(), unsafe_allow_html = True)
        st.write("#")
    
        #update button
        col1, col2, col3 = st.columns([3.5,3.25,1.05])
    
        with col3:
            btn_convert_tables = st.button("Convert", type="primary", key=f"btn_convert_tables")
    
            if btn_convert_tables:
                convert_tables_flag = True
    
        if convert_tables_flag:            
            with st.spinner("Running..."):
                
                #call ICEBERG_INSERT_DELTA_TABLES to add tables to MIGRATION_TABLE - USE sis_cmd_mgr since the proc executes commands that SiS cannot run
                ins_del_tbls = run_sis_cmd(f"""CALL ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.ICEBERG_INSERT_DELTA_TABLES('{st.session_state.delta_ev_name}', '{st.session_state.delta_target_db}', '{st.session_state.delta_target_sch}', {st.session_state.master_delta_table_list[0]["Table"]})""", True)
                    
                if not ins_del_tbls.empty:
                    ins_del_tbls_resp = json.loads(str(ins_del_tbls.iloc[0,0]))
                    
                    if ins_del_tbls_resp["result"] == True:
                        #call ICEBERG_MIGRATION_DISPATCHER proc
                        session.sql(f"CALL ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.ICEBERG_MIGRATION_DISPATCHER()").collect()

            st.success(f"Iceberg migration has started. Check the **Migration Log** page of the app for updates.")
        

    ###### Bottom Navigation ###### 
    st.divider()
    disable_back_button = True if st.session_state.current_step == 1 else False
    disable_next_button = True if st.session_state.current_step == 3 or (st.session_state.current_step == 1 and st.session_state.disable_step_2) or (st.session_state.current_step == 2 and st.session_state.disable_step_3) else False

    form_footer_cols = st.columns([14,1.875,1.875])

    form_footer_cols[0].button("Home", key="footer_home", type="secondary", on_click=set_page, args=["home"])
    form_footer_cols[1].button("Back", key="footer_back", type="secondary", on_click=set_form_step, args=["Back"], disabled=disable_back_button)
    form_footer_cols[2].button("Next", key="footer_next", type="primary", on_click=set_form_step, args=["Next"], disabled=disable_next_button)
    

def render_migration_log_view():
    st.markdown("<h2 style='text-align: center; color: black;'>Migration Log</h2>", unsafe_allow_html=True)
    st.write("")
    st.write("The table below provides details of each Iceberg migration run")
    st.write("")
    
    btn_check_log = st.button("Check Log", type="primary")
    st.write("")
    
    df_transcode_log = manual_migration_log_check()
    
    if btn_check_log:
        df_transcode_log = manual_migration_log_check()
    

    if not df_transcode_log.empty:
        st.markdown(df_transcode_log.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px'),('background-color','#D3D3D3')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 0).hide(axis = 0).applymap(highlight_log_status).to_html(), unsafe_allow_html = True)
        #u.paginate_data(df_transcode_log.style.set_table_styles([{'selector': 'th', 'props': [('font-size', '12px'),('background-color','#D3D3D3')]}]).set_properties(**{'color': '#000000','font-size': '12px','font-weight':'regular', 'width':'550px'}).hide(axis = 0).hide(axis = 0).applymap(highlight_log_status).to_html(), unsafe_allow_html = True)
    else:
        st.markdown("***No results available.***")
    st.write("#")

    #home button
    col1, col2, col3 = st.columns([3.5,3.5,0.975])

    with col1:
        st.write("")
        st.write("")
        st.button("Home", type="secondary", on_click=set_page, args=["home"])


def render_configuration_view():
    st.markdown("<h2 style='text-align: center; color: black;'>Configuration</h2>", unsafe_allow_html=True)
    st.write("")
    st.write("The form below shows the tool's current configuration settings. To update, edit any of the fields below and click *Update*.")
    st.write("#")

    update_config_flag = False
    config_updates = {}

    df_configuration_settings = pd.DataFrame(session.sql(f"""SELECT
                                                                tool_parameter
                                                                ,tool_value
                                                            FROM ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG WHERE LOWER(tool_name) = 'iceberg_migrator'""").collect())

    for index, row in df_configuration_settings.iterrows():
        parameter = str(row["TOOL_PARAMETER"])
        value = row["TOOL_VALUE"]

        if value == "":
            prefix = "<No Value Defined>"  
        else:
            prefix = "Current: "

        col1, col2 = st.columns(2, gap="small")
                    
        with col1:
            st.text_input("Parameter:", parameter, key=f"parameter_{index}", disabled=True)
        with col2:
            updated_value = value
            st.text_input("Value:", key=f"value_{index}", placeholder=f"{prefix}{value}")
            
            if st.session_state[f"value_{index}"] != "":
                updated_value = st.session_state[f"value_{index}"]
                
            config_updates.update({parameter:updated_value})


    st.write("#")

    #home button
    col1, col2, col3 = st.columns([3.5,3.25,1])

    with col1:
        st.write("")
        st.write("")
        st.button("Home", type="secondary", on_click=set_page, args=["home"])
    with col3:
        st.write("")
        st.write("")
        btn_update_config = st.button("Update", type="primary", key=f"btn_update_config")

        if btn_update_config:
            update_config_flag = True

    if update_config_flag:
        with st.spinner("Updating..."):
            for key, value in config_updates.items():
                session.sql(f"UPDATE ICEBERG_MIGRATOR_DB.ICEBERG_MIGRATOR.SNOWFLAKE_TOOL_CONFIG SET tool_value = '{value}' WHERE LOWER(tool_name) = 'iceberg_migrator' AND LOWER(tool_parameter) = ('{key}')").collect()
        
        st.success(f"Settings updated successfully 🎉")
        time.sleep(3)
        st.rerun()


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
        
        st.markdown("<h1 style='text-align: center; color: black;'>ICEBERG MIGRATOR</h1>", unsafe_allow_html=True)
        st.write("")
        st.write("The Iceberg Migrator tool allows a customer to perform bulk migrations of native Snowflake and Delta tables to Iceberg tables.")
        st.divider()


class home(BasePage):
    def __init__(self):
        self.name="home"
        
    def print_page(self):
        super().print_page()

        #clear session_state when at home screen
        clear_c2i_session_vars()
        
        #st.write("")        
        #col1, col2, col3 = st.columns([1,1,1])

        #with col2:
        #    st.write("")
        #    if st.button("Prerequisites", type="primary"):
        #        render_prereqs()

        col1, col2, col3, col4 = st.columns([1.0,0.85,0.85,1.25])

        with col2:
            st.write("")
            if st.button("Snowflake Prerequisites", type="primary"):
                render_sf_prereqs()
                
        with col3:
            st.write("")
            if st.button("Delta Prerequisites", type="primary"):
                render_delta_prereqs()

        st.divider()

        col1, col2 = st.columns(2, gap="small")
        with col1:
           st.markdown("<h3 style='text-align: center; color: black;'>External Volume</h3>", unsafe_allow_html=True)
           ev_col1, ev_col2, ev_col3 = st.columns([0.275,0.75,0.25], gap="small")
           with ev_col2:
               u.render_image_menu("img/ra_volume.png")
           st.markdown("""
                        Create a new External Volume to use for your Iceberg tables.
                        """)
           st.write("")

           ev_col1, ev_col2, ev_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with ev_col2:
            st.button("Create Volume", type="primary", on_click=set_page,args=("create_ev",), key="btn_create_ev")

        with col2:
           st.markdown("<h3 style='text-align: center; color: black;'>Catalog Integration</h3>", unsafe_allow_html=True)
           ci_col1, ci_col2, ci_col3 = st.columns([0.35,0.75,0.25], gap="small")
            
           with ci_col2:
               st.write("")
               u.render_image_menu("img/metadata.png")
           st.markdown("""
                        Create a new Catalog Integration for your Delta files in object storage.
                        """)
           st.write("")

           ci_col1, ci_col2, ctiol3 = st.columns([0.25,0.55,0.25], gap="small")
           with ci_col2:
                st.button("Create Integration", type="primary", on_click=set_page,args=("create_ci",), key="btn_create_ci") 

        st.write("")
        st.write("")
        st.write("")
        st.write("")
        
        col1, col2 = st.columns(2, gap="small")
        with col1:
           st.markdown("<h3 style='text-align: center; color: black;'>Snowflake Tables</h3>", unsafe_allow_html=True)
           cs_col1, cs_col2, cs_col3 = st.columns([0.30,0.75,0.25], gap="small")
            
           with cs_col2:
               st.write("")
               u.render_image_menu("img/ra_table.png")
           st.markdown("""
                        Choose existing FDN tables to migrate to Iceberg.
                        """)
           st.write("")

           cs_col1, cs_col2, cs_col3 = st.columns([0.2125,0.95,0.3], gap="small")
           with cs_col2:
                st.button("Choose Snowflake Tables", type="primary", on_click=set_page,args=("choose_snowflake_tables",), key="btn_choose_fdn")

        with col2:
           st.markdown("<h3 style='text-align: center; color: black;'>Delta Tables</h3>", unsafe_allow_html=True)
           cd_col1, cd_col2, cd_col3 = st.columns([0.3,0.75,0.25], gap="small")
            
           with cd_col2:
               st.write("")
               u.render_image_menu("img/delta_tables.png")
           st.markdown("""
                        Choose existing Delta table files from object storage to migrate to Iceberg.
                        """)
           st.write("")

           cd_col1, cd_col2, cd_col3 = st.columns([0.215,0.6,0.30], gap="small")
           with cd_col2:
                st.button("Choose Delta Tables", type="primary", on_click=set_page,args=("choose_delta_tables",), key="btn_choose_delta")

        st.write("")
        st.write("")
        st.write("")
        st.write("")
        
        col1, col2 = st.columns(2, gap="small")
        with col1:
           st.markdown("<h3 style='text-align: center; color: black;'>Migration Log</h3>", unsafe_allow_html=True)
           cq_col1, cq_col2, cq_col3 = st.columns([0.25,0.55,0.25], gap="small")
           with cq_col2:  
               u.render_image_menu("img/documentation.png")
           st.markdown("""
                        View the logs and status of each Iceberg migration run. 
                        """)
           st.write("")

           cq_col1, cq_col2, cq_col3 = st.columns([0.2,0.55,0.25], gap="small")
           with cq_col2: 
               st.button("View Migration Log", type="primary", on_click=set_page,args=("migration_log",), key="btn_migration_log") 
        
        with col2:
           st.markdown("<h3 style='text-align: center; color: black;'>Configuration</h3>", unsafe_allow_html=True)
           qc_col1, qc_col2, qc_col3 = st.columns([0.6,1.5,0.5], gap="small")
           with qc_col2:  
               u.render_image_menu("img/services.png")
           st.markdown("""
                        View and/or update the tool's configuration settings.
                        """)
           st.write("") 

           qc_col1, qc_col2, qc_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with qc_col2:
                st.button("View Configuration", type="primary", on_click=set_page,args=("configuration",), key="btn_configuration")
        



########################################################################### Create External Volume

class create_ev_page(BasePage):
    def __init__(self):
        self.name="create_ev"
        
    def print_page(self):
        super().print_page()

        #render create new External Volume wizard
        render_create_ev()

########################################################################### Create Catalog Integration

class create_ci_page(BasePage):
    def __init__(self):
        self.name="create_ci"
        
    def print_page(self):
        super().print_page()

        #render create new Catalog Integration page
        render_create_ci()

########################################################################### Choose Snowflake Tables

class choose_snowflake_tables_page(BasePage):
    def __init__(self):
        self.name="choose_snowflake_tables"
        
    def print_page(self):
        super().print_page()

        #render create Iceberg tables wizard
        render_choose_snowflake_tables_wizard_view()

########################################################################### Choose Delta Tables

class choose_delta_tables_page(BasePage):
    def __init__(self):
        self.name="choose_delta_tables"
        
    def print_page(self):
        super().print_page()

        #render create Iceberg tables wizard
        render_choose_delta_tables_wizard_view()

########################################################################### Migration Log

class migration_log(BasePage):
    def __init__(self):
        self.name="migration_log"
        
    def print_page(self):
        super().print_page()

        #render Choose Query wizard
        render_migration_log_view()

########################################################################### Configuration

class query_configuration_page(BasePage):
    def __init__(self):
        self.name="configuration"
        
    def print_page(self):
        super().print_page()

        #render Configuration view
        render_configuration_view()

############################################################################## Main ####################################################################################################

pages = [home(), create_ev_page(), create_ci_page(), choose_snowflake_tables_page(), choose_delta_tables_page(), migration_log(), query_configuration_page()]

session = get_active_session()

def main():
    for page in pages:
        if page.name == st.session_state.page:
            if page.name in ["migration_log"]:
                st.session_state.layout="wide"
            else:
                st.session_state.layout="centered"
            
            page.print_page();

main()