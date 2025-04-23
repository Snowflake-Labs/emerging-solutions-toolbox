# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.connector.pandas_tools import pd_writer
import snowflake.snowpark.functions as F
import base64
from abc import ABC, abstractmethod
import random
import json
import re
import time
import snowflake.permissions as permissions


if "layout" in st.session_state:
	st.set_page_config(layout=st.session_state.layout)

if "layout" not in st.session_state:
    st.session_state.layout="centered"

def render_image(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=585)

def render_image_summary(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=125)

def render_image_menu(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string, width=150)


def render_image_true_size(image: str):
    image_name = f'{image}'
    mime_type = image_name.split(".")[-1:][0].lower()        
    with open(image_name, "rb") as f:
        content_bytes = f.read()
    content_b64encoded = base64.b64encode(content_bytes).decode()
    image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
    st.image(image_string)

def set_page(page: str):
    st.session_state.page = page

def request(app_name, app_mode, ref_name, input_ref):
    start_request = False
    require_input_table = ""
    input_table = ""
    limit = ""
    limit_type = ""
    limit_interval = ""
    limit_reset_timestamp = ""
    limit_check_logic = ""
    total_requests = ""
    limit_msg = ""
    limit_interval_msg = ""
    limit_reset_msg = ""

    input_record_count = int(pd.DataFrame(session.sql(f"SELECT COUNT(*) FROM reference('{ref_name}','{input_ref}')").collect()).iloc[0,0])
    require_input_table = pd.DataFrame(session.sql(f"SELECT require_input_table FROM UTIL_APP.ALLOWED_PROCS_V WHERE LOWER(proc_name) = LOWER('enrich')").collect()).iloc[0,0]
            
    #get limit details
    limit = int(pd.DataFrame(session.sql(f"select value FROM {app_name}.UTIL_APP.METADATA_C_V where LOWER(key) = 'limit'").collect()).iloc[0,0])
    limit_type = pd.DataFrame(session.sql(f"select value FROM {app_name}.UTIL_APP.METADATA_C_V where LOWER(key) = 'limit_type'").collect()).iloc[0,0]
    limit_interval = pd.DataFrame(session.sql(f"select value FROM {app_name}.UTIL_APP.METADATA_C_V where LOWER(key) = 'limit_interval'").collect()).iloc[0,0]
    limit_enforced = pd.DataFrame(session.sql(f"select value FROM {app_name}.UTIL_APP.METADATA_C_V where LOWER(key) = 'limit_enforced'").collect()).iloc[0,0]
    limit_reset_timestamp = pd.DataFrame(session.sql(f"select value FROM {app_name}.UTIL_APP.METADATA_C_V where LOWER(key) = 'limit_reset_timestamp'").collect()).iloc[0,0]
    
    total_requests = int(pd.DataFrame(session.sql(f"select value FROM {app_name}.APP.LIMIT_TRACKER where LOWER(key) = 'total_requests'").collect()).iloc[0,0])
    current_record_count = int(pd.DataFrame(session.sql(f"SELECT value FROM {app_name}.APP.LIMIT_TRACKER WHERE LOWER(key) = 'records_processed_this_interval'").collect()).iloc[0,0])

    if limit_interval.lower() == "n/a":
        limit_interval_msg = ""
        limit_reset_msg = ""
    else:
        limit_interval_msg = f" for the {limit_interval} interval"
        limit_reset_msg = f"\nYour limits will reset on {limit_reset_timestamp}."
    
    limit_msg = f"You've reached your {app_mode.upper()} limit of {limit} {limit_type.split('_')[0]}{limit_interval_msg}. If you wish to run additional queries, please contact us to discuss our Paid or custom Enterprise solutions.{limit_reset_msg}"

    #get limit check logic
    if limit_type.lower() == 'requests':
        limit_check_logic = ((total_requests + 1) <= limit)
    
    if limit_type.lower() == 'records':
        limit_check_logic = ((current_record_count + input_record_count) <= limit)

    cols = pd.DataFrame(session.sql(f"show columns in table reference('{ref_name}','{input_ref}')").collect())
    
    if not cols.empty:
        match_key = st.selectbox("Select Match Key:", cols["column_name"])
        results_table = st.text_input("Please enter a name for the Results Table", key="txt_results_table_1")

        if results_table == "":
            btn_request = st.button('Run', key='run_1', type="primary", disabled = True)
        else:
            btn_request = st.button('Run', key='run_2', type="primary")
        
        if btn_request:
            if limit_enforced.lower() == 'n' or require_input_table.lower() == 'n'  or (limit_check_logic and input_record_count > 0):
                start_request = True
                session.sql(f"CREATE OR REPLACE VIEW {app_name}.UTIL_APP.CONSUMER_VIEW AS SELECT * FROM reference('{ref_name}','{input_ref}')").collect()
                input_table = f"{app_name}.UTIL_APP.CONSUMER_VIEW"
            else:
                st.warning(limit_msg)
                st.button('Contact Us to Upgrade', key='contact_4', type="primary", on_click=set_page,args=("contact",)) 

        if start_request:
            with st.spinner("Updating..."):
                call_request = session.sql(f"CALL {app_name}.PROCS_APP.REQUEST(OBJECT_CONSTRUCT('input_table','{input_table}', 'results_table', '{app_name}.RESULTS_APP.{results_table}', 'proc_name','enrich', 'proc_parameters',ARRAY_CONSTRUCT_COMPACT('{input_table}','{match_key}','{app_name}.RESULTS_APP.{results_table}'))::varchar)").collect()
                result = pd.DataFrame(call_request).iloc[0]['REQUEST']
                
                if 'ERROR:' in result:
                    st.error("**Call Failed**", icon="🚨")
                    st.write(result)
                else :
                    st.success(f"Enrichment complete 🎉. Results are in table: {app_name}.RESULTS_APP.{results_table}")
                    time.sleep(3)
                    st.rerun()


def save_form(app_name):    
    #Check if any inputs are missing.
    # List of columns to check
    columns_to_check = ['first_name', 'last_name', 'title', 'business_email', 'industry', 'contact_reason']
    empty_fields = [column for column in columns_to_check if not st.session_state[column]]

    if empty_fields:
        st.error(f"Please fill in the following required fields: {', '.join(empty_fields)}", icon="🚨")
    else:
        # Additional checks for 'Other' and 'contact_reason_text'
        if st.session_state.contact_reason == 'Other' and st.session_state.contact_reason_text == '':
            st.error("Please input Contact Reason Text", icon="🚨")
        else:
            # Clear 'contact_reason_text' if 'contact_reason' is not 'Other'
            if st.session_state.contact_reason != 'Other':
                st.session_state.contact_reason_text = ''
            # Call the stored procedure
            session.call(f"{app_name}.UTIL_APP.LOG_FORM",
                                st.session_state.first_name,
                                st.session_state.last_name,
                                st.session_state.title,
                                st.session_state.business_email,
                                st.session_state.industry,
                                st.session_state.contact_reason,
                                st.session_state.contact_reason_text)

            st.session_state.submitted = True 
    
    if st.session_state.submitted == True:
        st.success('Thank you for your inquiry.  You will be contacted shortly.')
        set_page("home")

class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass
    
    @abstractmethod
    def print_page(self):
        pass

    @abstractmethod
    def print_sidebar(self):
        pass

class BasePage(Page):
    def __init__(self):
        pass
    
    def print_page(self):
        render_image("snowflake-logo-color-rgb@2x.png")
        
        st.title(f"ACME Data Enrichment")
        st.write(st.__version__)

    def print_sidebar(self):
        st.write("ACME Data Enrichment")

class home(BasePage):
    def __init__(self):
        self.name="home"
        
    def print_page(self):
        super().print_page()

        st.session_state.layout="centered"

        app_mode = ''
        app_name = ''
        account_locator = ''
        consumer_name = ''
        app_key_metadata = ''
        app_key_local = ''
        
        app_name = pd.DataFrame(session.sql("SELECT CURRENT_DATABASE()").collect()).iloc[0,0]
        df_metadata_c_v = pd.DataFrame(session.sql(f"SELECT account_locator FROM {app_name}.UTIL_APP.METADATA_C_V").collect())
        
        if not df_metadata_c_v.empty:
            try :
                app_mode = pd.DataFrame(session.sql("SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'app_mode'").collect()).iloc[0,0]
                account_locator = pd.DataFrame(session.sql(f"SELECT account_locator FROM {app_name}.UTIL_APP.METADATA_C_V LIMIT 1").collect()).iloc[0,0]
                consumer_name =  pd.DataFrame(session.sql(f"SELECT consumer_name FROM {app_name}.UTIL_APP.METADATA_C_V LIMIT 1;").collect()).iloc[0,0]
                app_key_metadata= pd.DataFrame(session.sql(f"SELECT LOWER(value) FROM {app_name}.METADATA.METADATA_V WHERE LOWER(key) = 'app_key' AND LOWER(account_locator) = LOWER('{account_locator}') AND LOWER(consumer_name) = LOWER('{consumer_name}')").collect()).iloc[0,0]   
                app_key_local= pd.DataFrame(session.sql(f"SELECT LOWER(app_key) FROM {app_name}.APP.APP_KEY").collect()).iloc[0,0] 
            except :
                st.rerun()

            enable_check  = pd.DataFrame(session.sql(f"SELECT value FROM {app_name}.UTIL_APP.METADATA_C_V WHERE LOWER(key) = 'enabled'").collect()).iloc[0,0]
            trust_center_enforcement = pd.DataFrame(session.sql(f"SELECT value FROM {app_name}.METADATA.METADATA_V WHERE LOWER(key) = 'trust_center_enforcement'").collect()).iloc[0,0]
            events_shared = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'events_shared'").collect()).iloc[0,0]
            tracker_configured = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'tracker_configured'").collect()).iloc[0,0]
            trust_center_access = pd.DataFrame(session.sql(f"SELECT value FROM APP.APP_MODE WHERE LOWER(key) = 'trust_center_access'").collect()).iloc[0,0]
            run_count = pd.DataFrame(session.sql(f"SELECT COUNT(*) FROM APP.RUN_TRACKER").collect()).iloc[0,0]
            

            if events_shared.lower() == 'n' or tracker_configured.lower() == 'n':
                st.error('Please complete installation by running the setup script found in the ⓘ icon in the top right of the page.', icon="🚨")
            elif trust_center_enforcement.lower() == 'y' and trust_center_access.lower() == 'n':
                st.error('Trust Center Access has not been granted. Please ensure the appropriate grants were executed in the setup script found in the ⓘ icon in the top right of the page.', icon="🚨")
            elif (app_key_metadata == '') or (app_key_metadata != app_key_local):
                st.warning("Please wait a moment while we finish onboarding your account. This automated process may take a few minutes. If you continue to have issues, please contact us immediately.")
                st.button('Contact Us', key='contact_1', type="primary", on_click=set_page,args=("contact",))
            elif len(enable_check) > 0:
                if enable_check.lower()  == 'y':
                    if "reference_tables" not in st.session_state:
                        st.session_state.reference_tables = {}

                    if "reference_views" not in st.session_state:    
                        st.session_state.reference_views = {}

                    if "pickedObj" not in st.session_state:
                        st.session_state.pickedObj = ''

                    if "ref" not in st.session_state:
                        st.session_state.ref = ''

                    if "refs_selected" not in st.session_state:
                        st.session_state.refs_selected = {}

                    if "btn_clicked" not in st.session_state:
                        st.session_state.btn_clicked = ''
                    
                    st.header(f"App Version: {app_mode.upper()}")
                    st.write("#")
                    st.subheader("Please Select a Table or View to Enrich.")

                    reference_association_tbl = permissions.get_reference_associations("enrichment_table")
                    reference_association_vw = permissions.get_reference_associations("enrichment_view")

                    if len(reference_association_tbl) == 0 and len(reference_association_vw) == 0 :   
                        st.warning("""Please Select a Table or View""")
                    else:
                        if len(reference_association_tbl) > 0:   
                            ref_tables = {}
                            for table in reference_association_tbl:
                                sqlstring = f"""show columns in table reference('enrichment_table','{table}')"""
                                table_info = pd.DataFrame(session.sql(sqlstring).collect())
                                ref_tables[table_info["table_name"].iloc[0]] = table
                                st.session_state.upload_type = 'table'
                            st.session_state.reference_tables = ref_tables

                        if len(reference_association_vw) > 0:   
                            ref_views = {}
                            for view in reference_association_vw:
                                sqlstring = f"""show columns in table reference('enrichment_view','{view}')"""
                                view_info = pd.DataFrame(session.sql(sqlstring).collect())
                                ref_views[view_info["table_name"].iloc[0]] = view
                                
                                #only set the upload type to 'view' when Select View is clicked
                                if st.session_state.btn_clicked == 'view':
                                    st.session_state.upload_type = 'view' 
                            st.session_state.reference_views = ref_views

                    col1, col2, col3, col4 = st.columns(4, gap="small")

                    clicked_tbl = False
                    clicked_vw = False

                    with col1:  
                        clicked_tbl = st.button("Select Table")
                    with col2:
                        clicked_vw = st.button("Select View")       

                    if clicked_tbl:
                        st.session_state.btn_clicked = 'table'
                        st.session_state.upload_type = 'table'
                        permissions.request_reference("enrichment_table")
                        
                    if clicked_vw:
                        st.session_state.btn_clicked = 'view'
                        st.session_state.upload_type = 'view'
                        permissions.request_reference("enrichment_view")

                    if "upload_type" in st.session_state:
                        upType = st.session_state.upload_type
                        if(upType == "table"):
                            st.session_state.ref = 'enrichment_table'
                            st.session_state.refs_selected = st.session_state.reference_tables
                        if(upType == "view"):
                            st.session_state.ref = 'enrichment_view'
                            st.session_state.refs_selected = st.session_state.reference_views

                        #set selectbox text and values based on selected object
                        st.session_state.pickedObj = st.selectbox(f"{upType.capitalize()}",st.session_state.refs_selected.keys())

                        if(st.session_state.pickedObj):
                            st.write(f"You have chosen {upType}: {st.session_state.pickedObj}")
                            #call request
                            request(app_name, app_mode, st.session_state.ref, st.session_state.refs_selected[st.session_state.pickedObj])
                else:
                    st.warning("Your app usage has been disabled. Please contact us if you would like to re-enable your app usage.")
                    st.button('Contact Us', key='contact_2', type="primary", on_click=set_page,args=("contact",))
                
                #run history table
                st.write("#")
                st.header("Run History")

                if run_count == 0:
                    st.write("No previous runs exist.")
                else:
                    runs = pd.DataFrame(session.sql(f"SELECT * FROM APP.RUN_TRACKER").collect())
                    st.table(runs)
            else:
                st.warning("Please wait a moment while we finish onboarding your account. This automated process may take a few minutes. If you continue to have issues, please contact us immediately.")
                st.button('Contact Us', key='contact_3', type="primary", on_click=set_page,args=("contact",))         
        else:
            st.error('This account has not been onboarded. Please consult Provider to get enabled to use this app.', icon="🚨")

        
    
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))
    
########################################################################### Contact

class contact_page(BasePage):
    def __init__(self):
        self.name="contact"
        
    def print_page(self):
        super().print_page()

        app_name = pd.DataFrame(session.sql("SELECT CURRENT_DATABASE()").collect()).iloc[0,0]

        if 'submitted' not in st.session_state:
            st.session_state.submitted = False 
                
        col1, col2, col3 = st.columns((.1, 2, .1))
        with col2:
            st.title('Contact Us')
            st.write('Please Complete the Form')
            
            st.text_input('First Name', key='first_name')
            st.text_input('Last Name', key= 'last_name')
            st.text_input('Title', key= 'title')
            st.text_input('Business Email', key='business_email')
            st.selectbox('Industry', ['','Automotive','Consumer Goods and CPG (Consumer Packaged Goods)', 'Financial Services','Gaming',
                                        'Healthcare','Insurance', 'Media and Entertainment', 'Quick Serve Restaurants (QSR)', 'Retail',
                                        'Subscription Services', 'Technology', 'Telecommunications', 'Travel and Hospitality',
                                                'Other'],key='industry')

            st.selectbox('Reason for Contacting', ['','Upgrade Account - Paid','Upgrade Account - Enterprise', 'Questions about Account',
                                                'Other'],key='contact_reason')
        
            st.text_area('Details:', key='contact_reason_text')

        col1_button, col2_button = st.columns((5, 1))
        with col2_button:
            st.button('Submit', on_click=save_form, args=(app_name,))

    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))

############################################################################## Main ####################################################################################################


if "page" not in st.session_state:
    st.session_state.page="home"

pages = [home(), contact_page()]

session = get_active_session()

def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page();
            page.print_sidebar();

main()