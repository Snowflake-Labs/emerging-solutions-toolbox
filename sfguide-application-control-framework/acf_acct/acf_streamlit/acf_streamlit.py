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

#Import app python scripts
from acf.acf_create_app_package import create_app_package
from acf.acf_drop_app_package import drop_app_package
from acf.acf_manage_app_version import manage_app_version
from acf.acf_promote_app_package import promote_app_package
from acf.acf_onboard_consumer import onboard_consumer
from acf.acf_manage_consumer_controls import manage_consumer_controls
from acf.acf_re_enable_consumer import re_enable_consumer
from acf.acf_remove_consumer import remove_consumer
from acf.acf_remove_acf import remove_acf

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

#function to reset acm session vars
def reset_acm_session_vars():
    #controls session vars
    if "controls_df" in st.session_state:
        del st.session_state.controls_df

        
    #rules session vars
    if "rules_dict" in st.session_state:
        del st.session_state.rules_dict

    if "rule_counter" in st.session_state:
        del st.session_state.rule_counter

    if "rules_df" in st.session_state:
        del st.session_state.rules_df

        
    #application package session vars
    if "current_data" in st.session_state:
        del st.session_state.current_data
        
    if "master_data" in st.session_state:
        del st.session_state.master_data

    if "deselect_data" in st.session_state:
        del st.session_state.deselect_data

    if "new_app_pkg_name" in st.session_state:
        del st.session_state.new_app_pkg_name


    #manage versions session vars
    if "current_functions" in st.session_state:
        del st.session_state.current_functions

    if "master_functions" in st.session_state:
        del st.session_state.master_functions

    if "deselect_functions" in st.session_state:
        del st.session_state.deselect_functions

    if "current_procedures" in st.session_state:
        del st.session_state.current_procedures

    if "master_procedures" in st.session_state:
        del st.session_state.master_procedures

    if "deselect_procedures" in st.session_state:
        del st.session_state.deselect_procedures

    if "manage_app_pkg_name" in st.session_state:
        del st.session_state.manage_app_pkg_name

        
    #release directive session vars
    if "app_pkg_release_name" in st.session_state:
        del st.session_state.app_pkg_release_name

        
    #drop application package session vars
    if "drop_app_pkg_name" in st.session_state:
        del st.session_state.drop_app_pkg_name


    #onboard consumer session vars
    if "onboard_consumer_counter" in st.session_state:
        del st.session_state.onboard_consumer_counter

    if "onboard_consumer_list" in st.session_state:
        del st.session_state.onboard_consumer_list

        
    #manage consumer session vars
    if "manage_consumers" in st.session_state:
        del st.session_state.manage_consumers

    if "selected_managed_consumer" in st.session_state:
        del st.session_state.selected_managed_consumer


    #re-enable consumer session vars
    if "re_enable_consumer" in st.session_state:
        del st.session_state.re_enable_consumer

        
    #remove consumer session vars
    if "remove_consumer" in st.session_state:
        del st.session_state.remove_consumer

#create function to remove item from object list list when remove button clicked
def remove_item(key, sel_list:list, obj_list:list, desel_list:list):
    if sel_list and key in sel_list:
        sel_list.remove(key)
    
    if obj_list and key in obj_list:
        obj_list.remove(key)

    desel_list.append(key)

def set_page(page: str):
    st.session_state.page = page

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
        render_image("img/snowflake-logo-color-rgb@2x.png")
        
        if "current_org" not in st.session_state:
            st.session_state.current_org = ""
        
        if "current_acct" not in st.session_state:
            st.session_state.current_acct = ""
        
        if "current_db" not in st.session_state:
            st.session_state.current_db = ""

        if "app_code" not in st.session_state:
            st.session_state.app_code = ""

        

        #get org
        current_org = pd.DataFrame(session.sql("select system$return_current_org_name() as org").collect())
        
        if not current_org.empty:
            st.session_state.current_org = str(current_org["ORG"][0])

        #get acct
        current_acct = pd.DataFrame(session.sql("select current_account() as acct").collect())
        
        if not current_acct.empty:
            st.session_state.current_acct = str(current_acct["ACCT"][0])
        

        #get db
        current_db = pd.DataFrame(session.sql("select current_database() as db").collect())
        
        if not current_db.empty:
            st.session_state.current_db = str(current_db["DB"][0])
            st.session_state.app_code = re.search("P_(.*)_ACF_DB", st.session_state.current_db ).group(1)
        
        st.title(f"App Control Manager: {st.session_state.app_code}")
        st.write(st.__version__)

    def print_sidebar(self):
        st.write("App Control Manager")

class app_control_home(BasePage):
    def __init__(self):
        self.name="home"
        
    def print_page(self):
        super().print_page()

        reset_acm_session_vars()

        st.session_state.layout="centered"
        
        st.write("#")
        
        st.write(
            """Use the following options below to **Manage**
            or **Remove** an App built on the **App Control Framework**,
            along with the ability to **Manage** App Consumers.
            """
        )
        st.write("#")
        
        col1, col2, col3 = st.columns(3, gap="small")
        
        with col1:
           st.subheader("Manage App")
           ma_col1, ma_col2, ma_col3 = st.columns([0.25,3,0.25])
           with ma_col2: 
               render_image_menu("img/manage_app.png")
           st.markdown("""
                        Click the button below to \n
                        manage an existing Native app \n
                        built on the App \n
                        Control Framework.
                        """)
           st.write("#")
           st.button("Manage App", key="home_manage_app", type="primary", on_click=set_page,args=("manage_app",))

        with col2:
           st.subheader("Consumers")
           c_col1, c_col2, c_col3 = st.columns([0.1,2.1,0.1])
           with c_col2: 
               render_image_menu("img/manage_consumer.png")
           st.markdown("""
                        Click the button below to \n
                        manage existing \n
                        consumers of a \n
                        Native App.
                        """)
           st.write("#")
           st.button("Manage Consumers", key="home_manage_consumer", type="primary", on_click=set_page,args=("manage_consumers",)) 
        
        with col3:
           st.subheader("Remove ACF")
           ra_col1, ra_col2, ra_col3 = st.columns([0.1,2.1,0.1])
           with ra_col2: 
               render_image_menu("img/remove.png")
           st.markdown("""
                        Click the button below to \n 
                        remove all objects created \n
                        by the Application \n
                        Control Framework.
                        """)
           st.write("#")  
           st.button("Remove ACF", key="home_remove_acf", type="primary", on_click=set_page,args=("remove_acf",))
           st.caption(":red[⚠︎ NOTE:  The ACCOUNTADMIN role must be granted to the user in order to remove the ACF.]") 
    
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))

############################################################################## Manage App ####################################################################################################

class manage_app_page(BasePage):
    def __init__(self):
        self.name="manage_app"
    def print_page(self):
        super().print_page()
        
        st.session_state.layout="centered"

        st.write("#")
        
        st.write(
            """Use the following options below to manage this app.
            """
        )
        st.write("#")

        col1, col2 = st.columns(2, gap="small")
        
        with col1:
           st.subheader("Controls")
           render_image_menu("img/controls.png")
           st.markdown("""
                        Click the button below to \n
                        manage create/manage \n
                        application controls
                        """)
           st.write("")
           st.button("Controls", key="manage_app_controls", type="primary", on_click=set_page,args=("manage_app_controls",)) 
        
        with col2:
           st.subheader("Rules")
           render_image_menu("img/rules.png")
           st.markdown("""
                        Click the button below to \n
                        create/manage application \n
                        rules
                        """)
           st.write("")
           st.button("Rules", key="manage_app_rules", type="primary", on_click=set_page,args=("manage_app_rules",))

        st.write("#")
        
        col1, col2 = st.columns(2, gap="small")
        
        with col1:
           st.subheader("App Package")
           render_image_menu("img/app_pkg.png")
           st.markdown("""
                        Click the button below to \n
                        create/manage this app's \n
                        application package
                        """)
           st.write("")  
           st.button("App Package", key="manage_app_pkg", type="primary", on_click=set_page,args=("app_package",))

        with col2:
           st.subheader("Trust Center")
           render_image_menu("img/controls.png")
           st.markdown("""
                        Click the button below to \n
                        enable consumer Trust Center \n
                        to protect access to this app
                        """)
           st.write("")  
           st.button("Trust Center", key="manage_trust_center", type="primary", on_click=set_page,args=("trust_center",))


        st.write("#")
        st.write("#")
        
        col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap="small")

        with col1:
            st.button("Home", key="manage_app_home", type="primary", on_click=set_page,args=("home",)) 


    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))



########################################################################### Controls

class manage_app_controls_page(BasePage):
    def __init__(self):
        self.name="manage_app_controls"
        
    def print_page(self):
        super().print_page()

        #create controls dataframe in session_state
        st.session_state.controls_df = pd.DataFrame()
        
        st.header("Manage App Controls")
        st.markdown("Modify default controls and/or add any custom controls.")
        st.write("#")
        st.caption("Add/Modify Controls as needed.  :red[⚠︎ NOTE:  Deleting controls WILL cause the Native App not to function properly.]")

        controls_updated = False
        
        df_controls_edited = st.data_editor(pd.DataFrame(session.sql("SELECT * FROM METADATA.METADATA_DICTIONARY").collect()), num_rows= "dynamic")
        df_controls_snowpark = session.create_dataframe(df_controls_edited)

        st.write("#")
        
        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.55], gap="small")

        with col1:
            st.button("Home", key="manage_app_controls_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="manage_app_controls_back", type="primary", on_click=set_page,args=("manage_app",))
        with col4:
            btn_controls_update = st.button("Update", key="manage_app_controls_update", type="primary")

            if btn_controls_update:
                controls_updated = True
    
            if controls_updated:
                with st.spinner("Updating..."):
                    df_controls_snowpark.write.mode("overwrite").save_as_table("METADATA.METADATA_DICTIONARY",)
                st.success("Controls updated successfully 🎉")

    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Rules

class manage_app_rules_page(BasePage):
    def __init__(self):
        self.name="manage_app_rules"
        
    def print_page(self):
        super().print_page()

        if "rules_dict" not in st.session_state:
            st.session_state.rules_dict = {}

        if "rule_counter" not in st.session_state:
            st.session_state.rule_counter = 0           

        st.header("Manage App Rules")
        st.markdown("Create custom rules, based on custom controls, that can further control Consumer access to the app.")
        
        st.write("#")

        rules_updated = False
        rules_added = False

        st.subheader("Current Rules:")
        st.caption("Existing rules can modified here.")
        df_update_rules_edited = st.data_editor(pd.DataFrame(session.sql("SELECT * FROM METADATA.RULES_DICTIONARY").collect()), num_rows= "dynamic")
        df_update_rules_snowpark = pd.DataFrame()
        
        if not df_update_rules_edited.empty:
            df_update_rules_snowpark = session.create_dataframe(df_update_rules_edited)

        st.write("#")

        col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap="small")

        with col7:
            if df_update_rules_edited.empty:
                btn_rules_update = st.button("Update", key="manage_app_rules_update", type="primary", disabled=True)
            else:
                btn_rules_update = st.button("Update", key="manage_app_rules_update", type="primary")

        if btn_rules_update:
            rules_updated = True

        if rules_updated:
            with st.spinner("Updating..."):
                df_update_rules_snowpark.write.mode("overwrite").save_as_table("METADATA.RULES_DICTIONARY",)
            st.success("Rules updated successfully 🎉")
        

        st.divider()
        
        
        st.subheader("Add New Rules:")

        #create a dataframe to store rules
        st.session_state["rules_df"] = pd.DataFrame(columns=["rule_name", "rule_type", "rule", "controls_used", "description"])

        col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap="small")

        with col7:
            new_rule = st.button("\+ Rule", key ="manage_app_rules_add_rule", type="primary")

        if new_rule:
            st.session_state.rule_counter += 1

            if f"rule_{st.session_state.rule_counter}" not in st.session_state["rules_dict"]:
                st.session_state["rules_dict"][f"rule_{st.session_state.rule_counter}"] = {}

        for i in range(st.session_state.rule_counter):
            if f"rule_{i+1}" not in st.session_state["rules_dict"]:
                st.session_state["rules_dict"][f"rule_{i+1}"] = {}
            
            st.session_state["rules_dict"][f"rule_{i+1}"]["rule_name"] = ""
            st.session_state["rules_dict"][f"rule_{i+1}"]["container_expanded"] = True
            
            if i+1 < st.session_state.rule_counter:
                st.session_state["rules_dict"][f"rule_{i+1}"]["container_expanded"] = False
               
            #create a groups dictionary to store each group array
            if "groups" not in st.session_state["rules_dict"][f"rule_{i+1}"]:
                st.session_state["rules_dict"][f"rule_{i+1}"]["groups"] = {}

            #create a groups array for each rule to store each group and its condition(s)
            st.session_state["rules_dict"][f"rule_{i+1}"]["groups"]["groups"] = []

            #create a controls_used array to store controls used for each rule
            st.session_state["rules_dict"][f"rule_{i+1}"]["controls_used"] = []

            #get controls
            control_list = []
            controls = pd.DataFrame(session.sql("SELECT control_name FROM METADATA.METADATA_DICTIONARY").collect())

            if not controls.empty:
                control_list = controls["CONTROL_NAME"].values.tolist()
            
            st.session_state[f"rule_{i+1}_container"] = st.expander(label=f"Rule {i+1}", expanded=st.session_state["rules_dict"][f"rule_{i+1}"]["container_expanded"])
            
            with st.session_state[f"rule_{i+1}_container"]:
                st.session_state["rules_dict"][f"rule_{i+1}"]["rule_name"] = st.text_input("Rule Name", key=f"rule_{i+1}_name")
                st.session_state["rules_dict"][f"rule_{i+1}"]["rule_type"] = st.selectbox("Rule Type", options=["CUSTOM"], key=f"rule_{i+1}_type")                
                st.session_state["rules_dict"][f"rule_{i+1}"]["description"] = st.text_area("Description", key=f"rule_{i+1}_description")

                col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap="small")
        
                with col1:
                    remove_rule = st.button("\- Rule", key = f"new_page_rules_rule_{i+1}_remove_rule", type="primary")
                with col7:
                    new_group = st.button("\+ Group", key = f"new_page_rules_rule_{i+1}_add_group", type="primary")

                if f"rule_{i+1}_group_counter" not in st.session_state:
                        st.session_state[f"rule_{i+1}_group_counter"] = 0

                if new_group:
                    st.session_state[f"rule_{i+1}_group_counter"] += 1
        
                for j in range(st.session_state[f"rule_{i+1}_group_counter"]):

                    #set group dictionary
                    if f"group_{j+1}" not in st.session_state["rules_dict"][f"rule_{i+1}"]:
                            st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"] = {}

                    #set rule group array
                    if f"rule_{i+1}_group_{j+1}_arr" not in st.session_state:
                            st.session_state[f"rule_{i+1}_group_{j+1}_arr"] = []

                    #set conditional counter to 1, since each group must have at least 1 condition
                    if f"rule_{i+1}_group_{j+1}_condition_counter" not in st.session_state:
                            st.session_state[f"rule_{i+1}_group_{j+1}_condition_counter"] = 1
                    
                    col1, col2, col3, col4, col5, col6 = st.columns(6, gap="small")
                    with col1:
                        st.write("#")
                        st.subheader(f"Group {j+1}")
                    with col2:
                        st.write("#")
                        if j+1 == 1 and st.session_state[f"rule_{i+1}_group_counter"] > 1:
                            remove_group = st.button("\-", key = f"new_page_rules_remove_rule_{i+1}_group_{j+1}", type="primary", disabled=True)
                        else:
                            remove_group = st.button("\-", key = f"new_page_rules_remove_rule_{i+1}_group_{j+1}", type="primary")
                    
                    #set group logical field empty if this is first/only group, since there is not a previous group to compare this one to
                    if j+1 == 1:
                        st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]["logical"] = ""
                    else:
                        st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]["logical"] = st.selectbox("Group Logical", options=["AND", "OR", "NOT"], key=f"rule_{i+1}_group_{j+1}_logical")

                    #add group logical to rule group arr
                    if {"logical":st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]["logical"]} not in st.session_state[f"rule_{i+1}_group_{j+1}_arr"]:
                        st.session_state[f"rule_{i+1}_group_{j+1}_arr"].append({"logical":st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]["logical"]})
        
                    if remove_group:
                        st.session_state[f"rule_{i+1}_group_counter"] -= 1
                        del st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]
                        st.rerun()
                    
                    for k in range(st.session_state[f"rule_{i+1}_group_{j+1}_condition_counter"]):

                        #establish empty condition dictionary to add condition fields
                        if f"condition_{k+1}" not in st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]:
                                st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"] = {}
                        
                        col1, col2, col3, col4, col5, col6 = st.columns([2,1,2,1,1,1])
                        with col1:
                            st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"] = st.selectbox("Field", options=control_list + ["Other..."], key=f"rule_{i+1}_group_{j+1}_condition_{k+1}_field")
                            
                            if st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"] == "Other...":
                                st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"] = st.text_input("Enter field name...", key=f"rule_{i+1}_group_{j+1}_condition_{k+1}_other_field")
                            
                            if (st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"] in control_list) and (st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"] not in st.session_state["rules_dict"][f"rule_{i+1}"]["controls_used"]):
                                st.session_state["rules_dict"][f"rule_{i+1}"]["controls_used"].append(st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["field"])
                        with col2:
                            st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["comparison"] = st.selectbox("Compare", options=[">", ">=", "<", "<=", "=", "!="], key=f"rule_{i+1}_group_{j+1}_condition_{k+1}_comparison")
                        with col3:
                            st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["target"] = st.text_input("Target", key=f"rule_{i+1}_group_{j+1}_condition_{k+1}_target")
                        with col4:
                            if k+1 == 1:
                                st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["logical"] = ""
                            else:
                                st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]["logical"] = st.selectbox("Logical", options=["AND", "OR", "NOT"], key=f"rule_{i+1}_group_{j+1}_condition_{k+1}_logical") 
                        with col5:
                            st.write("#")
                            new_cond = st.button("\+ ",key =f"rule_{i+1}_group_{j+1}_condition_{k+1}_add_condition", type="primary")
                        with col6:
                            st.write("#")
                            if k+1 == 1:
                                remove_cond = st.button("\- ",key =f"rule_{i+1}_group_{j+1}_condition_{k+1}_remove_condition", type="primary", disabled=True)
                            else:
                                remove_cond = st.button("\- ",key =f"rule_{i+1}_group_{j+1}_condition_{k+1}_remove_condition", type="primary")

                        #add condition to rule group arr
                        if st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"] not in st.session_state[f"rule_{i+1}_group_{j+1}_arr"]:
                            st.session_state[f"rule_{i+1}_group_{j+1}_arr"].append(st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"])
                        
                        if new_cond:
                            st.session_state[f"rule_{i+1}_group_{j+1}_condition_counter"] += 1
                
                            for k in range(st.session_state[f"rule_{i+1}_group_{j+1}_condition_counter"]):
                                if f"condition_{k+1}" not in st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"]:
                                    st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"] = {}
                                    st.rerun()
        
                        if remove_cond:
                            st.session_state[f"rule_{i+1}_group_{j+1}_condition_counter"] -= 1
                            del st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"][f"condition_{k+1}"]

                            #remove condition from rule group arr
                            for c in st.session_state[f"rule_{i+1}_group_{j+1}_arr"]:
                                if c not in st.session_state["rules_dict"][f"rule_{i+1}"][f"group_{j+1}"].values():
                                    st.session_state[f"rule_{i+1}_group_{j+1}_arr"].remove(c)
                            
                            st.rerun()

                    #add rule group arr to rules dictionary
                    if st.session_state[f"rule_{i+1}_group_{j+1}_arr"] not in st.session_state["rules_dict"][f"rule_{i+1}"]["groups"]["groups"]:
                        st.session_state["rules_dict"][f"rule_{i+1}"]["groups"]["groups"].append(st.session_state[f"rule_{i+1}_group_{j+1}_arr"])                        

                if remove_rule:
                    st.session_state.rule_counter -= 1
                    del st.session_state["rules_dict"][f"rule_{i+1}"]
                    st.rerun()

       

            st.session_state["rules_df"].loc[len(st.session_state["rules_df"].index)] = [st.session_state["rules_dict"][f"rule_{i+1}"]["rule_name"]
                                                                                         ,st.session_state["rules_dict"][f"rule_{i+1}"]["rule_type"]
                                                                                         ,json.dumps(st.session_state["rules_dict"][f"rule_{i+1}"]["groups"])
                                                                                         ,','.join(st.session_state["rules_dict"][f"rule_{i+1}"]["controls_used"])
                                                                                         ,st.session_state["rules_dict"][f"rule_{i+1}"]["description"]
                                                                                        ] 

        
        #diagnostic mode
        col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")
        
        with col4:
        	diagnostics = st.checkbox("Diagnostic Mode: ", key="manage_rules_diagnostics_mode")

        if diagnostics:
            st.write("Rules Payload:")
            st.write(st.session_state["rules_df"])
        
        df_add_rules_snowpark = session.create_dataframe(st.session_state["rules_df"])
        

        st.write("#")

        col1, col2, col3, col4, col5, col6, col7 = st.columns(7, gap="small")

        #if there is at least one rule, enable adding the button
        with col7:
            if "rule_1_group_counter" in st.session_state and st.session_state.rule_1_group_counter > 0:
                btn_rules_added = st.button("Add", key="manage_app_rules_add", type="primary")

                if btn_rules_added:
                    rules_added = True

        if rules_added:
            with st.spinner("Updating..."):
                df_add_rules_snowpark.write.mode("append").save_as_table("METADATA.RULES_DICTIONARY",)
            st.success("Rules added successfully 🎉")
            time.sleep(2)
            st.rerun()

        st.write("#")
        st.write("#")

        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.5], gap="small")

        with col1:
            st.button("Home", key="manage_app_rules_home", type="primary", on_click=set_page,args=("home",)) 
        with col4:
            st.button("Back", key="manage_app_rules_back", type="primary", on_click=set_page,args=("manage_app",)) 

    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### App Package

class app_package_page(BasePage):
    def __init__(self):
        self.name="app_package"
    def print_page(self):
        super().print_page()
        
        st.session_state.layout="centered"

        st.write("#")
        st.write(
            """Use the following options below to create/manage this app's ppplication package(s).
            """
        )
        st.write("#")

        col1, col2 = st.columns(2, gap="small")
        
        with col1:
           st.subheader("Create App Package")
           cap_col1, cap_col2, cap_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with cap_col2:  
               render_image_menu("img/app_pkg.png")
           st.markdown("""
                        Click the button below to create a new Application Package
                        """)
           st.write("")
           st.button("Create", key="create_app_package_button", type="primary", on_click=set_page,args=("new_app_package",))

        with col2:
           st.subheader("Manage App Versions")
           v_col1, v_col2, v_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with v_col2:  
               render_image_menu("img/versions.png")
           st.markdown("""
                        Click the button below to create, patch, or drop a version
                        """)
           st.write("")
           st.button("Versions", key="manage_app_package_versions_button", type="primary", on_click=set_page,args=("app_package_version",)) 

        st.write("#")
        col1, col2 = st.columns(2, gap="small")

        with col1:
           st.subheader("Promote App Package")
           pap_col1, pap_col2, pap_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with pap_col2:  
               render_image_menu("img/promote.png")
           st.markdown("""
                        Click the button below to promote an application package to the PROD environment. 
                        """)
           st.write("")  
           st.button("Promote", key="manage_app_package_promote_button", type="primary", on_click=set_page,args=("promote_to_prod",))
        
        with col2:
           st.subheader("Drop App Package")
           dap_col1, dap_col2, dap_col3 = st.columns([0.5,1.5,0.5], gap="small")
           with dap_col2:  
               render_image_menu("img/remove.png")
           st.markdown("""
                        Click the button below to drop an application package. 
                        """)
           st.write("")  
           st.button("Drop", key="manage_app_package_drop_button", type="primary", on_click=set_page,args=("app_package_drop",))
           st.caption(":red[ **NOTE**: the Marketplace listing created from the application package must be first unpublished and deleted.]") 
        
        st.write("#")
        st.write("#")
        
        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.5], gap="small")

        with col1:
            st.button("Home", key="app_package_home", type="primary", on_click=set_page,args=("home",)) 
        with col4:
            st.button("Back", key="app_package_back", type="primary", on_click=set_page,args=("manage_app",)) 


    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))

##################################### Create App Package

class new_app_package_page(BasePage):
    def __init__(self):
        self.name="new_app_package"
               
    def print_page(self):
        super().print_page()

        #session vars
        if "current_data" not in st.session_state:
            st.session_state.current_data = []

        if "master_data" not in st.session_state:
            st.session_state.master_data = []

        if "deselect_data" not in st.session_state:
            st.session_state.deselect_data = []
        
        st.session_state.layout="centered"
        st.session_state.new_app_pkg_name = ""

        new_app_pkg = False
        create_app_pkg_btn = False
        
        st.header("New Application Package")
        st.markdown(f"Please provide a name for the application package.  This name will be prefixed by: **P_{st.session_state.app_code}_APP_PKG_**")
        st.caption(":red[⚠︎ NOTE:  No special nor whitespace characters are allowed.  Use underscores (_) in lieu of spaces.]")
        st.session_state.new_app_pkg_name = f"P_{st.session_state.app_code}_APP_PKG_" + st.text_input("**Appplication Package Name** :red[*]")

        if st.session_state.new_app_pkg_name == "":
            st.write(":red[⚠︎ Please enter your application package name]")

        pkg_list = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE '{st.session_state.new_app_pkg_name}'").collect())

        if not pkg_list.empty and (pkg_list["name"].eq(st.session_state.new_app_pkg_name.upper())).any():
            st.caption(":red[This application package already exists.  Please specify a different name.]")
        else:
            if st.session_state.new_app_pkg_name != f"P_{st.session_state.app_code}_APP_PKG_":
                create_app_pkg_btn = True

        st.write("#")
        st.subheader("Please select source table(s)/view(s).")
        st.caption(":red[⚠︎ NOTE:  The source table(s)/view(s) should be granted to the app admin role, prior to creating the application package.]")
        st.write("")
        
        dbs = pd.DataFrame(session.sql(f"SHOW DATABASES").collect())

        if not dbs.empty :
            selectdb = st.selectbox("Select Database:", dbs["name"])
            schemas = pd.DataFrame(session.sql("show schemas in database " + selectdb).collect())

        if not schemas.empty :   
            selectschema = st.selectbox("Select Schema:", schemas["name"])
            objects = pd.DataFrame(session.sql(f"show objects in schema {selectdb}.{selectschema}").collect())
            tbl_vws = pd.DataFrame(session.sql("select * from table(result_scan(last_query_id())) where upper(\"kind\") in ('TABLE', 'VIEW')").collect())

        datalist = [] # multiselect list of data objects

        if not tbl_vws.empty :
            datalist = st.multiselect("Select Table/Views(s):", tbl_vws["database_name"]+"."+tbl_vws["schema_name"]+"."+tbl_vws["name"], key=tbl_vws["database_name"]+"."+tbl_vws["schema_name"]+"."+tbl_vws["name"])
    
        for d in datalist:
            if d not in st.session_state.current_data:
                st.session_state.current_data.append(d)
            if d not in st.session_state.master_data:
                st.session_state.master_data.append(d)
            if d in st.session_state.deselect_data:
                st.session_state.deselect_data.remove(d)
        
        for s in st.session_state.current_data:
            if s not in st.session_state.master_data:
                st.session_state.master_data.append(s)
            if (s.split(".")[0] == selectdb) and (s.split(".")[1] == selectschema):
                if s not in datalist:
                    if not datalist and (not st.session_state.deselect_data):
                        st.session_state.current_data = []
                    else:
                        if s not in st.session_state.deselect_data:
                            st.session_state.deselect_data.append(s)

        for de in st.session_state.deselect_data:
            if len(st.session_state.deselect_data) > 0:
                remove_item(de, st.session_state.master_data, st.session_state.current_data, [])
        
        
        #diagnostics
        col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")

        with col4:
            diagnostics = st.checkbox("Diagnostic Mode: ", key="create_app_pkg_diagnostics_mode")
        
        
        st.write("#")
        st.subheader("Selected Source Data:")

        #create col container and remove button for selected objs 
        #cols created per row for consistent spacing
        for sd in st.session_state.master_data:
            col1, col2 = st.columns([6,1])
            with col1:
                st.markdown(sd)
            with col2:
                if sd in datalist:
                    st.button("Remove", key = sd, type="primary", disabled=True)
                else:
                    st.button("Remove", key = sd, type="primary", on_click=remove_item,args=(sd,st.session_state.current_data,st.session_state.master_data,st.session_state.deselect_data ,))
        
        st.write("#")

        if diagnostics:
            st.write("Current Source Data List")
            st.write(st.session_state.current_data)

            st.write("#")

            st.write("Master Source Data List")
            st.write(st.session_state.master_data)

            st.write("#")

            st.write("Deselected Source Data List")
            st.write(st.session_state.deselect_data)
        
        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.55], gap="small")

        with col1:
            st.button("Home", key="new_app_package_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="new_app_package_back", type="primary", on_click=set_page,args=("app_package",)) 
        with col4:
            if create_app_pkg_btn == False:
                btn_new_app_pkg = st.button("Create", key="new_page_data_create", type="primary", disabled = True)
            else:
                btn_new_app_pkg = st.button("Create", key="new_page_data_create", type="primary") 
            
                if btn_new_app_pkg:
                    new_app_pkg = True

        if new_app_pkg:
            with st.spinner("Updating..."):
                create_app_package(st.session_state.app_code,st.session_state.new_app_pkg_name,st.session_state.master_data)
            st.success(f"Application Package {st.session_state.new_app_pkg_name} successfully created 🎉")
            time.sleep(2)
            set_page("home")
            st.rerun()
            
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


##################################### Manage App Versions
        
class app_package_version_page(BasePage):
    def __init__(self):
        self.name="app_package_version"
               
    def print_page(self):
        super().print_page()
        
        #set/reset session vars
        st.session_state.layout="centered"
        st.session_state.manage_app_pkg_app_mode = ""
        st.session_state.manage_app_pkg_limit_enforced = "Y"
        st.session_state.app_funcs_env = ""
        st.session_state.app_procs_env = ""
        st.session_state.templates_env = ""
        st.session_state.streamlit_env = ""
        st.session_state.manage_app_pkg_name = ""

        if "current_functions" not in st.session_state:
            st.session_state.current_functions = []

        if "master_functions" not in st.session_state:
            st.session_state.master_functions = []

        if "deselect_functions" not in st.session_state:
            st.session_state.deselect_functions = []

        if "current_procedures" not in st.session_state:
            st.session_state.current_procedures = []

        if "master_procedures" not in st.session_state:
            st.session_state.master_procedures = []

        if "deselect_procedures" not in st.session_state:
            st.session_state.deselect_procedures = []

        version_option_flag = False
        manage_btn_enabled = False
        manage_version_label = "None"
        version = ""
        version_option = ""

        
        st.header("Application Package Versions")
        st.write("")
        dbs_pkgs = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_APP_PKG_%'").collect())
        pkgs = pd.DataFrame(session.sql("select * from table(result_scan(last_query_id())) where upper(\"kind\") = 'APPLICATION PACKAGE'").collect())

        
        if not pkgs.empty:
            st.session_state.manage_app_pkg_name = st.selectbox("Select Application Package", pkgs["name"])
            st.write("#")

            version_option = st.selectbox("Create, Patch, or Drop Version", ["CREATE","PATCH", "DROP"])

            version_list = pd.DataFrame(session.sql(f"SHOW STAGES IN SCHEMA {st.session_state.manage_app_pkg_name}.VERSIONS").collect())
            
            if version_option == "CREATE":
                manage_version_label = "Create"
                version = st.text_input("Version Name", key="app_pkg_version_create_version")
                if version != "":
                    manage_btn_enabled = True
                st.caption("It is recommended to use major/minor versoning i.e.: :blue[v1_0_0].  This will also be the name of the stage that stores the setup scripts for this app version")
                
                if not version_list.empty and (version_list["name"].eq(version.upper())).any():
                    st.caption(":red[This version already exists for this application package.  Please specify a different version name.]")
                    manage_btn_enabled = False

            if version_option == "PATCH":
                manage_version_label = "Patch"
                if not version_list.empty:
                    manage_btn_enabled = True
                    version = st.selectbox("Select Version:", version_list["name"], key="app_pkg_version_patch_version")
                else:
                    st.caption("No versions exist to patch")

            if version_option == "DROP":
                manage_version_label = "Drop"
                if not version_list.empty:
                    manage_btn_enabled = True
                    version = st.selectbox("Select Version:", version_list["name"], key="app_pkg_version_patch_version")
                else:
                    st.caption("No versions exist to drop")

        
        st.write("#")
        st.write("#")

        if version_option.upper() in ["CREATE", "PATCH"]:

            st.session_state.manage_app_pkg_app_mode = st.selectbox("Select App Mode", ["FREE","PAID", "ENTERPRISE"])
            st.write("#")

            if st.session_state.manage_app_pkg_app_mode in ("FREE", "PAID"):
                st.session_state.manage_app_pkg_limit_enforced = st.selectbox("Enforce Limits", ["Y","N"])
                st.write("#")

            environments = []

            dev_db = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_SOURCE_DB_DEV'").collect())
            if not dev_db.empty:
                environments.append("DEV")
        
            prod_db = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_SOURCE_DB_PROD'").collect())
            if not prod_db.empty:
                environments.append("PROD")

            st.subheader("Please select environment containing the Streamlit artifacts.")
            st.caption(":red[⚠︎ NOTE:  If the Streamlit artifacts have not changed since the latest release, choose **PROD**.]")
            st.session_state.streamlit_env = st.selectbox("Environment:", environments, key="app_pkg_version_streamlit_env")
            st.write("#")

            st.subheader("Please select environment containing the template files.")
            st.caption(":red[⚠︎ NOTE:  If the template files have not changed since the latest release, choose **PROD**.]")
            st.session_state.templates_env = st.selectbox("Environment:", environments, key="app_pkg_version_templates_env")
            st.write("#")
    
            
            st.subheader("Please select source function(s).")
            st.caption(":red[⚠︎ NOTE 1:  The source functions should be created by/granted to the app admin role, prior to creating the application package.]")
            st.caption(f":red[⚠︎ NOTE 2:  If **NONE** of the source functions have changed since the latest release, choose **P_{st.session_state.app_code}_SOURCE_DB_PROD**, otherwise choose **P_{st.session_state.app_code}_SOURCE_DB_DEV**]")
            
            st.write("")
            func_dbs = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_SOURCE_DB_%'").collect())
    
            if not func_dbs.empty :
                func_selectdb = st.selectbox("Select Database:", func_dbs["name"], key="app_pkg_version_func_select_db")
                st.session_state.app_funcs_env = func_selectdb
                func_schemas = pd.DataFrame(session.sql("show schemas in database " + func_selectdb).collect())
    
            if not func_schemas.empty :
                func_schemas_idx = 0
                if "FUNCS_APP" in func_schemas["name"].tolist():
                    func_schemas_idx = func_schemas["name"].tolist().index("FUNCS_APP")
    
                func_selectschema = st.selectbox("Select Schema:", func_schemas["name"], key="app_pkg_version_func_select_sch", index=func_schemas_idx)
                functions = pd.DataFrame(session.sql(f"show user functions in schema {func_selectdb}.{func_selectschema}").collect())
    
            functionlist = [] # multiselect list of functionlist
    
            if not functions.empty :
                functionlist = st.multiselect("Select Function(s):", functions["catalog_name"]+"."+functions["schema_name"]+"."+functions["arguments"].str.partition(" RETURN")[0], key=functions["name"])
            
            for d in functionlist:
                if d not in [i[0] for i in st.session_state.current_functions]:
                    st.session_state.current_functions.append([d, False])
                if d not in [i[0] for i in st.session_state.master_functions]:
                    st.session_state.master_functions.append([d, False])
                if d in [i[0] for i in st.session_state.deselect_functions]:
                    st.session_state.deselect_functions.remove([d, False])
            
            for s in st.session_state.current_functions:
                if s[0] not in [i[0] for i in st.session_state.master_functions]:
                    st.session_state.master_functions.append(s)
                if (s[0].split(".")[0] == func_selectdb) and (s[0].split(".")[1] == func_selectschema):
                    if s[0] not in functionlist:
                        if not functionlist and (not st.session_state.deselect_functions):
                            st.session_state.current_functions = []
                        else:
                            if s[0] not in [i[0] for i in st.session_state.deselect_functions]:
                                st.session_state.deselect_functions.append(s)
    
            for de in st.session_state.deselect_functions:
                if len(st.session_state.deselect_functions) > 0:
                    remove_item(de, st.session_state.master_functions, st.session_state.current_functions, [])
    
            st.write("#")

            #diagnostics
            col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")
            
            with col4:
	            func_diagnostics = st.checkbox("Diagnostic Mode: ", key="create_version_function_diagnostics_mode")
            
            st.subheader("Selected Functions:")
    
            #create col container and remove button for selected objs 
            #cols created per row for consistent spacing
            for sd in st.session_state.master_functions:
                if f"{sd[0]}_accessible" not in st.session_state:
                        st.session_state[f"{sd[0]}_accessible"] = False
                    
                col1, col2, col3 = st.columns([1,4,0.75])
                with col1:
                   m_f_index = st.session_state.master_functions.index(sd) if sd in st.session_state.master_functions else -1
                   c_f_index = st.session_state.current_functions.index(sd) if sd in st.session_state.current_functions else -1
    
                   st.session_state[f"{sd[0]}_accessible"] = st.checkbox("Accessible", key=f"app_pkg_version_{sd[0]}_accessible")
    
                   if st.session_state[f"{sd[0]}_accessible"]:
                       if m_f_index > -1:
                           st.session_state.master_functions[m_f_index][1] = True
                       if c_f_index > -1:
                           st.session_state.current_functions[c_f_index][1] = True
                   else:
                       if m_f_index > -1:
                           st.session_state.master_functions[m_f_index][1] = False
                       if c_f_index > -1:
                           st.session_state.current_functions[c_f_index][1] = False
                with col2:
                    st.markdown(sd[0])
                with col3:
                    if sd[0] in functionlist:
                        st.button("Remove", key = sd[0], type="primary", disabled=True)
                    else:
                        st.button("Remove", key = sd[0], type="primary", on_click=remove_item,args=(sd,st.session_state.current_functions,st.session_state.master_functions,st.session_state.deselect_functions ,))
    
            st.write("#")
            st.write("#")

            if func_diagnostics:
                st.write("Current Functions List")
                st.write(st.session_state.current_functions)
    
                st.write("#")
    
                st.write("Master Functions List")
                st.write(st.session_state.master_functions)
    
                st.write("#")
    
                st.write("Deselected Functions List")
                st.write(st.session_state.deselect_functions)

    
            st.subheader("Please select source procedure(s).")
            st.caption(":red[⚠︎ NOTE 1:  The source procedures should be created by/granted to the app admin role, prior to creating the application package.]")
            st.caption(f":red[⚠︎ NOTE 2:  If **NONE** of the source procedures have changed since the latest release, choose **P_{st.session_state.app_code}_SOURCE_DB_PROD**, otherwise choose **P_{st.session_state.app_code}_SOURCE_DB_DEV**]")
            st.write("")
            proc_dbs = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_SOURCE_DB_%'").collect())
    
            if not proc_dbs.empty :
                proc_selectdb = st.selectbox("Select Database:", proc_dbs["name"], key="app_pkg_version_proc_select_db")
                st.session_state.app_procs_env = proc_selectdb
                proc_schemas = pd.DataFrame(session.sql("show schemas in database " + proc_selectdb).collect())
    
            if  not proc_schemas.empty :
                proc_schemas_idx = 0
                if "PROCS_APP" in proc_schemas["name"].tolist():
                    proc_schemas_idx = proc_schemas["name"].tolist().index("PROCS_APP")
                    
                proc_selectschema = st.selectbox("Select Schema:", proc_schemas["name"], key="app_pkg_version_proc_select_sch", index=proc_schemas_idx)
                procedures = pd.DataFrame(session.sql(f"show user procedures in schema {proc_selectdb}.{proc_selectschema}").collect())
    
            procedurelist = [] # multiselect list of procedures
    
            if not procedures.empty :
                procedurelist = st.multiselect("Select Procedures(s):", procedures["catalog_name"]+"."+procedures["schema_name"]+"."+procedures["arguments"].str.partition(" RETURN")[0], key=procedures["name"])
        
            for d in procedurelist:
                if d not in [i[0] for i in st.session_state.current_procedures]:
                    st.session_state.current_procedures.append([d, False])
                if d not in [i[0] for i in st.session_state.master_procedures]:
                    st.session_state.master_procedures.append([d, False])
                if d in [i[0] for i in st.session_state.deselect_procedures]:
                    st.session_state.deselect_procedures.remove([d, False])
            
            for s in st.session_state.current_procedures:
                if s[0] not in [i[0] for i in st.session_state.master_procedures]:
                    st.session_state.master_procedures.append(s)
                if (s[0].split(".")[0] == proc_selectdb) and (s[0].split(".")[1] == proc_selectschema):
                    if s[0] not in procedurelist:
                        if not procedurelist and (not st.session_state.deselect_procedures):
                            st.session_state.current_procedures = []
                        else:
                            if s[0] not in [i[0] for i in st.session_state.deselect_procedures]:
                                st.session_state.deselect_procedures.append(s)
    
            for de in st.session_state.deselect_procedures:
                if len(st.session_state.deselect_procedures) > 0:
                    remove_item(de, st.session_state.master_procedures, st.session_state.current_procedures,[])
            
            st.write("#")

            #diagnostics
            col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")
            
            with col4:
	            proc_diagnostics = st.checkbox("Diagnostic Mode: ", key="create_version_procedure_diagnostics_mode")
            
            st.subheader("Selected Procedures:")
    
            #create col container and remove button for selected objs 
            #cols created per row for consistent spacing
            for sd in st.session_state.master_procedures:
                if f"{sd[0]}_input_table" not in st.session_state:
                        st.session_state[f"{sd[0]}_input_table"] = False
                    
                col1, col2, col3 = st.columns([1,5.7,1])
                with col1:
                   m_p_index = st.session_state.master_procedures.index(sd) if sd in st.session_state.master_procedures else -1
                   c_p_index = st.session_state.current_procedures.index(sd) if sd in st.session_state.current_procedures else -1
    
                   st.session_state[f"{sd[0]}_input_table"] = st.checkbox("Input Table", key=f"app_pkg_version_{sd[0]}_input_table")
    
                   if st.session_state[f"{sd[0]}_input_table"]:
                       if m_p_index > -1:
                           st.session_state.master_procedures[m_p_index][1] = True
                       if c_p_index > -1:
                           st.session_state.current_procedures[c_p_index][1] = True
                   else:
                       if m_p_index > -1:
                           st.session_state.master_procedures[m_p_index][1] = False
                       if c_p_index > -1:
                           st.session_state.current_procedures[c_p_index][1] = False
                with col2:
                    st.markdown(sd[0])
                with col3:
                    if sd in procedurelist:
                        st.button("Remove", key = sd, type="primary", on_click=remove_item,args=(sd,st.session_state.current_procedures,st.session_state.master_procedures,st.session_state.deselect_procedures ,), disabled=True)
                    else:
                        st.button("Remove", key = sd, type="primary", on_click=remove_item,args=(sd,st.session_state.current_procedures,st.session_state.master_procedures,st.session_state.deselect_procedures ,))
    
            st.write("#")

            if proc_diagnostics:
                st.write("Current Procedures List")
                st.write(st.session_state.current_procedures)
    
                st.write("#")
    
                st.write("Master Procedures List")
                st.write(st.session_state.master_procedures)
    
                st.write("#")
    
                st.write("Deselected Procedures List")
                st.write(st.session_state.deselect_procedures)
        
        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.525], gap="small")

        with col1:
            st.button("Home", key="app_package_version_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="app_package_version_back", type="primary", on_click=set_page,args=("app_package",)) 
        with col4:
            if manage_btn_enabled == False:
                btn_new_app_version = st.button(manage_version_label, key="app_package_new_app_version", type="primary", disabled = True)
            else:
                btn_new_app_version = st.button(manage_version_label, key="app_package_new_app_version", type="primary") 

                if btn_new_app_version:
                    version_option_flag = True

        if version_option_flag:
            with st.spinner("Updating..."):
                manage_app_version(st.session_state.app_code,st.session_state.manage_app_pkg_name,st.session_state.manage_app_pkg_app_mode,st.session_state.manage_app_pkg_limit_enforced,version_option,version,st.session_state.app_funcs_env,st.session_state.app_procs_env,st.session_state.templates_env,st.session_state.streamlit_env,st.session_state.master_functions,st.session_state.master_procedures)
            st.success(f"{version_option} for Application Package {st.session_state.manage_app_pkg_name} Version: {version} successful 🎉")
            time.sleep(2)
            set_page("home")
            st.rerun()
            
            
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))



##################################### Promote to Prod

class promote_to_prod_page(BasePage):
    def __init__(self):
        self.name="promote_to_prod"
    def print_page(self):
        super().print_page()

        st.session_state.layout="centered"
        
        promote_btn_enabled = False
        promote_flag = False

        st.header("Promote Application Package")

        st.write("#")

        #TODO:  select application package to use to promote to prod
        app_pkgs = pd.DataFrame(session.sql(f"SELECT * FROM P_{st.session_state.app_code}_ACF_DB.ACF_STREAMLIT.VERSION_HISTORY ORDER BY APPLICATION_PACKAGE, VERSION, PATCH DESC").collect())
    
        if not app_pkgs.empty:
            select_app_pkg = st.selectbox("Select Application Package:", app_pkgs["APPLICATION_PACKAGE"].drop_duplicates(), key="promote_select_app_pkg")
            app_pkgs_filtered = app_pkgs.query(f'APPLICATION_PACKAGE == "{select_app_pkg}"')
    
            select_vers = st.selectbox("Select Version:", app_pkgs_filtered["VERSION"].drop_duplicates(), key="promote_select_app_vers")
            versions_filtered = app_pkgs_filtered.query(f'VERSION == "{select_vers}"')

            select_patch = st.selectbox("Select Patch:", versions_filtered["PATCH"], key="promote_select_app_patch")
            patches_filtered = versions_filtered.query(f'PATCH == "{select_patch}"')

            st.write("#")

            if not patches_filtered.empty:
                app_funcs_env = patches_filtered["APP_FUNCS_ENV"].iloc[0]
                app_funcs_list = json.loads(patches_filtered["APP_FUNCS_LIST"].iloc[0])
                app_procs_env = patches_filtered["APP_PROCS_ENV"].iloc[0]
                app_procs_list = json.loads(patches_filtered["APP_PROCS_LIST"].iloc[0])
                templates_env = patches_filtered["TEMPLATES_ENV"].iloc[0]
                streamlit_env = patches_filtered["STREAMLIT_ENV"].iloc[0]
                
                promote_btn_enabled = True

                if promote_btn_enabled == False:
                    btn_promote_app_pkg = st.button("Promote", key="promote_app", type="primary", disabled=True)
                else:
                    btn_promote_app_pkg = st.button("Promote", key="promote_app", type="primary")

                if btn_promote_app_pkg:
                    promote_flag = True

                if promote_flag:
                    with st.spinner("Updating..."):
                        promote_app_package(st.session_state.app_code,app_funcs_env,app_funcs_list,app_procs_env,app_procs_list,templates_env,streamlit_env)
                    st.success(f"Application Package {select_app_pkg}, Version: {select_vers}, Patch {select_patch} successfully promoted to PROD 🎉")
                    time.sleep(2)
                    set_page("home")
                    st.rerun()


        st.write("#")
        st.write("#")
        
        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.525], gap="small")

        with col1:
            st.button("Cancel", key="promote_app_pkg_cancel", type="primary", on_click=set_page,args=("home",)) 


    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))        



##################################### Drop App Package
        
class app_package_drop_page(BasePage):
    def __init__(self):
        self.name="app_package_drop"
               
    def print_page(self):
        super().print_page()

        #delete session vars to reset
        
        
        #set/reset session vars
        st.session_state.layout="centered"
        st.session_state.drop_app_pkg_name = ""    
                
        drop_btn_enabled = False
        drop_flag = False        
        
        st.header("Drop Application Package")
        st.write("")
        dbs_pkgs = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{st.session_state.app_code}_APP_PKG_%'").collect())
        pkgs = pd.DataFrame(session.sql("select * from table(result_scan(last_query_id())) where upper(\"kind\") = 'APPLICATION PACKAGE'").collect())

        if pkgs.empty:
            st.caption("There are no application packages to drop.")
        
        if not pkgs.empty:
            drop_btn_enabled = True
            st.session_state.drop_app_pkg_name = st.selectbox("Select Application Package", pkgs["name"])

        st.write("#")
        st.write("#")

        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.525], gap="small")

        with col1:
            st.button("Home", key="app_package_drop_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="app_package_drop_back", type="primary", on_click=set_page,args=("app_package",)) 
        with col4:
            if drop_btn_enabled == False:
                btn_drop_app_pkg = st.button("Drop", key="app_package_drop", type="primary", disabled = True)
            else:
                btn_drop_app_pkg = st.button("Drop", key="app_package_drop", type="primary") 

                if btn_drop_app_pkg:
                    drop_flag = True

        if drop_flag:
            with st.spinner("Updating..."):
                drop_app_package(st.session_state.app_code,st.session_state.drop_app_pkg_name)
            st.success(f"Application Package {st.session_state.drop_app_pkg_name} successfully dropped 🎉")
            time.sleep(3)
            set_page("home")
            st.rerun()  
            
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Trust Center

class trust_center_page(BasePage):
    def __init__(self):
        self.name="trust_center"
               
    def print_page(self):
        super().print_page()

        #st.session_state.layout="wide"

        txt_tc_lookback_window = ""
        flag_disable_scanners_btn = True
        flag_scanners_selected = False
        btn_scanners = None
        de_tc_scanners = None
        
        st.header("Trust Center Enforcement")
        st.write("")
        enforce_tc_findings = st.selectbox("Use Trust Center to control access to the native app", options=["Choose...", "Y", "N"], key=f"trust_center_enforcement")

        if enforce_tc_findings.lower() == "y":
            st.write("")
            st.write(
                """Please select one or more Trust Center Scanners to enforce to control access to the native app.
                """
            )
            st.write("")
            st.warning("NOTE: Please do not edit the details below")
            st.write("")
            
            tc_scanners_list = pd.DataFrame(session.sql(f"""SELECT IFF((ID IN (SELECT SCANNER_ID FROM P_{st.session_state.app_code}_ACF_DB.TRUST_CENTER.SCANNERS)), True, False)
                                                            , * FROM SNOWFLAKE.TRUST_CENTER.SCANNERS ORDER BY ID, TRY_TO_DOUBLE(NAME);""").collect()).values.tolist()
            
            #create a dataframe from list_eligible_tasks
            tc_scanners_clmns = ['Select'
                                     ,'Name'
                                     ,'ID'
                                     ,'Description (Short)'
                                     ,'Description'
                                     ,'Scanner Package ID'
                                     ,'State'
                                     ,'Schedule'
                                     ,'Last Scan Timestamp'
                                    ]
            
            df_tc_scanners = pd.DataFrame(tc_scanners_list, columns = tc_scanners_clmns)
            
            #dynamically set data_editor height, based on number of rows in data frame
            de_scanner_height = int((len(df_tc_scanners) + 1.5) * 35 + 3.5)
            
            #de_tc_scanners = st.data_editor(
            de_tc_scanners = st.data_editor(
                df_tc_scanners
                ,height=de_scanner_height
                ,width=1500
                ,disabled = False
                ,use_container_width=False
                ,num_rows="fixed"
            )

            df_selected_tc_scanners = de_tc_scanners.query('Select == True')
            df_unselected_tc_scanners = de_tc_scanners.query('Select == False')
            
            if True in set(de_tc_scanners['Select']):
                flag_scanners_selected = True
                
            st.write("")
            tc_lookback_window = pd.DataFrame(session.sql(f"""SELECT value 
                                                          FROM P_{st.session_state.app_code}_ACF_DB.METADATA.METADATA 
                                                          WHERE account_locator = 'global' 
                                                          AND key = 'trust_center_lookback_in_days'""").collect()).iloc[0,0]
            txt_tc_lookback_window = st.text_input("Please enter the number of days (as a digit >= 0) to look back for findings:", f"{tc_lookback_window}", help="The current default lookback window is pre-populated from the METADATA table.")
            
            if txt_tc_lookback_window.isdigit() == False:
                st.write("")
                st.error("ERROR: Please enter a digit (integer) greater than zero.")
                st.write("")
        
        if enforce_tc_findings.lower() == "n" or (enforce_tc_findings.lower() == "y" and flag_scanners_selected == True and txt_tc_lookback_window.isdigit()):
            flag_disable_scanners_btn = False

        st.write("#")
        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.55], gap="small")

        with col1:
            st.button("Home", key="new_app_package_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="new_app_package_back", type="primary", on_click=set_page,args=("manage_app",)) 
        with col4:
            btn_scanners = st.button("Update", key="trust_center_add_scanners", type="primary", disabled = flag_disable_scanners_btn)
            
        if btn_scanners:
            with st.spinner("Updating..."):
                #update trust_center_enforcement flag
                session.sql(f"""UPDATE P_{st.session_state.app_code}_ACF_DB.METADATA.METADATA 
                                SET value = '{enforce_tc_findings}' WHERE LOWER(key) = 'trust_center_enforcement'""").collect()

                #add any scanners to the SCANNERS table
                if enforce_tc_findings.lower() == "y":
                    #update trust_center_lookback_in_days flag
                    session.sql(f"""UPDATE P_{st.session_state.app_code}_ACF_DB.METADATA.METADATA 
                                    SET value = '{txt_tc_lookback_window}' WHERE LOWER(key) = 'trust_center_lookback_in_days'""").collect()
                
                    for index, row in df_selected_tc_scanners.iterrows():
                        scanner_package_id = row["Scanner Package ID"]
                        scanner_id = row["ID"]
                        scanner_name = row["Name"]
                        scanner_description = row["Description"]
    
                        #insert selected scanner info
                        session.sql(f"""MERGE INTO P_{st.session_state.app_code}_ACF_DB.TRUST_CENTER.SCANNERS s USING 
                                        (SELECT
                                            '{scanner_package_id}' SCANNER_PACKAGE_ID
                                            ,'{scanner_id}' SCANNER_ID
                                            ,'{scanner_name}' SCANNER_NAME
                                            ,$${scanner_description}$$ SCANNER_DESCRIPTION
                                        ) AS ns
                                    ON 
                                        LOWER(s.SCANNER_ID) = LOWER(ns.SCANNER_ID)
                                    WHEN MATCHED THEN UPDATE SET 
                                        s.SCANNER_PACKAGE_ID = ns.SCANNER_PACKAGE_ID
                                        ,s.SCANNER_NAME = ns.SCANNER_NAME
                                        ,s.SCANNER_DESCRIPTION = ns.SCANNER_DESCRIPTION
                                    WHEN NOT MATCHED THEN INSERT (SCANNER_PACKAGE_ID, SCANNER_ID, SCANNER_NAME, SCANNER_DESCRIPTION) VALUES 
                                        (
                                            ns.SCANNER_PACKAGE_ID
                                            ,ns.SCANNER_ID
                                            ,ns.SCANNER_NAME
                                            ,ns.SCANNER_DESCRIPTION
                                        )""").collect()

                    for index, row in df_unselected_tc_scanners.iterrows():
                        scanner_package_id = row["Scanner Package ID"]
                        scanner_id = row["ID"]
                        scanner_name = row["Name"]
                        scanner_description = row["Description"]
    
                        #insert selected scanner info
                        session.sql(f"""MERGE INTO P_{st.session_state.app_code}_ACF_DB.TRUST_CENTER.SCANNERS s USING 
                                        (SELECT
                                            '{scanner_package_id}' SCANNER_PACKAGE_ID
                                            ,'{scanner_id}' SCANNER_ID
                                            ,'{scanner_name}' SCANNER_NAME
                                            ,$${scanner_description}$$ SCANNER_DESCRIPTION
                                        ) AS ns
                                    ON 
                                        LOWER(s.SCANNER_ID) = LOWER(ns.SCANNER_ID)
                                    WHEN MATCHED THEN DELETE""").collect()
                    
            st.success(f"Trust Center settings successfully updated 🎉")
            time.sleep(2)
            set_page("home")
            st.rerun()
            
    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))



########################################################################### Manage Consumers #################################################################################################

class manage_consumers_page(BasePage):
    def __init__(self):
        self.name="manage_consumers"
    def print_page(self):
        super().print_page()

        st.session_state.layout="centered"

        st.write("#")
        
        st.write(
            """Use the following options below to manage **Consumers** of this app.
            """
        )
        st.write("#")

        col1, col2 = st.columns(2, gap="small")
        
        with col1:
           st.subheader("Onboard Consumer")
           oc_col1, oc_col2, oc_col3 = st.columns([0.5,2.5,0.5], gap="small")
           with oc_col2:
               render_image_menu("img/onboard_consumer.png") 
           st.markdown("""
                        Click the button below to onboard a new \n
                        Consumer
                        """)
           st.write("")
           st.button("Onboard", key="onboard_consumer_button", type="primary", on_click=set_page,args=("onboard_consumer",))

        with col2:
           st.subheader("Consumer Controls")
           mc_col1, mc_col2, mc_col3 = st.columns([0.5,2.5,0.5], gap="small")
           with mc_col2:
               render_image_menu("img/controls.png")  
           st.markdown("""
                        Click the button below to manage Consumer-\n
                        specific app controls
                        """)
           st.write("")
           st.button("Manage", key="manage_consumer_controls_button", type="primary", on_click=set_page,args=("manage_consumer_controls",)) 

        st.write("#")
        col1, col2 = st.columns(2, gap="small")
        
        with col1:
           st.subheader("Re-enable Consumer")
           rec_col1, rec_col2, rec_col3 = st.columns([0.5,2.5,0.5], gap="small")
           with rec_col2:
               render_image_menu("img/manage_consumer.png")  
           st.markdown("""
                        Click the button below to re-enable a \n
                        Consumer
                        """)
           st.write("")  
           st.button("Re-enable", key="re-enable_consumer_button", type="primary", on_click=set_page,args=("re_enable_consumer",))

        with col2:
           st.subheader("Remove Consumer")
           rc_col1, rc_col2, rc_col3 = st.columns([0.5,2.5,0.5], gap="small")
           with rc_col2:
               render_image_menu("img/remove_consumer.png")  
           st.markdown("""
                        Click the button below to remove a \n
                        Consumer
                        """)
           st.write("")  
           st.button("Remove", key="remove_consumer_button", type="primary", on_click=set_page,args=("remove_consumer",))

        st.write("#")
        st.write("#")
        
        col1, col2, col3, col4 = st.columns([1,2.25,0.5,0.525], gap="small")

        with col1:
            st.button("Home", key="manage_consumers_home", type="primary", on_click=set_page,args=("home",)) 


    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Onboard Consumer

class onboard_consumer_page(BasePage):
    def __init__(self):
        self.name="onboard_consumer"
        
    def print_page(self):
        super().print_page()

        if "onboard_consumer_counter" not in st.session_state:
            st.session_state.onboard_consumer_counter = 0 

        if "onboard_consumer_list" not in st.session_state:
            st.session_state.onboard_consumer_list = {}

        onboard_consumer_enabled = False
        onboard_flag = False 

        st.header("Onboard Consumer")
        st.markdown("Onboard a new Consumer, specifying the Consumer's Snowflake Account, Name, and any control default values to override.")

        #st.write("#")
        #st.write("#")

        col1, col2, col3, col4, col5, col6 = st.columns([1,1,1,1,1,1.065], gap="small")

        with col6:
            new_consumer = st.button("\+ Consumer", key="onboard_add_consumer", type="primary")

        if new_consumer:
            st.session_state.onboard_consumer_counter += 1

        for i in range(st.session_state.onboard_consumer_counter):
            if f"onboard_consumer_{i+1}_control_counter" not in st.session_state:
                st.session_state[f"onboard_consumer_{i+1}_control_counter"] = 0

            if f"onboard_consumer_{i+1}_params" not in st.session_state.onboard_consumer_list:
                st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"] = {}
            
            if i+1 < st.session_state.onboard_consumer_counter:
                st.session_state[f"onboard_consumer_{i+1}_container_expanded"] = False
            else:
                st.session_state[f"onboard_consumer_{i+1}_container_expanded"] = True

            if f"onboard_consumer_{i+1}_account" not in st.session_state:
                st.session_state[f"onboard_consumer_{i+1}_account"] = ""

            if f"onboard_consumer_{i+1}_name" not in st.session_state:
                st.session_state[f"onboard_consumer_{i+1}_name"] = ""
    
            consumer_container = st.expander(label=f"Consumer {i+1}", expanded=st.session_state[f"onboard_consumer_{i+1}_container_expanded"])
                
            with consumer_container:
                if i+1 > 1:
                    btn_remove_consumer = st.button("\- Consumer", key=f"onboard_remove_consumer_{i+1}", type="primary")

                    if btn_remove_consumer:
                        st.session_state.onboard_consumer_counter -= 1
                        st.rerun()

                st.session_state[f"onboard_consumer_{i+1}_account"] = st.text_input("Consumer Account:", key=f"oc_{i+1}_acct")
                st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"].update({"consumer_account": st.session_state[f"onboard_consumer_{i+1}_account"]})
            
                st.session_state[f"onboard_consumer_{i+1}_name"] = st.text_input("Consumer Name:", "ENT_", key=f"oc_{i+1}_name")
                st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"].update({"consumer_name": st.session_state[f"onboard_consumer_{i+1}_name"]})

                if "control_overrides" not in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]:
                    st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"] = {}

                if "deselect_controls" not in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]:
                    st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"] = {}


                #choose controls to override
                controls = pd.DataFrame(session.sql("SELECT control_name, default_value FROM METADATA.METADATA_DICTIONARY WHERE set_via_onboard = TRUE").collect())
 
                control_list = []
                if not controls.empty:
                    control_list = st.multiselect("Select Controls to Override Defaults:", options=controls["CONTROL_NAME"], key=f"oc_{i+1}_override_control")

                for c in control_list:
                    dv_df = controls.loc[controls["CONTROL_NAME"] == f"{c}"].iloc[:,1]
                    dv = dv_df.at[dv_df.index[0]]
                    
                    if (c,dv) not in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"].items():
                        st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"].update({c:dv})

                    if (c,dv) in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"].items():
                        del st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"][c]

                for s in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"]:
                    if s not in control_list:
                        if not control_list and (not st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"]):
                            st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"] = {}
                        else:
                            if s not in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"]:
                                st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"].update({s:st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"][s]})

                for d in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"]:
                    if len(st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["deselect_controls"]) > 0:
                        if st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"] and d in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"]:
                            del st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"][d]

                col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")

                with col4:
                    diagnostics = st.checkbox("Diagnostic Mode: ", key=f"onboard_consumer{i+1}_diagnostics_mode")

                st.write("#")
                st.subheader("Selected Controls:")

                for sc in st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"]:  
                    if f"onboard_consumer_{i+1}_control_{sc}_override_value" not in st.session_state:
                        st.session_state[f"onboard_consumer_{i+1}_control_{sc}_override_value"] = ""
                    
                    col1, col2 = st.columns(2, gap="small")
                
                    with col1:
                        st.text_input("Control:", sc, key=f"consumer_{i+1}_{sc}", disabled=True)
                    with col2:
                        prefix = ""
                        if st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"][sc] == "":
                            prefix = "<No Default Value>"  
                        else:
                            prefix = "Default: "

                        st.session_state[f"onboard_consumer_{i+1}_{sc}_override_value"] = st.text_input("New Value:", key=f"consumer_1_{i+1}_{sc}_override", placeholder=prefix+st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"][sc])

                        st.session_state["onboard_consumer_list"][f"onboard_consumer_{i+1}_params"]["control_overrides"].update({sc:st.session_state[f"onboard_consumer_{i+1}_{sc}_override_value"]})


                if st.session_state[f"onboard_consumer_{i+1}_account"] != "" and st.session_state[f"onboard_consumer_{i+1}_name"] != "":
                    onboard_consumer_enabled = True

                if diagnostics:
                    st.write(st.session_state["onboard_consumer_list"])
                        
        st.write("#")
        st.write("#")                

        col1, col2, col3, col4 = st.columns([1,1.5,0.5,0.525], gap="small")

        with col1:
            st.button("Home", key="onboard_consumer_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="onboard_consumer_back", type="primary", on_click=set_page,args=("manage_consumers",)) 
        with col4:
            if onboard_consumer_enabled == False:
                btn_onboard_consumer = st.button("Onboard", key="onboard_consumer_onboard", type="primary", disabled=True)
            else:
                btn_onboard_consumer = st.button("Onboard", key="onboard_consumer_onboard", type="primary")

            if btn_onboard_consumer:
                    onboard_flag = True

        if onboard_flag:
            with st.spinner("Updating..."):
                onboard_consumer(st.session_state.app_code,st.session_state["onboard_consumer_list"])
            st.success(f"Consumer(s) onboarded successfully 🎉")
            time.sleep(3)
            set_page("home")
            st.rerun()  

            
        

    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Manage Consumer Controls

class manage_consumer_controls_page(BasePage):
    def __init__(self):
        self.name="manage_consumer_controls"
        
    def print_page(self):
        super().print_page()

        #set session parameters
        if "manage_consumers" not in st.session_state:
           st.session_state["manage_consumers"] = {} 
        
        if "consumer_controls" not in st.session_state.manage_consumers:
            st.session_state["manage_consumers"]["consumer_controls"] = {}

        if "deselect_controls" not in st.session_state.manage_consumers:
            st.session_state["manage_consumers"]["deselect_controls"] = {}

        if "master_consumer_list" not in st.session_state.manage_consumers:
            st.session_state["manage_consumers"]["master_consumer_list"] = []

        if "selected_managed_consumer" not in st.session_state:
            st.session_state.selected_managed_consumer = ""

        
        mangage_consumer_controls_enabled = False
        apply_consumers = False
        update_consumer_controls_flag = False

        st.header("Manage Consumer Controls")
        st.markdown("Update any of the control values set in the METADATA table for a single Consumer or group of Consumers.")

        st.write("#")
        #st.write("#")

        #choose consumers
        consumers_df = pd.DataFrame(session.sql("SELECT DISTINCT account_locator, consumer_name FROM METADATA.METADATA WHERE UPPER(account_locator) != 'GLOBAL'").collect())
        controls_df = pd.DataFrame()

        if consumers_df.empty:
            st.caption("There are no consumers.")
        
        if not consumers_df.empty:
            st.session_state.selected_managed_consumer = st.selectbox("Select Consumer:", consumers_df["CONSUMER_NAME"], key="manage_consumer_controls_select_consumer")                
            controls_df = pd.DataFrame(session.sql(f"SELECT key, value FROM METADATA.METADATA WHERE UPPER(consumer_name) = UPPER('{st.session_state.selected_managed_consumer}')").collect())

        control_list = []
        if not controls_df.empty:
            control_list = st.multiselect("Select Consumer Controls to Update:", options=controls_df["KEY"], key=f"mc_control_update")

        for c in control_list:
            v_df = controls_df.loc[controls_df["KEY"] == f"{c}"].iloc[:,1]
            v = v_df.at[v_df.index[0]]

            if (c,v) not in st.session_state["manage_consumers"]["consumer_controls"].items():
                st.session_state["manage_consumers"]["consumer_controls"].update({c:v})

            if (c,v) in st.session_state["manage_consumers"]["deselect_controls"].items():
                del st.session_state["manage_consumers"]["deselect_controls"][c]

        for s in st.session_state["manage_consumers"]["consumer_controls"]:
            if s not in control_list:
                if not control_list and (not st.session_state["manage_consumers"]["deselect_controls"]):
                    st.session_state["manage_consumers"]["consumer_controls"] = {}
                else:
                    if s not in st.session_state["manage_consumers"]["deselect_controls"]:
                        st.session_state["manage_consumers"]["deselect_controls"].update({s:st.session_state["manage_consumers"]["consumer_controls"][s]})

        for d in st.session_state["manage_consumers"]["deselect_controls"]:
            if len(st.session_state["manage_consumers"]["deselect_controls"]) > 0:
                if st.session_state["manage_consumers"]["consumer_controls"] and d in st.session_state["manage_consumers"]["consumer_controls"]:
                    del st.session_state["manage_consumers"]["consumer_controls"][d]
        

        col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")

        with col4:
            diagnostics = st.checkbox("Diagnostic Mode: ", key="manage_consumer_controls_diagnostics_mode")
        
        st.write("#")
        st.subheader("Selected Controls:")

        for sc in st.session_state["manage_consumers"]["consumer_controls"]:  
            if f"manage_consumer_control_{sc}_update_value" not in st.session_state:
                st.session_state[f"manage_consumer_control_{sc}_update_value"] = ""
            
            col1, col2, col3 = st.columns(3, gap="small")
        
            with col1:
                st.text_input("Control:", sc, key=f"manage_consumer_control_{sc}", disabled=True)
            with col2:
                prefix = ""
                if st.session_state["manage_consumers"]["consumer_controls"][sc] == "":
                    prefix = "<No Current Value>"  
                else:
                    prefix = "Current: "

                if sc.lower() == 'custom_attributes':
                    st.session_state[f"manage_consumer_control_{sc}_update_value"] = st.text_area("New Value:", key=f"consumer_control_{sc}_updates", placeholder=prefix+st.session_state["manage_consumers"]["consumer_controls"][sc])
                else:
                    st.session_state[f"manage_consumer_control_{sc}_update_value"] = st.text_input("New Value:", key=f"consumer_control_{sc}_updates", placeholder=prefix+st.session_state["manage_consumers"]["consumer_controls"][sc])

                if st.session_state[f"manage_consumer_control_{sc}_update_value"] == "":
                    st.session_state[f"manage_consumer_control_{sc}_update_value"] = st.session_state["manage_consumers"]["consumer_controls"][sc]

                st.session_state["manage_consumers"]["consumer_controls"].update({sc:st.session_state[f"manage_consumer_control_{sc}_update_value"]})
            with col3:
                st.write("#")
                clear_value = st.checkbox("Clear Value? ", key=f"manage_consumer_controls_clear_{sc}_value", help="Checking this box removes the current value stored for this control and overrides any new value entered")
                if clear_value:
                    st.session_state[f"manage_consumer_control_{sc}_update_value"] = ""
                    st.session_state["manage_consumers"]["consumer_controls"].update({sc:st.session_state[f"manage_consumer_control_{sc}_update_value"]})
        
        st.write("#")

        col1, col2, col3, col4 = st.columns([1,2.5,1.30,1.05], gap="small")

        with col4:
            if st.session_state["manage_consumers"]["consumer_controls"]:
                apply_consumers = st.checkbox("Apply to Other Consumers  ", key="manage_consumer_controls_apply_consumers")

        
        if apply_consumers:
            update_consumer_container = st.container()
            
            all_consumers = st.checkbox("Select all", key="manage_consumer_controls_all_consumers")

            consumer_df_filtered = consumers_df[consumers_df['CONSUMER_NAME'] != st.session_state.selected_managed_consumer]

            if all_consumers:
                update_consumers_list = update_consumer_container.multiselect("Select Consumer(s):", options=consumer_df_filtered["CONSUMER_NAME"], default=consumer_df_filtered["CONSUMER_NAME"],  key="manage_consumer_controls_select_apply_consumers")
            else:
                update_consumers_list = update_consumer_container.multiselect("Select Consumer(s):", options=consumer_df_filtered["CONSUMER_NAME"],  key="manage_consumer_controls_select_apply_consumers")

            st.session_state["manage_consumers"]["master_consumer_list"] = update_consumers_list


        if st.session_state.selected_managed_consumer not in st.session_state["manage_consumers"]["master_consumer_list"]:
            st.session_state["manage_consumers"]["master_consumer_list"].append(st.session_state.selected_managed_consumer)
        
        if st.session_state["manage_consumers"]["consumer_controls"]:
            mangage_consumer_controls_enabled = True

        
        st.write("#")
        st.write("#")
        
        if diagnostics:
            st.write(st.session_state["manage_consumers"])
                        
        st.write("#")
        st.write("#")                

        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.648], gap="small")

        with col1:
            st.button("Home", key="manage_consumer_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="manage_consumer_back", type="primary", on_click=set_page,args=("manage_consumers",)) 
        with col4:
            if mangage_consumer_controls_enabled == False:
                btn_manage_consumer_controls = st.button("Update", key="manage_consumer_update_controls", type="primary", disabled=True)
            else:
                btn_manage_consumer_controls = st.button("Update", key="manage_consumer_update_controls", type="primary")

            if btn_manage_consumer_controls:
                update_consumer_controls_flag = True

        if update_consumer_controls_flag:
            with st.spinner("Updating..."):
                manage_consumer_controls(st.session_state.app_code,st.session_state["manage_consumers"])
            st.success(f"Consumer control(s) updated successfully 🎉")
            time.sleep(3)
            set_page("home")
            st.rerun()  

            
        

    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Re-enable Consumer

class re_enable_consumer_page(BasePage):
    def __init__(self):
        self.name="re_enable_consumer"
        
    def print_page(self):
        super().print_page()

        #set session parameters
        if "re_enable_consumer" not in st.session_state:
           st.session_state["re_enable_consumer"] = {} 

        if "master_consumer_list" not in st.session_state.re_enable_consumer:
            st.session_state["re_enable_consumer"]["master_consumer_list"] = []

        if "comments" not in st.session_state.re_enable_consumer:
            st.session_state["re_enable_consumer"]["comments"] = ""

        
        #re_enable_consumer = False
        re_enable_consumer_flag = False

        st.header("Re-enable Consumer")
        st.markdown("Re-enable a single Consumer or group of Consumers.")

        st.write("#")
        #st.write("#")

        #choose consumers
        consumers_df = pd.DataFrame(session.sql("SELECT DISTINCT account_locator, consumer_name FROM METADATA.METADATA WHERE UPPER(account_locator) != 'GLOBAL' AND UPPER(key) = 'ENABLED' AND UPPER(value) = 'N'").collect())

        if consumers_df.empty:
            st.caption("There are no disabled consumers.")
        
        if not consumers_df.empty:
            st.session_state["re_enable_consumer"]["master_consumer_list"] = st.multiselect("Select Consumer(s):", consumers_df["CONSUMER_NAME"], key="manage_consumer_controls_select_consumer")

            if st.session_state["re_enable_consumer"]["master_consumer_list"] != []:
                re_enable_consumer_flag = True
            
            st.session_state["re_enable_consumer"]["comments"] = st.text_input("Comments (optional): ", key=f"re_enable_consumer_comments")            
        
        st.write("#")
        st.write("#")                

        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.75], gap="small")

        with col1:
            st.button("Home", key="re_enable_consumer_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="re_enable_consumer_back", type="primary", on_click=set_page,args=("manage_consumers",)) 
        with col4:
            if re_enable_consumer_flag == False:
                btn_re_enable_consumer = st.button("Re-enable", type="primary", disabled=True)
            else:
                btn_re_enable_consumer = st.button("Re-enable", type="primary")

        if btn_re_enable_consumer:
           if re_enable_consumer_flag:
                with st.spinner("Updating..."):
                    re_enable_consumer(st.session_state.app_code,st.session_state["re_enable_consumer"])
                st.success(f"Consumer(s) re-enabled successfully 🎉")
                time.sleep(3)
                set_page("home")
                st.rerun()  



    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))



########################################################################### Remove Consumer

class remove_consumer_page(BasePage):
    def __init__(self):
        self.name="remove_consumer"
        
    def print_page(self):
        super().print_page()

        #set session parameters
        if "remove_consumer" not in st.session_state:
           st.session_state["remove_consumer"] = {} 

        if "selected_consumers" not in st.session_state.remove_consumer:
            st.session_state["remove_consumer"]["selected_consumers"] = []
        
        if "master_consumer_list" not in st.session_state.remove_consumer:
            st.session_state["remove_consumer"]["master_consumer_list"] = []

        remove_consumer_flag = False

        st.header("Remove Consumer")
        st.markdown("Remove a single Consumer or group of Consumers.")

        st.write("#")
        #st.write("#")

        #choose consumers
        consumers_df = pd.DataFrame(session.sql("SELECT DISTINCT account_locator, consumer_name FROM METADATA.METADATA WHERE UPPER(account_locator) != 'GLOBAL'").collect())

        if consumers_df.empty:
            st.caption("There are no consumers to remove.")
        
        if not consumers_df.empty:
            st.session_state["remove_consumer"]["selected_consumers"] = st.multiselect("Select Consumer(s) to Remove:", options=consumers_df["CONSUMER_NAME"], key=f"remove_consumer_list")

            if st.session_state["remove_consumer"]["selected_consumers"] != []:
                remove_consumer_flag = True
            
            for c in st.session_state["remove_consumer"]["selected_consumers"]:
                a_df = consumers_df.loc[consumers_df["CONSUMER_NAME"] == f"{c}"].iloc[:,0]
                a = a_df.at[a_df.index[0]]

                if [a, c] not in st.session_state["remove_consumer"]["master_consumer_list"]:
                    st.session_state["remove_consumer"]["master_consumer_list"].append([a, c])

            for m in st.session_state["remove_consumer"]["master_consumer_list"]:
                if m[1] not in st.session_state["remove_consumer"]["selected_consumers"]:
                   st.session_state["remove_consumer"]["master_consumer_list"].remove(m) 
            
                        
        st.write("#")
        st.write("#")                

        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.648], gap="small")

        with col1:
            st.button("Home", key="remove_consumer_home", type="primary", on_click=set_page,args=("home",)) 
        with col3:
            st.button("Back", key="remove_consumer_back", type="primary", on_click=set_page,args=("manage_consumers",)) 
        with col4:
            if remove_consumer_flag == False:
                btn_remove_consumer = st.button("Remove", type="primary", disabled=True)
            else:
                btn_remove_consumer = st.button("Remove", type="primary")

        if btn_remove_consumer:
           if remove_consumer_flag:
                with st.spinner("Updating..."):
                    remove_consumer(st.session_state.app_code,st.session_state["remove_consumer"])
                st.success(f"Consumer(s) removed successfully 🎉")
                time.sleep(3)
                set_page("home")
                st.rerun()  



    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))


########################################################################### Remove App #################################################################################################

class remove_acf_page(BasePage):
    def __init__(self):
        self.name="remove_acf"
    def print_page(self):
        super().print_page()

        st.session_state.layout="centered"

        remove_acf_flag = False

        st.write("#")

        conf = st.text_input(f'Type: :red[ **{st.session_state.app_code}**] to confirm removal. ')

        if conf == st.session_state.app_code:
            btn_remove_acf = st.button("Remove", key="remove_acf", type="primary")
            remove_acf_flag = True
        else:
            btn_remove_acf = st.button("Remove", key="remove_acf", type="primary", disabled=True)

        if btn_remove_acf:
           if remove_acf_flag:
                with st.spinner("Updating..."):
                    remove_acf(st.session_state.app_code)
                st.success(f"App removed successfully 🎉")
                st.rerun() 

        st.write("#")
        st.write("#")
        
        col1, col2, col3, col4 = st.columns([1,2.5,0.5,0.5], gap="small")

        with col1:
            st.button("Cancel", key="remove_acf_cancel", type="primary", on_click=set_page,args=("home",)) 


    def print_sidebar(self):
        pass
        # with st.sidebar:
        #     st.button("go to page two",key='sidebar button', on_click=set_page,args=('two',))



############################################################################## Main ####################################################################################################


if "page" not in st.session_state:
    st.session_state.page="home"

pages = [app_control_home()
         ,manage_app_page(),manage_app_controls_page(),manage_app_rules_page(),app_package_page(),new_app_package_page(),app_package_version_page(),promote_to_prod_page(),app_package_drop_page(),trust_center_page()
         ,manage_consumers_page(),onboard_consumer_page(),manage_consumer_controls_page(),re_enable_consumer_page(),remove_consumer_page()
         ,remove_acf_page(),]

session = get_active_session()

def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page();
            page.print_sidebar();

main()