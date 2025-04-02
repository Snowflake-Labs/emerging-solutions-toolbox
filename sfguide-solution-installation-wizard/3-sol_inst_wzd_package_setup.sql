/*************************************************************************************************************
Script:             Solution Installation Wizard Package Setup
Create Date:        2024-03-26
Author:             B. Klein
Description:        Package setup for Solution Installation Wizard
Copyright © 2024 Snowflake Inc. All rights reserved
**************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-26          B. Klein                            Initial Creation
*************************************************************************************************************/

/* role selection */
use role sol_inst_wzd_role;
call system$wait(10);
use warehouse sol_inst_wzd_wh;

/*
    we intentionally do not grant the scripts table to the share
    instead, the scripts table will be recreated in the setup_script for the app
    this is to prevent adding dynamic code to the native app, which is flagged by the security scan
*/

/* create provider application database code repository - this database isn't necessary if automated by CI/CD tools, but helps with deployment via SQL */
create or replace database sol_inst_wzd_deployment_db comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';
create or replace schema sol_inst_wzd_deployment_db.code comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';
create or replace stage sol_inst_wzd_deployment_db.code.files_v1_p0 comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';
drop schema sol_inst_wzd_deployment_db.public;

/* helper stored procedure to help create the objects */
create or replace procedure sol_inst_wzd_deployment_db.code.put_to_stage(stage varchar,filename varchar, content varchar)
returns string
language python
runtime_version=3.10
packages=('snowflake-snowpark-python')
handler='put_to_stage'
comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}'
AS $$
import io
import os

def put_to_stage(session, stage, filename, content):
    local_path = '/tmp'
    local_file = os.path.join(local_path, filename)
    f = open(local_file, "w", encoding='utf-8')
    f.write(content)
    f.close()
    session.file.put(local_file, '@'+stage, auto_compress=False, overwrite=True)
    return "saved file "+filename+" in stage "+stage
$$;

create or replace table sol_inst_wzd_deployment_db.code.script (
	name varchar,
	script varchar(16777216)
);

/* defaults to request create database, since that is the most common and powerful way to create apps in a consumer's account */
insert into sol_inst_wzd_deployment_db.code.script (name , script)
values ('MANIFEST_V1',
$$
manifest_version: 1 # required
version:
  name: solution_installation_wizard
  label: “Solution Installation Wizard”
  comment: “Lets consumers deploy solutions into their environments”

artifacts:
  readme: README.md
  setup_script: script/setup_script.sql
  default_streamlit: app_shared.sol_inst_wzd_streamlit
  extension_code: true

privileges:
- CREATE DATABASE:
    description: "To create database objects related to the app"

$$)
;

/* readme for the app, accessible to the consumer */
insert into sol_inst_wzd_deployment_db.code.script (name , script)
values ( 'README_V1',$$
# Solution Installation Wizard

The Solution Installation Wizard app is designed to allow a consumer to deploy provider-defined solutions into their environment, typically including a share back to the original provider/super consumer.

$$);

/* add environment file for streamlit - includes all references to libraries that the Streamlit needs */
insert into sol_inst_wzd_deployment_db.code.script (name , script)
values ( 'ENVIRONMENT_V1',$$
name: sf_env
channels:
- snowflake
dependencies:
- snowflake-native-apps-permission

$$);

/* the scripts */
select listagg('insert into app_shared.script (workflow_name, workflow_description, script_name, script_order, is_autorun, is_autorun_code_visible, script_description, script_text) select $$' || workflow_name || '$$,$$' || workflow_description || '$$,$$' || script_name || '$$,' || script_order || ',' || is_autorun || ',' || is_autorun_code_visible || ',$$' || script_description || '$$,regexp_replace($$' || regexp_replace(script_text,'\\${2}',':::') || '$$,\':::\',\'$$\');', '\n')
from sol_inst_wzd_package.admin.script;

/* the streamlit code itself */
insert into sol_inst_wzd_deployment_db.code.script SELECT 'STREAMLIT_V1' AS NAME, REGEXP_REPLACE($$
"""
NOTE - this file is for testing, and will not be automatically tied into the app
To add it to the app, you must copy/paste it into the relevant insert statement in provider_app_setup.sql
"""

from snowflake.snowpark.context import get_active_session
import streamlit as st
from abc import ABC, abstractmethod
import io
import os
import re


# Check snowflake connection type
def set_session():
    try:
        import snowflake.permissions as permissions

        session = get_active_session()

        # will fail for a non native app
        privilege_check = permissions.get_held_account_privileges(["EXECUTE TASK"])

        st.session_state["streamlit_mode"] = "NativeApp"
    except:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account_1")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Wide mode
st.set_page_config(layout="wide")

# Initiate session
session = set_session()


class SnowflakeScriptRunner:
    """
    A class used to prepare and run a set of Snowflake scripts
    To use:
        - instantiate the class
        - run a prepare_ method to populate attributes
        - run an execute_ method to generate or run scripts

    Attributes are populated by prepare_ methods
    """

    def __init__(self):
        self.is_autorun = None
        self.original_script = None
        self.script_conn = None
        self.check_words = []
        self.replace_words = []
        self.prepared_script = None
        self.cleaned_script = None

    def execute(self):
        """
        Runs a series of SQL scripts with some automated replacements
        """
        if self.is_autorun is None or self.original_script is None:
            print("Run a prepare method first!")
        else:
            # Prepare scripts
            comment_code_regex_slash = re.compile(r"(?<!:)//.*")
            comment_code_regex_dash = re.compile(r"(?<!:)--.*")
            comment_code_regex_multi = re.compile(r"(?<!:)(/\*)(.|\n)*?(\*/)")
            snowsql_code_regex = re.compile(r"^(?<!:)!.*")
            semicolon_not_in_text_block_regex = re.compile(r";(?!\n\$)(?!`)(?!\n\n(snow|ret))(?!')")
            original_script = self.original_script
            script_conn = self.script_conn

            self.prepared_script = None
            self.cleaned_script = None

            print("Starting script")
            script_stream_original = io.StringIO(original_script)
            script_stream_no_multi = io.StringIO(re.sub(comment_code_regex_multi, '', original_script))
            prepared_script_text = ""
            cleaned_script_text = ""

            # Prepared scripts still contain comments
            for line in script_stream_original:
                # Replace values
                for check, replace in zip(self.check_words, self.replace_words):
                    line = line.replace(check, replace)

                # Remove SnowSQL lines to enable easier running in worksheets
                line = re.sub(snowsql_code_regex, '', line)

                prepared_script_text += line

            for line in script_stream_no_multi:
                # Replace values
                for check, replace in zip(self.check_words, self.replace_words):
                    line = line.replace(check, replace)

                # Remove SnowSQL lines to enable easier running in worksheets
                line = re.sub(snowsql_code_regex, '', line)

                # Remove commented SQL lines
                line = re.sub(comment_code_regex_slash, '', line)
                line = re.sub(comment_code_regex_dash, '', line)

                cleaned_script_text += line

            self.prepared_script = prepared_script_text
            self.cleaned_script = cleaned_script_text

            if self.is_autorun and script_conn is not None:
                print("Running statements for script")

                # Get statement list
                statement_list = re.split(semicolon_not_in_text_block_regex, cleaned_script_text)

                # Remove nones
                for j, item in enumerate(statement_list):
                    if not item:
                        statement_list.remove(item)

                # Strip empty space
                for j, item in enumerate(statement_list):
                    statement_list[j] = item.strip()

                # Remove blanks
                for j, item in enumerate(statement_list):
                    if item == "":
                        statement_list.remove(item)

                # Re-add semicolon
                for j, item in enumerate(statement_list):
                    statement_list[j] = statement_list[j] + ";"

                # Run statements
                for statement in statement_list:
                    script_conn.sql(statement).collect()
            else:
                print("Guided mode: Script generated but not run")

    def prepare(self, session, is_autorun, original_script, placeholders):
        """
        Prepares scripts to be run manually
        """
        check_words = [':::']
        replace_words = ['===']

        for key, value in placeholders.items():
            check_words.append(key)
            replace_words.append(value)

        self.is_autorun = is_autorun
        self.original_script = original_script
        self.session = session
        self.check_words = check_words
        self.replace_words = replace_words


snowflake_script_runner = SnowflakeScriptRunner()

# Retrieve some base account information for consumer and provider
if "current_organization_name" not in st.session_state:
    st.session_state.current_organization_name = session.sql("select current_organization_name()").collect()[0][0]

if "current_account_name" not in st.session_state:
    st.session_state.current_account_name = session.sql("select current_account_name()").collect()[0][0]

if "current_account_locator" not in st.session_state:
    st.session_state.current_account_locator = session.sql("select current_account()").collect()[0][0]

if "provider_organization_name" not in st.session_state:
    st.session_state.provider_organization_name = \
        session.sql("select provider_organization_name from app_shared.provider_account_identifier").collect()[0][0]

if "provider_account_name" not in st.session_state:
    st.session_state.provider_account_name = \
        session.sql("select provider_account_name from app_shared.provider_account_identifier").collect()[0][0]

if "provider_account_locator" not in st.session_state:
    st.session_state.provider_account_locator = \
        session.sql("select provider_account_locator from app_shared.provider_account_identifier").collect()[0][0]

if "workflows" not in st.session_state:
    workflows = []
    for row in session.sql("select distinct workflow_name, workflow_description from "
                           "app_shared.script").to_local_iterator():
        workflows.append([row[0], row[1]])

    st.session_state.workflows = workflows

if "placeholders" not in st.session_state:
    placeholders = {}
    for row in session.sql("select placeholder_text, replacement_value from app_shared.placeholder_definition where "
                           "consumer_organization_name = current_organization_name() and consumer_account_name = "
                           "current_account_name()").to_local_iterator():
        placeholders[row['PLACEHOLDER_TEXT']] = row['REPLACEMENT_VALUE']

    st.session_state.placeholders = placeholders

# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "Overview"


# Sets the page based on page name
def set_page(page: str):
    st.session_state.page = page


# Default sidebar used for every page
def set_default_sidebar():
    with st.sidebar:
        st.title("Solution Installation Wizard")
        st.markdown("")
        st.markdown("This application helps consumers deploy apps from a provider in a secure, transparent way.")
        st.markdown("Typically, the deployed app is meant to be listed back to the original provider.")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        if st.button(label="Return Home", help="Warning: Progress will be lost!"):
            # reset pages to default to ensure old workflow pages are cleared
            pages = [OverviewPage()]
            st.session_state.pages = pages
            set_page('Overview')
            st.experimental_rerun()


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


# used to set the name of the dynamic pages
def Page_init(self, name):
    self.name = name


class OverviewPage(Page):
    def __init__(self):
        self.name = "Overview"

    def print_page(self):
        st.title("Welcome!")
        st.header("Solution Installation Wizard")

        st.subheader("Please select a solution, hit continue, and follow the steps")

        st.write("")

        current_row = st.columns(3)

        for i, workflow_list in enumerate(st.session_state.workflows):
            workflow_name = workflow_list[0]
            workflow_description = workflow_list[1]

            # create a new row for every third workflow
            i_mod_col_count = i % 3

            if i_mod_col_count == 0 and i != 0:
                current_row = st.columns(3)

            tile = current_row[i_mod_col_count].container()
            tile.subheader(workflow_name)
            tile.subheader("❄️")
            tile.caption(workflow_description)
            if tile.button("Launch Deployment 🚀", key=workflow_name):
                launch_workflow(workflow_name)

    def print_sidebar(self):
        set_default_sidebar()


def set_dynamic_page(progress_percent, next_page, script_name, script_order, is_autorun,
                     is_autorun_code_visible, script_description, script_text):
    st.title("Step " + str(script_order) + " - " + script_name)
    st.progress(progress_percent)
    st.subheader("Step Description: " + script_description)

    placeholders = st.session_state.placeholders

    snowflake_script_runner.prepare(session, False, script_text, placeholders)
    snowflake_script_runner.execute()

    prepared_script = snowflake_script_runner.prepared_script

    if is_autorun:
        # Check permissions, prompt if necessary
        if st.session_state["streamlit_mode"] == "NativeApp":
            import snowflake.permissions as permissions

            if not permissions.get_held_account_privileges(["CREATE DATABASE"]):
                permissions.request_account_privileges(["CREATE DATABASE"])

        if is_autorun_code_visible:
            with st.expander("See Code"):
                st.code(prepared_script, line_numbers=True)

        with st.form("deploy_form"):
            submitted = st.form_submit_button("Run")

            if submitted:
                with st.spinner("Running script..."):
                    snowflake_script_runner.prepare(session, is_autorun, script_text, placeholders)
                    snowflake_script_runner.execute()

                st.success("Script run successfully!", icon="✅")
    else:
        st.caption("Please copy, paste, and run the following script in a SnowSight worksheet.")
        st.code(prepared_script, line_numbers=True)

    if next_page == "Overview":
        st.caption("Once run, please hit Finish to return Home")
        if st.button(label="Finish", help="Workflow complete!"):
            # reset pages to default to ensure old workflow pages are cleared
            pages = [OverviewPage()]
            st.session_state.pages = pages
            set_page('Overview')
            st.experimental_rerun()
    else:
        st.caption("Once run, please hit Next to proceed")
        st.button(label="Next", help="To the next workflow step", on_click=set_page, args=(next_page,))


def launch_workflow(selected_workflow):
    st.session_state["selected_workflow"] = selected_workflow

    script_rows = session.sql(
        "select distinct script_name, script_order, is_autorun, is_autorun_code_visible, script_description, "
        "script_text from app_shared.script where workflow_name = '" + selected_workflow + "'").collect()

    script_count = len(script_rows)

    # reset pages to default to ensure old workflow pages are cleared
    pages = [OverviewPage()]

    for i, row in enumerate(script_rows):
        index = i + 1

        page_class_name = f"Workflow{index}Page"
        page_name = f"Workflow {index}"

        progress_percent = round((index / script_count), 2)
        if progress_percent == 1:
            next_page = "Overview"
        else:
            next_page = f"Workflow {index + 1}"

        script_name = row['SCRIPT_NAME']
        script_order = row['SCRIPT_ORDER']
        is_autorun = row['IS_AUTORUN']
        is_autorun_code_visible = row['IS_AUTORUN_CODE_VISIBLE']
        script_description = row['SCRIPT_DESCRIPTION']
        script_text = row['SCRIPT_TEXT']

        dynamic_class_definition = {
            "__init__": Page_init,
            "print_page": eval("lambda self: set_dynamic_page("
                               + str(progress_percent) + ",'"
                               + next_page + "','"
                               + script_name + "',"
                               + script_order + ","
                               + str(is_autorun) + ","
                               + str(is_autorun_code_visible) + ",'''"
                               + script_description + "''','''"
                               + script_text + "''')"),
            "print_sidebar": lambda self: set_default_sidebar()
        }

        dynamic_class = type(page_class_name, (Page,), dynamic_class_definition)

        dynamic_object = dynamic_class(page_name)

        pages.append(dynamic_object)

    st.session_state.pages = pages

    set_page('Workflow 1')

    # Rerun page with set page to prevent double click
    st.experimental_rerun()


# Set starting set of pages
if "pages" not in st.session_state:
    st.session_state.pages = [OverviewPage()]

pages = st.session_state.pages


def main():
    for page in pages:
        if page.name == st.session_state.page:
            page.print_page()
            page.print_sidebar()


main()

$$,'===','$$') AS SCRIPT;

/* setup script that runs on application install/upgrade */
insert into sol_inst_wzd_deployment_db.code.script SELECT 'SETUP_V1' AS NAME, REGEXP_REPLACE($$
/*************************************************************************************************************
Script:             Solution Installation Wizard - Setup Script v1
Create Date:        2024-03-14
Author:             B. Klein
Description:        Solution Installation Wizard - Provider application setup script contains the objects that
                    the application will use when implemented on the consumer account.
Copyright © 2024 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2024-03-14          B. Klein                            Initial Creation
***************************************************************************************************************************************************/

/*** make sure this file is ready, but do not run in worksheet ***/

/* schema for sharing app objects with consumer */
create or alter versioned schema app_shared;

/* create streamlit */
create or replace streamlit app_shared.sol_inst_wzd_streamlit
  FROM '/code_artifacts/streamlit'
  MAIN_FILE = '/streamlit_ui.py'
  comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}'
;

create or replace secure view app_shared.provider_account_identifier comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}' as select * from sol_inst_wzd_package.admin.provider_account_identifier;
create or replace secure view app_shared.placeholder_definition comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}' as select * from sol_inst_wzd_package.admin.placeholder_definition;

/* Create script table */
create or replace table app_shared.script(
    workflow_name string,
    workflow_description string,
    script_name string,
    script_order string,
    is_autorun boolean,
    is_autorun_code_visible boolean,
    script_description string,
    script_text varchar(16777216)
) comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* Inserts scripts from script table here via REGEX */
SCRIPT_INSERT_LOCATOR

/* create application role for consumer access to objects and apply grants */
create application role if not exists sol_inst_wzd_app_role;
grant usage on schema app_shared to application role sol_inst_wzd_app_role;
grant usage on streamlit app_shared.sol_inst_wzd_streamlit to application role sol_inst_wzd_app_role;

/* grant read access, if relevant */
--grant select on table app_shared.provider_account_identifier to application role sol_inst_wzd_app_role;
--grant select on table app_shared.placeholder_definition to application role sol_inst_wzd_app_role;
--grant select on table app_shared.script to application role sol_inst_wzd_app_role;

$$,'SCRIPT_INSERT_LOCATOR',(
select listagg('insert into app_shared.script (workflow_name, workflow_description, script_name, script_order, is_autorun, is_autorun_code_visible, script_description, script_text) select $$' || workflow_name || '$$,$$' || workflow_description || '$$,$$' || script_name || '$$,' || script_order || ',' || is_autorun || ',' || is_autorun_code_visible || ',$$' || script_description || '$$,$$' || regexp_replace(script_text,'\\${2}',':::') || '$$;', '\n')
from sol_inst_wzd_package.admin.script
)) AS SCRIPT;


/* partial */
select listagg('insert into app_shared.script (workflow_name, workflow_description, script_name, script_order, is_autorun, is_autorun_code_visible, script_description, script_text) select $$' || workflow_name || '$$,$$' || workflow_description || '$$,$$' || script_name || '$$,' || script_order || ',' || is_autorun || ',' || is_autorun_code_visible || ',$$' || script_description || '$$,$$' || regexp_replace(script_text,'\\${2}',':::') || '$$;', '\n')
from sol_inst_wzd_package.admin.script;

/* full replacement */
select listagg('insert into app_shared.script (workflow_name, workflow_description, script_name, script_order, is_autorun, is_autorun_code_visible, script_description, script_text) select $$' || workflow_name || '$$,$$' || workflow_description || '$$,$$' || script_name || '$$,' || script_order || ',' || is_autorun || ',' || is_autorun_code_visible || ',$$' || script_description || '$$,regexp_replace($$' || regexp_replace(script_text,'\\${2}',':::') || '$$,\':::\',\'$$\');', '\n')
from sol_inst_wzd_package.admin.script;

/* put files into stage */
call sol_inst_wzd_deployment_db.code.put_to_stage('files_v1_p0','README.md', (select script from sol_inst_wzd_deployment_db.code.script where name = 'README_V1'));
call sol_inst_wzd_deployment_db.code.put_to_stage('files_v1_p0','manifest.yml',(select script from sol_inst_wzd_deployment_db.code.script where name = 'MANIFEST_V1'));
call sol_inst_wzd_deployment_db.code.put_to_stage('files_v1_p0/script','setup_script.sql', (select script from sol_inst_wzd_deployment_db.code.script where name = 'SETUP_V1'));
call sol_inst_wzd_deployment_db.code.put_to_stage('files_v1_p0/code_artifacts/streamlit','streamlit_ui.py', (select script from sol_inst_wzd_deployment_db.code.script where name = 'STREAMLIT_V1'));
call sol_inst_wzd_deployment_db.code.put_to_stage('files_v1_p0/code_artifacts/streamlit','environment.yml', (select script from sol_inst_wzd_deployment_db.code.script where name = 'ENVIRONMENT_V1'));

/* create application package version using code files */
alter application package sol_inst_wzd_package
    add version Version1 using '@sol_inst_wzd_deployment_db.code.files_v1_p0';

alter application package sol_inst_wzd_package
  set default release directive
  version = Version1
  patch = 0;

/* test creating the app locally */
drop application if exists solution_installation_wizard_app cascade;
create application solution_installation_wizard_app from application package sol_inst_wzd_package comment='{"origin":"sf_sit","name":"scad","version":{"major":1, "minor":0},"attributes":{"component":"scad"}}';

/* grant application role to role to see scripts in app metadata */
grant application role solution_installation_wizard_app.sol_inst_wzd_app_role to role sol_inst_wzd_role;

/* set external to share with accounts outside of your org - this will trigger a security scan, which *may* take up to 3 business days */
--alter application package sol_inst_wzd_package set distribution=EXTERNAL;

select 'List the app and share with a consumer' as DO_THIS_NEXT;
