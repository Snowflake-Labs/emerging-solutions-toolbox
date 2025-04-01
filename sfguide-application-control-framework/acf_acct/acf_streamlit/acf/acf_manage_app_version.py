import streamlit as st
import pandas as pd
import shutil as sh
import time as tm
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def manage_app_version(app_code, app_pkg_name, app_mode, limit_enforced, version_option, version, app_funcs_env, app_procs_env, templates_env, streamlit_env, func_list, proc_list):

  session = get_active_session()
  
  #get current date (for setup_script_template.txt file)
  current_date = pd.DataFrame(session.sql(f"SELECT CURRENT_DATE()::varchar").collect()).iloc[0,0]

  limit = ''
  limit_type = ''
  readme_template = ''
  readme = ''
  trust_center_access_cmd = ''
  trust_center_flag_cmd = ''

  #set limit based on app_mode
  if app_mode.lower() == 'free':
    limit = '5'
    limit_type = 'requests'
    readme_template = 'readme_free_template.txt'
    readme = 'readme_free.md'
  elif app_mode.lower() == 'paid':
    limit = '100000'
    limit_type = 'records'
    readme_template = 'readme_paid_template.txt'
    readme = 'readme_paid.md'
  else:
    limit = 'N/A'
    limit_type = 'N/A'
    readme_template = 'readme_enterprise_template.txt'
    readme = 'readme_enterprise.md'
    
  trust_center_enforcement = pd.DataFrame(session.sql(f"""SELECT value 
                                                          FROM P_{app_code}_ACF_DB.METADATA.METADATA 
                                                          WHERE account_locator = 'global' 
                                                          AND key = 'trust_center_enforcement'""").collect()).iloc[0,0]
  
  if trust_center_enforcement.lower() == 'y':
    trust_center_access_cmd = """
--grant native app access to the Trust Center views --REQUIRED TO ENABLE APP
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO APPLICATION IDENTIFIER($APP_NAME);
GRANT APPLICATION ROLE SNOWFLAKE.TRUST_CENTER_VIEWER TO APPLICATION IDENTIFIER($APP_NAME);
"""
    
    trust_center_flag_cmd = """
--set trust_center_access flag to 'y'
CALL PROCS_APP.TRUST_CENTER_ACCESS();
"""

  funcs_env = ''
  procs_env = ''

  if app_funcs_env.upper() == f'P_{app_code.upper()}_SOURCE_DB_DEV':
    funcs_env = 'DEV'

  if app_funcs_env.upper() == f'P_{app_code.upper()}_SOURCE_DB_PROD':
    funcs_env = 'PROD'


  if app_procs_env.upper() == f'P_{app_code.upper()}_SOURCE_DB_DEV':
    procs_env = 'DEV'

  if app_procs_env.upper() == f'P_{app_code.upper()}_SOURCE_DB_PROD':
    procs_env = 'PROD'

  #for each function get the signature, language, and body; then add to func_str
  func_str = ""
  func_log_str = ""

  for f in func_list:
     func = f[0]
     func_df = pd.DataFrame(session.sql(f"DESCRIBE FUNCTION {func}").collect())

     f_name = func.split(".")[2].split("(")[0]

     f_signature = func_df.loc[func_df['property'] == 'signature', 'value'].values[0]
     f_returns = func_df.loc[func_df['property'] == 'returns', 'value'].values[0]
     f_language = func_df.loc[func_df['property'] == 'language', 'value'].values[0]
     f_body = func_df.loc[func_df['property'] == 'body', 'value'].values[0]

     if f_language.upper() == 'JAVA':
       f_handler = func_df.loc[func_df['property'] == 'handler', 'value'].values[0]
       
       f_imports = func_df.loc[func_df['property'] == 'imports', 'value'].values[0]
       f_imports_str = ""
       
       if f_imports != '[]':
         f_imports_str = f"""IMPORTS = ({', '.join(["'/{}'".format(value.split("/")[1]) for value in f_imports.strip('][').split(",")])})"""

       
       f_target_path = func_df.loc[func_df['property'] == 'target_path', 'value'].values[0]
       f_target_path_str = ""
       
       if f_target_path != '[]':
         f_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""

       func_str += f"""CREATE OR REPLACE SECURE FUNCTION FUNCS_APP.{f_name}{f_signature}
RETURNS {f_returns}
LANGUAGE {f_language}
{f_imports_str}
HANDLER = '{f_handler}'
{f_target_path_str}
COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{f_name}","type":"function"}}}}';

"""
     elif f_language.upper() == 'PYTHON':
       f_runtime_version =  func_df.loc[func_df['property'] == 'runtime_version', 'value'].values[0] 

       f_imports = func_df.loc[func_df['property'] == 'imports', 'value'].values[0]
       f_imports_str = ""
       
       if f_imports != '[]':
         f_imports_str = f"""IMPORTS = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""


       f_handler = func_df.loc[func_df['property'] == 'handler', 'value'].values[0] 
            

       f_packages = func_df.loc[func_df['property'] == 'packages', 'value'].values[0]
       f_packages_str = ""

       if f_packages != '[]':
         f_packages_str = f"""PACKAGES = ({', '.join(["'{}'".format(value) for value in f_packages.strip('][').split(",")])})"""
       
       func_str += f"""CREATE OR REPLACE SECURE FUNCTION FUNCS_APP.{f_name}{f_signature}
RETURNS {f_returns}
LANGUAGE {f_language}
RUNTIME_VERSION = {f_runtime_version}
{f_imports_str}
HANDLER = '{f_handler}'
{f_packages_str}
COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{f_name}","type":"function"}}}}'
AS
$$
{f_body}
$$;

""" 
     elif f_language.upper() == 'SCALA':  
       f_handler = func_df.loc[func_df['property'] == 'handler', 'value'].values[0]
       
       f_imports = func_df.loc[func_df['property'] == 'imports', 'value'].values[0]
       f_imports_str = ""
       
       if f_imports != '[]':
         f_imports_str = f"""IMPORTS = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""


       f_runtime_version =  func_df.loc[func_df['property'] == 'runtime_version', 'value'].values[0] 
      

       f_packages = func_df.loc[func_df['property'] == 'packages', 'value'].values[0]
       f_packages_str = ""

       if f_packages != '[]':
         f_packages_str = f"""PACKAGES = ({', '.join(["'{}'".format(value) for value in f_packages.strip('][').split(",")])})"""     
       

       f_target_path = func_df.loc[func_df['property'] == 'target_path', 'value'].values[0]
       f_target_path_str = ""
       
       if f_target_path != '[]':
         f_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""

     
       func_str += f"""CREATE OR REPLACE SECURE FUNCTION FUNCS_APP.{f_name}{f_signature}
RETURNS {f_returns}
LANGUAGE {f_language}
{f_imports_str}
RUNTIME_VERSION = {f_runtime_version}
{f_packages_str}
{f_target_path_str}
HANDLER = '{f_handler}'
COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{f_name}","type":"function"}}}}'
AS
$$
{f_body}
$$;

"""
     else:
      func_str += f"""CREATE OR REPLACE SECURE FUNCTION FUNCS_APP.{f_name}{f_signature}
RETURNS {f_returns}
LANGUAGE {f_language}
COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{f_name}","type":"function"}}}}'
AS
$$
{f_body}
$$;

"""

     if func_list: 
      if f[1]:
        func_str += f"""GRANT USAGE ON FUNCTION FUNCS_APP.{func.split(".")[2]} TO APPLICATION ROLE APP_ROLE;
      
      """
     #add function to func_log_str    
     func_log_str += f"""snowflake.execute({{sqlText:`CALL UTIL_APP.APP_LOGGER('${{account_locator}}', '${{consumer_name}}', '${{app_key_local}}', '${{app_mode}}', 'log', 'install', '""', SYSDATE(), 'PROCESSING', '"${{current_db.toLocaleLowerCase()}}.funcs_app.{f_name} function created."');`}});
    """

  #for each procedure get the signature, language, and body; then add to func_str
  proc_str = ""
  proc_log_str = ""

  for p in proc_list:
     proc = p[0]

     p_df = pd.DataFrame(session.sql(f"DESCRIBE PROCEDURE {proc}").collect())

     p_name = proc.split(".")[2].split("(")[0]

     p_signature = p_df.loc[p_df['property'] == 'signature', 'value'].values[0]
     p_returns = p_df.loc[p_df['property'] == 'returns', 'value'].values[0]
     p_language = p_df.loc[p_df['property'] == 'language', 'value'].values[0]
     p_body = p_df.loc[p_df['property'] == 'body', 'value'].values[0]

     table_flag = "N"
     if p[1]: table_flag = "Y"
       
     proc_str += f"""INSERT INTO UTIL_APP.ALL_PROCS VALUES ('{p_name}','{p_signature}', '{table_flag}');
       
"""  

     if p_language.upper() in ['JAVA', 'SCALA']:
       p_handler = p_df.loc[p_df['property'] == 'handler', 'value'].values[0]
       
       p_imports = p_df.loc[p_df['property'] == 'imports', 'value'].values[0]
       p_imports_str = ""
       
       if p_imports != '[]':
         p_imports_str = f"""IMPORTS = ({', '.join(["'/{}'".format(value.split("/")[1]) for value in p_imports.strip('][').split(",")])})"""

       p_runtime_version =  p_df.loc[p_df['property'] == 'runtime_version', 'value'].values[0] 
       p_runtime_version_str = ""

       if p_runtime_version:
         p_runtime_version_str = f"RUNTIME_VERSION = {p_runtime_version}"

       p_packages =  p_df.loc[p_df['property'] == 'packages', 'value'].values[0] 
       p_packages_str = ""

       if p_packages != '[]':
         p_packages_str = f"""PACKAGES = ({', '.join(["'{}'".format(value) for value in p_packages.strip('][').split(",")])})"""
       
       p_target_path = p_df.loc[p_df['property'] == 'target_path', 'value'].values[0]
       p_target_path_str = ""
       
       if p_target_path != '[]':
         p_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in p_imports.strip('][').split(",")])})"""

       proc_str += f"""CREATE OR REPLACE SECURE PROCEDURE PROCS_APP.{p_name}{p_signature}
  RETURNS {p_returns}
  LANGUAGE {p_language}
  {p_runtime_version_str}
  {p_imports_str}
  HANDLER = '{p_handler}'
  {p_target_path_str}
  COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{p_name}","type":"procedure"}}}}';

"""
     elif p_language.upper() == 'PYTHON':
        p_runtime_version =  p_df.loc[p_df['property'] == 'runtime_version', 'value'].values[0] 

        p_imports = p_df.loc[p_df['property'] == 'imports', 'value'].values[0]
        p_imports_str = ""
        
        if p_imports != '[]':
          p_imports_str = f"""IMPORTS = ({', '.join(["{}".format(value) for value in p_imports.strip('][').split(",")])})"""


        p_handler = p_df.loc[p_df['property'] == 'handler', 'value'].values[0] 
            

        p_packages = p_df.loc[p_df['property'] == 'packages', 'value'].values[0]
        p_packages_str = ""

        if p_packages != '[]':
          p_packages_str = f"""PACKAGES = ({', '.join(["{}".format(value) for value in p_packages.strip('][').split(",")])})"""
        
        proc_str += f"""CREATE OR REPLACE SECURE PROCEDURE PROCS_APP.{p_name}{p_signature}
  RETURNS {p_returns}
  LANGUAGE {p_language}
  RUNTIME_VERSION = {p_runtime_version}
  {p_imports_str}
  HANDLER = '{p_handler}'
  {p_packages_str}
  COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{p_name}","type":"procedure"}}}}'
  EXECUTE AS OWNER
  AS
  $$
  {p_body}
  $$;

"""
     else:
        proc_str += f"""CREATE OR REPLACE SECURE PROCEDURE PROCS_APP.{p_name}{p_signature}
  RETURNS {p_returns}
  LANGUAGE {p_language}
  COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{p_name}","type":"procedure"}}}}'
  EXECUTE AS OWNER
  AS
  $$
  {p_body}
  $$;

"""
     #add procedure to proc_log_str    
     proc_log_str += f"""snowflake.execute({{sqlText:`CALL UTIL_APP.APP_LOGGER('${{account_locator}}', '${{consumer_name}}', '${{app_key_local}}', '${{app_mode}}', 'log', 'install', '""', SYSDATE(), 'PROCESSING', '"${{current_db.toLocaleLowerCase()}}.procs_app.{p_name} procedure created."');`}});
    """

  
  #var to store command statement to insert into commands table
  cmd_stmt = ""

  #create schema if create
  if version_option in ["CREATE", "PATCH"]:
    #get template files from appropriate environment, replace macros, and write to new file

    session.file.get(f'@P_{app_code}_SOURCE_DB_{templates_env}.ARTIFACTS.ARTIFACTS/templates/setup_script_template.txt', 'temp/templates')
    with open('temp/templates/setup_script_template.txt', 'r', encoding='utf-8') as st_file:
      with open(f"setup_script.sql", 'w', encoding='utf-8') as s_file:
        s_file.write(st_file.read().replace('[[CURRENT_DATE]]', current_date).replace('[[FUNCTIONS]]', func_str).replace('[[LOG_FUNCS]]',func_log_str).replace('[[PROCEDURES]]',proc_str).replace('[[LOG_PROCS]]',proc_log_str).replace('[[APP_CODE]]', app_code).replace('[[APP_MODE]]', app_mode.lower()).replace('[[LIMIT]]', limit).replace('[[LIMIT_TYPE]]', limit_type).replace('[[LIMIT_ENFORCED]]', limit_enforced))
    
    session.file.get(f'@P_{app_code}_SOURCE_DB_{templates_env}.ARTIFACTS.ARTIFACTS/templates/manifest_template.txt', 'temp/templates')
    with open('temp/templates/manifest_template.txt', 'r', encoding='utf-8') as mt_file:
      with open(f"manifest.yml", 'w', encoding='utf-8') as m_file:
        m_file.write(mt_file.read().replace('[[VERSION]]', version).replace('[[README]]', readme))

    session.file.get(f'@P_{app_code}_SOURCE_DB_{templates_env}.ARTIFACTS.ARTIFACTS/templates/{readme_template}', 'temp/templates')
    with open(f"temp/templates/{readme_template}", 'r', encoding='utf-8') as rmt_file:
      with open(f"{readme}", 'w', encoding='utf-8') as rm_file:
        rm_file.write(rmt_file.read().replace('[[APP_CODE]]', app_code).replace('[[TC_ACCESS]]', trust_center_access_cmd).replace('[[TC_FLAG]]', trust_center_flag_cmd))

    
    #get streamlit files
    session.file.get(f'@P_{app_code}_SOURCE_DB_{streamlit_env}.ARTIFACTS.ARTIFACTS/streamlit', 'temp/streamlit')


    if version_option == 'CREATE':
      #create stage for version
      session.sql(f"CREATE OR REPLACE STAGE {app_pkg_name}.VERSIONS.{version}").collect()
      #add this version to the application package
      cmd_stmt = f"ALTER APPLICATION PACKAGE {app_pkg_name} ADD VERSION {version} USING \\'@{app_pkg_name}.VERSIONS.{version}\\'"

    if version_option == 'PATCH':
      #add patch for version
      cmd_stmt = f"ALTER APPLICATION PACKAGE {app_pkg_name} ADD PATCH FOR VERSION {version} USING \\'@{app_pkg_name}.VERSIONS.{version}\\'"

    #put the files associated with this app version onto this stage
    session.file.put("setup_script.sql", f"@{app_pkg_name}.VERSIONS.{version}", auto_compress=False, overwrite=True)
    session.file.put("manifest.yml", f"@{app_pkg_name}.VERSIONS.{version}", auto_compress=False, overwrite=True)
    session.file.put(f"{readme}", f"@{app_pkg_name}.VERSIONS.{version}", auto_compress=False, overwrite=True)
    session.file.put("temp/streamlit/*", f"@{app_pkg_name}.VERSIONS.{version}/streamlit", auto_compress=False, overwrite=True)

    #remove temp folder
    sh.rmtree('temp', ignore_errors=True)
  
  if version_option == 'DROP':
    cmd_stmt = f"ALTER APPLICATION PACKAGE {app_pkg_name} DROP VERSION {version}"
    session.sql(f"DROP STAGE IF EXISTS {app_pkg_name}.VERSIONS.{version}").collect()

  #insert into commands table
  session.sql(f"""INSERT INTO P_{app_code}_ACF_DB.ACF_STREAMLIT.COMMANDS VALUES
                  ('{cmd_stmt}', SYSDATE())""").collect()
  
  #update VERSION_HISTORY
  if version_option in ["CREATE", "PATCH"]:
    show_vers_stmt = f"SHOW VERSIONS IN APPLICATION PACKAGE {app_pkg_name}"
    patch_insert_stmt = f"""INSERT INTO P_{app_code}_ACF_DB.ACF_STREAMLIT.VERSION_HISTORY SELECT
                            '{app_pkg_name}'
                            ,'{version}'
                            ,"patch"
                            ,'{app_mode}'
                            ,'{limit_enforced}'
                            ,'{funcs_env}'
                            ,{func_list}
                            ,'{procs_env}'
                            ,{proc_list}
                            ,'{templates_env}'
                            ,'{streamlit_env}'
                            , "created_on" 
                            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE LOWER("version") = LOWER('{version}') ORDER BY "created_on" DESC LIMIT 1"""

    #add delay, so that version appears
    tm.sleep(40)
    session.sql(f"""INSERT INTO P_{app_code}_ACF_DB.ACF_STREAMLIT.COMMANDS VALUES
                  ('{show_vers_stmt}', SYSDATE())
                  ,($${patch_insert_stmt}$$, DATEADD(milliseconds,100,SYSDATE()))""").collect()



  return f"APP VERSION {version_option.upper()} WAS SUCCESSFUL"