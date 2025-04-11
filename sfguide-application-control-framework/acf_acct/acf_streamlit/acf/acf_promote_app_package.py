import pandas as pd
import shutil as sh
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F

def promote_app_package(app_code, app_funcs_env, app_funcs_list, app_procs_env, app_procs_list, templates_env, streamlit_env):
  session = get_active_session()

  #create prod db if it does not exits
  prod_db = pd.DataFrame(session.sql(f"SHOW DATABASES LIKE 'P_{app_code}_SOURCE_DB_PROD'").collect())
  if prod_db.empty:
    session.sql(f"CREATE OR REPLACE DATABASE P_{app_code}_SOURCE_DB_PROD CLONE P_{app_code}_SOURCE_DB_DEV").collect()
  else:
    #if functions environment is dev, clone the funcs_app DB into prod db
    if app_funcs_env.upper() == 'DEV':
      #for each function get the signature, language, and body; then add to func_str
      
      for f in app_funcs_list:
        func_str = ""

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
          
          if f_target_path:
            f_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""

          func_str = f"""CREATE OR REPLACE FUNCTION P_{app_code}_SOURCE_DB_PROD.FUNCS_APP.{f_name}{f_signature}
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
          
          func_str = f"""CREATE OR REPLACE FUNCTION P_{app_code}_SOURCE_DB_PROD.FUNCS_APP.{f_name}{f_signature}
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
          
          if f_target_path:
            f_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in f_imports.strip('][').split(",")])})"""

        
          func_str = f"""CREATE OR REPLACE FUNCTION P_{app_code}_SOURCE_DB_PROD.FUNCS_APP.{f_name}{f_signature}
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
          func_str = f"""CREATE OR REPLACE FUNCTION P_{app_code}_SOURCE_DB_PROD.FUNCS_APP.{f_name}{f_signature}
    RETURNS {f_returns}
    LANGUAGE {f_language}
    COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{f_name}","type":"function"}}}}'
    AS
    $$
    {f_body}
    $$;

    """
        
        #create function
        session.sql(func_str).collect()


    #if procedures environment is dev, clone the procs_app DB into prod db
    if app_procs_env.upper() == 'DEV':
      #session.sql(f"CREATE OR REPLACE SCHEMA P_{app_code}_SOURCE_DB_PROD.PROCS_APP CLONE P_{app_code}_SOURCE_DB_DEV.PROCS_APP").collect()
      
      #for each procedure get the signature, language, and body; then add to proc_str
      for p in app_procs_list:
        proc_str = ""

        proc = p[0]

        p_df = pd.DataFrame(session.sql(f"DESCRIBE PROCEDURE {proc}").collect())

        p_name = proc.split(".")[2].split("(")[0]

        p_signature = p_df.loc[p_df['property'] == 'signature', 'value'].values[0]
        p_returns = p_df.loc[p_df['property'] == 'returns', 'value'].values[0]
        p_language = p_df.loc[p_df['property'] == 'language', 'value'].values[0]
        p_body = p_df.loc[p_df['property'] == 'body', 'value'].values[0] 

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
          
          if p_target_path:
            p_target_path_str = f"""TARGET_PATH = ({', '.join(["'{}'".format(value) for value in p_imports.strip('][').split(",")])})"""

          proc_str = f"""CREATE OR REPLACE PROCEDURE P_{app_code}_SOURCE_DB_PROD.PROCS_APP.{p_name}{p_signature}
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
            
            proc_str = f"""CREATE OR REPLACE PROCEDURE P_{app_code}_SOURCE_DB_PROD.PROCS_APP.{p_name}{p_signature}
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
          proc_str = f"""CREATE OR REPLACE PROCEDURE P_{app_code}_SOURCE_DB_PROD.PROCS_APP.{p_name}{p_signature}
    RETURNS {p_returns}
    LANGUAGE {p_language}
    COMMENT = '{{"origin":"sf_sit","name":"acf","version":{{"major":1, "minor":7}},"attributes":{{"env":"native_app","component":"{p_name}","type":"procedure"}}}}'
    EXECUTE AS OWNER
    AS
    $$
    {p_body}
    $$;

    """
        #create procedure
        session.sql(proc_str).collect()

  #if templates environment is dev, copy template files from dev to prod db
  if templates_env.upper() == 'DEV':
    #create ARTIFACTS stage in prod, if it does not exist
    session.sql(f"CREATE STAGE IF NOT EXISTS P_{app_code}_SOURCE_DB_PROD.ARTIFACTS.ARTIFACTS").collect()
    
    #get templates from DEV and put them in PROD ARTIFACTS stage
    session.file.get(f'@P_{app_code}_SOURCE_DB_DEV.ARTIFACTS.ARTIFACTS/templates', 'temp/templates')
    session.file.put("temp/templates/*", f"@P_{app_code}_SOURCE_DB_PROD.ARTIFACTS.ARTIFACTS/templates", auto_compress=False, overwrite=True)

    #remove temp folder
    sh.rmtree('temp', ignore_errors=True)

  if streamlit_env.upper() == 'DEV':
    #create ARTIFACTS stage in prod, if it does not exist
    session.sql(f"CREATE STAGE IF NOT EXISTS P_{app_code}_SOURCE_DB_PROD.ARTIFACTS.ARTIFACTS").collect()
    
    #get templates from DEV and put them in PROD ARTIFACTS stage
    session.file.get(f'@P_{app_code}_SOURCE_DB_DEV.ARTIFACTS.ARTIFACTS/streamlit', 'temp/streamlit')
    session.file.put("temp/streamlit/*", f"@P_{app_code}_SOURCE_DB_PROD.ARTIFACTS.ARTIFACTS/streamlit", auto_compress=False, overwrite=True)

    #remove temp folder
    sh.rmtree('temp', ignore_errors=True)

  return f"APP PACKAGE SUCCESSFULLY PROMOTED"