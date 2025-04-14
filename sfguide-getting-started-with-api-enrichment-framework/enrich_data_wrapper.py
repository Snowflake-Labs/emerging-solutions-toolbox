import pandas
import numpy as np
import json
import base64
import _snowflake
from _snowflake import vectorized


def enrich_data_wrapper(session,task_id):

  sql = f"SELECT JOB_SPECS FROM DATA.SCHEDULED_TASKS WHERE TASK_ID = {task_id}"
  table_dict = json.loads(session.sql(sql).collect()[0][0])
  sql = f"CALL SRC.enrich_data({table_dict})"
  session.sql(sql).collect()
  return "SUCCESS"