import pandas
import numpy as np
import json
import base64
import _snowflake
import streamlit as st
from _snowflake import vectorized


def enrich_data(session,table_dict):
  # limit for testing, remove when desired
  limit=10

  table_reference = table_dict["reference"]

  mapping = table_dict["mapping"]

  output_table = table_dict["output_table"]

  sql = "SELECT "

  i = 0
  for column, column_map in mapping.items():
    if i == len(mapping)-1:
      sql = sql+f"{column_map} as {column} "
    else:
      sql = sql+f"{column_map} as {column}, "

    i = i+1

  sql = sql+f"from reference('enrichment_table','{table_reference}') limit {limit}"

  address_data = session.sql(sql).to_pandas()

# Develop your logic for parsing the API and the Response below

  for index, row in address_data.iterrows():
      load = {
              "street_address": row["STREET_ADDRESS"],
              "city": row["CITY"],
              "region": row["REGION"],
              "postal_code": row["POSTAL_CODE"],
              "iso_country_code": row["ISO_COUNTRY_CODE"]
          }
      response = session.sql(f"SELECT SRC.GET_MATCH_ID({load})").collect()
      response_str = response[0][0].replace("'",'"')

      response = json.loads(str(response_str))
      if 'locations' in response:
          address_data.at[index,'latitude'] = response["locations"][0]["referencePosition"]["latitude"]
          address_data.at[index,'longitude'] = response["locations"][0]["referencePosition"]["longitude"]

# Add your logic above, the below logic is for saving out the dataframe
  session.sql("CREATE DATABASE IF NOT EXISTS ENRICHMENT").collect()
  session.sql("CREATE SCHEMA IF NOT EXISTS ENRICHMENT.DATA").collect()
  # address_data.to_snowflake(f"ENRICHMENT.DATA.{output_table}", if_exists='replace',index=False)
  session.write_pandas(address_data, table_name = output_table, database = "ENRICHMENT", schema="DATA", overwrite=True, auto_create_table=True)
  session.sql("GRANT USAGE ON DATABASE ENRICHMENT TO APPLICATION ROLE PUBLIC_DB_ROLE").collect()
  session.sql("GRANT USAGE ON SCHEMA ENRICHMENT.DATA TO APPLICATION ROLE PUBLIC_DB_ROLE").collect()
  session.sql(f"GRANT SELECT ON TABLE ENRICHMENT.DATA.{output_table} TO APPLICATION ROLE PUBLIC_DB_ROLE").collect()
  return "Success!"