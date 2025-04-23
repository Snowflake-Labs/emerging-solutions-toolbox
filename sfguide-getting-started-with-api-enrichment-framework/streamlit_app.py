# Import python packages
import streamlit as st
import yaml
import io
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# def setup():
#     session.sql("""CREATE TABLE IF NOT EXISTS HART_DB.ENRICHMENT.METADATA(KEY STRING, VALUE VARIANT)""").collect()

def save_fields(fields):
    fields_string = str(fields).replace("'",'"')
    session.sql(f"""
    MERGE into ENRICHMENT_PACKAGE.SHARE.METADATA m
    USING(SELECT 'FIELDS' as KEY, PARSE_JSON('{str(fields_string)}') as VALUE) s on s.KEY = m.KEY
    WHEN MATCHED THEN UPDATE SET m.VALUE = s.VALUE
    WHEN NOT MATCHED THEN INSERT (KEY,VALUE) VALUES (s.KEY,s.VALUE)    
    """).collect()

def manage_fields(inc):
    st.session_state.num_fields = st.session_state.num_fields+inc

current_fields = session.sql("SELECT VALUE FROM ENRICHMENT_PACKAGE.SHARE.METADATA WHERE KEY = 'FIELDS'").collect()



st.title("Enrichment Admin App")
st.divider()
st.subheader("Fields")


    # setup()
if len(current_fields) == 0:
    st.session_state.num_fields = 1
else:
    current_fields = json.loads(current_fields[0][0])["fields"]
    st.session_state.num_fields = len(current_fields)


num_fields = st.session_state.num_fields

fields = ['']*num_fields


for num in  range(0,num_fields):
    if 0 <= num < len(current_fields):
        default_val = current_fields[num]
    else:
        default_val = ''
    fields[num] = st.text_input("Field Name "+str(num+1),key=str(num)+"user_input", value=default_val)

st.button("Add Field", on_click=manage_fields, args=(1,))
st.button("Subtract Field", on_click=manage_fields, args=(-1,))

st.divider()

st.button("Save", type="primary",on_click=save_fields, args=({"fields":fields},))

st.write(fields)

# fd = session.file.get_stream("@HART_DB.ENRICHMENT.APP/manifest.yml")


# valuesYaml = yaml.load(fd, Loader=yaml.FullLoader)

# # manifest = ''
# # for line in iter(fd.readline, ''):
# #     manifest = yaml.safe_load(line)

# test = fd.read(100)

# st.write(valuesYaml)

# session.file.put_stream(input_stream=io.BytesIO(bytes(yaml.dump(valuesYaml),"UTF-8")),stage_location="@HART_DB.ENRICHMENT.APP/manifest.yml",overwrite=True, auto_compress = False)


