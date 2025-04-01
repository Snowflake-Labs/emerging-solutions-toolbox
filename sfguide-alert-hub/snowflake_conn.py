import os

import streamlit as st
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark.session import Session

# Loads private key
pkb = ""

if st.secrets["local_key_path"] != "":
    with open(st.secrets["local_key_path"], "rb") as key:
        p_key = serialization.load_pem_private_key(
            key.read(),
            password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
            backend=default_backend()
        )

    # Stores public key
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())


# Create Snowpark Session
def init_snowpark_session(account):
    connection_parameters = dict(st.secrets[account])

    # Add public key, if present
    if pkb != "":
        connection_parameters["private_key"] = pkb

    return Session.builder.configs(connection_parameters).create()
