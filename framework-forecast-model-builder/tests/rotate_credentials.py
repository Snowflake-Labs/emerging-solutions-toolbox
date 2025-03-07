#!/usr/bin/env python3

import os
import secrets

import boto3
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import load_dotenv

load_dotenv(override=True)


def create_private_and_public_key() -> tuple[bytes, bytes, bytes]:
    private_key_passphrase = secrets.token_urlsafe(25).encode()
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encrypted_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=(
            serialization.BestAvailableEncryption(private_key_passphrase)
        ),
    )
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return encrypted_private_key, private_key_passphrase, public_key


def rotate_public_key(public_key: bytes) -> None:
    with snowflake.connector.connect() as conn, conn.cursor() as cursor:
        public_key_str = "".join(public_key.decode("utf-8").split("\n")[1:-2])
        cursor.execute(
            f"""ALTER USER {os.environ.get("SNOWFLAKE_USER")}
            SET RSA_PUBLIC_KEY = '{public_key_str}';"""
        )
    return None


def main() -> None:
    encrypted_private_key, private_key_passphrase, public_key = (
        create_private_and_public_key()
    )
    session = boto3.session.Session(region_name=os.environ.get("AWS_REGION_NAME"))
    client = session.client("secretsmanager")
    try:
        client.create_secret(
            Name=os.environ.get("AWS_SECRET_PRIVATE_KEY_NAME"),
            SecretString=encrypted_private_key.decode("utf-8"),
        )
    except client.exceptions.ResourceExistsException:
        client.update_secret(
            SecretId=os.environ.get("AWS_SECRET_PRIVATE_KEY_NAME"),
            SecretString=encrypted_private_key.decode("utf-8"),
        )
    try:
        client.create_secret(
            Name=os.environ.get("AWS_SECRET_PRIVATE_KEY_PASSPHRASE_NAME"),
            SecretBinary=private_key_passphrase,
        )
    except client.exceptions.ResourceExistsException:
        client.update_secret(
            SecretId=os.environ.get("AWS_SECRET_PRIVATE_KEY_PASSPHRASE_NAME"),
            SecretBinary=private_key_passphrase,
        )
    rotate_public_key(public_key)
    return None


if __name__ == "__main__":
    main()
