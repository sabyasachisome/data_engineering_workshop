import paramiko
import pandas as pd
import pymysql
import logging
import watchtower
import boto3
from datetime import datetime
import time
import configparser
import os

# -----------------------------
# AWS REGION
# -----------------------------
boto3.setup_default_session(region_name="us-east-1")

# -----------------------------
# LOAD CONFIG
# -----------------------------
config = configparser.ConfigParser()
config.read("config.ini")

# -----------------------------
# LOGGING SETUP
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_stream = f"test_pipeline_{timestamp}"
log_group = "data_engineering"

logger = logging.getLogger(log_stream)
logger.setLevel(logging.INFO)

logger.addHandler(
    watchtower.CloudWatchLogHandler(
        log_group=log_group,
        stream_name=log_stream
    )
)

logger.info("Pipeline started")

# -----------------------------
# SFTP CONFIG
# -----------------------------
hostname = config["SFTP"]["hostname"]
username = config["SFTP"]["username"]
key_path = config["SFTP"]["key_path"]
remote_dir = config["SFTP"]["remote_dir"]

# -----------------------------
# MYSQL CONFIG
# -----------------------------
db_config = {
    "host": config["MYSQL"]["host"],
    "user": config["MYSQL"]["user"],
    "password": config["MYSQL"]["password"],
    "database": config["MYSQL"]["database"]
}

# -----------------------------
# CONNECT TO SFTP
# -----------------------------
key = paramiko.RSAKey.from_private_key_file(key_path)

transport = paramiko.Transport((hostname, 22))
transport.connect(username=username, pkey=key)

sftp = paramiko.SFTPClient.from_transport(transport)

logger.info("Connected to SFTP")

# -----------------------------
# MYSQL CONNECTION (reuse)
# -----------------------------
conn = pymysql.connect(**db_config)
cursor = conn.cursor()

# -----------------------------
# DOWNLOAD + PROCESS FILES
# -----------------------------
files = sftp.listdir(remote_dir)

for file in files:
    if file.endswith(".csv"):
        try:
            remote_path = f"{remote_dir}/{file}"
            local_path = f"/home/ubuntu/{file}"

            logger.info(f"Downloading {file}")
            sftp.get(remote_path, local_path)

            # -----------------------------
            # LOAD INTO MYSQL
            # -----------------------------
            df = pd.read_csv(local_path)

            for _, row in df.iterrows():
                cursor.execute(
                    "INSERT INTO employees (id, name, salary) VALUES (%s, %s, %s)",
                    (row['id'], row['name'], row['salary'])
                )

            conn.commit()

            logger.info(f"Inserted {file}")

            # -----------------------------
            # DELETE LOCAL FILE ✅
            # -----------------------------
            os.remove(local_path)
            logger.info(f"Deleted local file {file}")

        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")

# -----------------------------
# CLOSE CONNECTIONS
# -----------------------------
cursor.close()
conn.close()
sftp.close()
transport.close()

logger.info("Pipeline completed")