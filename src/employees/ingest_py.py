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
import re
import pyarrow

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
source_name= config["DATADETAILS"]["source"]
bucket_name= config["DATADETAILS"]["bucket_name"]

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_stream = f"{source_name}_pipeline_{timestamp}"
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
# s3 client
# -----------------------------

s3_client= boto3.client('s3')

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

# # -----------------------------
# # MYSQL CONNECTION (reuse)
# # -----------------------------
# conn = pymysql.connect(**db_config)
# cursor = conn.cursor()

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
            # EXTRACT DATE FROM FILENAME
            # -----------------------------
            match = re.search(r'(\d{8})', file)
            if not match:
                raise ValueError(f"No date found in filename {file}")

            date_str = match.group(1)  # 20260409
            src_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

            logger.info(f"Extracted src_date: {src_date}")

            # -----------------------------
            # READ CSV
            # -----------------------------
            df = pd.read_csv(local_path)

            # -----------------------------
            # CONVERT TO PARQUET
            # -----------------------------
            parquet_file = local_path.replace(".csv", ".parquet")
            df.to_parquet(parquet_file, index=False)

            logger.info(f"Converted to parquet: {parquet_file}")

            # -----------------------------
            # BUILD S3 KEY (PARTITIONED)
            # -----------------------------
            s3_key = f"{source_name}/src_date={src_date}/{os.path.basename(parquet_file)}"

            logger.info(f"Uploading to S3: {s3_key}")

            # -----------------------------
            # UPLOAD TO S3
            # -----------------------------
            s3_client.upload_file(parquet_file, bucket_name, s3_key)

            logger.info(f"Uploaded to s3://{bucket_name}/{s3_key}")

            # -----------------------------
            # CLEANUP LOCAL FILES
            # -----------------------------
            os.remove(local_path)
            os.remove(parquet_file)

            logger.info(f"Deleted local files for {file}")

        except Exception as e:
            logger.error(f"Error processing {file}: {str(e)}")

# -----------------------------
# CLOSE CONNECTIONS
# -----------------------------
# cursor.close()
# conn.close()
sftp.close()
transport.close()

logger.info("Pipeline completed")