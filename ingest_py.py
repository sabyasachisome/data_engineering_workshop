import paramiko
import pandas as pd
import pymysql
import logging
import watchtower
import boto3
from datetime import datetime
import time

source= "employees"

boto3.setup_default_session(region_name="us-east-1")


timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_stream = f"{source}_pipeline_{timestamp}"
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

logger.error("THIS IS A TEST ERROR FOR CLOUDWATCH")



# -----------------------------
# SFTP CONFIG (EC2-1)
# -----------------------------
hostname = "3.92.96.96"
username = "ubuntu"
key_path_in_ec2_2 = "/home/ubuntu/ec2_sftp_keys.pem"

remote_dir = "/home/ubuntu/sftp-drop"

# -----------------------------
# MYSQL CONFIG (EC2-2)
# -----------------------------
db_config = {
    "host": "localhost",
    "user": "de_user",
    "password": "de_password",
    "database": "data_engineering"
}

# -----------------------------
# CONNECT TO SFTP
# -----------------------------
key = paramiko.RSAKey.from_private_key_file(key_path_in_ec2_2)

transport = paramiko.Transport((hostname, 22))
transport.connect(username=username, pkey=key)

sftp = paramiko.SFTPClient.from_transport(transport)

logger.info("Connected to SFTP")

# -----------------------------
# DOWNLOAD FILES
# -----------------------------
files = sftp.listdir(remote_dir)

for file in files:
    if file.endswith(".csv"):
        remote_path = f"{remote_dir}/{file}"
        local_path = f"/home/ubuntu/{file}"

        logger.info(f"Downloading {file}")
        sftp.get(remote_path, local_path)

        # -----------------------------
        # LOAD INTO MYSQL
        # -----------------------------
        df = pd.read_csv(local_path)

        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO employees (id, name, salary) VALUES (%s, %s, %s)",
                (row['id'], row['name'], row['salary'])
            )

        conn.commit()
        conn.close()

        logger.info(f"Inserted {file}")

# -----------------------------
# CLOSE CONNECTION
# -----------------------------
sftp.close()
transport.close()

logger.info("Pipeline completed")