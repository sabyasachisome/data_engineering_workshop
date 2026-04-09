#!/bin/bash

# -----------------------------
# System update
# -----------------------------
apt update -y

# -----------------------------
# Install Apache (web server)
# -----------------------------
apt install -y apache2
systemctl start apache2
systemctl enable apache2

# -----------------------------
# Install SSH (SFTP)
# -----------------------------
apt install -y openssh-server
systemctl start ssh
systemctl enable ssh

# -----------------------------
# Install Python + pip
# -----------------------------
apt install -y python3-pip

# -----------------------------
# Install MySQL
# -----------------------------
apt install -y mysql-server
systemctl start mysql
systemctl enable mysql

# -----------------------------
# Install Python libraries
# -----------------------------
pip3 install paramiko pandas pymysql boto3 watchtower --break-system-packages

# -----------------------------
# Create SFTP folder
# -----------------------------
mkdir -p /home/ubuntu/sftp-drop
chmod 755 /home/ubuntu/sftp-drop

# -----------------------------
# Fix ownership
# -----------------------------
chown -R ubuntu:ubuntu /home/ubuntu/sftp-drop

# -----------------------------
# Web server test page
# -----------------------------
echo "<h1>Data Pipeline Server - $(hostname -f)</h1>" > /var/www/html/index.html

# -----------------------------
# Permissions for web server
# -----------------------------
chmod -R 755 /var/www/html