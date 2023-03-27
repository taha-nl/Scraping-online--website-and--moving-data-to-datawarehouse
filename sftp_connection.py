import csv
import io
import pysftp
from cryptography.fernet import Fernet

# Read the contents of the CSV file
with open('immobilier_avito-final.csv', 'r',encoding="utf-8") as file:
    contents = file.read()

# Encrypt the contents of the file using AES encryption
key = Fernet.generate_key()
fernet = Fernet(key)
encrypted_contents = fernet.encrypt(contents.encode())

# Connect to the SFTP server
with pysftp.Connection('ip_address', username='sftp_user', password='password') as sftp:
    # Change to the remote directory where you want to upload the file
    sftp.cwd('sftp_user')

    # Upload the encrypted file to the SFTP server
    with sftp.open('encrypted_avito.csv', 'wb') as file:
        file.write(encrypted_contents)
