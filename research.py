# from cryptography.fernet import Fernet
# print(Fernet.generate_key().decode())

# # iexxhYXMpnP751Ae5uQBKqKfxLGCQfw5Othy3qB0uQk=
from dotenv import load_dotenv
import os
load_dotenv()

rds_host = os.getenv("RDS_HOST")
rds_user = os.getenv("RDS_USER")
rds_password = os.getenv("RDS_PASSWORD")
rds_db = os.getenv("RDS_DB")
rds_port = os.getenv("RDS_PORT")


print(rds_host)
print(rds_user)
print(rds_password)
print(rds_db)
print(rds_port)