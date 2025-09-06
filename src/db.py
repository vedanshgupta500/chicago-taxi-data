# src/db.py

import os
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase

load_dotenv()

USER = os.getenv("DB_USER")
PASS = os.getenv("DB_PASS")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT", 3306)
NAME = os.getenv("DB_NAME")

# Added '?charset=utf8mb4' to handle modern MySQL authentication
CONN_STR = f"mysql+pymysql://{USER}:{PASS}@{HOST}:{PORT}/{NAME}?charset=utf8mb4"

db = SQLDatabase.from_uri(CONN_STR)