import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
table_name = os.getenv("TABLE_NAME")
csv_file = os.getenv("CSV_FILE", "chicago-taxi-data.csv")

# Load CSV
df = pd.read_csv(csv_file)

# Connect to MySQL
conn = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password
)
cursor = conn.cursor()

# Create database if not exists
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
cursor.execute(f"USE {db_name}")

# Create table based on CSV headers (all as VARCHAR for prototyping)
column_defs = ", ".join(f"`{col}` VARCHAR(255)" for col in df.columns)
cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
cursor.execute(f"CREATE TABLE {table_name} ({column_defs})")

# Insert data
for _, row in df.iterrows():
    values = [str(val).replace("'", "''") for val in row.tolist()]
    placeholders = ", ".join(["%s"] * len(values))
    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.execute(query, values)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Successfully created table `{table_name}` and inserted {len(df)} rows.")
