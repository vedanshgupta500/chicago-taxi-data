import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Config
csv_file = "chicago-taxi-data.csv"
table_name = "taxi_data"

# Load CSV
df = pd.read_csv(csv_file, low_memory=False)

# Clean column names
df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
columns = df.columns.tolist()

# Function: infer SQL type
def infer_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "TEXT"

# Manual overrides for known sensitive columns
manual_types = {
    'pickup_census_tract': 'DOUBLE',
    'dropoff_census_tract': 'DOUBLE'
}

# Create column definitions
column_defs = []
for col in columns:
    col_type = manual_types.get(col, infer_sql_type(df[col]))
    column_defs.append(f"`{col}` {col_type}")

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
    {', '.join(column_defs)}
);
"""

# Connect to MySQL
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
)
cursor = conn.cursor()

# Create table
cursor.execute(create_table_sql)
print("✅ Table created or already exists.")

# Prepare INSERT statement
column_list = ", ".join(f"`{col}`" for col in columns)
placeholder_list = ", ".join(["%s"] * len(columns))
insert_sql = f"INSERT INTO `{table_name}` ({column_list}) VALUES ({placeholder_list})"

# Insert rows
row_count = 0
for i, row in df.iterrows():
    values = tuple(None if pd.isna(v) else v for v in row.values)
    try:
        cursor.execute(insert_sql, values)
        row_count += 1
    except Exception as e:
        print(f"❌ Skipped row {i} due to error: {e}")

# Finalize
conn.commit()
cursor.close()
conn.close()

print(f"\n✅ Inserted {row_count} rows into `{table_name}`.")
