import pandas as pd
import hashlib

def extract_schema(file_path: str) -> str:
    df = pd.read_csv(file_path, nrows=0)
    return ", ".join(df.columns.tolist())

def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()
