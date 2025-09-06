import os
import pandas as pd
import hashlib
import mysql.connector
from dotenv import load_dotenv
from langchain_community.vectorstores import Chroma
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain_core.documents import Document

# ----------------------------
# Load Environment Variables
# ----------------------------
load_dotenv()
google_api_key = os.getenv("GOOGLE_API_KEY")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
table_name = os.getenv("TABLE_NAME")
csv_file = os.getenv("CSV_FILE", "chicago-taxi-data.csv")
persist_directory = "vectorstore"

# ----------------------------
# Step 1: Extract Schema & Compute Hash
# ----------------------------
def extract_schema(file_path):
    df = pd.read_csv(file_path, nrows=10)

    def infer_sql_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATETIME"
        else:
            return "VARCHAR(255)"

    schema_lines = []
    for col in df.columns:
        clean_col = col.strip().replace(" ", "_").lower()
        sql_type = infer_sql_type(df[col])
        schema_lines.append(f"- {clean_col}: {sql_type}")

    return "\n".join(schema_lines)



def compute_hash(text):
    return hashlib.sha256(text.encode()).hexdigest()

schema_text = extract_schema(csv_file)
schema_hash = compute_hash(schema_text)
hash_file_path = os.path.join(persist_directory, "schema.hash")

# ----------------------------
# Step 2: Conditionally Embed Schema
# ----------------------------
if os.path.exists(hash_file_path):
    with open(hash_file_path, "r") as f:
        existing_hash = f.read()
else:
    existing_hash = ""

if schema_hash != existing_hash:
    print("Schema changed or not cached. Embedding new schema...")
    document = Document(page_content=schema_text)
    vectorstore = Chroma.from_documents(
        documents=[document],
        embedding=GoogleGenerativeAIEmbeddings(
            model="models/embedding-001", google_api_key=google_api_key
        ),
        persist_directory=persist_directory,
    )
    vectorstore.persist()
    with open(hash_file_path, "w") as f:
        f.write(schema_hash)
else:
    print("Schema unchanged. Using cached embedding.")

# ----------------------------
# Step 3: Load Vector Store and Retriever
# ----------------------------
vectorstore = Chroma(
    embedding_function=GoogleGenerativeAIEmbeddings(
        model="models/embedding-001", google_api_key=google_api_key
    ),
    persist_directory=persist_directory,
)
retriever = vectorstore.as_retriever()

# ----------------------------
# Step 4: Define Prompt & Gemini LLM
# ----------------------------
prompt = ChatPromptTemplate.from_template(
    """
    You are a MySQL assistant. You can only query a table named `taxi_data`.

    The table has the following columns:
    {context}

    Rules:
    - Do NOT make up column names — use only those listed above.
    - Always wrap column names and table names in backticks (e.g., `trip_total`).
    - The `trip_start_timestamp` and `trip_end_timestamp` columns are stored as strings (VARCHAR), **not** as DATETIME.
    - These timestamp strings are in this format: `%m/%d/%Y %h:%i:%s %p` (e.g., `01/15/2024 03:45:00 PM`).
    - When filtering by date, always convert them like this:

        STR_TO_DATE(`trip_start_timestamp`, '%m/%d/%Y %h:%i:%s %p')

    Example:
    To filter January 2024 trips, use:
    WHERE STR_TO_DATE(`trip_start_timestamp`, '%m/%d/%Y %h:%i:%s %p') BETWEEN '2024-01-01' AND '2024-01-31 23:59:59'

    Now, interpret the question and write the correct MySQL query.

    Question: {question}
    """
)



llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash-latest", google_api_key=google_api_key
)

# ----------------------------
# Step 5: Execute MySQL Query
# ---------------------------- 
def run_mysql_query(query):
    try:
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
        )
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()
    except Exception as e:
        print("❌ Error executing MySQL query:", e)

# ----------------------------
# Step 6: Answer Question via SQL
# ----------------------------
def answer_question(question):
    docs = retriever.invoke(question)
    context = "\n".join(doc.page_content for doc in docs)
    messages = prompt.format_messages(context=context, question=question)
    response = llm.invoke(messages)

    # Clean LLM output
    generated_sql = response.content.strip().strip("`")

    # Remove any leading code block markers like "sql\n"
    if generated_sql.lower().startswith("sql"):
        generated_sql = generated_sql[3:].strip()

    print("\n--- Generated SQL ---\n")
    print(generated_sql)

    print("\n--- MySQL Output ---\n")
    run_mysql_query(generated_sql)

# Main Entry
# ----------------------------
if __name__ == "__main__":
    user_question = input("\n❓ Enter your SQL-style question: ")
    answer_question(user_question)