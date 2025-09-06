import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import mysql.connector

# You must install this library: pip install thefuzz python-Levenshtein
from thefuzz import process

from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
# UPDATED: Import the legacy agent initializer
from langchain.agents import AgentType, initialize_agent
from langchain.tools import tool, Tool

# --- 1. Load env vars and VALIDATE them ---
load_dotenv()
DB_USER        = os.getenv("DB_USER")
DB_PASSWORD    = os.getenv("DB_PASSWORD")
DB_HOST        = os.getenv("DB_HOST", "localhost")
DB_PORT        = os.getenv("DB_PORT", "3306")
DB_NAME        = os.getenv("DB_NAME")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not all([DB_USER, DB_PASSWORD, DB_NAME, GOOGLE_API_KEY]):
    print("❌ CRITICAL ERROR: Environment variables not loaded.")
    print("   Please ensure a '.env' file exists and contains all required variables.")
    sys.exit(1)
print("✅ (Step 1) Environment variables loaded successfully.")


# --- 2. Direct Connection Test ---
try:
    print("\n--- (Step 2) Attempting direct MySQL connection... ---")
    conn = mysql.connector.connect(
        user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=int(DB_PORT), database=DB_NAME
    )
    conn.close()
    print("✅ (Step 2) Direct MySQL connection successful. Credentials are CORRECT.")
except mysql.connector.Error as err:
    print(f"❌ (Step 2) Direct MySQL connection FAILED: {err}")
    print("\n🛑 This is a database credentials or permissions issue. Please fix it and try again.")
    sys.exit(1)


# --- 3. SQLAlchemy & LangChain Connection ---
try:
    print("\n--- (Step 3) Initializing SQLAlchemy and LangChain connection... ---")
    db_url = URL.create(
        drivername="mysql+mysqlconnector",
        username=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
    )
    engine = create_engine(db_url)
    db = SQLDatabase(engine=engine)
    print("✅ (Step 3) SQLAlchemy and LangChain connection successful.")
except Exception as e:
    print(f"❌ (Step 3) SQLAlchemy connection failed unexpectedly: {e}")
    sys.exit(1)


# --- 4. Prepare Agent Tools ---
LOCATION_MAP = {"lake view": 6, "loop": 32, "near north side": 8, "ohare": 76, "near west side": 28}

@tool
def get_location_id(location_name: str) -> str:
    """
    Finds the exact numerical ID for a given Chicago community area name.
    Use this tool first. Only use it for exact, case-insensitive matches.
    """
    normalized_name = location_name.lower().strip().replace("the ", "")
    location_id = LOCATION_MAP.get(normalized_name)
    if location_id:
        print(f"Tool 'get_location_id' found ID {location_id} for '{location_name}'")
        return f"The ID for '{location_name}' is {location_id}."
    else:
        print(f"Tool 'get_location_id' could not find an exact match for '{location_name}'")
        return f"Error: No exact match found for '{location_name}'. Try using the find_approximate_location tool."

@tool
def find_approximate_location(location_name: str) -> str:
    """
    Finds the closest matching location name if an exact match isn't found.
    Use this tool as a fallback if `get_location_id` returns an error.
    It returns the best guess for the location name. You still need to get the ID for this guess.
    """
    best_match, score = process.extractOne(location_name.lower(), LOCATION_MAP.keys())
    if score > 75:
        print(f"Tool 'find_approximate_location' found best match '{best_match}' for '{location_name}' with score {score}")
        return f"The closest match for '{location_name}' is '{best_match}'. You should now use get_location_id with this corrected name."
    else:
        print(f"Tool 'find_approximate_location' could not find a confident match for '{location_name}'")
        return f"Error: Could not find a location similar to '{location_name}'."


def run_sql_query_with_error_handling(query: str) -> str:
    """Runs the SQL query and returns the result, or a formatted error message."""
    try:
        return db.run(query)
    except Exception as e:
        return f"Error executing SQL query: {e}"

sql_query_tool = Tool(
    name="sql_database_query_tool",
    func=run_sql_query_with_error_handling,
    description="Use this to run a SINGLE, valid SQL query against the database. The input MUST be a SQL query string."
)

# --- 5. Initialize the Agent (with Schema and Instructions) ---
llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash-latest", google_api_key=GOOGLE_API_KEY, temperature=0)
tools = [get_location_id, find_approximate_location, sql_query_tool]

# NEW: Fetch the database schema to give the agent context.
try:
    db_schema = db.get_table_info()
except Exception as e:
    print(f"Could not fetch database schema: {e}")
    db_schema = "Error: Could not fetch schema."

# NEW: Create a detailed instruction prefix for the agent.
AGENT_INSTRUCTIONS_PREFIX = f"""
You are an expert assistant for querying a Chicago taxi database.
You must follow these steps:
1.  Identify the location names in the user's question (e.g., "from A to B").
2.  For EACH location, use the `get_location_id` tool to find its numerical ID.
3.  If `get_location_id` fails, use `find_approximate_location` to get a better name, then use `get_location_id` again on that suggestion.
4.  Once you have the numerical IDs, construct a valid SQL query to answer the question.
5.  Execute the query using the `sql_database_query_tool`.

**IMPORTANT DATABASE SCHEMA:**
You have access to the following tables and columns:
{db_schema}

Use this schema to construct your SQL queries. Do not guess table or column names.
Now, begin!
"""

# The `initialize_agent` function is a robust way to create an agent.
# We use `agent_kwargs` to pass our custom instructions.
agent_executor = initialize_agent(
    tools,
    llm,
    agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True,
    handle_parsing_errors=True,
    agent_kwargs={
        "prefix": AGENT_INSTRUCTIONS_PREFIX
    }
)


# --- 6. Interactive loop ---
if __name__ == "__main__":
    print("\n" + "="*50)
    print("🤖 Smart SQL Agent is ready. Ask: “What’s the fare from Lake View to the Loop?”")
    print("   Or try: “What is the fare from Lakeview to O'Hare and then to the Loop?”")
    print("Type ‘exit’ to quit.\n")
    while True:
        query = input("❓ Your Question: ")
        if query.lower() in ("exit", "quit"):
            break
        try:
            # Use agent_executor.run() for this type of agent
            result = agent_executor.run(query)
            print("\n✅ Agent Response:\n", result, "\n")
        except Exception as e:
            print(f"❌ Agent error: {e}")