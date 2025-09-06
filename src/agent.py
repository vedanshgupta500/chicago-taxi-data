# src/agent.py

import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits import create_sql_agent

from .db import db

load_dotenv()

# Initialize the language model
llm = ChatOpenAI(
    model="gpt-4",
    temperature=0,
    openai_api_key=os.getenv("OPENAI_API_KEY")
)

# Create the modern, specialized SQL agent
agent_executor = create_sql_agent(
    llm=llm,
    db=db,
    agent_type="openai-tools",
    verbose=True
)

def query_agent(natural_lang: str) -> str:
    """
    Takes an English question and returns the agent's answer.
    """
    # Use the 'invoke' method which returns a dictionary.
    # The final answer is in the 'output' key.
    response = agent_executor.invoke({"input": natural_lang})
    return response["output"]