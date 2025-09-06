import os
from dotenv import load_dotenv
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_core.documents import Document
from schema_utils import extract_schema, compute_hash

load_dotenv()
schema_file = "chicago-taxi-data.csv"
schema_text = extract_schema(schema_file)
schema_hash = compute_hash(schema_text)

persist_directory = "vectorstore"
hash_file_path = os.path.join(persist_directory, "schema.hash")

if os.path.exists(hash_file_path):
    with open(hash_file_path, "r") as f:
        existing_hash = f.read()
else:
    existing_hash = ""

if schema_hash != existing_hash:
    print("Schema changed or no cache found. Re-embedding schema...")
    documents = [Document(page_content=schema_text)]
    embeddings = OpenAIEmbeddings()
    vectorstore = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=persist_directory,
    )
    vectorstore.persist()
    with open(hash_file_path, "w") as f:
        f.write(schema_hash)
else:
    print("Schema unchanged. Using cached embeddings.")