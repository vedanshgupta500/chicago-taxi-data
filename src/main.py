# src/main.py

from .agent import query_agent

def run_console():
    """
    Starts a command-line loop to chat with the taxi data agent.
    """
    print("🚕 Chicago Taxi Data Agent is ready!")
    print("Ask anything about your data (e.g., 'How many trips were there?').")
    print("Type 'exit' or 'quit' to end.")
    
    try:
        while True:
            question = input(">> ")
            if question.lower() in ("exit", "quit"):
                break
            
            answer = query_agent(question)
            print(f"\n{answer}\n")

    except KeyboardInterrupt:
        print("\nGoodbye!")

if __name__ == "__main__":
    run_console()