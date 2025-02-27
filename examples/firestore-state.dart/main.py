from langgraph.libs.langgraph.langgraph.graph import FirestoreStateGraph
from langgraph.prebuilt import firestore_node
from typing_extensions import TypedDict

# Define state schema
class GraphState(TypedDict):
    question: str
    documents: list
    answer: str

# Create a Firestore-backed graph
graph = FirestoreStateGraph(
    GraphState,
    collection_name="my_rag_states",
    credentials_path="path/to/firebase-credentials.json",
    project_id="my-project-id"
)

# Define a node that directly interacts with Firestore
@firestore_node(collection_name="my_rag_states")
def retrieve(state, firestore_state=None, firestore_adapter=None, thread_id=None):
    """Retrieve documents from a vector store."""
    question = state["question"]
    
    # You can directly read from Firestore
    current_state = firestore_state or {}
    
    # Do retrieval logic...
    documents = ["doc1", "doc2"]
    
    # Return updates to state
    return {"documents": documents}

# Add nodes to the graph
graph.add_node("retrieve", retrieve)
# ... add more nodes and edges

# Compile the graph
compiled_graph = graph.compile()

# Run the graph with a specific thread_id
result = compiled_graph.invoke(
    {"question": "What is LangGraph?"},
    {"configurable": {"thread_id": "user_123"}}
)