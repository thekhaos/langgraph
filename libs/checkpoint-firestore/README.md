# LangGraph Firestore Checkpoint

Implementation of LangGraph CheckpointSaver that uses Google Cloud Firestore database for persistence.

## Installation

```bash
pip install langgraph-checkpoint-firestore
```

## Usage

```python
from langgraph.checkpoint import FirestoreSaver
from langgraph.graph import StateGraph

# Initialize the Firestore saver
# Option 1: Using default application credentials
memory = FirestoreSaver("your-project-id")

# Option 2: Using a service account key file
memory = FirestoreSaver("your-project-id", "path/to/service-account-key.json")

# Create a graph with the Firestore checkpointer
builder = StateGraph(int)
builder.add_node("add_one", lambda x: x + 1)
builder.set_entry_point("add_one")
builder.set_finish_point("add_one")

graph = builder.compile(checkpointer=memory)

# Use the graph with persistence
config = {"configurable": {"thread_id": "unique-thread-id"}}
result = graph.invoke(3, config)

# Retrieve state from a previous run
state = graph.get_state(config)
```

## Requirements

- Python 3.9+
- firebase-admin
- langgraph-checkpoint

## Features

- Persistent storage of LangGraph checkpoints in Firestore
- Thread-based organization of checkpoints
- Support for checkpoint namespaces
- Retrieval of checkpoint history

## Limitations

- Async operations are not currently supported
- For high-throughput production use cases, consider using PostgreSQL with `langgraph-checkpoint-postgres`

## API Reference

### FirestoreSaver

```python
FirestoreSaver(project_id, credentials_path=None, collection_name="langgraph_checkpoints")
```

**Parameters:**

- `project_id` (str): The Google Cloud project ID for your Firestore database
- `credentials_path` (str, optional): Path to a service account key file. If not provided, default application credentials will be used.
- `collection_name` (str, optional): Name of the Firestore collection to store checkpoints. Defaults to "langgraph_checkpoints".

**Methods:**

- `setup()`: Initialize the Firestore connection
- `put(config, checkpoint, metadata, run_metadata)`: Store a checkpoint
- `get(config)`: Retrieve the latest checkpoint for a thread
- `list(config)`: List all checkpoints for a thread
- `get_by_id(config, checkpoint_id)`: Retrieve a specific checkpoint by ID

## Setting up Firestore

1. Create a Google Cloud project or use an existing one
2. Enable the Firestore API in your project
3. Create a Firestore database in Native mode
4. Set up authentication:
   - For local development: Create a service account key and download the JSON file
   - For production: Use application default credentials

For more information, see the [Firestore documentation](https://firebase.google.com/docs/firestore).