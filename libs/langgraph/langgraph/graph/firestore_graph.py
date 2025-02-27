"""StateGraph implementation that uses Firestore for state management."""

from typing import Any, Dict, Optional, Type, Union
import uuid

from langgraph.graph.state import StateGraph
from langgraph.store.firestore import FirestoreStateAdapter
from checkpoint import Checkpointer

class FirestoreStateGraph(StateGraph):
    """A StateGraph that uses Firestore for state management.
    
    This graph implementation stores and synchronizes state with Firestore,
    allowing for real-time updates and persistence.
    """
    
    def __init__(
        self,
        state_schema: Optional[Type[Any]] = None,
        config_schema: Optional[Type[Any]] = None,
        *,
        input: Optional[Type[Any]] = None,
        output: Optional[Type[Any]] = None,
        collection_name: str = "langgraph_states",
        app_name: Optional[str] = None,
        credentials_path: Optional[str] = None,
        project_id: Optional[str] = None,
        thread_id: Optional[str] = None,
    ):
        """Initialize a FirestoreStateGraph.
        
        Args:
            state_schema: The schema class that defines the state
            config_schema: The schema class that defines the configuration
            input: The input schema (defaults to state_schema)
            output: The output schema (defaults to state_schema)
            collection_name: Name of the Firestore collection to use
            app_name: Optional name for the Firebase app
            credentials_path: Path to Firebase credentials JSON file
            project_id: Firebase project ID
            thread_id: Optional thread ID to use for the state document
        """
        super().__init__(
            state_schema=state_schema,
            config_schema=config_schema,
            input=input,
            output=output,
        )
        
        self.firestore_adapter = FirestoreStateAdapter(
            collection_name=collection_name,
            app_name=app_name,
            credentials_path=credentials_path,
            project_id=project_id,
        )
        
        # Generate or use provided thread_id
        self.thread_id = thread_id or f"thread_{uuid.uuid4().hex}"
    
    def compile(
        self,
        checkpointer: Optional[Checkpointer] = None,
        *,
        interrupt_before: Optional[Union[str, list[str]]] = None,
        interrupt_after: Optional[Union[str, list[str]]] = None,
        debug: bool = False,
        name: Optional[str] = None,
    ):
        """Compile the graph with Firestore state management.
        
        This overrides the default compile method to use the Firestore adapter.
        """
        # Use the Firestore adapter as the store
        return super().compile(
            checkpointer=checkpointer,
            store=self.firestore_adapter,
            interrupt_before=interrupt_before,
            interrupt_after=interrupt_after,
            debug=debug,
            name=name or f"FirestoreGraph-{self.thread_id}",
        )