"""Decorator for creating nodes that interact directly with Firestore."""

from typing import Any, Callable, Dict, Optional, TypeVar, cast
import functools
import inspect

from langgraph.store.firestore import FirestoreStateAdapter

T = TypeVar("T")

def firestore_node(
    collection_name: str = "langgraph_states",
    app_name: Optional[str] = None,
    credentials_path: Optional[str] = None,
    project_id: Optional[str] = None,
):
    """Decorator for creating nodes that interact directly with Firestore.
    
    This decorator injects a Firestore client into the node function, allowing
    direct interaction with the state stored in Firestore.
    
    Args:
        collection_name: Name of the Firestore collection to use
        app_name: Optional name for the Firebase app
        credentials_path: Path to Firebase credentials JSON file
        project_id: Firebase project ID
        
    Returns:
        A decorator function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create Firestore adapter
            firestore_adapter = FirestoreStateAdapter(
                collection_name=collection_name,
                app_name=app_name,
                credentials_path=credentials_path,
                project_id=project_id,
            )
            
            # Get thread_id from config if available
            thread_id = None
            if "config" in kwargs:
                config = kwargs["config"]
                if "configurable" in config and "thread_id" in config["configurable"]:
                    thread_id = config["configurable"]["thread_id"]
            
            # If we have a thread_id, inject the Firestore document
            if thread_id:
                # Get current state from Firestore
                state = firestore_adapter.get(thread_id) or {}
                
                # Inject state into kwargs
                kwargs["firestore_state"] = state
                kwargs["firestore_adapter"] = firestore_adapter
                kwargs["thread_id"] = thread_id
            
            # Call the original function
            result = func(*args, **kwargs)
            
            # If the function returns a dict and we have a thread_id, update Firestore
            if isinstance(result, dict) and thread_id:
                firestore_adapter.update(thread_id, result)
            
            return result
        
        return wrapper
    
    return decorator