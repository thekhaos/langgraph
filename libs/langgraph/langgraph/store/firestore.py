"""Firestore implementation of BaseStore for LangGraph."""

from typing import Any, Dict, List, Optional, Type, Union
import time
import uuid
import logging
from datetime import datetime
from firebase_admin import firestore, initialize_app
import firebase_admin

from checkpoint import BaseStore
from langgraph.errors import CheckpointError

logger = logging.getLogger(__name__)

class FirestoreStateAdapter(BaseStore):
    """Firestore implementation of BaseStore for LangGraph.
    
    This adapter allows LangGraph state to be stored and synchronized with
    Firestore in real-time. Each state key in the graph becomes a field in a
    Firestore document.
    """
    
    def __init__(
        self,
        collection_name: str = "langgraph_states",
        app_name: Optional[str] = None,
        credentials_path: Optional[str] = None,
        project_id: Optional[str] = None,
    ):
        """Initialize the Firestore state adapter.
        
        Args:
            collection_name: Name of the Firestore collection to use
            app_name: Optional name for the Firebase app
            credentials_path: Path to Firebase credentials JSON file
            project_id: Firebase project ID
        """
        self.collection_name = collection_name
        
        # Initialize Firebase app if not already initialized
        try:
            if credentials_path:
                cred = firebase_admin.credentials.Certificate(credentials_path)
                if app_name:
                    self.app = initialize_app(cred, name=app_name, options={"projectId": project_id})
                else:
                    self.app = initialize_app(cred, options={"projectId": project_id})
            else:
                if app_name:
                    self.app = initialize_app(name=app_name)
                else:
                    self.app = initialize_app()
        except ValueError:
            # App already exists
            if app_name:
                self.app = firebase_admin.get_app(app_name)
            else:
                self.app = firebase_admin.get_app()
        
        self.db = firestore.client(self.app)
        self.collection = self.db.collection(collection_name)
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a state by key from Firestore.
        
        Args:
            key: The state key (document ID)
            
        Returns:
            The state dict or None if not found
        """
        doc_ref = self.collection.document(key)
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None
    
    def put(self, key: str, value: Dict[str, Any]) -> None:
        """Store a state in Firestore.
        
        Args:
            key: The state key (document ID)
            value: The state dict to store
        """
        doc_ref = self.collection.document(key)
        # Add metadata
        value["_updated_at"] = datetime.now()
        doc_ref.set(value)
    
    def update(self, key: str, value: Dict[str, Any]) -> None:
        """Update a state in Firestore.
        
        Args:
            key: The state key (document ID)
            value: The state dict to update
        """
        doc_ref = self.collection.document(key)
        # Add metadata
        value["_updated_at"] = datetime.now()
        doc_ref.update(value)
    
    def list_keys(self) -> List[str]:
        """List all state keys in the collection.
        
        Returns:
            List of state keys (document IDs)
        """
        docs = self.collection.stream()
        return [doc.id for doc in docs]
    
    def delete(self, key: str) -> None:
        """Delete a state from Firestore.
        
        Args:
            key: The state key (document ID)
        """
        doc_ref = self.collection.document(key)
        doc_ref.delete()
    
    def watch(self, key: str, callback: callable):
        """Watch a state for changes and call the callback when it changes.
        
        Args:
            key: The state key (document ID)
            callback: Function to call when the state changes
        
        Returns:
            A function that can be called to stop watching
        """
        doc_ref = self.collection.document(key)
        
        # Create a callback on_snapshot function to capture changes
        def on_snapshot(doc_snapshot, changes, read_time):
            for doc in doc_snapshot:
                callback(doc.id, doc.to_dict())
        
        # Watch the document
        watch = doc_ref.on_snapshot(on_snapshot)
        
        # Return a function to unsubscribe
        return lambda: watch.unsubscribe()