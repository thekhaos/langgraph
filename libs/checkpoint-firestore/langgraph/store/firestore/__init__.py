from typing import Any, Dict, List, Optional, Tuple, Union, cast
from contextlib import contextmanager
import threading
from collections.abc import Iterator, Sequence

from firebase_admin import credentials, firestore, initialize_app
from google.cloud.firestore_v1 import Client as FirestoreClient
from google.cloud.firestore_v1.collection import CollectionReference
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.transaction import Transaction

from langgraph.store.base import BaseStore, Item, ItemNotFound


class FirestoreStore(BaseStore):
    """Firestore-backed store for LangGraph.

    !!! example "Examples"
        Basic setup and usage:
        ```python
        from langgraph.store.firestore import FirestoreStore

        # Using default application credentials
        store = FirestoreStore("your-project-id")

        # Using a service account key file
        store = FirestoreStore("your-project-id", "path/to/service-account-key.json")

        # Store and retrieve data
        store.put(("users", "123"), "prefs", {"theme": "dark"})
        item = store.get(("users", "123"), "prefs")
        ```

    Note:
        The Firestore store organizes data in collections and documents. Each namespace
        component becomes part of the collection path, and the key becomes the document ID.

    Warning:
        Make sure your Firebase project has Firestore enabled and proper authentication
        is set up.
    """

    __slots__ = ("client", "lock")

    def __init__(
        self,
        project_id: str,
        service_account_key_path: Optional[str] = None,
    ) -> None:
        """Initialize a FirestoreStore.

        Args:
            project_id: The Google Cloud project ID.
            service_account_key_path: Optional path to a service account key file.
                If not provided, default application credentials will be used.
        """
        super().__init__()
        self.lock = threading.Lock()

        # Initialize Firebase app with credentials
        if service_account_key_path:
            cred = credentials.Certificate(service_account_key_path)
            app = initialize_app(cred, name=f"langgraph-{threading.get_ident()}")
        else:
            app = initialize_app(name=f"langgraph-{threading.get_ident()}")

        self.client = firestore.client(app)

    def _get_collection_path(self, namespace: Tuple[str, ...]) -> str:
        """Convert a namespace tuple to a Firestore collection path.

        Args:
            namespace: The namespace tuple.

        Returns:
            The Firestore collection path.
        """
        return "/".join(namespace)

    def _get_collection(self, namespace: Tuple[str, ...]) -> CollectionReference:
        """Get a Firestore collection reference for the given namespace.

        Args:
            namespace: The namespace tuple.

        Returns:
            A Firestore collection reference.
        """
        collection_path = self._get_collection_path(namespace)
        return self.client.collection(collection_path)

    def get(self, namespace: Tuple[str, ...], key: str) -> Optional[Item]:
        """Get an item from the store.

        Args:
            namespace: The namespace tuple.
            key: The key within the namespace.

        Returns:
            The item if found, None otherwise.
        """
        collection = self._get_collection(namespace)
        doc_ref = collection.document(key)
        doc = doc_ref.get()

        if not doc.exists:
            return None

        data = doc.to_dict()
        return Item(
            namespace=namespace,
            key=key,
            value=data.get("value"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )

    def put(self, namespace: Tuple[str, ...], key: str, value: Any) -> None:
        """Put an item into the store.

        Args:
            namespace: The namespace tuple.
            key: The key within the namespace.
            value: The value to store.
        """
        collection = self._get_collection(namespace)
        doc_ref = collection.document(key)
        
        # Use server timestamp for created_at and updated_at
        server_timestamp = firestore.SERVER_TIMESTAMP
        
        # Check if document exists to determine if we need to set created_at
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.update({
                "value": value,
                "updated_at": server_timestamp
            })
        else:
            doc_ref.set({
                "value": value,
                "created_at": server_timestamp,
                "updated_at": server_timestamp
            })

    def delete(self, namespace: Tuple[str, ...], key: str) -> None:
        """Delete an item from the store.

        Args:
            namespace: The namespace tuple.
            key: The key within the namespace.
        """
        collection = self._get_collection(namespace)
        doc_ref = collection.document(key)
        doc_ref.delete()

    def list_namespaces(
        self,
        prefix: Optional[Sequence[str]] = None,
        suffix: Optional[Sequence[str]] = None,
        max_depth: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Tuple[str, ...]]:
        """List namespaces in the store.

        Args:
            prefix: Optional prefix to filter namespaces.
            suffix: Optional suffix to filter namespaces.
            max_depth: Optional maximum depth of namespaces to return.
            limit: Optional maximum number of namespaces to return.

        Returns:
            A list of namespace tuples.
        """
        # This is a simplified implementation that lists collections
        # A more complete implementation would need to recursively list collections
        collections = self.client.collections()
        namespaces = []

        for collection in collections:
            namespace = tuple(collection.id.split("/"))
            
            # Apply filters
            if prefix and not namespace[:len(prefix)] == prefix:
                continue
            if suffix and not namespace[-len(suffix):] == suffix:
                continue
            if max_depth and len(namespace) > max_depth:
                continue
                
            namespaces.append(namespace)
            
            if limit and len(namespaces) >= limit:
                break
                
        return namespaces

    def search(
        self,
        namespace_prefix: Sequence[str],
        filter: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Item]:
        """Search for items in the store.

        Args:
            namespace_prefix: The namespace prefix to search in.
            filter: Optional filter to apply to the search.
            limit: Optional maximum number of items to return.
            offset: Optional number of items to skip.

        Returns:
            A list of matching items.
        """
        collection_path = "/".join(namespace_prefix)
        collection = self.client.collection(collection_path)
        query = collection
        
        # Apply filters if provided
        if filter:
            for key, value in filter.items():
                query = query.where(f"value.{key}", "==", value)
        
        # Apply pagination
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
            
        results = query.stream()
        items = []
        
        for doc in results:
            data = doc.to_dict()
            items.append(
                Item(
                    namespace=tuple(namespace_prefix),
                    key=doc.id,
                    value=data.get("value"),
                    created_at=data.get("created_at"),
                    updated_at=data.get("updated_at"),
                )
            )
            
        return items

    async def aget(
        self, namespace: Tuple[str, ...], key: str
    ) -> Optional[Item]:
        """Async version of get."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.get(namespace, key)

    async def aput(
        self, namespace: Tuple[str, ...], key: str, value: Any
    ) -> None:
        """Async version of put."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        self.put(namespace, key, value)

    async def adelete(self, namespace: Tuple[str, ...], key: str) -> None:
        """Async version of delete."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        self.delete(namespace, key)

    async def alist_namespaces(
        self,
        prefix: Optional[Sequence[str]] = None,
        suffix: Optional[Sequence[str]] = None,
        max_depth: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Tuple[str, ...]]:
        """Async version of list_namespaces."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.list_namespaces(prefix, suffix, max_depth, limit)

    async def asearch(
        self,
        namespace_prefix: Sequence[str],
        filter: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Item]:
        """Async version of search."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.search(namespace_prefix, filter, limit, offset)