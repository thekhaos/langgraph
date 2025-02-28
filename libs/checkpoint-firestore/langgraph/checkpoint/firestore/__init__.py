import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, Optional, Dict, List, Tuple, cast

from firebase_admin import credentials, firestore, initialize_app
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.store.firestore import FirestoreStore


class FirestoreSaver:
    """Firestore-backed checkpoint saver for LangGraph.

    !!! example "Examples"
        Basic setup and usage:
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

    Note:
        The Firestore saver organizes checkpoints in collections and documents. Each checkpoint
        is stored as a document with associated metadata and channel values.

    Warning:
        Make sure your Firebase project has Firestore enabled and proper authentication
        is set up.

        Async operations are not currently supported natively. The async methods use the
        synchronous implementations under the hood.
    """

    lock: threading.Lock
    jsonplus_serde = JsonPlusSerializer()

    def __init__(
        self,
        project_id: str,
        service_account_key_path: Optional[str] = None,
        serde: Optional[SerializerProtocol] = None,
    ) -> None:
        """Initialize a FirestoreSaver.

        Args:
            project_id: The Google Cloud project ID.
            service_account_key_path: Optional path to a service account key file.
                If not provided, default application credentials will be used.
            serde: Optional serializer/deserializer to use for checkpoint data.
                If not provided, JsonPlusSerializer will be used.
        """
        self.store = FirestoreStore(project_id, service_account_key_path)
        self.lock = threading.Lock()
        self.serde = serde or self.jsonplus_serde

    def _get_thread_id(self, config: RunnableConfig) -> str:
        """Get the thread ID from the config.

        Args:
            config: The runnable config.

        Returns:
            The thread ID.
        """
        configurable = config.get("configurable", {})
        return str(configurable.get("thread_id", "default"))

    def _get_checkpoint_namespace(self, config: RunnableConfig) -> str:
        """Get the checkpoint namespace from the config.

        Args:
            config: The runnable config.

        Returns:
            The checkpoint namespace.
        """
        configurable = config.get("configurable", {})
        return str(configurable.get("checkpoint_namespace", ""))

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        limit: Optional[int] = None,
    ) -> List[CheckpointTuple]:
        """List checkpoints for a thread.

        Args:
            config: The runnable config containing the thread ID.
            limit: Optional maximum number of checkpoints to return.

        Returns:
            A list of checkpoint tuples (id, metadata).
        """
        if config is None:
            return []

        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_namespace(config)

        # Get all checkpoints for this thread
        items = self.store.search(
            ("checkpoints", thread_id, checkpoint_ns),
            limit=limit,
        )

        # Convert to checkpoint tuples
        return [
            (item.key, cast(CheckpointMetadata, item.value.get("metadata", {})))
            for item in items
        ]

    def get(
        self,
        config: RunnableConfig,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """Get a checkpoint.

        Args:
            config: The runnable config containing the thread ID.
            checkpoint_id: Optional checkpoint ID. If not provided, the latest checkpoint
                will be returned.

        Returns:
            The checkpoint if found, None otherwise.
        """
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_namespace(config)

        # If no checkpoint ID is provided, get the latest checkpoint
        if checkpoint_id is None:
            checkpoints = self.list(config, limit=1)
            if not checkpoints:
                return None
            checkpoint_id = checkpoints[0][0]

        # Get the checkpoint document
        checkpoint_item = self.store.get(
            ("checkpoints", thread_id, checkpoint_ns), checkpoint_id
        )
        if checkpoint_item is None:
            return None

        checkpoint_data = checkpoint_item.value
        checkpoint = cast(Checkpoint, checkpoint_data.get("checkpoint", {}))

        # Get channel values
        channel_values = {}
        for channel, version in checkpoint.get("channel_versions", {}).items():
            channel_item = self.store.get(
                ("checkpoint_blobs", thread_id, checkpoint_ns, channel), str(version)
            )
            if channel_item and channel_item.value.get("type") != "empty":
                channel_values[channel] = self.serde.loads_typed(
                    (
                        channel_item.value.get("type", ""),
                        channel_item.value.get("blob", None),
                    )
                )

        # Get pending sends
        pending_sends = []
        pending_sends_item = self.store.get(
            ("checkpoint_writes", thread_id, checkpoint_ns, checkpoint_id), "pending_sends"
        )
        if pending_sends_item:
            for send_data in pending_sends_item.value.get("sends", []):
                pending_sends.append(
                    self.serde.loads_typed(
                        (send_data.get("type", ""), send_data.get("blob", None))
                    )
                )

        # Combine everything into a complete checkpoint
        return {
            **checkpoint,
            "channel_values": channel_values,
            "pending_sends": pending_sends,
        }

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Save a checkpoint.

        Args:
            config: The runnable config containing the thread ID.
            checkpoint: The checkpoint to save.
            metadata: Additional metadata to save with the checkpoint.
            new_versions: New channel versions as of this write.

        Returns:
            Updated configuration after storing the checkpoint.
        """
        thread_id = self._get_thread_id(config)
        checkpoint_ns = self._get_checkpoint_namespace(config)
        checkpoint_id = checkpoint["id"]

        # Store channel values
        for channel, value in checkpoint.get("channel_values", {}).items():
            if channel in new_versions:
                version = new_versions[channel]
                channel_type, channel_blob = self.serde.dumps_typed(value)
                self.store.put(
                    ("checkpoint_blobs", thread_id, checkpoint_ns, channel),
                    str(version),
                    {
                        "type": channel_type,
                        "blob": channel_blob,
                    },
                )

        # Store pending sends
        if checkpoint.get("pending_sends"):
            sends_data = []
            for send in checkpoint["pending_sends"]:
                send_type, send_blob = self.serde.dumps_typed(send)
                sends_data.append({"type": send_type, "blob": send_blob})

            self.store.put(
                ("checkpoint_writes", thread_id, checkpoint_ns, checkpoint_id),
                "pending_sends",
                {"sends": sends_data},
            )

        # Store the checkpoint without channel values and pending sends
        checkpoint_data = {
            **checkpoint,
            "channel_values": {},
            "pending_sends": [],
        }

        # Store the checkpoint document
        self.store.put(
            ("checkpoints", thread_id, checkpoint_ns),
            checkpoint_id,
            {
                "checkpoint": checkpoint_data,
                "metadata": metadata,
                "parent_checkpoint_id": metadata.get("parents", {}).get(checkpoint_ns),
            },
        )

        return config

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        limit: Optional[int] = None,
    ) -> List[CheckpointTuple]:
        """Async version of list."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.list(config, limit=limit)

    async def aget(
        self,
        config: RunnableConfig,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """Async version of get."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.get(config, checkpoint_id)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Async version of put."""
        # Firestore Python client doesn't have native async support
        # We'll use the synchronous version in an async context
        return self.put(config, checkpoint, metadata, new_versions)