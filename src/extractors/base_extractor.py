from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseExtractor(ABC):
    """
        This is the blueprint that all data extractors need to follow.This is the "father"

        Think of it like a contract - every extractor (MongoDB, Kafka)
        must have these same methods, but each one implements them differently.
        """

    def __init__(self, config: Dict[str, Any]):
        "Instructor"
        "Dict-Dictionary with keys as String and values as any type"
        self.config = config
        self.connection = None

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        "Actually pulling the data from the source"
        "Returns: A list of records, where each record is a dictionary"
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        " Clean up and close the connection when we're done."
        "Returns: True if the connection was cleaned up, False otherwise"
        pass

    def __enter__(self):
        """Context manager entry - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.disconnect()
