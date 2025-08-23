from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseLoader(ABC):
    """
    Base class for all data loaders.

    This class defines the interface that all loaders must implement.
    Each loader is responsible for taking transformed data and loading it
    into a target destination like databases, files, or APIs.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the loader with optional configuration.

        Args:
            config: Dictionary with loader-specific settings
        """
        self.config = config or {}
        self.connection = None

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the target destination.

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abstractmethod
    def load(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load transformed data into the target destination.

        Args:
            transformed_data: List of transformed records ready for loading

        Returns:
            Dictionary with load results and statistics
        """
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """
        Close connection to the target destination.

        Returns:
            True if disconnection successful, False otherwise
        """
        pass

    def validate_input(self, transformed_data: List[Dict[str, Any]]) -> bool:
        """
        Validate that the input data is in the expected transformed format.

        Args:
            transformed_data: Transformed data to validate

        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(transformed_data, list):
            return False

        for record in transformed_data:
            if not isinstance(record, dict):
                return False

            # Check that record has the expected structure from transformers
            if "data" not in record or "metadata" not in record:
                return False

        return True

    def get_loader_info(self) -> Dict[str, str]:
        """
        Get information about this loader.

        Returns:
            Dictionary with loader metadata
        """
        return {
            "loader_type": self.__class__.__name__,
            "description": "Override in subclass",
            "target_destination": "Override in subclass"
        }

    def _extract_data_for_loading(self, transformed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Helper method to extract only the data portion for loading.

        Args:
            transformed_data: List of transformed records with data and metadata

        Returns:
            List of data dictionaries ready for loading
        """
        return [record["data"] for record in transformed_data]

    def _prepare_batch(self, data: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
        """
        Helper method to split data into batches for efficient loading.

        Args:
            data: Data to batch
            batch_size: Size of each batch

        Returns:
            List of batches
        """
        batches = []
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batches.append(batch)
        return batches

    def _create_load_result(self, success: bool, records_processed: int,
                            records_loaded: int, errors: List[str] = None) -> Dict[str, Any]:
        """
        Helper method to create standardized load results.

        Args:
            success: Whether the overall load was successful
            records_processed: Total number of records processed
            records_loaded: Number of records successfully loaded
            errors: List of error messages, if any

        Returns:
            Standardized load result dictionary
        """
        return {
            "success": success,
            "records_processed": records_processed,
            "records_loaded": records_loaded,
            "records_failed": records_processed - records_loaded,
            "errors": errors or [],
            "load_rate": records_loaded / records_processed if records_processed > 0 else 0.0
        }

    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to the target destination.

        Returns:
            Dictionary with connection test results
        """
        try:
            connection_result = self.connect()
            if connection_result:
                self.disconnect()
                return {
                    "connection_successful": True,
                    "message": "Successfully connected to target destination"
                }
            else:
                return {
                    "connection_successful": False,
                    "message": "Failed to connect to target destination"
                }
        except Exception as e:
            return {
                "connection_successful": False,
                "message": f"Connection test failed with error: {str(e)}"
            }

    def __enter__(self):
        """Context manager entry - establish connection."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.disconnect()