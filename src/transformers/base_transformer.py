
from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseTransformer(ABC):
    """
    Base class for all data transformers.

     This class defines the interface that all transformers must implement.
    Each transformer is responsible for taking parsed data and applying
    specific transformations like field mapping, type conversion, or data cleaning.

    """
    def __init__(self, config: Dict[str, Any]=None):
        """
        Initialize the transformer with optional configuration.
        config: Dictionary with transformer-specific settings
        """
        self.config = config or {}

    @abstractmethod
    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply transformation to parsed data.
        Returns:List of transformed records in standardized format
        """
        pass

    def validate_input(self, parsed_data: List[Dict[str, Any]]) -> bool:
        """
            Validate that the input data is in the expected parsed format.
            Returns: True if data is valid, False otherwise
        """
        if not isinstance (parsed_data, list):
            return False

        for record in parsed_data:
            if not isinstance(record, dict):
                return False

            # Check that record has the expected structure from parsers
            if "data" not in record or "metadata" not in record:
                return False

            if not isinstance(record["data"], dict):
                return False

            if not isinstance(record["metadata"], dict):
                return False

        return True

    def get_transformer_info(self)-> Dict[str, str]:
        """
         Get information about this transformer.
         Returns: Dictionary with transformer metadata

        """
        return{
            "transformer_type": self.__class__.__name__,
            "description": "Override in subclass",
            "supported_operations": "Override in subclass"
        }

    def _extract_data_only(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Helper method to extract only the data portion from parsed records.
        Returns:List of parsed records with data and metadata
        """
        return [record["data"] for record in parsed_data]

    def _preserve_metadata(self, original_records: List[Dict[str, Any]],
                           transformed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
         Helper method to preserve metadata while updating data.
         Returns: List of records with transformed data and preserved metadata
        """
        result = []
        for i, (original, new_data) in enumerate(zip(original_records, transformed_data)):
            # zip() combines two lists element by element into pairs
            # enumerate() adds index numbers to each item in a loop
            result.append({
                "data": new_data,
                "metadata": original["metadata"]
            })

        return result

    def _add_transformation_metadata(self, records: List[Dict[str, Any]],
                                     transformation_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
         Helper method to add transformation metadata to records.
         Returns:  List of records with added transformation metadata
        """
        for record in records:
            if "transformations_applied" not in record["metadata"]:
                record["metadata"]["transformations_applied"] = []

            record["metadata"]["transformations_applied"].append(transformation_info)
        return records







