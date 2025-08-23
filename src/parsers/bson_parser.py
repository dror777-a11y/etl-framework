from datetime import datetime
from typing import Dict, Any, List
from .base_parser import BaseParser


class BsonParser(BaseParser):
    """
    BSON parser for MongoDB document data.
    This parser handles BSON data from MongoDB extractors and converts it into
    the standardized format required by transformers.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the BSON parser."""
        super().__init__(config)
        # Configuration options for BSON parsing
        self.convert_objectid = self.config.get("convert_objectid", True)  # convert into a string
        self.convert_datetime = self.config.get("convert_datetime", True)
        self.preserve_id_field = self.config.get("preserve_id_field", True)

    def parse(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse raw MongoDB data containing BSON documents.
        Returns: List of parsed records in standardized format
        """
        # Validate input first
        if not self.validate_input(raw_data):
            raise ValueError("Invalid input format - expected list of dictionaries")

        parsed_records = []

        for record in raw_data:
            try:
                parsed_record = self._parse_single_record(record)
                if parsed_record:
                    parsed_records.append(parsed_record)
            except Exception as e:
                print(f"Warning: Failed to parse MongoDB record: {e}")
                # Creating fallback record for MongoDB
                fallback_record = self._create_fallback_record(record, str(e))
                parsed_records.append(fallback_record)

        return parsed_records

    def _parse_single_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single MongoDB record.
        Returns: Parsed record in standardized format
        """
        # Because MongoDB records are already python dict we only need a cleanup
        cleaned_data = self._clean_bson_data(record)

        # Extract metadata
        metadata = self._extract_metadata(record)

        # Return in standard format
        return {
            "data": cleaned_data,
            "metadata": metadata
        }

    def _clean_bson_data(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean BSON-specific data types and convert to standard types.
        Returns: Cleaned document with standard Python types
        """
        cleaned = {}  # empty dict

        for key, value in record.items():
            if key == "_id":  # Handle MongoDB ObjectId
                if self.preserve_id_field:
                    if self.convert_objectid:  # ObjectId is already converted to string by MongoExtractor
                        cleaned["_id"] = str(value)  # convert to string
                    else:
                        cleaned["_id"] = value  # if already string
                # If preserve_id_field is False, skip the _id field

            elif isinstance(value, datetime):  # Handle datetime objects
                if self.convert_datetime:
                    cleaned[key] = value.isoformat()
                else:
                    cleaned[key] = value

            elif isinstance(value, dict):  # Recursively clean nested documents
                cleaned[key] = self._clean_bson_data(value)

            elif isinstance(value, list):  # Clean lists that might contain BSON objects
                cleaned[key] = self._clean_bson_list(value)

            else:  # already in standard types
                cleaned[key] = value

        return cleaned

    def _clean_bson_list(self, items: List[Any]) -> List[Any]:
        """
        Clean a list that might contain BSON objects.
        Returns: Cleaned list with standard Python types
        """
        cleaned_items = []

        for item in items:
            if isinstance(item, dict):
                cleaned_items.append(self._clean_bson_data(item))
            elif isinstance(item, datetime):
                if self.convert_datetime:
                    cleaned_items.append(item.isoformat())
                else:
                    cleaned_items.append(item)
            elif isinstance(item, list):
                cleaned_items.append(self._clean_bson_list(item))
            else:
                cleaned_items.append(item)

        return cleaned_items

    def _extract_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from MongoDB record.
        Returns: Dictionary with metadata information
        """
        metadata = {
            "source_type": "mongodb",
            "document_id": str(record.get("_id")) if "_id" in record else None,
            "field_count": len(record),
            "has_nested_objects": self._has_nested_objects(record),
        }

        # Add any additional MongoDB-specific metadata
        if "_id" in record:
            metadata["original_id_type"] = type(record["_id"]).__name__

        return metadata

    def _has_nested_objects(self, record: Dict[str, Any]) -> bool:
        """
        Check if the record contains nested objects or arrays.
        Returns: True if record contains nested structures, False otherwise
        """
        for value in record.values():
            if isinstance(value, (dict, list)):
                return True
        return False

    def _create_fallback_record(self, record: Dict[str, Any], error_message: str) -> Dict[str, Any]:
        """
        Create a fallback record when parsing fails.
        Returns: Fallback record in standardized format
        """
        return {
            "data": {
                "raw_document": record,
                "parse_error": error_message,
                "parsing_status": "failed"
            },
            "metadata": {
                "source_type": "mongodb",
                "document_id": str(record.get("_id")) if "_id" in record else None,
                "parse_error": True
            }
        }

    def get_parser_info(self) -> Dict[str, str]:
        """
        Get information about this BSON parser.
        Returns: Dictionary with parser metadata
        """
        return {
            "parser_type": "BsonParser",
            "supported_formats": "MongoDB BSON documents",
            "description": "Parses BSON data from MongoDB extractors into standardized format",
            "convert_objectid": str(self.convert_objectid),
            "convert_datetime": str(self.convert_datetime),
            "preserve_id_field": str(self.preserve_id_field)
        }