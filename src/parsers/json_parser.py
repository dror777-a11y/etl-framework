import json
from typing import Dict, Any, List
from .base_parser import BaseParser

class JsonParser(BaseParser):
    """
     JSON parser for Kafka message data.

    This parser handles JSON data from Kafka extractors and converts it into
    the standardized format required by transformers.
     """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.strict_mode = self.config.get("strict_mode", False)
        self.handle_malformed= self.config.get("handle_malformed", True)

    def parse(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse raw Kafka data containing JSON messages.
        Returns:List of parsed records in standardized format

        """
        if not self.validate_input(raw_data):
            raise ValueError("Invalid input format - expected list of dictionaries")

        parsed_records = []

        for record in raw_data:
            try:
                parsed_record =self._parse_single_record(record) #_parse_single_record- the next function
                if parsed_record:
                    parsed_records.append(parsed_record)

            except Exception as e:
                if self.strict_mode:
                    raise e
                else:
                    print(f"Warning: Failed to parse record: {e}")

                    fallback_record = self._create_fallback_record(record, str(e))
                    parsed_records.append(fallback_record)

        return parsed_records

    def _parse_single_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a single Kafka record.
        Returns:Parsed record in standardized format

        """
        raw_value=record.get("raw_value")
        if raw_value is None:
            raise ValueError("No 'raw_value' field found in record")

        #try to parse as JSON
        try:
            if isinstance(raw_value, str):
                parsed_data=json.loads(raw_value) #json.loads-Turning JSON-formatted string into Python object.
            else:
        # If it's already a dict
                parsed_data=raw_value
        except json.JSONDecodeError as e:
            if self.handle_malformed:  # If JSON parsing fails, treat as plain text
                parsed_data = {"raw_text": raw_value}
            else:
                raise ValueError(f"Invalid JSON format: {e}")

        # Extract metadata from the Kafka record
        metadata=self.extract_metadata(record)

        # Return in standardized format
        return{
            "data": parsed_data,
            "metadata": metadata,
        }

    def extract_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
         Extract metadata from Kafka record.
         Returns:Dictionary with metadata information
        """
        return {
             "source_type": "kafka",
            "topic": record.get("topic"),
            "partition": record.get("partition"),
            "offset": record.get("offset"),
            "timestamp": record.get("timestamp"),
            "key": record.get("key"),
            "headers": record.get("headers", {})
        }

    def _create_fallback_record(self, record: Dict[str, Any], error_message: str) -> Dict[str, Any]:
        """
        create a fallback record when parsing fails
         Returns:Fallback record in standardized format

        """
        return{
            "data": {
                "raw_content": record.get("raw_value"),
                "parse_error": error_message,
                "parsing_status": "failed"
            },
            "metadata": self._extract_metadata(record)
        }

    def get_parser_info(self) -> Dict[str, str]:
        """
        Get information about this JSON parser.
        Returns: Dictionary with parser metadata

        """
        return {
            "parser_type": "JsonParser",
            "supported_formats": "JSON strings from Kafka messages",
            "description": "Parses JSON data from Kafka extractors into standardized format",
            "strict_mode": str(self.strict_mode),
            "handle_malformed": str(self.handle_malformed)
        }




