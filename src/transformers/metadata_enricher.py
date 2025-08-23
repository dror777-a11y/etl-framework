from typing import Dict, Any, List
from datetime import datetime
import uuid
from .base_transformer import BaseTransformer


class MetadataEnricher(BaseTransformer):
    """
    Metadata enrichment transformer that adds standard metadata fields.

    This transformer adds consistent metadata fields like createdAt, processedAt,
    unique identifiers, and other tracking information as required by the target format.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the metadata enricher with enrichment rules.

        Expected config format:
        {
            "add_created_at": True,
            "add_processed_at": True,
            "add_unique_id": True,
            "add_source_info": True,
            "created_at_field": "createdAt",
            "processed_at_field": "processedAt",
            "id_field": "recordId",
            "datetime_format": "iso",  # "iso", "timestamp", "custom"
            "custom_datetime_format": "%Y-%m-%d %H:%M:%S",
            "id_prefix": "",
            "additional_metadata": {}
        }
        """
        super().__init__(config)

        # Control which metadata fields to add
        self.add_created_at = self.config.get("add_created_at", True)
        self.add_processed_at = self.config.get("add_processed_at", True)
        self.add_unique_id = self.config.get("add_unique_id", False)
        self.add_source_info = self.config.get("add_source_info", True)

        # Field name configuration
        self.created_at_field = self.config.get("created_at_field", "createdAt")
        self.processed_at_field = self.config.get("processed_at_field", "processedAt")
        self.id_field = self.config.get("id_field", "recordId")

        # Datetime format configuration
        self.datetime_format = self.config.get("datetime_format", "iso")
        self.custom_datetime_format = self.config.get("custom_datetime_format", "%Y-%m-%d %H:%M:%S")

        # ID generation configuration
        self.id_prefix = self.config.get("id_prefix", "")

        # Additional static metadata to add
        self.additional_metadata = self.config.get("additional_metadata", {})

        # Store processing timestamp
        self.processing_timestamp = datetime.now()

    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply metadata enrichment to parsed data.

        Args:
            parsed_data: List of parsed records from previous transformers

        Returns:
            List of records with enriched metadata fields
        """
        if not self.validate_input(parsed_data):
            raise ValueError("Invalid input format - expected parsed data with data and metadata")

        # Extract data for processing
        data_only = self._extract_data_only(parsed_data)

        # Enrich each record with metadata
        enriched_data = []
        enrichment_stats = {
            "records_processed": 0,
            "created_at_added": 0,
            "processed_at_added": 0,
            "unique_ids_added": 0,
            "source_info_added": 0
        }

        for i, record in enumerate(data_only):
            original_metadata = parsed_data[i]["metadata"]
            enriched_record, record_stats = self._enrich_record(record, original_metadata)
            enriched_data.append(enriched_record)

            # Update statistics
            for key, value in record_stats.items():
                enrichment_stats[key] += value
            enrichment_stats["records_processed"] += 1

        # Preserve metadata and add transformation info
        result = self._preserve_metadata(parsed_data, enriched_data)

        # Add transformation metadata
        transformation_info = {
            "type": "metadata_enrichment",
            "timestamp": datetime.now().isoformat(),
            "transformer": "MetadataEnricher",
            "enrichment_stats": enrichment_stats,
            "fields_added": self._get_added_fields()
        }

        return self._add_transformation_metadata(result, transformation_info)

    def _enrich_record(self, record: Dict[str, Any], original_metadata: Dict[str, Any]) -> tuple:
        """
        Enrich a single record with metadata fields.

        Args:
            record: Single data record to enrich
            original_metadata: Original metadata from parser

        Returns:
            Tuple of (enriched_record, enrichment_stats)
        """
        enriched_record = record.copy()
        stats = {
            "created_at_added": 0,
            "processed_at_added": 0,
            "unique_ids_added": 0,
            "source_info_added": 0
        }

        # Add createdAt field (as specified in requirements)
        if self.add_created_at:
            enriched_record[self.created_at_field] = self._format_datetime(self.processing_timestamp)
            stats["created_at_added"] = 1

        # Add processedAt field
        if self.add_processed_at:
            enriched_record[self.processed_at_field] = self._format_datetime(datetime.now())
            stats["processed_at_added"] = 1

        # Add unique identifier
        if self.add_unique_id:
            unique_id = self._generate_unique_id()
            enriched_record[self.id_field] = unique_id
            stats["unique_ids_added"] = 1

        # Add source information
        if self.add_source_info:
            source_type = original_metadata.get("source_type", "unknown")
            enriched_record["dataSource"] = source_type

            # Add specific source details
            if source_type == "kafka":
                topic = original_metadata.get("topic")
                if topic:
                    enriched_record["sourceTopic"] = topic
            elif source_type == "mongodb":
                doc_id = original_metadata.get("document_id")
                if doc_id:
                    enriched_record["sourceDocumentId"] = doc_id

            stats["source_info_added"] = 1

        # Add any additional static metadata
        for key, value in self.additional_metadata.items():
            if key not in enriched_record:  # Don't override existing fields
                enriched_record[key] = value

        return enriched_record, stats

    def _format_datetime(self, dt: datetime) -> str:
        """
        Format datetime according to configuration.

        Args:
            dt: Datetime to format

        Returns:
            Formatted datetime string
        """
        if self.datetime_format == "iso":
            return dt.isoformat()
        elif self.datetime_format == "timestamp":
            return str(int(dt.timestamp()))
        elif self.datetime_format == "custom":
            return dt.strftime(self.custom_datetime_format)
        else:
            # Default to ISO format
            return dt.isoformat()

    def _generate_unique_id(self) -> str:
        """
        Generate a unique identifier for a record.

        Returns:
            Unique identifier string
        """
        unique_id = str(uuid.uuid4())

        if self.id_prefix:
            return f"{self.id_prefix}_{unique_id}"

        return unique_id

    def _get_added_fields(self) -> List[str]:
        """Get list of fields that will be added by this transformer."""
        added_fields = []

        if self.add_created_at:
            added_fields.append(self.created_at_field)
        if self.add_processed_at:
            added_fields.append(self.processed_at_field)
        if self.add_unique_id:
            added_fields.append(self.id_field)
        if self.add_source_info:
            added_fields.extend(["dataSource", "sourceTopic", "sourceDocumentId"])

        # Add additional metadata field names
        added_fields.extend(self.additional_metadata.keys())

        return added_fields

    def get_transformer_info(self) -> Dict[str, str]:
        """
        Get information about this metadata enricher.

        Returns:
            Dictionary with transformer metadata
        """
        return {
            "transformer_type": "MetadataEnricher",
            "description": "Adds standard metadata fields like createdAt and source information",
            "supported_operations": "Metadata field addition, datetime formatting, unique ID generation",
            "fields_added": str(self._get_added_fields()),
            "datetime_format": self.datetime_format,
            "created_at_field": self.created_at_field,
            "processed_at_field": self.processed_at_field,
            "additional_metadata_count": str(len(self.additional_metadata))
        }

    def add_metadata_field(self, field_name: str, field_value: Any):
        """
        Add a new static metadata field to be added to all records.

        Args:
            field_name: Name of the metadata field
            field_value: Value to add for all records
        """
        self.additional_metadata[field_name] = field_value

    def remove_metadata_field(self, field_name: str):
        """
        Remove a static metadata field.

        Args:
            field_name: Name of the field to remove
        """
        if field_name in self.additional_metadata:
            del self.additional_metadata[field_name]

    def set_datetime_format(self, format_type: str, custom_format: str = None):
        """
        Set the datetime format for timestamp fields.

        Args:
            format_type: "iso", "timestamp", or "custom"
            custom_format: Custom format string if format_type is "custom"
        """
        valid_formats = ["iso", "timestamp", "custom"]
        if format_type not in valid_formats:
            raise ValueError(f"Invalid format type. Must be one of: {valid_formats}")

        self.datetime_format = format_type
        if format_type == "custom" and custom_format:
            self.custom_datetime_format = custom_format

    def get_enrichment_preview(self, sample_record: Dict[str, Any],
                               sample_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preview what fields would be added to a sample record.

        Args:
            sample_record: Sample data record
            sample_metadata: Sample metadata

        Returns:
            Dictionary showing what the enriched record would look like
        """
        enriched_record, _ = self._enrich_record(sample_record, sample_metadata)
        return enriched_record