from typing import Dict, Any, List
from datetime import datetime
from .base_transformer import BaseTransformer

class FieldMapper(BaseTransformer):
    """
    Field mapping transformer that standardizes field names across different data sources.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the field mapper with mapping configuration.

        Expected config format:
        {
            "field_mappings": {
                "kafka": {
                    "firstName": ["first_name", "user_name", "f_name"],
                    "lastName": ["last_name", "surname"],
                    "age": ["user_age", "person_age"]
                },
                "mongodb": {
                    "firstName": ["customer_name", "name"],
                    "age": ["customer_age"]
                }
            },
            "keep_unmapped_fields": True,  # Whether to keep fields not in mapping
            "case_sensitive": False        # Whether field matching is case sensitive
        """
        super().__init__(config)
        self.field_mappings = self.config.get("field_mappings", {}) # Extract field mapping rules from config
        self.keep_unmapped_fields = self.config.get("keep_unmapped_fields", True) # Control whether to preserve fields not defined in mappings
        self.case_sensitive = self.config.get("case_sensitive", False) # Control case sensitivity in field name matching

        # ***Createing reverse mapping for faster lookup***
        self._create_reverse_mapping()


    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            """
            Apply field mapping transformations to parsed data.
            Returns: List of records with mapped field names
            """
            if not self.validate_input(parsed_data):
                raise ValueError("Invalid input format - expected parsed data with data and metadata")

            data_only=self._extract_data_only(parsed_data)

            #Apply field mapping to each record
            mapped_data = []
            for i, record in enumerate(data_only): #gives an index number to each item
                source_type=parsed_data[i]["metadata"].get("source_type", "unknown")
                mapped_record = self._map_fields(record, source_type)
                mapped_data.append(mapped_record)

             # Preserve metadata and add transformation info
            result=self._preserve_metadata(parsed_data, mapped_data)

            # Add transformation metadata
            transformation_info={
                "type": "field_mapping",
                "timestamp": datetime.now().isoformat(),
                "transformer": "FieldMapper",
                "fields_mapped": len([k for mapping in self.field_mappings.values() for k in mapping.keys()])
            }
            return self._add_transformation_metadata(result, transformation_info)

    def _create_reverse_mapping(self):
            """
            Create reverse mapping from source field names to target field names.
        This makes field lookup faster during transformation.
            """
            self.reverse_mapping = {}

            for source_type, mapping in self.field_mappings.items():
                self.reverse_mapping[source_type] = {}

                for target_field, source_fields in mapping.items():
                    for source_field in source_fields:
                        lookup_key= source_field if self.case_sensitive else source_field.lower()
                        self.reverse_mapping[source_type][lookup_key] = target_field

    def _map_fields(self, record: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        mapped_record = {}
        source_mappings = self.reverse_mapping.get(source_type, {})

        for field_name, field_value in record.items():
            # Check if field should be mapped
            if field_name in source_mappings:
                target_field = source_mappings[field_name]
                mapped_record[target_field] = field_value
            elif self.keep_unmapped_fields:
                mapped_record[field_name] = field_value

        return mapped_record

    def get_transformer_info(self) -> Dict[str, str]:
            """
            Get information about this field mapper.
            Returns: Dictionary with transformer metadata
            """
            mapped_sources=list(self.field_mappings.keys())
            total_mappings= sum(len(mappings) for mappings in self.field_mappings.values())

            return{
                "transformer_type": "FieldMapper",
                "description": "Maps field names from source-specific to standardized names",
                "supported_operations": "Field name mapping and standardization",
                "mapped_sources": str(mapped_sources),
                "total_field_mappings": str(total_mappings),
                "keep_unmapped_fields": str(self.keep_unmapped_fields),
                "case_sensitive": str(self.case_sensitive)
            }

    def add_mapping(self,source_type: str,target_field: str, source_fields: List[str]):
            """
             Add a new field mapping rule dynamically.
            """
            if source_type not in self.field_mappings:
                self.field_mappings[source_type] = {}

            self.field_mappings[source_type][target_field] = source_fields

            # Recreate reverse mapping
            self._create_reverse_mapping()


    def remove_mapping(self, source_type: str, target_field: str):
            """
            remove a field mapping rule

            """
            if source_type in self.field_mappings and target_field in self.field_mappings[source_type]:
                del self.field_mappings[source_type][target_field]

                # Recreate reverse mapping
                self._create_reverse_mapping()


    def get_mapping_stats(self) -> Dict[str, Any]:
            """
            Get statistics about current field mappings.
            Returns: Dictionary with mapping statistics
            """
            stats= {
                "total_source_types": len(self.field_mappings),
                "mappings_per_source": {},
                "all_target_fields": set(),
                "total_source_fields": 0
            }

            for source_type, mappings in self.field_mappings.items():
                stats["mappings_per_source"][source_type] = len(mappings)
                stats["all_target_fields"].update(mappings.keys())
                stats["total_source_fields"] += sum(len(source_fields) for source_fields in mappings.values())

            stats["all_target_fields"] = list(stats["all_target_fields"])
            stats["unique_target_fields"] = len(stats["all_target_fields"])

            return stats









