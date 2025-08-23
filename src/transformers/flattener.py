from typing import Dict, Any, List, Union
from datetime import datetime
from .base_transformer import BaseTransformer


class Flattener(BaseTransformer):
    """
    Flattener transformer that converts nested JSON structures into flat dictionaries.

    This transformer handles nested objects and arrays by creating flattened field names
    using dot notation or custom separators. Essential for preparing complex data
    structures for relational database storage.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the flattener with flattening configuration.

        Expected config format:
        {
            "separator": ".",                    # Separator for nested field names
            "max_depth": 10,                    # Maximum depth to flatten
            "array_handling": "index",          # "index", "enumerate", or "concat"
            "preserve_arrays": False,           # Keep arrays as JSON strings
            "null_value_handling": "keep",      # "keep", "remove", or "empty_string"
            "flatten_objects": True,            # Whether to flatten nested objects
            "flatten_arrays": True,             # Whether to flatten arrays
            "array_index_format": "[{index}]",  # Format for array indices
            "custom_flatteners": {}             # Field-specific flattening rules
        }
        """
        super().__init__(config)

        # Core flattening configuration
        self.separator = self.config.get("separator", ".")
        self.max_depth = self.config.get("max_depth", 10)
        self.array_handling = self.config.get("array_handling", "index")  # index, enumerate, concat
        self.preserve_arrays = self.config.get("preserve_arrays", False)
        self.null_value_handling = self.config.get("null_value_handling", "keep")  # keep, remove, empty_string

        # Control what gets flattened
        self.flatten_objects = self.config.get("flatten_objects", True)
        self.flatten_arrays = self.config.get("flatten_arrays", True)

        # Array formatting
        self.array_index_format = self.config.get("array_index_format", "[{index}]")

        # Custom flattening rules for specific fields
        self.custom_flatteners = self.config.get("custom_flatteners", {})

    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply flattening transformation to parsed data.

        Args:
            parsed_data: List of parsed records from previous transformers

        Returns:
            List of records with flattened nested structures
        """
        if not self.validate_input(parsed_data):
            raise ValueError("Invalid input format - expected parsed data with data and metadata")

        # Extract data for processing
        data_only = self._extract_data_only(parsed_data)

        # Flatten each record
        flattened_data = []
        flattening_stats = {
            "records_processed": 0,
            "objects_flattened": 0,
            "arrays_flattened": 0,
            "fields_created": 0,
            "max_depth_reached": 0
        }

        for record in data_only:
            flattened_record, record_stats = self._flatten_record(record)
            flattened_data.append(flattened_record)

            # Update flattening statistics
            for key, value in record_stats.items():
                if key == "max_depth_reached":
                    flattening_stats[key] = max(flattening_stats[key], value)
                else:
                    flattening_stats[key] += value
            flattening_stats["records_processed"] += 1

        # Preserve metadata and add transformation info
        result = self._preserve_metadata(parsed_data, flattened_data)

        # Add transformation metadata
        transformation_info = {
            "type": "flattening",
            "timestamp": datetime.now().isoformat(),
            "transformer": "Flattener",
            "flattening_stats": flattening_stats,
            "configuration": self._get_flattening_config()
        }

        return self._add_transformation_metadata(result, transformation_info)

    def _flatten_record(self, record: Dict[str, Any]) -> tuple:
        """
        Flatten a single record's nested structures.

        Args:
            record: Single data record to flatten

        Returns:
            Tuple of (flattened_record, flattening_stats)
        """
        flattened = {}
        stats = {
            "objects_flattened": 0,
            "arrays_flattened": 0,
            "fields_created": 0,
            "max_depth_reached": 0
        }

        self._flatten_dict(record, flattened, "", 0, stats)

        return flattened, stats

    def _flatten_dict(self, obj: Dict[str, Any], result: Dict[str, Any],
                      prefix: str, depth: int, stats: Dict[str, int]):
        """
        Recursively flatten a dictionary object.

        Args:
            obj: Dictionary to flatten
            result: Result dictionary to store flattened values
            prefix: Current field name prefix
            depth: Current nesting depth
            stats: Statistics to update
        """
        stats["max_depth_reached"] = max(stats["max_depth_reached"], depth)

        if depth >= self.max_depth:
            # Convert to string if max depth reached
            result[prefix or "deep_object"] = str(obj)
            return

        for key, value in obj.items():
            new_key = self._create_field_name(prefix, key)

            # Check for custom flattening rules
            if new_key in self.custom_flatteners:
                custom_rule = self.custom_flatteners[new_key]
                result[new_key] = self._apply_custom_flattener(value, custom_rule)
                stats["fields_created"] += 1
                continue

            self._flatten_value(value, result, new_key, depth, stats)

    def _flatten_value(self, value: Any, result: Dict[str, Any],
                       field_name: str, depth: int, stats: Dict[str, int]):
        """
        Flatten a single value based on its type.

        Args:
            value: Value to flatten
            result: Result dictionary
            field_name: Field name for this value
            depth: Current depth
            stats: Statistics to update
        """
        if value is None:
            self._handle_null_value(result, field_name)

        elif isinstance(value, dict) and self.flatten_objects:
            if value:  # Non-empty dict
                stats["objects_flattened"] += 1
                self._flatten_dict(value, result, field_name, depth + 1, stats)
            else:
                result[field_name] = {}
                stats["fields_created"] += 1

        elif isinstance(value, list) and self.flatten_arrays:
            if self.preserve_arrays:
                result[field_name] = str(value)
                stats["fields_created"] += 1
            else:
                self._flatten_array(value, result, field_name, depth, stats)

        else:
            # Simple value (string, number, boolean) or preserved complex type
            result[field_name] = value
            stats["fields_created"] += 1

    def _flatten_array(self, arr: List[Any], result: Dict[str, Any],
                       field_name: str, depth: int, stats: Dict[str, int]):
        """
        Flatten an array based on the configured array handling strategy.

        Args:
            arr: Array to flatten
            result: Result dictionary
            field_name: Field name for this array
            depth: Current depth
            stats: Statistics to update
        """
        if not arr:  # Empty array
            result[field_name] = []
            stats["fields_created"] += 1
            return

        stats["arrays_flattened"] += 1

        if self.array_handling == "index":
            # Create indexed fields: field[0], field[1], etc.
            for i, item in enumerate(arr):
                indexed_field = f"{field_name}{self.array_index_format.format(index=i)}"
                self._flatten_value(item, result, indexed_field, depth, stats)

        elif self.array_handling == "enumerate":
            # Create enumerated fields: field_0, field_1, etc.
            for i, item in enumerate(arr):
                enumerated_field = f"{field_name}_{i}"
                self._flatten_value(item, result, enumerated_field, depth, stats)

        elif self.array_handling == "concat":
            # Concatenate simple values, preserve complex ones as JSON
            simple_values = []
            complex_count = 0

            for i, item in enumerate(arr):
                if isinstance(item, (str, int, float, bool)):
                    simple_values.append(str(item))
                elif item is None:
                    simple_values.append("")
                else:
                    # Handle complex items separately
                    complex_field = f"{field_name}_complex_{complex_count}"
                    self._flatten_value(item, result, complex_field, depth, stats)
                    complex_count += 1

            # Join simple values
            if simple_values:
                result[field_name] = ", ".join(simple_values)
                stats["fields_created"] += 1

    def _create_field_name(self, prefix: str, key: str) -> str:
        """
        Create a field name by combining prefix and key.

        Args:
            prefix: Current prefix
            key: Field key

        Returns:
            Combined field name
        """
        if not prefix:
            return key
        return f"{prefix}{self.separator}{key}"

    def _handle_null_value(self, result: Dict[str, Any], field_name: str):
        """
        Handle null values based on configuration.

        Args:
            result: Result dictionary
            field_name: Field name for null value
        """
        if self.null_value_handling == "keep":
            result[field_name] = None
        elif self.null_value_handling == "empty_string":
            result[field_name] = ""
        # "remove" option: don't add the field at all

    def _apply_custom_flattener(self, value: Any, rule: Dict[str, Any]) -> Any:
        """
        Apply custom flattening rule to a value.

        Args:
            value: Value to apply rule to
            rule: Custom flattening rule

        Returns:
            Processed value
        """
        rule_type = rule.get("type", "string")

        if rule_type == "string":
            return str(value)
        elif rule_type == "json":
            import json
            return json.dumps(value) if value is not None else None
        elif rule_type == "first_element" and isinstance(value, list):
            return value[0] if value else None
        elif rule_type == "length" and isinstance(value, (list, dict, str)):
            return len(value)
        elif rule_type == "keys" and isinstance(value, dict):
            return list(value.keys())
        else:
            return value

    def _get_flattening_config(self) -> Dict[str, Any]:
        """Get current flattening configuration."""
        return {
            "separator": self.separator,
            "max_depth": self.max_depth,
            "array_handling": self.array_handling,
            "preserve_arrays": self.preserve_arrays,
            "flatten_objects": self.flatten_objects,
            "flatten_arrays": self.flatten_arrays,
            "null_value_handling": self.null_value_handling
        }

    def get_transformer_info(self) -> Dict[str, str]:
        """
        Get information about this flattener.

        Returns:
            Dictionary with transformer metadata
        """
        return {
            "transformer_type": "Flattener",
            "description": "Flattens nested JSON structures into flat dictionaries",
            "supported_operations": "Object flattening, array handling, custom field rules",
            "separator": self.separator,
            "max_depth": str(self.max_depth),
            "array_handling": self.array_handling,
            "flatten_objects": str(self.flatten_objects),
            "flatten_arrays": str(self.flatten_arrays),
            "custom_rules": str(len(self.custom_flatteners))
        }

    def add_custom_flattener(self, field_path: str, rule: Dict[str, Any]):
        """
        Add a custom flattening rule for a specific field.

        Args:
            field_path: Path to the field (using dots)
            rule: Flattening rule configuration
        """
        self.custom_flatteners[field_path] = rule

    def remove_custom_flattener(self, field_path: str):
        """
        Remove a custom flattening rule.

        Args:
            field_path: Path to the field to remove rule for
        """
        if field_path in self.custom_flatteners:
            del self.custom_flatteners[field_path]

    def get_flattening_preview(self, sample_nested_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preview how nested data would be flattened.

        Args:
            sample_nested_data: Sample nested data structure

        Returns:
            Dictionary showing flattened result
        """
        flattened = {}
        stats = {
            "objects_flattened": 0,
            "arrays_flattened": 0,
            "fields_created": 0,
            "max_depth_reached": 0
        }

        self._flatten_dict(sample_nested_data, flattened, "", 0, stats)

        return {
            "flattened_data": flattened,
            "statistics": stats
        }

    def get_flattening_stats(self) -> Dict[str, Any]:
        """
        Get statistics about flattening configuration.

        Returns:
            Dictionary with flattening statistics
        """
        return {
            "configuration": self._get_flattening_config(),
            "custom_rules_count": len(self.custom_flatteners),
            "custom_rules": list(self.custom_flatteners.keys()),
            "array_handling_strategy": self.array_handling,
            "max_depth_limit": self.max_depth
        }