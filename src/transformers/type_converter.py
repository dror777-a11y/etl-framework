from typing import Dict, Any, List, Union
from datetime import datetime
from .base_transformer import BaseTransformer


class TypeConverter(BaseTransformer):
    """
    Type conversion transformer that standardizes data types across records.

    This transformer converts field values to specified target types based on
    configuration rules
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the type converter with conversion rules.

        Expected config format:
        {
            "type_conversions": {
                "age": "int",
                "price": "float",
                "active": "bool",
                "created_date": "datetime",
                "name": "str"
            },
            "strict_mode": False,           # Whether to fail on conversion errors
            "default_values": {             # Default values for failed conversions
                "int": 0,
                "float": 0.0,
                "bool": False,
                "str": ""
            }
        }
        """
        super().__init__(config)
        # Extract type conversion rules from config
        self.type_conversions = self.config.get("type_conversions", {})
        # Control behavior on conversion failures
        self.strict_mode = self.config.get("strict_mode", False)
        # Default values to use when conversion fails
        self.default_values = self.config.get("default_values", {
            "int": 0,
            "float": 0.0,
            "bool": False,
            "str": "",
            "datetime": None
        })

    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply type conversion transformations to parsed data.

        Args:
            parsed_data: List of parsed records from previous transformers

        Returns:
            List of records with converted data types
        """
        if not self.validate_input(parsed_data):
            raise ValueError("Invalid input format - expected parsed data with data and metadata")

        # Extract data for processing
        data_only = self._extract_data_only(parsed_data)

        # Apply type conversions to each record
        converted_data = []
        conversion_stats = {"successful": 0, "failed": 0, "skipped": 0}

        for record in data_only:
            converted_record, record_stats = self._convert_record_types(record)
            converted_data.append(converted_record)

            # Update conversion statistics
            for key, value in record_stats.items():
                conversion_stats[key] += value

        # Preserve metadata and add transformation info
        result = self._preserve_metadata(parsed_data, converted_data)

        # Add transformation metadata with statistics
        transformation_info = {
            "type": "type_conversion",
            "timestamp": datetime.now().isoformat(),
            "transformer": "TypeConverter",
            "conversions_attempted": len(self.type_conversions),
            "conversion_stats": conversion_stats,
            "strict_mode": self.strict_mode
        }

        return self._add_transformation_metadata(result, transformation_info)

    def _convert_record_types(self, record: Dict[str, Any]) -> tuple:
        """
        Convert data types for a single record.

        Args:
            record: Single data record to convert

        Returns:
            Tuple of (converted_record, conversion_stats)
        """
        converted_record = {}
        stats = {"successful": 0, "failed": 0, "skipped": 0}

        for field_name, field_value in record.items():
            if field_name in self.type_conversions:
                target_type = self.type_conversions[field_name]
                converted_value, success = self._convert_value(field_value, target_type, field_name)
                converted_record[field_name] = converted_value

                if success:
                    stats["successful"] += 1
                else:
                    stats["failed"] += 1
            else:
                # Field not in conversion rules - keep as is
                converted_record[field_name] = field_value
                stats["skipped"] += 1

        return converted_record, stats

    def _convert_value(self, value: Any, target_type: str, field_name: str) -> tuple:
        """
        Convert a single value to the target type.

        Args:
            value: Value to convert
            target_type: Target data type as string
            field_name: Name of the field (for error reporting)

        Returns:
            Tuple of (converted_value, success_flag)
        """
        if value is None:
            return None, True

        try:
            if target_type == "int":
                # Handle string numbers and floats
                if isinstance(value, str):
                    # Remove whitespace and handle empty strings
                    value = value.strip()
                    if not value:
                        return self.default_values.get("int", 0), False
                    # Try to convert string to float first, then to int
                    return int(float(value)), True
                elif isinstance(value, (int, float)):
                    return int(value), True
                else:
                    return self._handle_conversion_error(target_type, field_name, value)

            elif target_type == "float":
                if isinstance(value, str):
                    value = value.strip()
                    if not value:
                        return self.default_values.get("float", 0.0), False
                    return float(value), True
                elif isinstance(value, (int, float)):
                    return float(value), True
                else:
                    return self._handle_conversion_error(target_type, field_name, value)

            elif target_type == "bool":
                if isinstance(value, bool):
                    return value, True
                elif isinstance(value, str):
                    value_lower = value.lower().strip()
                    if value_lower in ("true", "yes", "1", "on"):
                        return True, True
                    elif value_lower in ("false", "no", "0", "off", ""):
                        return False, True
                    else:
                        return self._handle_conversion_error(target_type, field_name, value)
                elif isinstance(value, (int, float)):
                    return bool(value), True
                else:
                    return self._handle_conversion_error(target_type, field_name, value)

            elif target_type == "str":
                return str(value), True

            elif target_type == "datetime":
                if isinstance(value, datetime):
                    return value, True
                elif isinstance(value, str):
                    # Try common datetime formats
                    formats = [
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d",
                        "%m/%d/%Y",
                        "%d/%m/%Y"
                    ]

                    for fmt in formats:
                        try:
                            return datetime.strptime(value.strip(), fmt), True
                        except ValueError:
                            continue

                    return self._handle_conversion_error(target_type, field_name, value)
                else:
                    return self._handle_conversion_error(target_type, field_name, value)

            else:
                # Unknown target type
                print(f"Warning: Unknown target type '{target_type}' for field '{field_name}'")
                return value, False

        except Exception as e:
            return self._handle_conversion_error(target_type, field_name, value, str(e))

    def _handle_conversion_error(self, target_type: str, field_name: str,
                                 original_value: Any, error_msg: str = None) -> tuple:
        """
        Handle conversion errors based on strict mode setting.

        Args:
            target_type: Target type that failed
            field_name: Field name that failed conversion
            original_value: Original value that couldn't be converted
            error_msg: Optional error message

        Returns:
            Tuple of (fallback_value, success_flag)
        """
        if self.strict_mode:
            error_text = f"Type conversion failed for field '{field_name}': "
            error_text += f"Cannot convert '{original_value}' to {target_type}"
            if error_msg:
                error_text += f" ({error_msg})"
            raise ValueError(error_text)
        else:
            # Use default value in non-strict mode
            default_value = self.default_values.get(target_type, original_value)
            print(f"Warning: Failed to convert '{field_name}' value '{original_value}' "
                  f"to {target_type}, using default: {default_value}")
            return default_value, False

    def get_transformer_info(self) -> Dict[str, str]:
        """
        Get information about this type converter.

        Returns:
            Dictionary with transformer metadata
        """
        return {
            "transformer_type": "TypeConverter",
            "description": "Converts field values to specified data types",
            "supported_operations": "Type conversion with error handling",
            "supported_types": "int, float, bool, str, datetime",
            "total_conversions": str(len(self.type_conversions)),
            "strict_mode": str(self.strict_mode),
            "conversion_rules": str(dict(self.type_conversions))
        }

    def add_conversion_rule(self, field_name: str, target_type: str):
        """
        Add a new type conversion rule.

        Args:
            field_name: Name of field to convert
            target_type: Target data type
        """
        supported_types = ["int", "float", "bool", "str", "datetime"]
        if target_type not in supported_types:
            raise ValueError(f"Unsupported type '{target_type}'. "
                             f"Supported types: {supported_types}")

        self.type_conversions[field_name] = target_type

    def remove_conversion_rule(self, field_name: str):
        """
        Remove a type conversion rule.

        Args:
            field_name: Name of field to remove conversion for
        """
        if field_name in self.type_conversions:
            del self.type_conversions[field_name]

    def get_conversion_stats(self) -> Dict[str, Any]:
        """
        Get statistics about configured type conversions.

        Returns:
            Dictionary with conversion statistics
        """
        type_counts = {}
        for target_type in self.type_conversions.values():
            type_counts[target_type] = type_counts.get(target_type, 0) + 1

        return {
            "total_rules": len(self.type_conversions),
            "types_distribution": type_counts,
            "configured_fields": list(self.type_conversions.keys()),
            "strict_mode": self.strict_mode,
            "default_values": dict(self.default_values)
        }