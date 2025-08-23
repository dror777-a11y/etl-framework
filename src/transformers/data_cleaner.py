from typing import Dict, Any, List, Union
from datetime import datetime
import re
from .base_transformer import BaseTransformer


class DataCleaner(BaseTransformer):
    """
    Data cleaning transformer that standardizes and validates field values.

    This transformer handles common data quality issues like whitespace,
    null values, invalid formats, and provides data validation.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the data cleaner with cleaning rules.

        Expected config format:
        {
            "cleaning_rules": {
                "trim_whitespace": True,
                "remove_empty_strings": True,
                "standardize_nulls": True,
                "validate_emails": True,
                "clean_phone_numbers": True
            },
            "null_values": ["", "null", "NULL", "None", "N/A", "n/a"],
            "validation_rules": {
                "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "phone": r"^[\+]?[1-9][\d]{0,15}$"
            },
            "strict_validation": False
        }
        """
        super().__init__(config)

        # Extract cleaning configuration
        cleaning_rules = self.config.get("cleaning_rules", {})
        self.trim_whitespace = cleaning_rules.get("trim_whitespace", True)
        self.remove_empty_strings = cleaning_rules.get("remove_empty_strings", True)
        self.standardize_nulls = cleaning_rules.get("standardize_nulls", True)
        self.validate_emails = cleaning_rules.get("validate_emails", False)
        self.clean_phone_numbers = cleaning_rules.get("clean_phone_numbers", False)

        # Null value representations to standardize
        self.null_values = set(self.config.get("null_values", [
            "", "null", "NULL", "None", "N/A", "n/a", "undefined", "UNDEFINED"
        ]))

        # Validation patterns
        self.validation_rules = self.config.get("validation_rules", {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^[\+]?[1-9][\d]{0,15}$"
        })

        # Whether to fail on validation errors
        self.strict_validation = self.config.get("strict_validation", False)

    def transform(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply data cleaning transformations to parsed data.

        Args:
            parsed_data: List of parsed records from previous transformers

        Returns:
            List of records with cleaned data
        """
        if not self.validate_input(parsed_data):
            raise ValueError("Invalid input format - expected parsed data with data and metadata")

        # Extract data for processing
        data_only = self._extract_data_only(parsed_data)

        # Apply cleaning to each record
        cleaned_data = []
        cleaning_stats = {
            "records_processed": 0,
            "fields_trimmed": 0,
            "nulls_standardized": 0,
            "emails_validated": 0,
            "phones_cleaned": 0,
            "validation_errors": 0
        }

        for record in data_only:
            cleaned_record, record_stats = self._clean_record(record)
            cleaned_data.append(cleaned_record)

            # Update cleaning statistics
            for key, value in record_stats.items():
                cleaning_stats[key] += value
            cleaning_stats["records_processed"] += 1

        # Preserve metadata and add transformation info
        result = self._preserve_metadata(parsed_data, cleaned_data)

        # Add transformation metadata
        transformation_info = {
            "type": "data_cleaning",
            "timestamp": datetime.now().isoformat(),
            "transformer": "DataCleaner",
            "cleaning_stats": cleaning_stats,
            "rules_applied": self._get_active_rules()
        }

        return self._add_transformation_metadata(result, transformation_info)

    def _clean_record(self, record: Dict[str, Any]) -> tuple:
        """
        Clean a single record's data.

        Args:
            record: Single data record to clean

        Returns:
            Tuple of (cleaned_record, cleaning_stats)
        """
        cleaned_record = {}
        stats = {
            "fields_trimmed": 0,
            "nulls_standardized": 0,
            "emails_validated": 0,
            "phones_cleaned": 0,
            "validation_errors": 0
        }

        for field_name, field_value in record.items():
            cleaned_value = self._clean_field_value(field_name, field_value, stats)

            # Only include field if it's not an empty string (when remove_empty_strings is True)
            if not (self.remove_empty_strings and cleaned_value == ""):
                cleaned_record[field_name] = cleaned_value

        return cleaned_record, stats

    def _clean_field_value(self, field_name: str, value: Any, stats: Dict[str, int]) -> Any:
        """
        Clean a single field value.

        Args:
            field_name: Name of the field
            value: Value to clean
            stats: Statistics dictionary to update

        Returns:
            Cleaned value
        """
        if value is None:
            return None

        # Convert to string for processing if not already
        if isinstance(value, str):
            cleaned_value = value

            # Trim whitespace
            if self.trim_whitespace:
                original_length = len(cleaned_value)
                cleaned_value = cleaned_value.strip()
                if len(cleaned_value) != original_length:
                    stats["fields_trimmed"] += 1

            # Standardize null values
            if self.standardize_nulls and cleaned_value in self.null_values:
                stats["nulls_standardized"] += 1
                return None

            # Email validation and cleaning
            if self.validate_emails and self._is_email_field(field_name):
                cleaned_value = self._clean_email(cleaned_value, stats)

            # Phone number cleaning
            if self.clean_phone_numbers and self._is_phone_field(field_name):
                cleaned_value = self._clean_phone_number(cleaned_value, stats)

            return cleaned_value

        else:
            # Non-string values - return as is
            return value

    def _is_email_field(self, field_name: str) -> bool:
        """Check if field name suggests it contains an email."""
        email_indicators = ["email", "mail", "e_mail", "electronic_mail"]
        field_lower = field_name.lower()
        return any(indicator in field_lower for indicator in email_indicators)

    def _is_phone_field(self, field_name: str) -> bool:
        """Check if field name suggests it contains a phone number."""
        phone_indicators = ["phone", "tel", "mobile", "cell", "number"]
        field_lower = field_name.lower()
        return any(indicator in field_lower for indicator in phone_indicators)

    def _clean_email(self, email: str, stats: Dict[str, int]) -> str:
        """
        Clean and validate email address.

        Args:
            email: Email address to clean
            stats: Statistics to update

        Returns:
            Cleaned email address
        """
        if not email:
            return email

        # Basic cleaning
        cleaned_email = email.lower().strip()

        # Validate format
        if "email" in self.validation_rules:
            pattern = self.validation_rules["email"]
            if re.match(pattern, cleaned_email):
                stats["emails_validated"] += 1
                return cleaned_email
            else:
                stats["validation_errors"] += 1
                if self.strict_validation:
                    raise ValueError(f"Invalid email format: {email}")
                # Return original if validation fails in non-strict mode
                return email

        return cleaned_email

    def _clean_phone_number(self, phone: str, stats: Dict[str, int]) -> str:
        """
        Clean phone number by removing formatting characters.

        Args:
            phone: Phone number to clean
            stats: Statistics to update

        Returns:
            Cleaned phone number
        """
        if not phone:
            return phone

        # Remove common formatting characters
        cleaned_phone = re.sub(r'[^\d\+]', '', phone.strip())

        # Validate format if pattern exists
        if "phone" in self.validation_rules:
            pattern = self.validation_rules["phone"]
            if re.match(pattern, cleaned_phone):
                stats["phones_cleaned"] += 1
                return cleaned_phone
            else:
                stats["validation_errors"] += 1
                if self.strict_validation:
                    raise ValueError(f"Invalid phone format: {phone}")
                # Return original if validation fails
                return phone

        stats["phones_cleaned"] += 1
        return cleaned_phone

    def _get_active_rules(self) -> List[str]:
        """Get list of active cleaning rules."""
        active_rules = []

        if self.trim_whitespace:
            active_rules.append("trim_whitespace")
        if self.remove_empty_strings:
            active_rules.append("remove_empty_strings")
        if self.standardize_nulls:
            active_rules.append("standardize_nulls")
        if self.validate_emails:
            active_rules.append("validate_emails")
        if self.clean_phone_numbers:
            active_rules.append("clean_phone_numbers")

        return active_rules

    def get_transformer_info(self) -> Dict[str, str]:
        """
        Get information about this data cleaner.

        Returns:
            Dictionary with transformer metadata
        """
        return {
            "transformer_type": "DataCleaner",
            "description": "Cleans and validates field values for data quality",
            "supported_operations": "Whitespace trimming, null standardization, email/phone validation",
            "active_rules": str(self._get_active_rules()),
            "null_value_count": str(len(self.null_values)),
            "strict_validation": str(self.strict_validation),
            "validation_patterns": str(len(self.validation_rules))
        }

    def add_null_value(self, null_representation: str):
        """Add a new null value representation to standardize."""
        self.null_values.add(null_representation)

    def remove_null_value(self, null_representation: str):
        """Remove a null value representation."""
        self.null_values.discard(null_representation)

    def add_validation_rule(self, field_type: str, pattern: str):
        """Add a new validation rule."""
        self.validation_rules[field_type] = pattern

    def get_cleaning_stats(self) -> Dict[str, Any]:
        """
        Get statistics about cleaning configuration.

        Returns:
            Dictionary with cleaning statistics
        """
        return {
            "active_rules": self._get_active_rules(),
            "null_values_configured": list(self.null_values),
            "validation_rules_count": len(self.validation_rules),
            "strict_validation_enabled": self.strict_validation,
            "email_validation_enabled": self.validate_emails,
            "phone_cleaning_enabled": self.clean_phone_numbers
        }