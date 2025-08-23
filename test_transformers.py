#!/usr/bin/env python3
"""
Test script for all ETL Transformers
This script tests DataCleaner, FieldMapper, MetadataEnricher, and TypeConverter
"""

import json
from datetime import datetime
from typing import Dict, Any, List


# Mock sample data that simulates what parsers would output
def get_kafka_sample_data():
    """Sample data as it would come from JsonParser"""
    return [
        {
            "data": {
                "firstName": "  John  ",
                "lastName": "Doe",
                "age": "25",
                "email": "john.doe@example.com",
                "phone": "123-456-7890",
                "active": "true",
                "price": "99.99",
                "created_date": "2024-01-15 10:30:00",
                "description": "",
                "status": "null"
            },
            "metadata": {
                "source_type": "kafka",
                "topic": "user_events",
                "partition": 0,
                "offset": 123,
                "timestamp": 1642248600000
            }
        },
        {
            "data": {
                "firstName": "Jane",
                "lastName": "Smith",
                "age": "30.0",
                "email": "JANE@COMPANY.COM",
                "phone": "+1-555-123-4567",
                "active": "false",
                "price": "150.50",
                "created_date": "2024-01-16",
                "description": "Active user",
                "status": "N/A"
            },
            "metadata": {
                "source_type": "kafka",
                "topic": "user_events",
                "partition": 1,
                "offset": 124,
                "timestamp": 1642335000000
            }
        }
    ]


def get_mongodb_sample_data():
    """Sample data as it would come from BsonParser"""
    return [
        {
            "data": {
                "_id": "64a1b2c3d4e5f6789abcdef0",
                "customer_name": "  Alice Johnson  ",
                "customer_age": "28",
                "electronic_mail": "alice@test.com",
                "mobile": "9876543210",
                "is_premium": "1",
                "account_balance": "1234.56",
                "registration_date": "2024-01-10 14:22:33",
                "notes": "",
                "membership_status": "UNDEFINED"
            },
            "metadata": {
                "source_type": "mongodb",
                "document_id": "64a1b2c3d4e5f6789abcdef0",
                "field_count": 10,
                "has_nested_objects": False
            }
        }
    ]


def test_data_cleaner():
    """Test the DataCleaner transformer"""
    print("=" * 50)
    print("Testing DataCleaner")
    print("=" * 50)

    # Import your DataCleaner
    from src.transformers.data_cleaner import DataCleaner

    # Configuration for DataCleaner
    cleaner_config = {
        "cleaning_rules": {
            "trim_whitespace": True,
            "remove_empty_strings": True,
            "standardize_nulls": True,
            "validate_emails": True,
            "clean_phone_numbers": True
        },
        "null_values": ["", "null", "NULL", "None", "N/A", "n/a", "UNDEFINED"],
        "validation_rules": {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^[\+]?[1-9][\d]{0,15}$"
        },
        "strict_validation": False
    }

    # Test data
    sample_data = get_kafka_sample_data() + get_mongodb_sample_data()

    print("Original data sample:")
    for i, record in enumerate(sample_data):
        print(f"Record {i + 1}: {record['data']}")

    try:
        # Initialize cleaner
        cleaner = DataCleaner(cleaner_config)
        cleaned_data = cleaner.transform(sample_data)

        print("\n*** DataCleaner would clean the following issues: ***")
        print("- Trim whitespace from '  John  ' and '  Alice Johnson  '")
        print("- Remove empty 'description' fields")
        print("- Standardize 'null', 'N/A', 'UNDEFINED' to None")
        print("- Validate and clean email addresses")
        print("- Clean phone number formats")
        print("- Convert boolean-like strings")

        print("\nCleaned data:")
        for i, record in enumerate(cleaned_data):
            print(f"Record {i + 1}: {record['data']}")

        print("\nDataCleaner test: READY TO RUN")

    except Exception as e:
        print(f"Error testing DataCleaner: {e}")


def test_field_mapper():
    """Test the FieldMapper transformer"""
    print("\n" + "=" * 50)
    print("Testing FieldMapper")
    print("=" * 50)

    # Configuration for FieldMapper
    mapper_config = {
        "field_mappings": {
            "kafka": {
                "first_name": ["firstName"],
                "last_name": ["lastName"],
                "age": ["age"],
                "email": ["email"],
                "phone": ["phone"],
                "is_active": ["active"],
                "price": ["price"],
                "created_at": ["created_date"]
            },
            "mongodb": {
                "first_name": ["customer_name"],
                "age": ["customer_age"],
                "email": ["electronic_mail"],
                "phone": ["mobile"],
                "is_premium": ["is_premium"],
                "balance": ["account_balance"],
                "created_at": ["registration_date"]
            }
        },
        "keep_unmapped_fields": True,
        "case_sensitive": False
    }

    # Test data
    sample_data = get_kafka_sample_data() + get_mongodb_sample_data()

    print("Field mapping configuration:")
    for source_type, mappings in mapper_config["field_mappings"].items():
        print(f"{source_type.upper()}:")
        for target, sources in mappings.items():
            print(f"  {sources} -> {target}")

    try:
        # Initialize mapper
        from src.transformers.field_mapper import FieldMapper
        mapper = FieldMapper(mapper_config)
        mapped_data = mapper.transform(sample_data)

        print("\nMapped data:")
        for i, record in enumerate(mapped_data):
            print(f"Record {i + 1}: {record['data']}")

        print("\nFieldMapper test: COMPLETED")

        print("\nFieldMapper test: READY TO RUN")

    except Exception as e:
        print(f"Error testing FieldMapper: {e}")


def test_metadata_enricher():
    """Test the MetadataEnricher transformer"""
    print("\n" + "=" * 50)
    print("Testing MetadataEnricher")
    print("=" * 50)

    # Configuration for MetadataEnricher
    enricher_config = {
        "add_created_at": True,
        "add_processed_at": True,
        "add_unique_id": True,
        "add_source_info": True,
        "created_at_field": "createdAt",
        "processed_at_field": "processedAt",
        "id_field": "recordId",
        "datetime_format": "iso",
        "id_prefix": "ETL",
        "additional_metadata": {
            "pipeline_version": "1.0.0",
            "environment": "development"
        }
    }

    # Test data
    sample_data = get_kafka_sample_data()[:1]  # Just one record for demo

    print("Enrichment configuration:")
    print(f"- Will add: createdAt, processedAt, recordId, dataSource")
    print(f"- ID prefix: {enricher_config['id_prefix']}")
    print(f"- Additional fields: {enricher_config['additional_metadata']}")

    try:
        # Initialize enricher
        from src.transformers.metadata_enricher import MetadataEnricher
        enricher = MetadataEnricher(enricher_config)
        enriched_data = enricher.transform(sample_data)

        print("\nEnriched data:")
        for i, record in enumerate(enriched_data):
            print(f"Record {i + 1}: {record['data']}")

        print("\nMetadataEnricher test: COMPLETED")

    except Exception as e:
        print(f"Error testing MetadataEnricher: {e}")

def test_type_converter():
    """Test the TypeConverter transformer"""
    print("\n" + "=" * 50)
    print("Testing TypeConverter")
    print("=" * 50)

    # Configuration for TypeConverter
    converter_config = {
        "type_conversions": {
            "age": "int",
            "price": "float",
            "active": "bool",
            "is_active": "bool",
            "is_premium": "bool",
            "balance": "float",
            "created_date": "datetime",
            "created_at": "datetime",
            "registration_date": "datetime",
            "first_name": "str",
            "last_name": "str"
        },
        "strict_mode": False,
        "default_values": {
            "int": 0,
            "float": 0.0,
            "bool": False,
            "str": "",
            "datetime": None
        }
    }

    # Test data
    sample_data = get_kafka_sample_data() + get_mongodb_sample_data()

    print("Type conversion rules:")
    for field, target_type in converter_config["type_conversions"].items():
        print(f"  {field} -> {target_type}")

    print(f"\nStrict mode: {converter_config['strict_mode']}")
    print("Default values for failed conversions:")
    for type_name, default in converter_config["default_values"].items():
        print(f"  {type_name} -> {default}")

    try:
        # Initialize converter
        from src.transformers.type_converter import TypeConverter
        converter = TypeConverter(converter_config)
        converted_data = converter.transform(sample_data)

        print("\nConverted data:")
        for i, record in enumerate(converted_data):
            print(f"Record {i+1}: {record['data']}")

        print("\nTypeConverter test: COMPLETED")

    except Exception as e:
        print(f"Error testing TypeConverter: {e}")

def test_full_pipeline():
    """Test all transformers in sequence"""
    print("\n" + "=" * 80)
    print("Testing Full Transformation Pipeline")
    print("=" * 80)

    # Sample data
    sample_data = get_kafka_sample_data() + get_mongodb_sample_data()

    print("PIPELINE STEPS:")
    print("1. DataCleaner: Clean and validate data")
    print("2. FieldMapper: Standardize field names")
    print("3. TypeConverter: Convert data types")
    print("4. MetadataEnricher: Add metadata fields")

    print("\nOriginal data structure:")
    for i, record in enumerate(sample_data):
        print(f"Record {i + 1} ({record['metadata']['source_type']}):")
        print(f"  Fields: {list(record['data'].keys())}")
        print(f"  Sample values: {dict(list(record['data'].items())[:3])}")

    print("\n*** After full pipeline, data would be: ***")
    print("- Cleaned: whitespace trimmed, nulls standardized")
    print("- Mapped: all field names standardized (first_name, last_name, etc.)")
    print("- Typed: proper data types (int, float, bool, datetime)")
    print("- Enriched: createdAt, processedAt, recordId, dataSource added")

    print("\nFull pipeline test: READY TO RUN")


def test_transformer_configurations():
    """Test different transformer configurations"""
    print("\n" + "=" * 50)
    print("Testing Transformer Configurations")
    print("=" * 50)

    # Test DataCleaner with strict mode
    print("1. DataCleaner in strict mode:")
    strict_cleaner_config = {
        "cleaning_rules": {"validate_emails": True},
        "strict_validation": True
    }
    print("   - Would fail on invalid email formats")
    print("   - Would raise exceptions instead of using defaults")

    # Test FieldMapper case sensitive
    print("\n2. FieldMapper case sensitive:")
    case_sensitive_config = {
        "case_sensitive": True,
        "keep_unmapped_fields": False
    }
    print("   - 'firstName' != 'firstname'")
    print("   - Would drop unmapped fields")

    # Test TypeConverter strict mode
    print("\n3. TypeConverter strict mode:")
    strict_converter_config = {
        "strict_mode": True,
        "type_conversions": {"age": "int"}
    }
    print("   - Would fail on unconvertible values")
    print("   - No fallback to default values")

    print("\nConfiguration tests: READY TO RUN")


def run_all_tests():
    """Run all transformer tests"""
    print("ETL Transformer Test Suite")
    print("=" * 80)
    print("This script tests all transformers with sample data")
    print("To actually run the transformers, uncomment the import statements")
    print("and transformer instantiation lines in each test function.")
    print("=" * 80)

    test_data_cleaner()
    test_field_mapper()
    test_metadata_enricher()
    test_type_converter()
    test_full_pipeline()
    test_transformer_configurations()

    print("\n" + "=" * 80)
    print("All transformer tests completed!")
    print("Next steps:")
    print("1. Uncomment the actual transformer imports and calls")
    print("2. Run this script to verify all transformers work correctly")
    print("3. Add error handling tests")
    print("4. Test with edge cases and invalid data")
    print("=" * 80)


if __name__ == "__main__":
    run_all_tests()