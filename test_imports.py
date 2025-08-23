#!/usr/bin/env python3
"""
Test script to verify all ETL components can be imported
"""


def test_imports():
    """Test importing all ETL components"""
    try:
        # Test config manager
        from src.config_manager.config_manager import ConfigManager
        print("✓ ConfigManager imported successfully")

        # Test extractors
        from src.extractors.kafka_extractor import KafkaExtractor
        from src.extractors.mongo_extractor import MongoExtractor
        print("✓ Extractors imported successfully")

        # Test parsers
        from src.parsers.json_parser import JsonParser
        from src.parsers.bson_parser import BsonParser
        print("✓ Parsers imported successfully")

        # Test transformers
        from src.transformers.data_cleaner import DataCleaner
        from src.transformers.field_mapper import FieldMapper
        from src.transformers.type_converter import TypeConverter
        from src.transformers.metadata_enricher import MetadataEnricher
        print("✓ Transformers imported successfully")

        # Test loader
        from src.loaders.mssql_loader import MSSQLLoader
        print("✓ Loader imported successfully")

        # Test main pipeline
        from src.etl_pipeline import ETLPipeline
        print("✓ ETL Pipeline imported successfully")

        print("\n🎉 All imports successful! Framework is ready to use.")
        return True

    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    test_imports()