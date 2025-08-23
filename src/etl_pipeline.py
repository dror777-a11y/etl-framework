import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import traceback

from src.config_manager.config_manager import ConfigManager
from src.extractors.kafka_extractor import KafkaExtractor
from src.extractors.mongo_extractor import MongoExtractor
from src.parsers.json_parser import JsonParser
from src.parsers.bson_parser import BsonParser
from src.transformers.data_cleaner import DataCleaner
from src.transformers.field_mapper import FieldMapper
from src.transformers.type_converter import TypeConverter
from src.transformers.metadata_enricher import MetadataEnricher
from src.transformers.flattener import Flattener
from src.loaders.mssql_loader import MSSQLLoader

class ETLPipeline:
    """
    Main ETL Pipeline class that orchestrates the entire ETL process.

    This class coordinates the Extract-Transform-Load workflow by managing
    extractors, parsers, transformers, and loaders based on configuration.
    """

    def __init__(self, config_path: str):
        """
        Initialize the ETL pipeline with configuration.

        Args:
            config_path: Path to configuration file or directory
        """
        self.config_path = config_path
        self.config_manager = ConfigManager(config_path)

        # Pipeline components
        self.extractor = None
        self.parser = None
        self.transformers = []
        self.loader = None

        # Pipeline state
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'records_extracted': 0,
            'records_parsed': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': []
        }

        # Set up logging
        self._setup_logging()

        # Initialize pipeline components
        self._initialize_components()

    def _setup_logging(self):
        """Set up logging configuration."""
        pipeline_config = self.config_manager.get_pipeline_config()
        log_level = pipeline_config.get('logging_level', 'INFO')

        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
            ]
        )

        self.logger = logging.getLogger('ETLPipeline')
        self.logger.info("ETL Pipeline logging initialized")

    def _initialize_components(self):
        """Initialize all ETL components based on configuration."""
        try:
            self.logger.info("Initializing ETL pipeline components...")

            # Initialize extractor and parser
            self._initialize_extractor_and_parser()

            # Initialize transformers
            self._initialize_transformers()

            # Initialize loader
            self._initialize_loader()

            self.logger.info("All ETL components initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize ETL components: {e}")
            raise

    def _initialize_extractor_and_parser(self):
        """Initialize extractor and parser based on source type."""
        source_config = self.config_manager.get_source_config()
        source_type = source_config.get('type', '').lower()

        extractor_config = self.config_manager.get_extractor_config()
        parser_config = self.config_manager.get_parser_config()

        if source_type == 'kafka':
            self.extractor = KafkaExtractor(extractor_config)
            self.parser = JsonParser(parser_config)
            self.logger.info("Initialized Kafka extractor and JSON parser")

        elif source_type == 'mongodb':
            self.extractor = MongoExtractor(extractor_config)
            self.parser = BsonParser(parser_config)
            self.logger.info("Initialized MongoDB extractor and BSON parser")

        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def _initialize_transformers(self):
        """Initialize transformers based on configuration."""
        transformer_configs = self.config_manager.get_transformer_configs()

        # Define transformer creation mapping
        transformer_classes = {
            'data_cleaner': DataCleaner,
            'field_mapper': FieldMapper,
            'type_converter': TypeConverter,
            'flattener': Flattener,
            'metadata_enricher': MetadataEnricher
        }

        # Default transformer order for pipeline
        default_order = [
            'data_cleaner',  # First: clean the data
            'flattener',  # Second: flatten nested structures
            'field_mapper',  # Third: standardize field names
            'type_converter',  # Fourth: convert data types
            'metadata_enricher'  # Last: add metadata
        ]

        # Initialize transformers in order
        for transformer_name in default_order:
            if transformer_name in transformer_configs:
                transformer_class = transformer_classes[transformer_name]
                transformer_config = transformer_configs[transformer_name]

                transformer = transformer_class(transformer_config)
                self.transformers.append(transformer)

                self.logger.info(f"Initialized {transformer_name} transformer")

        self.logger.info(f"Initialized {len(self.transformers)} transformers")

    def _initialize_loader(self):
        """Initialize loader based on target type."""
        target_config = self.config_manager.get_target_config()
        target_type = target_config.get('type', '').lower()

        loader_config = self.config_manager.get_loader_config()

        if target_type == 'mssql':
            self.loader = MSSQLLoader(loader_config)
            self.logger.info("Initialized MSSQL loader")
        else:
            raise ValueError(f"Unsupported target type: {target_type}")

    def run(self) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.

        Returns:
            Dictionary with pipeline execution results and statistics
        """
        self.logger.info("Starting ETL pipeline execution...")
        self.pipeline_stats['start_time'] = datetime.now()

        try:
            # Step 1: Extract
            raw_data = self._extract_data()

            # Step 2: Parse
            parsed_data = self._parse_data(raw_data)

            # Step 3: Transform
            transformed_data = self._transform_data(parsed_data)

            # Step 4: Load
            load_result = self._load_data(transformed_data)

            # Calculate final statistics
            self._finalize_stats(success=True)

            self.logger.info("ETL pipeline completed successfully")

            return {
                'success': True,
                'pipeline_stats': self.pipeline_stats,
                'load_result': load_result,
                'message': 'ETL pipeline completed successfully'
            }

        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            self.logger.error(f"Error details: {traceback.format_exc()}")

            self.pipeline_stats['errors'].append(str(e))
            self._finalize_stats(success=False)

            return {
                'success': False,
                'pipeline_stats': self.pipeline_stats,
                'error': str(e),
                'message': 'ETL pipeline failed'
            }

    def _extract_data(self) -> List[Dict[str, Any]]:
        """Extract raw data from source."""
        self.logger.info("Starting data extraction...")

        try:
            # Connect to source
            if not self.extractor.connect():
                raise Exception("Failed to connect to data source")

            # Extract data
            raw_data = self.extractor.extract()
            self.pipeline_stats['records_extracted'] = len(raw_data)

            self.logger.info(f"Extracted {len(raw_data)} raw records")

            # Disconnect from source
            self.extractor.disconnect()

            return raw_data

        except Exception as e:
            if self.extractor:
                self.extractor.disconnect()
            raise Exception(f"Data extraction failed: {e}")

    def _parse_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse raw data into standardized format."""
        self.logger.info("Starting data parsing...")

        try:
            parsed_data = self.parser.parse(raw_data)
            self.pipeline_stats['records_parsed'] = len(parsed_data)

            self.logger.info(f"Parsed {len(parsed_data)} records")

            return parsed_data

        except Exception as e:
            raise Exception(f"Data parsing failed: {e}")

    def _transform_data(self, parsed_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform parsed data using configured transformers."""
        self.logger.info("Starting data transformation...")

        try:
            transformed_data = parsed_data

            # Apply each transformer in sequence
            for i, transformer in enumerate(self.transformers):
                transformer_name = transformer.__class__.__name__
                self.logger.info(f"Applying {transformer_name} ({i + 1}/{len(self.transformers)})")

                transformed_data = transformer.transform(transformed_data)

                self.logger.info(f"Completed {transformer_name}")

            self.pipeline_stats['records_transformed'] = len(transformed_data)
            self.logger.info(f"Transformed {len(transformed_data)} records")

            return transformed_data

        except Exception as e:
            raise Exception(f"Data transformation failed: {e}")

    def _load_data(self, transformed_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load transformed data to target destination."""
        self.logger.info("Starting data loading...")

        try:
            # Connect to target
            if not self.loader.connect():
                raise Exception("Failed to connect to data target")

            # Load data
            load_result = self.loader.load(transformed_data)
            self.pipeline_stats['records_loaded'] = load_result.get('records_loaded', 0)

            # Disconnect from target
            self.loader.disconnect()

            self.logger.info(f"Loaded {load_result.get('records_loaded', 0)} records")

            return load_result

        except Exception as e:
            if self.loader:
                self.loader.disconnect()
            raise Exception(f"Data loading failed: {e}")

    def _finalize_stats(self, success: bool):
        """Finalize pipeline statistics."""
        self.pipeline_stats['end_time'] = datetime.now()

        if self.pipeline_stats['start_time']:
            duration = self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
            self.pipeline_stats['duration_seconds'] = duration.total_seconds()

        self.pipeline_stats['success'] = success

    def validate_configuration(self) -> Dict[str, Any]:
        """
        Validate the pipeline configuration.

        Returns:
            Dictionary with validation results
        """
        self.logger.info("Validating pipeline configuration...")

        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Validate source configuration
        source_validation = self.config_manager.validate_source_config()
        if not source_validation['valid']:
            validation_results['valid'] = False
            validation_results['errors'].extend(source_validation['errors'])
        validation_results['warnings'].extend(source_validation.get('warnings', []))

        # Validate target configuration
        target_validation = self.config_manager.validate_target_config()
        if not target_validation['valid']:
            validation_results['valid'] = False
            validation_results['errors'].extend(target_validation['errors'])
        validation_results['warnings'].extend(target_validation.get('warnings', []))

        # Test connections
        try:
            self._test_connections(validation_results)
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Connection test failed: {e}")

        return validation_results

    def _test_connections(self, validation_results: Dict[str, Any]):
        """Test connections to source and target."""
        # Test source connection
        self.logger.info("Testing source connection...")
        if self.extractor:
            if self.extractor.connect():
                self.extractor.disconnect()
                self.logger.info("Source connection successful")
            else:
                validation_results['errors'].append("Cannot connect to data source")
                validation_results['valid'] = False

        # Test target connection
        self.logger.info("Testing target connection...")
        if self.loader:
            if self.loader.connect():
                self.loader.disconnect()
                self.logger.info("Target connection successful")
            else:
                validation_results['errors'].append("Cannot connect to data target")
                validation_results['valid'] = False

    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get information about the pipeline configuration and components.

        Returns:
            Dictionary with pipeline information
        """
        config_summary = self.config_manager.get_config_summary()

        return {
            'pipeline_name': self.config_manager.get_config_section('pipeline.name', 'ETL Pipeline'),
            'pipeline_version': self.config_manager.get_config_section('pipeline.version', '1.0.0'),
            'config_path': self.config_path,
            'source_type': config_summary.get('source_type'),
            'target_type': config_summary.get('target_type'),
            'extractor_class': self.extractor.__class__.__name__ if self.extractor else None,
            'parser_class': self.parser.__class__.__name__ if self.parser else None,
            'transformer_classes': [t.__class__.__name__ for t in self.transformers],
            'loader_class': self.loader.__class__.__name__ if self.loader else None,
            'transformers_count': len(self.transformers)
        }

    def dry_run(self, max_records: int = 10) -> Dict[str, Any]:
        """
        Perform a dry run of the pipeline with limited data for testing.

        Args:
            max_records: Maximum number of records to process

        Returns:
            Dictionary with dry run results
        """
        self.logger.info(f"Starting ETL pipeline dry run (max {max_records} records)...")

        try:
            # Extract limited data
            if not self.extractor.connect():
                raise Exception("Failed to connect to data source")

            # Limit extraction for dry run
            original_limit = self.extractor.config.get('limit', 0)
            self.extractor.config['limit'] = max_records

            raw_data = self.extractor.extract()
            self.extractor.disconnect()

            # Restore original limit
            if original_limit > 0:
                self.extractor.config['limit'] = original_limit

            # Parse data
            parsed_data = self.parser.parse(raw_data)

            # Transform data
            transformed_data = parsed_data
            for transformer in self.transformers:
                transformed_data = transformer.transform(transformed_data)

            return {
                'success': True,
                'records_extracted': len(raw_data),
                'records_parsed': len(parsed_data),
                'records_transformed': len(transformed_data),
                'sample_raw_data': raw_data[:3] if raw_data else [],
                'sample_parsed_data': parsed_data[:3] if parsed_data else [],
                'sample_transformed_data': transformed_data[:3] if transformed_data else [],
                'message': 'Dry run completed successfully'
            }

        except Exception as e:
            self.logger.error(f"Dry run failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Dry run failed'
            }

    def get_stats(self) -> Dict[str, Any]:
        """
        Get current pipeline statistics.

        Returns:
            Dictionary with pipeline statistics
        """
        return dict(self.pipeline_stats)


def main():
    """
    Main function for running ETL pipeline from command line.
    """
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Run ETL Pipeline')
    parser.add_argument('config', help='Path to configuration file or directory')
    parser.add_argument('--validate', action='store_true', help='Validate configuration only')
    parser.add_argument('--dry-run', type=int, metavar='N', help='Perform dry run with N records')
    parser.add_argument('--info', action='store_true', help='Show pipeline information')

    args = parser.parse_args()

    try:
        # Initialize pipeline
        pipeline = ETLPipeline(args.config)

        if args.info:
            # Show pipeline information
            info = pipeline.get_pipeline_info()
            print("\n=== ETL Pipeline Information ===")
            for key, value in info.items():
                print(f"{key}: {value}")

        elif args.validate:
            # Validate configuration
            validation = pipeline.validate_configuration()
            print("\n=== Configuration Validation ===")
            print(f"Valid: {validation['valid']}")

            if validation['errors']:
                print("Errors:")
                for error in validation['errors']:
                    print(f"  - {error}")

            if validation['warnings']:
                print("Warnings:")
                for warning in validation['warnings']:
                    print(f"  - {warning}")

        elif args.dry_run:
            # Perform dry run
            result = pipeline.dry_run(args.dry_run)
            print("\n=== Dry Run Results ===")
            print(f"Success: {result['success']}")

            if result['success']:
                print(f"Records extracted: {result['records_extracted']}")
                print(f"Records parsed: {result['records_parsed']}")
                print(f"Records transformed: {result['records_transformed']}")
            else:
                print(f"Error: {result['error']}")

        else:
            # Run full pipeline
            result = pipeline.run()
            print("\n=== Pipeline Execution Results ===")
            print(f"Success: {result['success']}")

            if result['success']:
                stats = result['pipeline_stats']
                print(f"Duration: {stats['duration_seconds']:.2f} seconds")
                print(f"Records extracted: {stats['records_extracted']}")
                print(f"Records parsed: {stats['records_parsed']}")
                print(f"Records transformed: {stats['records_transformed']}")
                print(f"Records loaded: {stats['records_loaded']}")
            else:
                print(f"Error: {result['error']}")

    except Exception as e:
        print(f"Failed to run ETL pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()