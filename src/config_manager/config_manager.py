import yaml
import json
import os
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """
    Configuration manager for ETL pipeline settings.

    This class handles loading and validation of configuration files in YAML or JSON format,
    providing a unified interface for accessing ETL pipeline configurations.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to configuration file or directory
        """
        self.config_path = config_path
        self.config_data: Dict[str, Any] = {}
        self.config_loaded = False

        if config_path and os.path.exists(config_path):
            self.load_config(config_path)

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from file or directory.

        Args:
            config_path: Path to configuration file (.yaml, .yml, .json) or directory

        Returns:
            Dictionary containing configuration data

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config format is invalid
        """
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Configuration path not found: {config_path}")

        if path.is_file():
            # Single config file
            self.config_data = self._load_single_config(path)
        elif path.is_dir():
            # Config directory - load all config files
            self.config_data = self._load_config_directory(path)
        else:
            raise ValueError(f"Invalid config path: {config_path}")

        self.config_path = str(path)
        self.config_loaded = True

        # Validate configuration
        self._validate_config()

        return self.config_data

    def _load_single_config(self, file_path: Path) -> Dict[str, Any]:
        """
        Load configuration from a single file.

        Args:
            file_path: Path to configuration file

        Returns:
            Configuration dictionary
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                if file_path.suffix.lower() in ['.yaml', '.yml']:
                    return yaml.safe_load(file) or {}
                elif file_path.suffix.lower() == '.json':
                    return json.load(file)
                else:
                    raise ValueError(f"Unsupported config file format: {file_path.suffix}")

        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML format in {file_path}: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in {file_path}: {e}")
        except Exception as e:
            raise ValueError(f"Error loading config file {file_path}: {e}")

    def _load_config_directory(self, dir_path: Path) -> Dict[str, Any]:
        """
        Load all configuration files from a directory.

        Args:
            dir_path: Path to configuration directory

        Returns:
            Combined configuration dictionary
        """
        config_data = {}
        config_files = []

        # Find all config files
        for ext in ['*.yaml', '*.yml', '*.json']:
            config_files.extend(dir_path.glob(ext))

        if not config_files:
            raise ValueError(f"No configuration files found in {dir_path}")

        # Load each config file
        for config_file in sorted(config_files):
            file_config = self._load_single_config(config_file)

            # Use filename (without extension) as config section
            section_name = config_file.stem
            config_data[section_name] = file_config

        return config_data

    def _validate_config(self):
        """
        Validate the loaded configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        if not isinstance(self.config_data, dict):
            raise ValueError("Configuration must be a dictionary")

        # Validate required sections exist
        required_sections = ['source', 'target']

        for section in required_sections:
            if section not in self.config_data:
                print(f"Warning: Required section '{section}' not found in configuration")

    def get_source_config(self) -> Dict[str, Any]:
        """
        Get data source configuration.

        Returns:
            Source configuration dictionary
        """
        return self.config_data.get('source', {})

    def get_target_config(self) -> Dict[str, Any]:
        """
        Get data target configuration.

        Returns:
            Target configuration dictionary
        """
        return self.config_data.get('target', {})

    def get_transformations_config(self) -> Dict[str, Any]:
        """
        Get transformations configuration.

        Returns:
            Transformations configuration dictionary
        """
        return self.config_data.get('transformations', {})

    def get_pipeline_config(self) -> Dict[str, Any]:
        """
        Get pipeline-level configuration.

        Returns:
            Pipeline configuration dictionary
        """
        return self.config_data.get('pipeline', {})

    def get_extractor_config(self) -> Dict[str, Any]:
        """
        Get extractor configuration based on source type.

        Returns:
            Extractor configuration dictionary
        """
        source_config = self.get_source_config()
        source_type = source_config.get('type', '').lower()

        if source_type == 'kafka':
            return source_config.get('kafka', {})
        elif source_type == 'mongodb':
            return source_config.get('mongodb', {})
        else:
            return source_config

    def get_parser_config(self) -> Dict[str, Any]:
        """
        Get parser configuration based on source type.

        Returns:
            Parser configuration dictionary
        """
        source_config = self.get_source_config()
        source_type = source_config.get('type', '').lower()

        parser_config = source_config.get('parser', {})

        # Add source-specific defaults
        if source_type == 'kafka':
            parser_config.setdefault('strict_mode', False)
            parser_config.setdefault('handle_malformed', True)
        elif source_type == 'mongodb':
            parser_config.setdefault('convert_objectid', True)
            parser_config.setdefault('convert_datetime', True)

        return parser_config

    def get_transformer_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all transformer configurations.

        Returns:
            Dictionary mapping transformer names to their configurations
        """
        transformations = self.get_transformations_config()

        transformer_configs = {}

        # Default transformer order and configs
        default_transformers = {
            'data_cleaner': {},
            'field_mapper': {},
            'type_converter': {},
            'flattener': {},
            'metadata_enricher': {}
        }

        # Override with config file settings
        for transformer_name, transformer_config in transformations.items():
            if transformer_config.get('enabled', True):
                transformer_configs[transformer_name] = transformer_config.get('config', {})

        # Add any missing default transformers
        for transformer_name, default_config in default_transformers.items():
            if transformer_name not in transformer_configs:
                transformer_configs[transformer_name] = default_config

        return transformer_configs

    def get_loader_config(self) -> Dict[str, Any]:
        """
        Get loader configuration based on target type.

        Returns:
            Loader configuration dictionary
        """
        target_config = self.get_target_config()
        target_type = target_config.get('type', '').lower()

        if target_type == 'mssql':
            return target_config.get('mssql', {})
        else:
            return target_config

    def get_config_section(self, section_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation path.

        Args:
            section_path: Dot-separated path to configuration value (e.g., 'source.kafka.topic')
            default: Default value if path not found

        Returns:
            Configuration value or default
        """
        try:
            value = self.config_data
            for key in section_path.split('.'):
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            return value
        except (KeyError, TypeError):
            return default

    def set_config_value(self, section_path: str, value: Any):
        """
        Set configuration value using dot notation path.

        Args:
            section_path: Dot-separated path to configuration value
            value: Value to set
        """
        keys = section_path.split('.')
        current = self.config_data

        # Navigate to parent of target key
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]

        # Set the final value
        current[keys[-1]] = value

    def save_config(self, output_path: Optional[str] = None, format: str = 'yaml'):
        """
        Save current configuration to file.

        Args:
            output_path: Path to save file (defaults to original path)
            format: Output format ('yaml' or 'json')
        """
        if not output_path:
            if not self.config_path:
                raise ValueError("No output path specified and no original config path available")
            output_path = self.config_path

        output_path = Path(output_path)

        try:
            with open(output_path, 'w', encoding='utf-8') as file:
                if format.lower() == 'yaml':
                    yaml.dump(self.config_data, file, default_flow_style=False, indent=2)
                elif format.lower() == 'json':
                    json.dump(self.config_data, file, indent=2, ensure_ascii=False)
                else:
                    raise ValueError(f"Unsupported output format: {format}")

        except Exception as e:
            raise ValueError(f"Error saving config to {output_path}: {e}")

    def validate_source_config(self) -> Dict[str, Any]:
        """
        Validate source configuration and return validation results.

        Returns:
            Dictionary with validation results
        """
        source_config = self.get_source_config()
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Check source type
        source_type = source_config.get('type')
        if not source_type:
            validation_results['valid'] = False
            validation_results['errors'].append("Source type is required")
        elif source_type.lower() not in ['kafka', 'mongodb']:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Unsupported source type: {source_type}")

        # Validate source-specific configuration
        if source_type:
            if source_type.lower() == 'kafka':
                kafka_config = source_config.get('kafka', {})
                if not kafka_config.get('topic'):
                    validation_results['errors'].append("Kafka topic is required")

            elif source_type.lower() == 'mongodb':
                mongodb_config = source_config.get('mongodb', {})
                if not mongodb_config.get('database'):
                    validation_results['errors'].append("MongoDB database is required")
                if not mongodb_config.get('collection'):
                    validation_results['errors'].append("MongoDB collection is required")

        return validation_results

    def validate_target_config(self) -> Dict[str, Any]:
        """
        Validate target configuration and return validation results.

        Returns:
            Dictionary with validation results
        """
        target_config = self.get_target_config()
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }

        # Check target type
        target_type = target_config.get('type')
        if not target_type:
            validation_results['valid'] = False
            validation_results['errors'].append("Target type is required")
        elif target_type.lower() not in ['mssql']:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Unsupported target type: {target_type}")

        # Validate target-specific configuration
        if target_type and target_type.lower() == 'mssql':
            mssql_config = target_config.get('mssql', {})
            required_fields = ['server', 'database', 'table_name']

            for field in required_fields:
                if not mssql_config.get(field):
                    validation_results['errors'].append(f"MSSQL {field} is required")

        return validation_results

    def get_config_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current configuration.

        Returns:
            Configuration summary dictionary
        """
        if not self.config_loaded:
            return {'status': 'No configuration loaded'}

        source_config = self.get_source_config()
        target_config = self.get_target_config()
        transformations = self.get_transformations_config()

        return {
            'config_path': self.config_path,
            'source_type': source_config.get('type'),
            'target_type': target_config.get('type'),
            'transformers_configured': len(transformations),
            'transformer_names': list(transformations.keys()),
            'pipeline_settings': self.get_pipeline_config()
        }

    def create_sample_config(self, output_path: str, source_type: str = 'kafka', target_type: str = 'mssql'):
        """
        Create a sample configuration file.

        Args:
            output_path: Path where to save the sample config
            source_type: Type of data source ('kafka' or 'mongodb')
            target_type: Type of data target ('mssql')
        """
        sample_config = self._generate_sample_config(source_type, target_type)

        # Save the sample config
        with open(output_path, 'w', encoding='utf-8') as file:
            yaml.dump(sample_config, file, default_flow_style=False, indent=2)

        print(f"Sample configuration created at: {output_path}")

    def _generate_sample_config(self, source_type: str, target_type: str) -> Dict[str, Any]:
        """Generate sample configuration based on source and target types."""

        if source_type.lower() == 'kafka':
            source_config = {
                'type': 'kafka',
                'kafka': {
                    'bootstrap_servers': ['localhost:9092'],
                    'topic': 'user_events',
                    'group_id': 'etl_consumer_group',
                    'auto_offset_reset': 'earliest',
                    'max_messages': 1000
                },
                'parser': {
                    'strict_mode': False,
                    'handle_malformed': True
                }
            }
        else:  # mongodb
            source_config = {
                'type': 'mongodb',
                'mongodb': {
                    'host': 'localhost',
                    'port': 27017,
                    'database': 'source_db',
                    'collection': 'documents',
                    'username': None,
                    'password': None,
                    'query': {},
                    'limit': 0
                },
                'parser': {
                    'convert_objectid': True,
                    'convert_datetime': True,
                    'preserve_id_field': True
                }
            }

        target_config = {
            'type': 'mssql',
            'mssql': {
                'server': 'localhost',
                'database': 'etl_target',
                'table_name': 'processed_data',
                'username': 'sa',
                'password': 'YourPassword123',
                'port': 1433,
                'batch_size': 1000,
                'create_table': True,
                'truncate_before_load': False,
                'upsert_mode': False
            }
        }

        transformations_config = {
            'data_cleaner': {
                'enabled': True,
                'config': {
                    'cleaning_rules': {
                        'trim_whitespace': True,
                        'remove_empty_strings': True,
                        'standardize_nulls': True,
                        'validate_emails': True,
                        'clean_phone_numbers': True
                    }
                }
            },
            'field_mapper': {
                'enabled': True,
                'config': {
                    'field_mappings': {
                        'kafka': {
                            'first_name': ['firstName'],
                            'last_name': ['lastName'],
                            'email': ['email']
                        }
                    },
                    'keep_unmapped_fields': True
                }
            },
            'type_converter': {
                'enabled': True,
                'config': {
                    'type_conversions': {
                        'age': 'int',
                        'price': 'float',
                        'active': 'bool'
                    }
                }
            },
            'flattener': {
                'enabled': True,
                'config': {
                    'separator': '.',
                    'max_depth': 10,
                    'array_handling': 'index'
                }
            },
            'metadata_enricher': {
                'enabled': True,
                'config': {
                    'add_created_at': True,
                    'add_processed_at': True,
                    'created_at_field': 'createdAt'
                }
            }
        }

        pipeline_config = {
            'name': 'Sample ETL Pipeline',
            'description': 'Sample configuration for ETL pipeline',
            'version': '1.0.0',
            'logging_level': 'INFO'
        }

        return {
            'source': source_config,
            'target': target_config,
            'transformations': transformations_config,
            'pipeline': pipeline_config
        }