# ETL Framework Project

## Overview

This project implements a generic ETL (Extract-Transform-Load) framework that can process data from various sources and load it into Microsoft SQL Server. I built this as a modular system where each component can be easily replaced or extended.

## What it does

The framework takes data from MongoDB or Kafka, cleans and transforms it according to business rules, and loads it into a SQL Server database. Everything is controlled through configuration files, so you can adapt it for different data sources without changing code.

## Architecture

I designed this with a clear separation of concerns:

- **Extractors** pull raw data from sources (MongoDB collections, Kafka topics)
- **Parsers** convert raw data into a standard format
- **Transformers** clean, reshape, and enrich the data
- **Loaders** insert the final data into the target database

The pipeline runs these components in sequence, with full error handling and rollback capabilities.

## Key Features

### Data Sources
- MongoDB document extraction with flexible queries
- Kafka message consumption with offset management
- Easy to add new sources by implementing the base extractor interface

### Data Transformations
- **Data Cleaning**: Removes whitespace, standardizes null values, validates emails/phones
- **Field Mapping**: Converts different source field names to standard schema
- **Type Conversion**: Automatically converts strings to proper data types
- **Flattening**: Handles nested JSON by creating flat database-friendly fields
- **Metadata Enrichment**: Adds tracking fields like createdAt, recordId, dataSource

### Configuration Management
- YAML-based configuration for all pipeline settings
- Separate configs for different source-target combinations
- Schema validation to catch configuration errors early

### Operational Features
- Batch processing for efficient database loading
- Comprehensive logging at all pipeline stages
- Dry run capability for testing with sample data
- Automatic table creation with proper data types
- Connection pooling and timeout handling

## Project Structure

```
etl_framework/
├── src/                    # Source code
│   ├── extractors/        # Data extraction components
│   ├── parsers/           # Raw data parsing
│   ├── transformers/      # Data transformation logic
│   ├── loaders/           # Database loading
│   ├── config_manager/    # Configuration handling
│   └── etl_pipeline.py    # Main pipeline orchestrator
├── config/                # Configuration files
├── tests/                 # Test scripts
└── requirements.txt       # Python dependencies
```

## Usage Examples

### Basic Pipeline Execution
```bash
python -m src.etl_pipeline config/kafka_to_mssql_config.yaml
```

### Configuration Validation
```bash
python -m src.etl_pipeline config/mongodb_to_mssql_config.yaml --validate
```

### Testing with Limited Data
```bash
python -m src.etl_pipeline config/kafka_to_mssql_config.yaml --dry-run 100
```

## Configuration

The system uses YAML files to define all pipeline behavior. Here's what you can configure:

- **Source settings**: Connection details, queries, message limits
- **Transformation rules**: Field mappings, data type conversions, cleaning rules
- **Target settings**: Database connection, table names, batch sizes
- **Pipeline options**: Logging levels, error handling, metadata fields

## Technical Implementation

### Error Handling
I implemented comprehensive error handling at every stage. The system can continue processing even if individual records fail, and it provides detailed logging about what went wrong.

### Performance Considerations
- Batch processing reduces database round trips
- Connection pooling manages database resources
- Memory-efficient streaming for large datasets
- Configurable batch sizes based on available resources

### Data Quality
The transformation pipeline includes multiple data quality checks:
- Schema validation against expected formats
- Data type validation with fallback defaults
- Duplicate detection and handling
- Referential integrity checks where applicable

## Dependencies

The project uses standard Python libraries for data processing:
- `pymongo` for MongoDB connectivity
- `kafka-python` for Kafka integration
- `pyodbc` for SQL Server connections
- `PyYAML` for configuration management
- `pandas` for data manipulation

## Testing

I included test scripts for each major component:
- Unit tests for individual transformers
- Integration tests for full pipeline execution
- Configuration validation tests
- Sample data generators for testing

## Future Enhancements

Some ideas for extending this framework:
- Add support for additional data sources (PostgreSQL, REST APIs)
- Implement real-time streaming processing
- Add data lineage tracking
- Build a web UI for pipeline monitoring
- Add support for schema evolution

## Development Notes

This was built with maintainability in mind. Each component follows the same interface pattern, making it easy to add new functionality. The configuration-driven approach means most changes can be made without touching code.

The biggest challenge was handling the variety of data formats and ensuring reliable error recovery. I solved this by implementing a standardized internal data format that all components work with.

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Configure your data sources and targets in the YAML files
4. Run the pipeline with your chosen configuration

The system is designed to work out of the box once you provide valid connection details for your data sources and target database.