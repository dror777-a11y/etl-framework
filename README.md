# ETL Framework

Generic ETL framework that processes data from MongoDB and Kafka sources and loads it into Microsoft SQL Server.

## Overview 

This project implements a modular ETL pipeline following the Extract → Parse → Transform → Load pattern. The framework standardizes field names, converts data types, flattens nested JSON, and adds metadata fields before loading data into MSSQL.

## Setup Instructions

### Prerequisites
- Python 3.8+
- Microsoft SQL Server
- MongoDB (if using MongoDB source)
- Apache Kafka (if using Kafka source)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/dror777-a11y/etl-framework.git
cd etl-framework
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure your data sources and target database in the YAML configuration files (see Example Config Files section).

## How to Run ETL for Each Source

### Kafka Source
```bash
# Test with dry run (processes only 10 records)
python src/etl_pipeline.py config/kafka_to_mssql_config.yaml --dry-run 10

# Validate configuration
python src/etl_pipeline.py config/kafka_to_mssql_config.yaml --validate

# Run full pipeline
python src/etl_pipeline.py config/kafka_to_mssql_config.yaml
```

### MongoDB Source
```bash
# Test with dry run (processes only 10 records)
python src/etl_pipeline.py config/mongodb_to_mssql_config.yaml --dry-run 10

# Validate configuration
python src/etl_pipeline.py config/mongodb_to_mssql_config.yaml --validate

# Run full pipeline
python src/etl_pipeline.py config/mongodb_to_mssql_config.yaml
```

## Project Structure

```
etl_framework/
├── src/                           # Source code
│   ├── config_manager/           # Configuration handling
│   ├── extractors/               # Data extraction components
│   │   ├── kafka_extractor.py    # Kafka message extraction
│   │   └── mongo_extractor.py    # MongoDB document extraction
│   ├── parsers/                  # Raw data parsing
│   │   ├── json_parser.py        # JSON parsing for Kafka
│   │   └── bson_parser.py        # BSON parsing for MongoDB
│   ├── transformers/             # Data transformation logic
│   │   ├── field_mapper.py       # Standardize field names
│   │   ├── type_converter.py     # Convert data types
│   │   ├── flattener.py          # Flatten nested JSON
│   │   ├── data_cleaner.py       # Clean and validate data
│   │   └── metadata_enricher.py  # Add metadata fields
│   ├── loaders/                  # Database loading
│   │   └── mssql_loader.py       # MSSQL Server loading
│   └── etl_pipeline.py           # Main pipeline orchestrator
├── config/                       # Configuration files
│   ├── kafka_to_mssql_config.yaml
│   ├── mongodb_to_mssql_config.yaml
│   └── simple_config.yaml
├── tests/                        # Test scripts
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## Design Decisions

The framework follows a strict 4-stage ETL pattern:

### 1. Extract
- **MongoDB**: Connects to collections and extracts documents with flexible queries
- **Kafka**: Consumes messages from topics with offset management

### 2. Parse  
- **JSON Parser**: Processes Kafka messages into standardized format
- **BSON Parser**: Processes MongoDB documents, handling ObjectIds and datetime objects

### 3. Transform
Data flows through transformers in sequence:
- **Field Mapping**: Standardizes field names across different sources
- **Type Conversion**: Converts strings to appropriate data types (int, float, bool, datetime)
- **Flattening**: Converts nested JSON structures to flat database-friendly format  
- **Data Cleaning**: Removes whitespace, standardizes null values, validates data
- **Metadata Enrichment**: Adds createdAt timestamp (automatically applied)

### 4. Load
- **MSSQL Loader**: Batch inserts with automatic table creation and error handling

## Configuration

The system uses YAML files for all configuration. Key sections:

```yaml
source:              # Data source configuration
  type: kafka        # mongodb or kafka
  kafka:            # Source-specific settings
    bootstrap_servers: ["localhost:9092"]
    topic: "events"

target:             # Target database configuration  
  type: mssql
  mssql:
    server: "localhost"
    database: "etl_target"
    table_name: "processed_data"

transformations:    # Transformation configuration
  field_mapper:
    enabled: true
    config:
      field_mappings:
        kafka:
          first_name: ["firstName", "user_name"]
```

## Example Config Files

- **`config/kafka_to_mssql_config.yaml`** - Complete Kafka to MSSQL pipeline configuration
- **`config/mongodb_to_mssql_config.yaml`** - Complete MongoDB to MSSQL pipeline configuration  
- **`config/simple_config.yaml`** - Minimal test configuration for quick testing

## Dependencies

The project uses these Python libraries:
- `PyYAML` - Configuration file parsing
- `pyodbc` - SQL Server connectivity  
- `pymongo` - MongoDB connectivity
- `kafka-python` - Kafka integration
- `colorlog` - Enhanced logging

## Features

### Error Handling
- Comprehensive error handling at every pipeline stage
- Failed records logged with detailed error messages
- Pipeline continues processing even if individual records fail

### Performance
- Batch processing for efficient database operations
- Configurable batch sizes
- Connection pooling and timeout management
- Memory-efficient streaming for large datasets

### Operational
- Dry run capability for testing
- Configuration validation
- Detailed logging at all stages
- Automatic database table creation

## Testing

Run the included test scripts:

```bash
# Test all transformers
python test_transformers.py

# Test extractors (requires running MongoDB/Kafka)
python test_extractors.py

# Test parsers  
python test_parsers.py
```