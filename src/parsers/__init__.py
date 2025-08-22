"""
Parsers Module

This module contains all data parsers that implement the Parse phase of ETL.
Each parser is responsible for converting raw data from extractors into
a standardized format that transformers can work with.

Available Parsers:
- BaseParser: Abstract base class for all parsers
- JsonParser: Parses JSON data from Kafka extractors
- BsonParser: Parses BSON data from MongoDB extractors
"""

from .base_parser import BaseParser
from .json_parser import JsonParser
from .bson_parser import BsonParser

__all__ = [
    'BaseParser',
    'JsonParser',
    'BsonParser'
]

# Version info
__version__ = '1.0.0'