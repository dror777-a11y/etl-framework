
from abc import ABC, abstractmethod
from typing import Dict, Any, List

class BaseParser(ABC):  #abstract class
    """
    Base class for all data parsers.

    Each parser is responsible for taking raw data from extractors and
    converting it into a standardized format that transformers can work with.
    """

    def __init__(self, config: Dict[str, Any]= None):
        "config: Dictionary with parser-specific settings"
        self.config = config or {}

    @abstractmethod
    def parse(self, raw_data: Dict[str, Any]) -> List [Dict[str, Any]]:
        "Parse raw data from extractors into standardized format."
        "raw_data-List of raw records from extractors"

        pass


    def validate_input(self, raw_data: Dict[str, Any]) -> bool:
        """
         Validate that the input data is in the expected format.
        Returns: True if data is valid, False otherwise.
        """
        if not isinstance(raw_data, list): #Not a list
            return False
        for record in raw_data:
            if not isinstance(record, dict): # not a dict
                return False

        return True

    def get_parser_info(self) -> Dict[str, str]:
        """ Get information about this parser.
            Returns: Dictionary with parser metadata
         """
        return {
            "parser_type": self.__class__.__name__,
            "supported_formats": "Override in subclass",
            "description": "Override in subclass"
        }