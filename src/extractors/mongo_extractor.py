from typing import Dict, Any, List
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from .base_extractor import BaseExtractor


class MongoExtractor(BaseExtractor):
    """Special extractor for MongoDB"""

    def __init__(self, config: Dict[str, Any]):
        """Set up the MongoDB extractor with connection details."""
        super().__init__(config)
        self.client = None
        self.database = None
        self.collection = None

    def connect(self) -> bool:
        """Connect to MongoDB using the provided configuration.
        Returns: True if connection successful, False otherwise"""

        try:
            host = self.config.get("host", "localhost")
            port = self.config.get("port", 27017)
            username = self.config.get("username")
            password = self.config.get("password")

            if username and password:
                connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
            else:
                connection_string = f"mongodb://{host}:{port}/"

            # Create client and test connection
            self.client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)

            # Test the connection
            self.client.admin.command('ping')

            # Get database and collection
            db_name = self.config.get("database")
            collection_name = self.config.get("collection")

            if not db_name or not collection_name:
                raise ValueError("Database and collection names are required in config")

            self.database = self.client[db_name]
            self.collection = self.database[collection_name]

            print(f"Successfully connected to MongoDB: {db_name}.{collection_name}")
            return True

        except ConnectionFailure:
            print("Failed to connect to MongoDB - connection error")
            return False
        except PyMongoError as e:
            print(f"MongoDB error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error connecting to MongoDB: {e}")
            return False

    def extract(self) -> List[Dict[str, Any]]:
        """Extract data from MongoDB collection.
        Returns: List of documents from the collection"""

        if not self.collection:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")

        try:
            # Get query parameters from config
            query = self.config.get("query", {})
            limit = self.config.get("limit", 0)  # 0 means no limit

            # Execute query
            if limit > 0:
                cursor = self.collection.find(query).limit(limit)
            else:
                cursor = self.collection.find(query)

            # Convert cursor to list and handle ObjectId
            documents = []
            for doc in cursor:
                # Convert ObjectId to string for JSON serialization later
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
                documents.append(doc)

            print(f"Extracted {len(documents)} documents from MongoDB")
            return documents

        except PyMongoError as e:
            print(f"Error extracting data from MongoDB: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error during extraction: {e}")
            return []

    def disconnect(self) -> bool:
        """Close the MongoDB connection.
        Returns: True if disconnection successful, False otherwise"""

        try:
            if self.client:
                self.client.close()
                self.client = None
                self.database = None
                self.collection = None
                print("Disconnected from MongoDB")
            return True

        except Exception as e:
            print(f"Error disconnecting from MongoDB: {e}")
            return False