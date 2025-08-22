"""
Simple test script to verify that our extractors work correctly.
Run this to check if MongoDB and Kafka extractors can connect and extract data.
"""

from src.extractors.mongo_extractor import MongoExtractor
from src.extractors.kafka_extractor import KafkaExtractor


def test_mongo_extractor():
    """Test MongoDB extractor with a simple configuration"""
    print("=" * 50)
    print("Testing MongoDB Extractor")
    print("=" * 50)

    # Simple config for local MongoDB
    config = {
        "host": "localhost",
        "port": 27017,
        "database": "test_db",
        "collection": "test_collection",
        "limit": 5  # Just get 5 records for testing
    }

    try:
        # Test the extractor
        with MongoExtractor(config) as extractor:
            print("✅ MongoDB connection successful!")

            # Try to extract some data
            data = extractor.extract()
            print(f"✅ Extracted {len(data)} records from MongoDB")

            # Show first record if any
            if data:
                print("📄 Sample record:")
                print(data[0])
            else:
                print("ℹ️  No data found (collection might be empty)")

    except Exception as e:
        print(f"❌ MongoDB test failed: {e}")
        print("💡 Make sure MongoDB is running and accessible")


def test_kafka_extractor():
    """Test Kafka extractor with a simple configuration"""
    print("\n" + "=" * 50)
    print("Testing Kafka Extractor")
    print("=" * 50)

    # Simple config for local Kafka
    config = {
        "bootstrap_servers": ["localhost:9092"],
        "topic": "test_topic",
        "group_id": "test_consumer_group",
        "max_messages": 3,  # Just get 3 messages for testing
        "timeout_ms": 5000  # Wait 5 seconds max
    }

    try:
        # Test the extractor
        with KafkaExtractor(config) as extractor:
            print("✅ Kafka connection successful!")

            # Try to extract some data
            data = extractor.extract()
            print(f"✅ Extracted {len(data)} messages from Kafka")

            # Show first message if any
            if data:
                print("📄 Sample message:")
                print(data[0])
            else:
                print("ℹ️  No messages found (topic might be empty or timeout reached)")

    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        print("💡 Make sure Kafka is running and accessible")


def test_extractors_basic():
    """Test basic extractor functionality without external dependencies"""
    print("\n" + "=" * 50)
    print("Testing Basic Extractor Functionality")
    print("=" * 50)

    # Test MongoDB extractor initialization
    try:
        mongo_config = {"host": "test", "database": "test", "collection": "test"}
        mongo_extractor = MongoExtractor(mongo_config)
        print("✅ MongoDB extractor initialization successful")
    except Exception as e:
        print(f"❌ MongoDB extractor initialization failed: {e}")

    # Test Kafka extractor initialization
    try:
        kafka_config = {"topic": "test"}
        kafka_extractor = KafkaExtractor(kafka_config)
        print("✅ Kafka extractor initialization successful")
    except Exception as e:
        print(f"❌ Kafka extractor initialization failed: {e}")


if __name__ == "__main__":
    print("🚀 Starting Extractor Tests...")

    # Always run basic tests
    test_extractors_basic()

    # Ask user what to test
    print("\n" + "=" * 50)
    print("Choose what to test:")
    print("1. MongoDB Extractor (requires MongoDB running)")
    print("2. Kafka Extractor (requires Kafka running)")
    print("3. Both")
    print("4. Skip external tests")

    choice = input("Enter your choice (1-4): ").strip()

    if choice in ["1", "3"]:
        test_mongo_extractor()

    if choice in ["2", "3"]:
        test_kafka_extractor()

    if choice == "4":
        print("ℹ️  Skipping external tests")

    print("\n🎉 Testing complete!")
    print("\n💡 To test with real data:")
    print("   - For MongoDB: Create a 'test_db' database with a 'test_collection'")
    print("   - For Kafka: Create a 'test_topic' and send some messages to it")