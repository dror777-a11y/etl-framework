"""
Simple test script to verify that our parsers work correctly.
This tests the parsing functionality with sample data.
"""

from src.parsers.json_parser import JsonParser
from src.parsers.bson_parser import BsonParser
from datetime import datetime


def test_json_parser():
    """Test JsonParser with sample Kafka data"""
    print("=" * 50)
    print("Testing JsonParser")
    print("=" * 50)

    # Sample data that would come from KafkaExtractor
    sample_kafka_data = [
        {
            "raw_value": '{"user": "John", "age": 30, "city": "New York"}',
            "topic": "user_events",
            "partition": 0,
            "offset": 123,
            "timestamp": 1642345678000,
            "key": "user_123",
            "headers": {"source": "mobile_app"}
        },
        {
            "raw_value": '{"product": "laptop", "price": 999.99, "available": true}',
            "topic": "product_events",
            "partition": 1,
            "offset": 124,
            "timestamp": 1642345679000,
            "key": "product_456",
            "headers": {}
        }
    ]

    try:
        # Test JsonParser
        parser = JsonParser()

        print("✅ JsonParser created successfully")
        print(f"Parser info: {parser.get_parser_info()}")

        # Parse the data
        parsed_data = parser.parse(sample_kafka_data)

        print(f"✅ Parsed {len(parsed_data)} records")

        # Show results
        for i, record in enumerate(parsed_data):
            print(f"\n📄 Record {i + 1}:")
            print(f"  Data: {record['data']}")
            print(f"  Source type: {record['metadata']['source_type']}")
            print(f"  Topic: {record['metadata']['topic']}")

        return True

    except Exception as e:
        print(f"❌ JsonParser test failed: {e}")
        return False


def test_json_parser_with_malformed():
    """Test JsonParser with malformed JSON"""
    print("\n" + "=" * 50)
    print("Testing JsonParser with Malformed JSON")
    print("=" * 50)

    # Sample data with broken JSON
    malformed_data = [
        {
            "raw_value": '{"user": "John", "age":}',  # Broken JSON
            "topic": "user_events",
            "partition": 0,
            "offset": 125
        },
        {
            "raw_value": 'not json at all',  # Not JSON
            "topic": "text_events",
            "partition": 0,
            "offset": 126
        }
    ]

    try:
        parser = JsonParser({"handle_malformed": True})
        parsed_data = parser.parse(malformed_data)

        print(f"✅ Handled {len(parsed_data)} malformed records")

        for i, record in enumerate(parsed_data):
            print(f"\n📄 Record {i + 1}:")
            if "raw_text" in record['data']:
                print(f"  Fallback record created: {record['data']['raw_text']}")
            else:
                print(f"  Data: {record['data']}")

        return True

    except Exception as e:
        print(f"❌ Malformed JSON test failed: {e}")
        return False


def test_bson_parser():
    """Test BsonParser with sample MongoDB data"""
    print("\n" + "=" * 50)
    print("Testing BsonParser")
    print("=" * 50)

    # Sample data that would come from MongoExtractor
    # Note: Using strings for ObjectId since we already convert them in MongoExtractor
    sample_mongo_data = [
        {
            "_id": "507f1f77bcf86cd799439011",
            "name": "John Doe",
            "age": 30,
            "createdAt": datetime(2023, 1, 15, 10, 30),
            "address": {
                "city": "New York",
                "street": "5th Avenue",
                "zipcode": "10001"
            },
            "hobbies": ["reading", "swimming", "coding"]
        },
        {
            "_id": "507f1f77bcf86cd799439012",
            "name": "Jane Smith",
            "age": 25,
            "createdAt": datetime(2023, 2, 10, 14, 45),
            "skills": [
                {"name": "Python", "level": "expert"},
                {"name": "JavaScript", "level": "intermediate"}
            ]
        }
    ]

    try:
        # Test BsonParser
        parser = BsonParser()

        print("✅ BsonParser created successfully")
        print(f"Parser info: {parser.get_parser_info()}")

        # Parse the data
        parsed_data = parser.parse(sample_mongo_data)

        print(f"✅ Parsed {len(parsed_data)} records")

        # Show results
        for i, record in enumerate(parsed_data):
            print(f"\n📄 Record {i + 1}:")
            print(f"  Data: {record['data']}")
            print(f"  Source type: {record['metadata']['source_type']}")
            print(f"  Document ID: {record['metadata']['document_id']}")
            print(f"  Has nested objects: {record['metadata']['has_nested_objects']}")

        return True

    except Exception as e:
        print(f"❌ BsonParser test failed: {e}")
        return False


def test_bson_parser_configurations():
    """Test BsonParser with different configurations"""
    print("\n" + "=" * 50)
    print("Testing BsonParser Configurations")
    print("=" * 50)

    sample_data = [{
        "_id": "507f1f77bcf86cd799439013",
        "name": "Test User",
        "createdAt": datetime(2023, 3, 1, 9, 0)
    }]

    try:
        # Test without preserving ID
        parser1 = BsonParser({"preserve_id_field": False})
        result1 = parser1.parse(sample_data)

        print("✅ Test without _id field:")
        print(f"  Data keys: {list(result1[0]['data'].keys())}")

        # Test without converting datetime
        parser2 = BsonParser({"convert_datetime": False})
        result2 = parser2.parse(sample_data)

        print("✅ Test without datetime conversion:")
        print(f"  createdAt type: {type(result2[0]['data']['createdAt'])}")

        return True

    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Starting Parser Tests...")

    success_count = 0

    # Run all tests
    tests = [
        test_json_parser,
        test_json_parser_with_malformed,
        test_bson_parser,
        test_bson_parser_configurations
    ]

    for test_func in tests:
        if test_func():
            success_count += 1

    print("\n" + "=" * 50)
    print("🎉 Parser Testing Complete!")
    print(f"✅ {success_count}/{len(tests)} tests passed")

    if success_count == len(tests):
        print("🎊 All parsers working correctly!")
    else:
        print("⚠️  Some tests failed - check the output above")