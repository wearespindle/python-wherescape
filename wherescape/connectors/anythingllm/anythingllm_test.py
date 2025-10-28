"""
Test script for AnythingLLM connector.

This script tests the anythingllm_wrapper functions using environment variables
for configuration.

Required environment variables:
- ANYTHINGLLM_EMBED_UUID: The embed UUID
- ANYTHINGLLM_API_KEY: The API key for authentication
- ANYTHINGLLM_BASE_URL: The base URL for the API endpoint

Example:
    export ANYTHINGLLM_EMBED_UUID="760d470f-6cc8-4dee-93c4-d82011bc4cc9"
    export ANYTHINGLLM_API_KEY="your-api-key"
    export ANYTHINGLLM_BASE_URL="https://anythingllm.eu-staging.holodeck.voys.nl/api"
    python anythingllm_test.py
"""

import json
import logging
import os
import sys

from .anythingllm_wrapper import _flatten_chat, get_all_embed_chats


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def check_environment_variables():
    """
    Check if required environment variables are set.

    Returns:
        tuple: (embed_uuid, api_key, base_url) or (None, None, None) if any are missing
    """
    embed_uuid = os.getenv("ANYTHINGLLM_EMBED_UUID")
    api_key = os.getenv("ANYTHINGLLM_API_KEY")
    base_url = os.getenv("ANYTHINGLLM_BASE_URL")

    missing = []
    if not embed_uuid:
        missing.append("ANYTHINGLLM_EMBED_UUID")
    if not api_key:
        missing.append("ANYTHINGLLM_API_KEY")
    if not base_url:
        missing.append("ANYTHINGLLM_BASE_URL")

    if missing:
        logging.warning("=" * 80)
        logging.warning("MISSING REQUIRED ENVIRONMENT VARIABLES")
        logging.warning("=" * 80)
        for var in missing:
            logging.warning(f"  - {var}")
        logging.warning("")
        logging.warning("Please set the following environment variables:")
        logging.warning("")
        logging.warning('  export ANYTHINGLLM_EMBED_UUID="your-embed-uuid"')
        logging.warning('  export ANYTHINGLLM_API_KEY="your-api-key"')
        logging.warning('  export ANYTHINGLLM_BASE_URL="https://your-instance.com/api"')
        logging.warning("")
        logging.warning("=" * 80)
        return None, None, None

    return embed_uuid, api_key, base_url


def test_get_all_embed_chats(embed_uuid, api_key, base_url):
    """
    Test the get_all_embed_chats function.

    Args:
        embed_uuid: The embed UUID
        api_key: The API key
        base_url: The base URL
    """
    logging.info("=" * 80)
    logging.info("Testing get_all_embed_chats()")
    logging.info("=" * 80)
    logging.info(f"Embed UUID: {embed_uuid}")
    logging.info(f"Base URL: {base_url}")
    logging.info("")

    try:
        chats = get_all_embed_chats(embed_uuid, api_key, base_url)

        logging.info("=" * 80)
        logging.info("RESULTS")
        logging.info("=" * 80)
        logging.info(f"Total chats retrieved: {len(chats)}")

        if chats:
            logging.info(f"First chat ID: {chats[0].get('id')}")
            logging.info(f"Last chat ID: {chats[-1].get('id')}")
            logging.info("")
            logging.info("Sample chat (first record):")
            logging.info("-" * 80)
            for key, value in chats[0].items():
                # Truncate long values for display
                if isinstance(value, str) and len(value) > 100:
                    display_value = value[:100] + "..."
                else:
                    display_value = value
                logging.info(f"  {key}: {display_value}")
            logging.info("")
            logging.info("Fields present in data:")
            logging.info(f"  {list(chats[0].keys())}")
        else:
            logging.info("No chats retrieved (empty result)")

        logging.info("=" * 80)
        logging.info("TEST COMPLETED SUCCESSFULLY")
        logging.info("=" * 80)

    except Exception as e:
        logging.error("=" * 80)
        logging.error("TEST FAILED")
        logging.error("=" * 80)
        logging.error(f"Error: {e}")
        logging.error("", exc_info=True)
        sys.exit(1)


def test_flatten_chat():
    """
    Test the _flatten_chat function with sample chat data.

    This test verifies that the function correctly flattens nested chat structures
    including response data, metrics, and connection information.
    """
    logging.info("=" * 80)
    logging.info("Testing _flatten_chat()")
    logging.info("=" * 80)

    # Sample chat data with nested structures
    sample_chat = {
        "id": 123,
        "prompt": "What is the weather today?",
        "session_id": "session-abc-123",
        "include": True,
        "embed_id": 456,
        "usersId": 789,
        "createdAt": "2024-01-15T10:30:00Z",
        "response": json.dumps({
            "text": "The weather is sunny today.",
            "type": "text",
            "attachments": ["file1.pdf", "file2.png"],
            "sources": [
                {"title": "Weather Report", "url": "https://example.com/weather"},
                {"title": "Climate Data", "url": "https://example.com/climate"}
            ],
            "metrics": {
                "completion_tokens": 15,
                "prompt_tokens": 8,
                "total_tokens": 23,
                "outputTps": 12.5,
                "duration": 1200
            }
        }),
        "connection_information": json.dumps({
            "host": "example.com",
            "ip": "192.168.1.100",
            "username": "testuser"
        })
    }

    try:
        # Test normal case
        logging.info("Testing with complete chat data...")
        result = _flatten_chat(sample_chat)

        # Verify all expected fields are present
        expected_fields = [
            "id", "prompt", "session_id", "include", "embed_id", "user_id", "created_at",
            "response_text", "response_type", "response_attachments", "response_sources",
            "response_sources_count", "metrics_completion_tokens", "metrics_prompt_tokens",
            "metrics_total_tokens", "metrics_output_tps", "metrics_duration",
            "connection_host", "connection_ip", "connection_username"
        ]

        logging.info(f"Fields in result: {len(result)}")
        missing_fields = [field for field in expected_fields if field not in result]

        if missing_fields:
            logging.error(f"Missing expected fields: {missing_fields}")
            sys.exit(1)

        # Verify specific values
        assert result["id"] == 123, f"Expected id=123, got {result['id']}"
        assert result["prompt"] == "What is the weather today?", "Expected prompt mismatch"
        assert result["response_text"] == "The weather is sunny today.", "Expected response_text mismatch"
        assert result["response_sources_count"] == 2, f"Expected 2 sources, got {result['response_sources_count']}"
        assert result["metrics_completion_tokens"] == 15, "Expected 15 completion tokens"
        assert result["connection_ip"] == "192.168.1.100", "Expected IP mismatch"

        logging.info("✓ All expected fields present")
        logging.info("✓ All values match expected results")
        logging.info("")

        # Test with missing/invalid response JSON
        logging.info("Testing with invalid response JSON...")
        sample_chat_invalid = sample_chat.copy()
        sample_chat_invalid["response"] = "invalid json {"
        result_invalid = _flatten_chat(sample_chat_invalid)

        assert result_invalid["response_text"] == "invalid json {", "Should store invalid JSON as-is"
        assert result_invalid["response_sources_count"] == 0, "Should default to 0 sources"
        assert result_invalid["metrics_completion_tokens"] is None, "Should default to None"

        logging.info("✓ Invalid JSON handled correctly")
        logging.info("")

        # Test with missing connection_information
        logging.info("Testing with missing connection information...")
        sample_chat_no_conn = sample_chat.copy()
        del sample_chat_no_conn["connection_information"]
        result_no_conn = _flatten_chat(sample_chat_no_conn)

        assert result_no_conn["connection_host"] is None, "Should default to None"
        assert result_no_conn["connection_ip"] is None, "Should default to None"
        assert result_no_conn["connection_username"] is None, "Should default to None"

        logging.info("✓ Missing connection information handled correctly")
        logging.info("")

        logging.info("Sample flattened chat:")
        logging.info("-" * 80)
        for key, value in result.items():
            # Truncate long values for display
            if isinstance(value, str) and len(value) > 80:
                display_value = value[:80] + "..."
            else:
                display_value = value
            logging.info(f"  {key}: {display_value}")

        logging.info("")
        logging.info("=" * 80)
        logging.info("TEST COMPLETED SUCCESSFULLY")
        logging.info("=" * 80)

    except AssertionError as e:
        logging.error("=" * 80)
        logging.error("TEST FAILED - ASSERTION ERROR")
        logging.error("=" * 80)
        logging.error(f"Assertion failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error("=" * 80)
        logging.error("TEST FAILED")
        logging.error("=" * 80)
        logging.error(f"Error: {e}")
        logging.error("", exc_info=True)
        sys.exit(1)


def main():
    """Main function to run tests."""
    logging.info("AnythingLLM Connector Test Script")
    logging.info("")

    # First run unit test for _flatten_chat (doesn't require env vars)
    test_flatten_chat()

    # Check environment variables for integration test
    embed_uuid, api_key, base_url = check_environment_variables()

    if not all([embed_uuid, api_key, base_url]):
        logging.warning("Skipping integration test (get_all_embed_chats) - missing environment variables")
        logging.info("")
        logging.info("=" * 80)
        logging.info("UNIT TESTS COMPLETED - INTEGRATION TESTS SKIPPED")
        logging.info("=" * 80)
        return

    # Run integration test
    test_get_all_embed_chats(embed_uuid, api_key, base_url)


if __name__ == "__main__":
    main()
