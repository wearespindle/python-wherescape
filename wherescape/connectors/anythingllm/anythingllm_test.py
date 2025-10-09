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

import os
import sys
import logging
from .anythingllm_wrapper import get_all_embed_chats

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


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


def main():
    """Main function to run tests."""
    logging.info("AnythingLLM Connector Test Script")
    logging.info("")

    # Check environment variables
    embed_uuid, api_key, base_url = check_environment_variables()

    if not all([embed_uuid, api_key, base_url]):
        logging.error("Cannot proceed without required environment variables")
        sys.exit(1)

    # Run tests
    test_get_all_embed_chats(embed_uuid, api_key, base_url)


if __name__ == "__main__":
    main()
