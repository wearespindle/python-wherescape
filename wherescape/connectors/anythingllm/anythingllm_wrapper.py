import json
import logging

import requests


def get_all_embed_chats(embed_uuid, api_key, base_url):
    """
    Get all chats from an embed using the embed API endpoint.
    This endpoint returns all chats for a given embed UUID.

    Note: The AnythingLLM embed API does not support pagination or datetime filtering.
    This function returns all chats without any filtering.

    Args:
        embed_uuid: The embed UUID
        api_key: The API key for authentication
        base_url: The base URL for the API endpoint

    Returns:
        List of flattened dictionaries containing chat data, ready for database insertion
    """
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    logging.info(f"Fetching all chats from embed '{embed_uuid}'...")

    # Construct the full URL with embed_uuid
    url = f"{base_url}/v1/embed/{embed_uuid}/chats"
    logging.info(f"Fetching chats from URL: {url}")

    try:
        response = requests.get(url=url, headers=headers, timeout=30)
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching chats from embed {embed_uuid}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching chats from embed {embed_uuid}: {e}")
        return []

    # Check response status code
    if response.status_code != 200:
        logging.error(f"API returned status code {response.status_code}")
        return []

    data = response.json()
    chats = data.get("chats", [])

    if not chats:
        logging.info(f"No chats returned from embed {embed_uuid}")
        return []

    logging.info(f"Retrieved {len(chats)} chats from embed")

    # Process all chats
    all_chats = []
    for chat in chats:
        all_chats.append(_flatten_chat(chat))

    logging.info(f"Completed: retrieved {len(all_chats)} total chats")
    return all_chats


def _flatten_chat(chat):
    """
    Flatten the nested chat structure from the embed API into a flat dictionary
    suitable for database insertion.

    Args:
        chat: Dictionary containing the raw chat data from the embed API

    Returns:
        Flattened dictionary with all nested fields promoted to top level
    """
    # Parse the response JSON string
    response_data = {}
    try:
        response_obj = json.loads(chat.get("response", "{}"))
        response_data = {
            "response_text": response_obj.get("text", ""),
            "response_type": response_obj.get("type", ""),
            "response_attachments": json.dumps(response_obj.get("attachments", [])),
            "response_sources": json.dumps(response_obj.get("sources", [])),
            "response_sources_count": len(response_obj.get("sources", [])),
            "metrics_completion_tokens": response_obj.get("metrics", {}).get("completion_tokens"),
            "metrics_prompt_tokens": response_obj.get("metrics", {}).get("prompt_tokens"),
            "metrics_total_tokens": response_obj.get("metrics", {}).get("total_tokens"),
            "metrics_output_tps": response_obj.get("metrics", {}).get("outputTps"),
            "metrics_duration": response_obj.get("metrics", {}).get("duration"),
        }
    except (json.JSONDecodeError, TypeError):
        # If response is not valid JSON, store as-is
        response_data = {
            "response_text": chat.get("response", ""),
            "response_type": None,
            "response_attachments": "[]",
            "response_sources": "[]",
            "response_sources_count": 0,
            "metrics_completion_tokens": None,
            "metrics_prompt_tokens": None,
            "metrics_total_tokens": None,
            "metrics_output_tps": None,
            "metrics_duration": None,
        }

    # Parse connection_information JSON string
    connection_data = {}
    try:
        conn_obj = json.loads(chat.get("connection_information", "{}"))
        connection_data = {
            "connection_host": conn_obj.get("host"),
            "connection_ip": conn_obj.get("ip"),
            "connection_username": conn_obj.get("username"),
        }
    except (json.JSONDecodeError, TypeError):
        connection_data = {
            "connection_host": None,
            "connection_ip": None,
            "connection_username": None,
        }

    # Combine all fields into a flat structure
    flat_chat = {
        "id": chat.get("id"),
        "prompt": chat.get("prompt"),
        "session_id": chat.get("session_id"),
        "include": chat.get("include"),
        "embed_id": chat.get("embed_id"),
        "user_id": chat.get("usersId"),
        "created_at": chat.get("createdAt"),
    }

    # Merge all dictionaries
    flat_chat.update(connection_data)
    flat_chat.update(response_data)

    return flat_chat
