"""
This module is used to create metadata for AnythingLLM chats.

The module assumes that a WhereScape load object has been created.

The module needs to be called from WhereScape using anythingllm_create_metadata().

The module creates column metadata for the flattened chat structure returned
by get_all_embed_chats() and adds it to the WhereScape load object in the
repository database.
"""

import logging
import os
from datetime import datetime

from ...wherescape import WhereScape
from ...wherescape.helper_functions import prepare_metadata_query


# Define expected columns matching the flattened chat structure from get_all_embed_chats()
EXPECTED_COLUMNS = [
    "id",
    "prompt",
    "session_id",
    "include",
    "embed_id",
    "user_id",
    "created_at",
    "connection_host",
    "connection_ip",
    "connection_username",
    "response_text",
    "response_type",
    "response_attachments",
    "response_sources",
    "response_sources_count",
    "metrics_completion_tokens",
    "metrics_prompt_tokens",
    "metrics_total_tokens",
    "metrics_output_tps",
    "metrics_duration",
]


def anythingllm_create_metadata():
    """Create metadata for AnythingLLM chats load table."""
    start_time = datetime.now()
    logging.info(
        f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for anythingllm_create_metadata"
    )

    # Initialise WhereScape (logging is initialised through WhereScape object)
    wherescape = WhereScape()
    wherescape_object_id = wherescape.object_key
    embed_uuid = os.getenv("WSL_SRCCFG_USER", "unknown")

    # Use the shared expected columns definition
    columns = EXPECTED_COLUMNS

    # Define display names for the columns
    display_names = [
        "ID",
        "Prompt",
        "Session ID",
        "Include",
        "Embed ID",
        "[GDPR_MEDIUM] User ID",
        "Created At",
        "Connection Host",
        "[GDPR_MEDIUM] Connection IP",
        "[GDPR_MEDIUM] Connection Username",
        "Response Text",
        "Response Type",
        "Response Attachments",
        "Response Sources",
        "Response Sources Count",
        "Metrics Completion Tokens",
        "Metrics Prompt Tokens",
        "Metrics Total Tokens",
        "Metrics Output TPS",
        "Metrics Duration",
    ]

    # Define data types for the columns
    types = [
        "bigint",  # id
        "text",  # prompt
        "text",  # session_id
        "bool",  # include
        "int",  # embed_id
        "int",  # user_id (nullable)
        "timestamp",  # created_at
        "text",  # connection_host
        "text",  # connection_ip
        "text",  # connection_username
        "text",  # response_text
        "text",  # response_type
        "text",  # response_attachments (JSON string)
        "text",  # response_sources (JSON string)
        "int",  # response_sources_count
        "int",  # metrics_completion_tokens
        "int",  # metrics_prompt_tokens
        "int",  # metrics_total_tokens
        "numeric",  # metrics_output_tps
        "numeric",  # metrics_duration
    ]

    logging.info("Preparing metadata for %d columns", len(columns) + 2)

    # Prepare the SQL (wherescape_os version automatically adds dss_record_source and dss_load_date)
    sql = prepare_metadata_query(
        wherescape_object_id,
        f"AnythingLLM embed - {embed_uuid}",
        columns=columns,
        display_names=display_names,
        types=types,
    )

    # Execute the SQL
    wherescape.push_to_meta(sql)
    wherescape.main_message = f"Created {len(columns) + 2} columns in metadata table for embed {embed_uuid}"

    # Final logging
    end_time = datetime.now()
    logging.info(
        f"Time elapsed: {(end_time - start_time).seconds} seconds for anythingllm_create_metadata"
    )
