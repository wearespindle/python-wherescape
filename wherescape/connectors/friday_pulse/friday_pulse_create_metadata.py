"""
Module for creating metadata for Friday Pulse data in WhereScape.

This module creates column metadata for the flattened Friday Pulse result structure
and adds it to the WhereScape load object in the repository database.
"""

import logging
from datetime import datetime

from ...wherescape import WhereScape
from ...helper_functions import prepare_metadata_query


# Define expected columns matching the flattened result structure from get_all_results()
EXPECTED_COLUMNS = [
    "sample_date",
    "score",
    "response_rate",
    "response_count",
    "total_count",
    "question_count",
    "topic_code",
    "topic_name",
]


def friday_pulse_create_metadata():
    """Create metadata for Friday Pulse results load table."""
    start_time = datetime.now()

    # Initialise WhereScape (logging is initialised through WhereScape object)
    wherescape = WhereScape()
    logging.info(
        f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for friday_pulse_create_metadata"
    )
    wherescape_object_id = wherescape.object_key

    # Use the shared expected columns definition
    columns = EXPECTED_COLUMNS

    # Define display names for the columns
    display_names = [
        "Date when the pulse survey was conducted",
        "Overall satisfaction score (0-100)",
        "Percentage of invited users who responded",
        "Number of users who responded",
        "Total number of users invited",
        "Number of questions in the survey",
        "Unique code identifying the topic/category",
        "Human-readable name of the topic/category",
    ]

    # Define data types for the columns
    types = [
        "date",  # sample_date
        "numeric",  # score
        "numeric",  # response_rate
        "int",  # response_count
        "int",  # total_count
        "int",  # question_count
        "text",  # topic_code
        "text",  # topic_name
    ]

    logging.info(f"Preparing metadata for {len(columns)} columns")

    # Prepare the SQL (dss_record_source and dss_load_date are added automatically)
    sql = prepare_metadata_query(
        wherescape_object_id,
        "Friday Pulse API - questions",
        columns=columns,
        display_names=display_names,
        types=types,
    )

    # Execute the SQL
    try:
        wherescape.push_to_meta(sql)
        wherescape.main_message = (
            f"Created {len(columns)} columns in metadata table for Friday Pulse results"
        )
    except Exception as e:
        logging.error(f"Failed to create metadata: {e}")
        raise

    # Final logging
    end_time = datetime.now()
    logging.info(
        f"Time elapsed: {(end_time - start_time).seconds} seconds for friday_pulse_create_metadata"
    )
