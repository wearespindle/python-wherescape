"""
Module for creating metadata for Friday Pulse data in WhereScape.

This module creates column metadata by analyzing sample data from the API
and adds it to the WhereScape load object in the repository database.
"""

import logging
import os
from datetime import datetime

from ...helper_functions import (
    create_column_names,
    create_display_names,
    get_metadata_from_sample_data,
    prepare_metadata_query,
)
from ...wherescape import WhereScape
from .friday_pulse_wrapper import FridayPulseClient


def friday_pulse_create_metadata():
    """
    Create metadata for Friday Pulse load table.

    This function:
    1. Initializes WhereScape object
    2. Gets the source file name from the load object
    3. Determines the subject (topics, groups, group_types, group_notes, general_results, group_results)
    4. Fetches sample data for the subject
    5. Analyzes sample data to determine columns and types
    6. Creates metadata in WhereScape repository
    """
    start_time = datetime.now()

    # Step 1: Initialize WhereScape object
    wherescape = WhereScape()
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for friday_pulse_create_metadata")
    wherescape_object_id = wherescape.object_key

    # Step 2: Get the table name from the load object
    table_name = wherescape.table
    if not table_name:
        logging.error("No table name found in WhereScape load object")
        wherescape.main_message = "Error: table name not set in load object"
        return

    logging.info(f"Creating metadata for table: {table_name}")

    # Get API token and initialize client
    bearer_token = os.getenv("WSL_SRCCFG_APIKEY")
    if not bearer_token:
        logging.error("Connection for load table not set properly")
        wherescape.main_message = "Error: Connection for load table not set properly"
        return

    # Initialize Friday Pulse client
    client = FridayPulseClient(bearer_token)

    # Step 3 & 4: Determine subject and fetch sample data
    sample_data = []
    source_description = ""

    if "topics" in table_name:
        logging.info("Fetching topics data...")
        sample_data = client.get_topics()
        source_description = "topics"
    elif "group_types" in table_name:
        logging.info("Fetching group types data...")
        sample_data = client.get_group_types()
        source_description = "group_types"
    elif "groups" in table_name:
        logging.info("Fetching groups data...")
        sample_data = client.get_groups()
        source_description = "groups"
    elif "group_notes" in table_name:
        logging.info("Fetching group notes data (latest)...")
        # Fetch latest notes without date filter for sample
        sample_data = client.get_group_notes()
        source_description = "group_notes"
    elif "group_results" in table_name:
        logging.info("Fetching group results data (latest)...")
        # Fetch latest results without date filter for sample
        sample_data = client.get_group_results()
        source_description = "group_results"
    elif "general_notes" in table_name:
        logging.info("Fetching general notes data (latest)...")
        # Fetch latest notes without date filter for sample
        sample_data = client.get_general_notes()
        source_description = "general_notes"
    elif "group_risks" in table_name:
        logging.info("Fetching group risks data (latest)...")
        # Fetch latest risks without date filter for sample
        sample_data = client.get_group_risks()
        source_description = "group_risks"
    elif "general_risks" in table_name:
        logging.info("Fetching general risks data (latest)...")
        # Fetch latest risks without date filter for sample
        sample_data = client.get_general_risks()
        source_description = "general_risks"
    elif "general_results" in table_name:
        logging.info("Fetching general results data (latest)...")
        # Fetch latest results without date filter for sample
        sample_data = client.get_general_results()
        source_description = "general_results"
    else:
        logging.error(
            f"Unknown table name pattern: '{table_name}'. "
            f"Table name must contain one of: topics, group_types, groups, group_notes, group_results, "
            f"general_notes, group_risks, general_risks, general_results, results"
        )
        wherescape.main_message = f"Error: Unknown subject in table name '{table_name}'"
        return

    if not sample_data:
        logging.error(f"No sample data retrieved for {source_description}")
        wherescape.main_message = f"Error: No data available from {source_description} API"
        return

    logging.info(f"Retrieved {len(sample_data)} sample records for analysis")

    # Step 5: Analyze sample data
    columns, types = get_metadata_from_sample_data(sample_data)

    if not columns:
        logging.error("No columns could be determined from sample data")
        wherescape.main_message = "Error: Could not analyze data structure"
        return

    logging.info(f"Discovered {len(columns)} columns: {columns}")
    logging.info(f"Inferred types: {types}")

    # Use helper functions to create column names and display names
    column_names = create_column_names(columns)
    display_names = create_display_names(columns)

    # Prepare metadata SQL
    sql = prepare_metadata_query(
        wherescape_object_id,
        f"Friday Pulse API - {source_description}",
        columns=column_names,
        display_names=display_names,
        types=types,
    )

    # Execute the SQL
    try:
        wherescape.push_to_meta(sql)
        wherescape.main_message = (
            f"Created {len(columns)} columns in metadata table for Friday Pulse {source_description}"
        )
        logging.info(f"Successfully created metadata for {source_description}")
    except Exception as e:
        logging.error(f"Failed to create metadata: {e}")
        wherescape.main_message = f"Error creating metadata: {e}"
        raise

    # Final logging
    end_time = datetime.now()
    logging.info(f"Time elapsed: {(end_time - start_time).seconds} seconds for friday_pulse_create_metadata")
