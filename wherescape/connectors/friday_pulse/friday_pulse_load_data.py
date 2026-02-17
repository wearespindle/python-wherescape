"""
Module for loading Friday Pulse data into WhereScape.

This module fetches data from the Friday Pulse API and loads it into
the WhereScape data warehouse. It supports multiple endpoint types.
"""

import logging
import os
from datetime import datetime, timedelta

from ...helper_functions import create_column_names
from ...wherescape import WhereScape
from .friday_pulse_wrapper import FridayPulseClient


def get_incremental_since_date(
    wherescape: WhereScape, hwm_param_name: str, lookback_weeks: int, data_type: str
) -> str | None:
    """
    Get the since_date for incremental loading based on high water mark parameter.

    Args:
        wherescape: WhereScape instance to read parameters from
        hwm_param_name: Name of the high water mark parameter
        lookback_weeks: Number of weeks to look back from the high water mark
        data_type: Description of the data type being loaded (for logging)

    Returns:
        since_date string in YYYY-MM-DD format, or None for full load
    """
    since_date = None
    try:
        since_date = wherescape.read_parameter(hwm_param_name)
        if since_date and since_date.strip():
            # Parse the datetime and subtract lookback period
            since_datetime = datetime.strptime(since_date.strip()[:10], "%Y-%m-%d")
            since_datetime = since_datetime - timedelta(weeks=lookback_weeks)
            since_date = since_datetime.strftime("%Y-%m-%d")
            logging.info(
                f"Incremental load: fetching {data_type} after {since_date} ({lookback_weeks} weeks before HWM)"
            )
        else:
            logging.info(f"Full load: {hwm_param_name} parameter not set")
    except Exception as e:
        logging.warning(f"Could not read {hwm_param_name} parameter, performing full load: {e}")

    return since_date


def friday_pulse_load_data(lookback_weeks: int = 3):
    """
    Main Friday Pulse load data function. Loads data from Friday Pulse and pushes it to
    the warehouse. This is the glue between the friday_pulse_wrapper and WhereScape.

    This function:
    1. Initializes WhereScape object
    2. Gets the table name from the load object
    3. Determines the subject (topics, groups, group_types, group_notes, general_results, group_results, etc.)
    4. Reads the high water mark parameter for incremental loading (if applicable)
    5. Fetches data from the Friday Pulse API with optional date filtering
    6. Prepares and inserts data into the target warehouse table
    7. Updates task log with row counts

    Args:
        lookback_weeks: Number of weeks to look back from the high water mark to capture
                       late responses. Default is 3 weeks. Only applies to endpoints
                       that support date filtering (general_results, group_results, group_notes,
                       general_notes, general_risks, group_risks).
    """
    start_time = datetime.now()

    # Initialise WhereScape (logging is initialised through WhereScape object)
    wherescape = WhereScape()
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for friday_pulse_load_data")

    # Get the table name to determine which endpoint to use
    table_name = wherescape.table
    if not table_name:
        logging.error("No table name found in WhereScape load object")
        wherescape.main_message = "Error: table name not set in load object"
        return

    logging.info(f"Loading data for table: {table_name}")

    # Get API token and full table name with schema
    bearer_token = os.getenv("WSL_SRCCFG_APIKEY")
    if not bearer_token:
        logging.error("Connection for load table not set properly")
        wherescape.main_message = "Error: Connection for load table not set properly"
        return

    table_name_with_schema = wherescape.load_full_name

    # Generate HWM parameter name: replace "load_" prefix with "ds_"
    # Example: load_friday_pulse_question -> HWM_ds_friday_pulse_question
    if table_name.startswith("load_"):
        hwm_table_name = table_name.replace("load_", "ds_", 1)
    else:
        hwm_table_name = table_name
    hwm_param_name = f"HWM_{hwm_table_name}"

    # Initialize Friday Pulse client
    client = FridayPulseClient(bearer_token)

    # Determine which endpoint to use and fetch data
    values = []
    source_description = ""

    if "topics" in table_name:
        logging.info("Loading topics data...")
        values = client.get_topics()
        source_description = "topics"

    elif "group_types" in table_name:
        logging.info("Loading group types data...")
        values = client.get_group_types()
        source_description = "group_types"

    elif "groups" in table_name:
        logging.info("Loading groups data...")
        values = client.get_groups()
        source_description = "groups"

    elif "group_notes" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "group notes")
        logging.info("Loading group notes data...")
        values = client.get_group_notes(since_date=since_date)
        source_description = "group_notes"

    elif "group_results" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "group results")
        logging.info("Loading group results data...")
        values = client.get_group_results(since_date=since_date)
        source_description = "group_results"

    elif "general_notes" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "general notes")
        logging.info("Loading general notes data...")
        values = client.get_general_notes(since_date=since_date)
        source_description = "general_notes"

    elif "group_risks" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "group risks")
        logging.info("Loading group risks data...")
        values = client.get_group_risks(since_date=since_date)
        source_description = "group_risks"

    elif "general_risks" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "general risks")
        logging.info("Loading general risks data...")
        values = client.get_general_risks(since_date=since_date)
        source_description = "general_risks"

    elif "general_results" in table_name:
        since_date = get_incremental_since_date(wherescape, hwm_param_name, lookback_weeks, "results")
        logging.info("Loading general results data...")
        values = client.get_general_results(since_date=since_date)
        source_description = "general_results"

    else:
        logging.error(
            f"Unknown table name pattern: '{table_name}'. "
            f"Table name must contain one of: topics, group_types, groups, group_notes, "
            f"group_results, general_notes, group_risks, general_risks, general_results, results"
        )
        wherescape.main_message = f"Error: Unknown subject in table name '{table_name}'"
        return

    # Check if we have data
    if values:
        # Get column names from the first record
        if len(values) > 0:
            original_columns = list(values[0].keys())
        else:
            logging.info("No data received from Friday Pulse")
            wherescape.main_message = "No data received from Friday Pulse"
            wherescape.update_task_log(inserted=0)
            return

        # Prepare columns names for query
        columns = create_column_names(original_columns)
        columns.append("dss_record_source")
        columns.append("dss_load_date")

        # Append dss column data to all rows, ensuring column order matches
        rows = []
        for record in values:
            # Extract values in the same order as original_columns
            row = [record.get(col) for col in original_columns]
            row.append(f"Friday Pulse API - {source_description}")
            row.append(start_time)
            rows.append(row)

        # Prepare the sql
        logging.info("Preparing insert query")
        column_names_string = ",".join(column for column in columns)
        question_mark_string = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name_with_schema} ({column_names_string}) VALUES ({question_mark_string})"

        # Execute the sql
        wherescape.push_many_to_target(sql, rows)

        # Set success message
        wherescape.main_message = (
            f"Loaded {len(rows)} Friday Pulse {source_description} records into {table_name_with_schema}"
        )
        wherescape.update_task_log(inserted=len(rows))
        logging.info(f"Successfully loaded {len(rows)} records")

    else:
        wherescape.main_message = f"No new {source_description} data received from Friday Pulse"
        wherescape.update_task_log(inserted=0)
        logging.info(f"No data received for {source_description}")

    # Final logging
    end_time = datetime.now()
    logging.info(f"Time elapsed: {(end_time - start_time).seconds} seconds for friday_pulse_load_data")
