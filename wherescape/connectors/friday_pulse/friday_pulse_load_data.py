"""
Module for loading Friday Pulse data into WhereScape.

This module fetches results from the Friday Pulse API and loads them into
the WhereScape data warehouse.
"""

import logging
import os
from datetime import datetime, timedelta

from ...helper_functions import create_column_names
from ...wherescape import WhereScape
from .friday_pulse_create_metadata import EXPECTED_COLUMNS
from .friday_pulse_wrapper import get_all_results


def friday_pulse_load_data(lookback_weeks: int = 3):
    """
    Main Friday Pulse load data function. Loads data from Friday Pulse and pushes it to
    the warehouse. This is the glue between the friday_pulse_wrapper and WhereScape.

    Args:
        lookback_weeks: Number of weeks to look back from the high water mark to capture
                       late responses. Default is 3 weeks. This ensures that results
                       updated after the initial survey date are still captured.
    """
    start_time = datetime.now()

    # Initialise WhereScape (logging is initialised through WhereScape object)
    wherescape_instance = WhereScape()
    logging.info(
        f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for friday_pulse_load_data"
    )

    # Get the relevant values from WhereScape
    bearer_token = os.getenv("WSL_SRCCFG_APIKEY")
    table_name = wherescape_instance.load_full_name

    # Get the maximum sample_date already loaded for incremental loading
    # Go back N weeks from the high water mark to capture late responses
    since_date = None
    try:
        since_date = wherescape_instance.read_parameter("HWM_ds_friday_pulse_question")
        if since_date and since_date.strip():
            # Parse the datetime (format: YYYY-MM-DD HH:MM:SS.mmm) and subtract lookback period
            since_datetime = datetime.strptime(since_date.strip()[:10], "%Y-%m-%d")
            since_datetime = since_datetime - timedelta(weeks=lookback_weeks)
            since_date = since_datetime.strftime("%Y-%m-%d")
            logging.info(
                f"Incremental load: fetching results after {since_date} ({lookback_weeks} weeks before HWM)"
            )
        else:
            logging.info("Full load: HWM_ds_friday_pulse_question parameter not set")
    except Exception as e:
        logging.warning(
            f"Could not read HWM_ds_friday_pulse_question parameter, performing full load: {e}"
        )

    # Request data from Friday Pulse
    logging.info("Requesting data from Friday Pulse")
    values = get_all_results(bearer_token, since_date=since_date)

    if values:
        # Get column names from the first record
        if len(values) > 0:
            columns = list(values[0].keys())
        else:
            logging.info("No data received from Friday Pulse")
            return

        # Check if received columns match expected columns
        received_set = set(columns)
        expected_set = set(EXPECTED_COLUMNS)

        if received_set != expected_set:
            missing_columns = expected_set - received_set
            extra_columns = received_set - expected_set

            warning_msg = "Column mismatch detected in Friday Pulse data!"
            if missing_columns:
                warning_msg += f" Missing columns: {sorted(missing_columns)}."
            if extra_columns:
                warning_msg += f" Unexpected columns: {sorted(extra_columns)}."

            logging.warning(warning_msg)

        # Prepare columns names for query
        columns = create_column_names(columns)
        columns.append("dss_record_source")
        columns.append("dss_load_date")

        # Append dss column data to all rows
        rows = []
        for record in values:
            row = list(record.values())
            row.append("Friday Pulse API - questions")
            row.append(start_time)
            rows.append(row)

        # Prepare the sql
        logging.info("Preparing insert query")
        column_names_string = ",".join(column for column in columns)
        question_mark_string = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

        # Execute the sql
        wherescape_instance.push_many_to_target(sql, rows)

        # Set success message
        wherescape_instance.main_message = (
            f"Loaded {len(rows)} Friday Pulse results into {table_name}"
        )
        wherescape_instance.update_task_log(inserted=len(rows))

    else:
        wherescape_instance.main_message = "No new results received from Friday Pulse"
        wherescape_instance.update_task_log(inserted=0)

    # Final logging
    end_time = datetime.now()
    logging.info(
        f"Time elapsed: {(end_time - start_time).seconds} seconds for friday_pulse_load_data"
    )


if __name__ == "__main__":
    friday_pulse_load_data()
