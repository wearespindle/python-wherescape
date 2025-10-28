import logging
import os
from datetime import datetime

from ... import WhereScape
from ...helper_functions import create_column_names
from .jira_wrapper import Jira


def jira_load_data_project(is_legacy=False):
    """
    Function to be called from the host script in WhereScape. Will import
    project data to the load table.

    Args:
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
    """
    jira_load_data("project", is_legacy=is_legacy)


def jira_load_data_issue(is_legacy=False):
    """
    Function to be called from the host script in WhereScape. Will import
    all issue data to the load table.

    Args:
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
    """
    jira_load_data("issue", is_legacy=is_legacy)


def jira_load_data_issue_incremental(is_legacy=False):
    """
    Function to be called from the host script in WhereScape. Will import
    issue data to the load table that has been added or modified in the last
    period. Defaults to 48 hours. Ideally for running every 24 hours.

    Args:
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
    """
    jira_load_data("issue", use_high_water_mark=True, is_legacy=is_legacy)


def jira_load_data(load_type, use_high_water_mark=False, since=None, is_legacy=False):
    """
    Main jira load data function. Loads data from Jira and pushes it to
    the warehouse. This is the glue between the jira_wrapper and WhereScape.

    Args:
        load_type (str): Type of data to load - either "project" or "issue"
        use_high_water_mark (bool): If True, loads only data modified since the
            stored high water mark. Defaults to False (full load).
        since (str): Optional datetime string to load data modified since this date.
            Overridden by high water mark if use_high_water_mark is True.
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
    """
    start_time = datetime.now()
    # First initialise WhereScape to setup logging
    logging.info("Connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for jira_load_data")

    # Get the high_water_mark if applicable
    if use_high_water_mark:
        since = wherescape_instance.read_parameter("jira_high_water_mark")
        if since == "":
            logging.info("High water mark is empty so fetching all issues")
            since = None
        else:
            logging.info(f"Using high water mark: {since}")

    # Get the relevant values from WhereScape
    base_url = os.getenv("WSL_SRCCFG_URL")
    user = os.getenv("WSL_SRCCFG_USER")
    apikey = os.getenv("WSL_SRCCFG_APIKEY")
    table_name = wherescape_instance.load_full_name

    # Request data from Jira.
    logging.info("Requesting data from Jira")
    jira_instance = Jira(user, apikey, base_url)
    if load_type == "project":
        columns = jira_instance.project_column_names_and_types()
        values = jira_instance.get_all_projects()
    elif load_type == "issue":
        columns = jira_instance.issue_column_names_and_types()
        values = jira_instance.get_all_issues(since)
    else:
        raise Exception("Wrong jira load type supplied")

    if values:
        # Prepare columns names for query.
        if not is_legacy:
            columns = create_column_names(columns)
        else:
            from ...helper_functions import create_legacy_column_names

            columns = create_legacy_column_names(columns)
        columns.append("dss_record_source")
        columns.append("dss_load_date")

        # Append dss column data to all rows.
        rows = []
        for row in values:
            row.append("Jira api - " + load_type)
            row.append(start_time)
            rows.append(row)

        # Prepare the sql
        logging.info("Preparing insert query")
        column_names_string = ",".join(column for column in columns)
        question_mark_string = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

        # Execute the sql
        wherescape_instance.push_many_to_target(sql, rows)
        logging.info(f"Successfully inserted {len(rows)} rows in to the load table.")

        # Update the high_water_mark. Will also be updated if use_high_water_mark=False
        wherescape_instance.write_parameter(
            "jira_high_water_mark", start_time.strftime("%Y-%m-%d %H:%M")
        )
        logging.info(f"New high water mark is: {start_time.strftime('%Y-%m-%d %H:%M')}")

        # Add success message
        wherescape_instance.main_message = (
            f"Successfully inserted {len(rows)} rows in to the load table."
        )

    else:
        logging.info("No object changes received from JIRA")

    # Final logging
    end_time = datetime.now()
    logging.info(f"Time elapsed: {(end_time - start_time).seconds} seconds for jira_load_data")
