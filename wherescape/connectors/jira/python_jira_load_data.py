from datetime import datetime
import logging
import os

from .jira_wrapper import Jira
from ... import WhereScape
from ...helper_functions import create_column_names


def jira_load_data_project():
    """
    Function to be called from the host script in WhereScape. Will import
    project data to the load table.
    """
    jira_load_data("project")


def jira_load_data_issue():
    """
    Function to be called from the host script in WhereScape. Will import
    all issue data to the load table.
    """
    jira_load_data("issue")


def jira_load_data_issue_incremental():
    """
    Function to be called from the host script in WhereScape. Will import
    issue data to the load table that has been added or modified in the last
    period. Defaults to 48 hours. Ideally for running every 24 hours.
    """
    jira_load_data("issue", use_high_water_mark=True)


def jira_load_data(load_type, use_high_water_mark=False, since=None):
    """
    Main jira load data function. Loads data from Jira and pushes it to
    the warehouse. This is the glue between the jira_wrapper and WhereScape.
    """
    start_time = datetime.now()
    # First initialise WhereScape to setup logging
    logging.info("Connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for jira_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Get the high_water_mark if applicable
    if use_high_water_mark:
        since = wherescape_instance.read_parameter("jira_high_water_mark")
        if since == "":
            logging.info("High water mark is empty so fetching all issues")
            since = None
        else:
            logging.info("Using high water mark: %s" % since)

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
        columns = create_column_names(columns)
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
        logging.info(
            "New high water mark is: %s" % start_time.strftime("%Y-%m-%d %H:%M")
        )

        # Add success message
        wherescape_instance.main_message = (
            f"Successfully inserted {len(rows)} rows in to the load table."
        )

    else:
        logging.info("No object changes received from JIRA")

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for jira_load_data" % (end_time - start_time).seconds
    )
