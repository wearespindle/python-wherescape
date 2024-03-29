from datetime import datetime
import logging
import os

from .jira_wrapper import Jira
from ...wherescape import WhereScape
from ...helper_functions import (
    prepare_metadata_query,
    create_column_names,
    create_display_names,
)


def jira_create_metadata_project():
    """
    Function to be called from the host script in WhereScape. Will create the
    metadata for the project load table.
    """
    jira_create_metadata("projects")


def jira_create_metadata_issue():
    """
    Function to be called from the host script in WhereScape. Will create the
    metadata for the issue load table.
    """
    jira_create_metadata("issues")


def jira_create_metadata(title):
    """
    Main jira metadata function. Creates the metadata queries and pushes them
    to the WhereScape repository. This is the glue between the jira_wrapper
    and WhereScape.
    """
    start_time = datetime.now()
    # First initialise WhereScape to setup logging
    logging.info("Connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for jira_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    # Get the relevant values from WhereScape
    base_url = os.getenv("WSL_SRCCFG_URL")
    user = os.getenv("WSL_SRCCFG_USER")
    apikey = os.getenv("WSL_SRCCFG_APIKEY")
    wherescape_object_id = wherescape_instance.object_key

    # Initialise the Jira connector and get the columns.
    logging.info("Getting the column data from Jira.")
    jira_instance = Jira(user, apikey, base_url)
    if title == "projects":
        columns = jira_instance.project_column_names_and_types()
    elif title == "issues":
        columns = jira_instance.issue_column_names_and_types()
    python_types = list(columns.values())
    columns = list(columns.keys())

    # Translate python types to Postgres types
    postgres_types = []
    for python_type in python_types:
        if python_type == "int":
            postgres_types.append("numeric")
        elif python_type == "object":
            postgres_types.append("text")
        elif python_type == "datetime64[ns]":
            postgres_types.append("timestamp")
        elif python_type == "boolean":
            postgres_types.append("boolean")

    # Prepare the sql
    logging.info("Preparing metadata query")
    column_names = create_column_names(columns)
    display_names = create_display_names(columns)
    sql = prepare_metadata_query(
        wherescape_object_id,
        "Jira api - " + title,
        columns=column_names,
        display_names=display_names,
        types=postgres_types,
        source_columns=columns,
    )

    # Execute the sql
    wherescape_instance.push_to_meta(sql)
    logging.info(f"Metadata table for {title} created.")
    wherescape_instance.main_message = f"Metadata table for JIRA {title} created."

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for jira_load_data" % (end_time - start_time).seconds
    )
