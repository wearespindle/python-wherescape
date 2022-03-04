from datetime import datetime
import logging

from .jira_wrapper import Jira

from wherescape import WhereScape
from wherescape.helper_functions import (
    prepare_metadata_query,
    create_column_names,
    create_display_names,
)


def jira_create_metadata_project():
    wherescape_instance = WhereScape()
    user = wherescape_instance.read_parameter("jira_user")
    apikey = wherescape_instance.read_parameter("jira_key")
    base_url = wherescape_instance.top_level_name
    jira_instance = Jira(user, apikey, base_url)

    columns = jira_instance.project_column_names_and_types()

    jira_create_metadata(
        wherescape_instance, list(columns.keys()), list(columns.values()), "projects"
    )


def jira_create_metadata_issue():
    wherescape_instance = WhereScape()
    user = wherescape_instance.read_parameter("jira_user")
    apikey = wherescape_instance.read_parameter("jira_key")
    base_url = wherescape_instance.top_level_name
    jira_instance = Jira(user, apikey, base_url)

    columns = jira_instance.issue_column_names_and_types()

    jira_create_metadata(
        wherescape_instance, list(columns.keys()), list(columns.values()), "issues"
    )


def jira_create_metadata(wherescape_instance, columns, python_types, title):
    start_time = datetime.now()
    logging.info(
        "Start time: %s for jira_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    wherescape_object_id = wherescape_instance.object_key
    types = []
    for python_type in python_types:
        if python_type == "int":
            types.append("numeric")
        elif python_type == "object":
            types.append("text")
        elif python_type == "datetime64[ns]":
            types.append("timestamp")
        elif python_type == "boolean":
            types.append("boolean")

    # Get the Jira database (definition)
    logging.info(f'Fetched database definition "{title}" from JIRA')

    # Prepare the sql
    column_names = create_column_names(columns)
    display_names = create_display_names(columns)
    sql = prepare_metadata_query(
        wherescape_object_id,
        "Jira api - " + title,
        columns=column_names,
        display_names=display_names,
        types=types,
        source_columns=columns,
    )

    # Execute the sql
    wherescape_instance.push_to_meta(sql)
    logging.info(f"Metadata table for {title} created.")
    wherescape_instance.main_message = f"Created metadata table for {title} created."

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for jira_load_data" % (end_time - start_time).seconds
    )
