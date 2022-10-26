from datetime import datetime
import logging

from .gitlab_data_types_column_names import COLUMN_NAMES_AND_DATA_TYPES
from ... import WhereScape
from ...helper_functions import (
    prepare_metadata_query,
    create_column_names,
    create_display_names,
)


def gitlab_create_metadata_smart():
    """
    Function to create the metadata for Gitlab objects. Will look at the load
    table names to determine the object type. So in order for this funtion to
    work as intended, correct load table names need to be chosen. Can be
    easily extended with more object types.
    """
    wherescape_instance = WhereScape()

    table_name = wherescape_instance.table
    if "project" in table_name:
        title = "projects"
    elif "tag" in table_name:
        title = "tags"
    elif "issue" in table_name:
        title = "issues"
    elif "pipeline" in table_name:
        title = "pipelines"
    elif "merge_request" in table_name:
        title = "merge_requests"
    elif "commit" in table_name:
        title = "commits"
    elif "branch" in table_name:
        title = "branches"
    else:
        raise Exception("Could not find the specific Gitlab object type")
    columns = COLUMN_NAMES_AND_DATA_TYPES[title]

    gitlab_create_metadata(
        wherescape_instance,
        list(columns.keys()),
        list(columns.values()),
        title,
    )


def gitlab_create_metadata(wherescape_instance, columns, python_types, title):
    """
    Main Gitlab metadata function. Creates the metadata queries and pushes
    them to the WhereScape repository. This is the glue between the
    gitlab_wrapper and WhereScape.
    """
    start_time = datetime.now()
    logging.info(
        "Start time: %s for Gitlab_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
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

    # Get the Gitlab database (definition)
    logging.info(f'Fetched database definition "{title}" from Gitlab')

    # Prepare the sql
    column_names = create_column_names(columns)
    display_names = create_display_names(columns)
    sql = prepare_metadata_query(
        wherescape_object_id,
        "Gitlab api - " + title,
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
        "Time elapsed: %s seconds for gitlab_load_data"
        % (end_time - start_time).seconds
    )
