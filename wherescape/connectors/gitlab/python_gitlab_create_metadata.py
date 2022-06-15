from datetime import datetime
import logging

from wherescape.connectors.gitlab.gitlab_data_types_column_names import (
    COLUMN_NAMES_AND_DATA_TYPES,
)
from wherescape import WhereScape
from wherescape.helper_functions import (
    prepare_metadata_query,
    create_column_names,
    create_display_names,
)


def gitlab_create_all_metadata():
    wherescape_instance = WhereScape()

    for resource_type in COLUMN_NAMES_AND_DATA_TYPES:
        for key, value in resource_type:
            gitlab_create_metadata(
                wherescape_instance,
                list(value.keys()),
                list(value.values()),
                key,
            )


def gitlab_create_metadata(wherescape_instance, columns, python_types, title):
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
        "Time elapsed: %s seconds for Gitlab_load_data"
        % (end_time - start_time).seconds
    )
