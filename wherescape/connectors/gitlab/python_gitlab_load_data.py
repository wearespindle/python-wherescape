from datetime import datetime
import logging

from . import Gitlab
from .gitlab_data_types_column_names import COLUMN_NAMES_AND_DATA_TYPES
from ... import WhereScape
from ...helper_functions import create_column_names


def gitlab_load_data_smart():
    """
    Function to load the data for Gitlab objects. Will look at the load table
    names to determine the object type. So in order for this funtion towork as
    intended, correct load table names need to be chosen. Can be easily
    extended with more object types.
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
    else:
        raise Exception("Could not find the specific Gitlab object type")

    gitlab_load_data(wherescape_instance, title)


def gitlab_load_data(wherescape_instance, load_type):
    """
    Main Gitlab load data function. Loads data from Gitlab and pushes it to
    the warehouse. This is the glue between the gitlab_wrapper and WhereScape.
    """
    start_time = datetime.now()
    logging.info(
        "Start time: %s for gitlab_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    access_token = wherescape_instance.read_parameter("gitlab_access_token")
    base_url = wherescape_instance.read_parameter("gitlab_base_url")
    table_name = wherescape_instance.load_full_name

    since = wherescape_instance.read_parameter("gitlab_high_water_mark")
    gitlab_instance = Gitlab(access_token, base_url, since)

    if load_type == "projects":
        columns = COLUMN_NAMES_AND_DATA_TYPES["projects"]
        values = gitlab_instance.get_projects()
    elif load_type == "pipelines":
        columns = COLUMN_NAMES_AND_DATA_TYPES["pipelines"]
        values = gitlab_instance.get_pipelines()
    elif load_type == "issues":
        columns = COLUMN_NAMES_AND_DATA_TYPES["issues"]
        values = gitlab_instance.get_issues()
    elif load_type == "tags":
        columns = COLUMN_NAMES_AND_DATA_TYPES["tags"]
        values = gitlab_instance.get_release_tags()
    elif load_type == "merge_requests":
        columns = COLUMN_NAMES_AND_DATA_TYPES["merge_requests"]
        values = gitlab_instance.get_merge_requests()
    else:
        raise Exception("Wrong gitlab load type supplied")

    if len(values) > 0:
        columns = create_column_names(columns)
        columns.append("dss_record_source")
        columns.append("dss_load_date")

        rows = []
        for row in values:
            row.append("gitlab api - " + load_type)
            row.append(start_time)

            rows.append(row)

        # Prepare the sql
        column_names_string = ",".join(column for column in columns)
        question_mark_string = ",".join("?" for _ in columns)
        sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

        # Execute the sql
        wherescape_instance.push_many_to_target(sql, rows)
        logging.info(f"{len(rows)} rows successfully inserted in {table_name}")
        wherescape_instance.main_message = (
            f"{load_type.capitalize()} successfully loaded {len(rows)} rows"
        )
    else:
        logging.info(f"No modified values found for {load_type.capitalize()}")
        wherescape_instance.main_message = (
            f"No modified values found for {load_type.capitalize()}"
        )

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for gitlab_load_data"
        % (end_time - start_time).seconds
    )
