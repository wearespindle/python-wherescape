from datetime import datetime
import logging
import os

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
        load_type = "projects"
    elif "tag" in table_name:
        load_type = "tags"
    elif "issue" in table_name:
        load_type = "issues"
    elif "pipeline" in table_name:
        load_type = "pipelines"
    elif "merge_request" in table_name:
        load_type = "merge_requests"
    elif "commit" in table_name:
        load_type = "commits"
    elif "branch_commit" in table_name:
        load_type = "branch_commits"
    elif "branch" in table_name:
        load_type = "branches"
    else:
        raise Exception("Could not find the specific Gitlab object type")

    gitlab_load_data(wherescape_instance, load_type)


def gitlab_load_data(wherescape_instance, load_type):
    """
    Main Gitlab load data function. Loads data from Gitlab and pushes it to
    the warehouse. This is the glue between the gitlab_wrapper and WhereScape.
    """
    start_time = datetime.now()
    logging.info(
        "Start time: %s for gitlab_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    base_url = os.getenv("WSL_SRCCFG_URL")
    access_token = os.getenv("WSL_SRCCFG_APIKEY")
    table_name = wherescape_instance.load_full_name

    since = wherescape_instance.read_parameter("gitlab_high_water_mark")
    gitlab_instance = Gitlab(access_token, base_url, since)

    columns = COLUMN_NAMES_AND_DATA_TYPES[load_type]
    if load_type == "projects":
        values = gitlab_instance.get_projects()
    elif load_type == "pipelines":
        values = gitlab_instance.get_pipelines()
    elif load_type == "issues":
        values = gitlab_instance.get_issues()
    elif load_type == "tags":
        values = gitlab_instance.get_release_tags()
    elif load_type == "merge_requests":
        values = gitlab_instance.get_merge_requests()
    elif load_type == "commits":
        values = gitlab_instance.get_commits()
    elif "branch_commit" in table_name:
        values = gitlab_instance.get_commits_with_branch_name()
    elif load_type == "branches":
        values = gitlab_instance.get_branches()
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
