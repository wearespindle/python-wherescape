import logging
import os
from datetime import datetime

from ... import WhereScape
from ...helper_functions import create_column_names
from . import Gitlab
from .gitlab_data_types_column_names import COLUMN_NAMES_AND_DATA_TYPES


def gitlab_load_data_smart(is_legacy=False):
    """
    Function to load the data for Gitlab objects. Will look at the load table
    names to determine the object type. So in order for this funtion towork as
    intended, correct load table names need to be chosen. Can be easily
    extended with more object types.

    Args:
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
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
    elif "merge_request_commit" in table_name:
        load_type = "merge_request_commits"
    elif "merge_request" in table_name:
        load_type = "merge_requests"
    elif "branch_commit" in table_name:
        load_type = "branch_commits"
    elif "commit" in table_name:
        load_type = "commits"
    elif "branch" in table_name:
        load_type = "branches"
    else:
        raise Exception("Could not find the specific Gitlab object type")

    gitlab_load_data(wherescape_instance, load_type, is_legacy)


def gitlab_load_data(wherescape_instance, load_type, is_legacy=False):
    """
    Main Gitlab load data function. Loads data from Gitlab and pushes it to
    the warehouse. This is the glue between the gitlab_wrapper and WhereScape.

    Args:
        wherescape_instance (WhereScape): WhereScape instance for database operations
        load_type (str): Type of GitLab object to load (e.g., "projects", "issues",
            "pipelines", "merge_requests", "tags", "commits", "branches")
        is_legacy (bool): If True, uses create_legacy_column_names() which adds
            numbered suffixes to ALL columns (e.g., column_name_001). If False,
            uses create_column_names() which only adds numbers when needed for
            uniqueness. Set to True for existing tables that were created with
            the legacy naming convention. Defaults to False.
    """
    start_time = datetime.now()
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')} for gitlab_load_data")

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
    elif load_type == "merge_request_commits":
        values = gitlab_instance.get_merge_request_commits()
    elif load_type == "merge_requests":
        values = gitlab_instance.get_merge_requests()
    elif load_type == "branch_commits":
        values = gitlab_instance.get_commits_with_branch_name()
    elif load_type == "commits":
        values = gitlab_instance.get_commits()
    elif load_type == "branches":
        values = gitlab_instance.get_branches()
    else:
        raise Exception("Wrong gitlab load type supplied")

    if len(values) > 0:
        if not is_legacy:
            columns = create_column_names(columns)
        else:
            from ...helper_functions import create_legacy_column_names

            columns = create_legacy_column_names(columns)
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
        wherescape_instance.main_message = f"No modified values found for {load_type.capitalize()}"

    # Final logging
    end_time = datetime.now()
    logging.info(f"Time elapsed: {(end_time - start_time).seconds} seconds for gitlab_load_data")
