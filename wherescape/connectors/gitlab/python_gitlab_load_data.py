from datetime import datetime
import logging

from . import Gitlab
from ... import WhereScape
from ...helper_functions import create_column_names
from ...connectors.gitlab.gitlab_data_types_column_names import (
    COLUMN_NAMES_AND_DATA_TYPES,
)


def gitlab_load_data_smart():
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
    start_time = datetime.now()
    logging.info(
        "Start time: %s for gitlab_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    access_token = wherescape_instance.read_parameter("gitlab_access_token")
    base_url = wherescape_instance.read_parameter("gitlab_base_url")
    table_name = wherescape_instance.load_full_name

    gitlab_instance = Gitlab(access_token, base_url)

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

    columns = create_column_names(columns)
    columns.append("dss_record_source")
    columns.append("dss_load_date")

    rows = []
    for row in values:
        # row = list(row.tolist())
        row.append("gitlab api - " + load_type)
        row.append(start_time)
        rows.append(row)
        # break

    # Prepare the sql
    column_names_string = ",".join(column for column in columns)
    question_mark_string = ",".join("?" for _ in columns)
    sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"
    logging.info(sql)
    logging.info(rows)

    # Execute the sql
    wherescape_instance.push_many_to_target(sql, rows)
    logging.info(f"Data successfully inserted in to the load table.")

    # Final logging
    end_time = datetime.now()
    logging.info(
        "Time elapsed: %s seconds for notion_load_data"
        % (end_time - start_time).seconds
    )
