from datetime import datetime
import logging

from .jira_wrapper import Jira

from wherescape import WhereScape
from wherescape.helper_functions import create_column_names


def jira_load_data_project():
    jira_load_data("project")


def jira_load_data_issue():
    jira_load_data("issue")


def jira_load_data(load_type):
    wherescape_instance = WhereScape()
    start_time = datetime.now()
    logging.info(
        "Start time: %s for jira_load_data" % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )

    user = wherescape_instance.read_parameter("jira_user")
    apikey = wherescape_instance.read_parameter("jira_apikey")
    base_url = wherescape_instance.base_uri
    table_name = wherescape_instance.load_full_name

    jira_instance = Jira(user, apikey, base_url)
    if load_type == "project":
        columns = jira_instance.project_column_names_and_types()
        values = jira_instance.get_all_projects()
    elif load_type == "issue":
        columns = jira_instance.issue_column_names_and_types()
        values = jira_instance.get_all_issues()
    else:
        raise Exception("Wrong jira load type supplied")

    columns = create_column_names(columns)
    columns.append("dss_record_source")
    columns.append("dss_load_date")

    rows = []
    for row in values:
        # row = list(row.tolist())
        row.append("Jira api - " + load_type)
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
