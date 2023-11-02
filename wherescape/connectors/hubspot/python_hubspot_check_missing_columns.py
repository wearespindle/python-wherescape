"""
Script that compares the columns from the Hubspot api with the columns in the
WhereScape load tables. It will load all missing columns in a load table and
compares it to the ds tables. If new ones have appeared, it add them to the
WhereScape warning log messages so they turn up in the daily report in Slack.
"""
import logging
from hubspot.crm.properties.exceptions import ForbiddenException

from wherescape import WhereScape
from wherescape.connectors.hubspot import Hubspot


def compare_columns(wherescape_columns, hubspot_columns):
    # Function checking columns missing in either WhereScape or Hubspot
    missing_in_wherescape = []
    for hubspot_column in hubspot_columns:
        if hubspot_column not in wherescape_columns:
            missing_in_wherescape.append(hubspot_column)

    missing_in_hubspot = []
    for wherescape_column in wherescape_columns:
        if wherescape_column not in hubspot_columns and wherescape_column[0:3] != "dss":
            missing_in_hubspot.append(wherescape_column)

    return missing_in_wherescape, missing_in_hubspot


def create_table_rows(
    missing_in_wherescape, missing_in_hubspot, environment, table_name
):
    # Creating the table row list for the insert query
    table_rows = []
    if len(missing_in_wherescape) > 0:
        for row in missing_in_wherescape:
            table_rows.append((environment, row, table_name, "wherescape"))
    if len(missing_in_hubspot) > 0:
        for row in missing_in_hubspot:
            table_rows.append((environment, row, table_name, "hubspot"))

    return table_rows


def get_ds_rows(wherescape_instance):
    # Get the datastore rows for comparison with the new load rows
    table_name = f"datastore.ds{wherescape_instance.table[4:]}"
    sql = f"select hubspot_environment, column_name, hubspot_load_table_name, missing_where from {table_name}"  # noqa: E501
    rows = wherescape_instance.query_target(sql)
    return rows


def hubspot_check_missing_columns(wherescape_instance, hubspot_instance, table_name, object_type, environment):

    # Query the hubspot api per environment and object_type and compare with
    # the wherescape objects.
    sql = "SELECT lt_obj_key FROM ws_load_tab WHERE lt_table_name = ?"
    results = wherescape_instance.query_meta(sql, [table_name])
    table_rows = []

    if len(results) > 0:
        lc_obj_key = results[0][0]
        sql = "SELECT lc_col_name FROM ws_load_col WHERE lc_obj_key = ?"
        results = wherescape_instance.query_meta(sql, [lc_obj_key])
        wherescape_columns = [item[0] for item in results]
        api_response = None
        try:
            api_response = (
                hubspot_instance.client.crm.properties.core_api.get_all(
                    object_type=object_type, archived=False
                )
            )
        except ForbiddenException:
            logging.info(
                f"No access for type {object_type} via the Hubspot api for {environment}"  # noqa: E501
            )
        else:
            api_results = api_response.to_dict()
            hubspot_columns = []
            for result in api_results["results"]:
                hubspot_columns.append(result["name"])
            missing_in_wherescape, missing_in_hubspot = compare_columns(
                wherescape_columns, hubspot_columns
            )
            table_rows = create_table_rows(
                missing_in_wherescape,
                missing_in_hubspot,
                environment,
                table_name,
            )
    else:
        logging.info(
            f"No load table exists for Hubspot object {object_type} and environment {environment} ({table_name})"  # noqa: E501
        )

    return table_rows
    
def compare_load_and_ds(wherescape_instance, table_rows):
    # Get the datastore rows to compare to the new load rows
    ds_rows = get_ds_rows(wherescape_instance)
    # For comparison we need tuples instead of lists
    ds_rows = [tuple(row) for row in ds_rows]
    new_rows_in_load_table = [row for row in table_rows if row not in ds_rows]
    if len(new_rows_in_load_table) > 0:
        for row in new_rows_in_load_table:
            logging.warning(
                f"Hubspot missing column: the {row[0]} environment in {row[3]}: table {row[2]} is missing column {row[1]}"  # noqa: E501
            )
    else:
        logging.info("No new missing rows in Hubspot objects found.")

    # Pushing the missing columns to the load table
    wherescape_instance.push_many_to_target(
        "insert into %s values (?, ?, ?, ?, null, null)"
        % wherescape_instance.load_full_name,
        new_rows_in_load_table,
    )
