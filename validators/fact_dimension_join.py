"""Module with function to validate fact-dimension joins."""
import csv
import logging
import os
from datetime import datetime

from wherescape_os.wherescape import WhereScape


def check_fact_dimension_join(output_file_location=""):
    """
    Check fact-dimension joins.

    Retrieves from the repository:
    - all fact table names from the repository and,
    - all columns with foreign keys to dimensions

    Does a count of all references to 0-dimension keys.
    And a count of all records in the fact table.

    Creates a file in output_file_location (WSL_WORKDIR by default)
    """
    start_time = datetime.now()
    logging.info(
        "Start time: %s for check_fact_dimension_join"
        % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    wherescape = WhereScape()

    date = datetime.now().strftime("%Y-%m-%d")

    # create empty list to store results
    result_rows = []

    # get all fact tables and all dimension keys
    sql = """
    -- Get table names and column names from WhereScape repository
    --     that contain dimension keys from all fact tables
    select
        ws_dbc_target.dt_schema
        , ws_fact_tab.ft_table_name
        , ws_fact_col.fc_col_name
    from
        dbo.ws_fact_col
        left join dbo.ws_fact_tab on fc_obj_key = ft_obj_key
        left join dbo.ws_obj_object on oo_obj_key = ft_obj_key
        left join dbo.ws_dbc_target on dt_target_key = oo_target_key
    where
        -- key_type is also shown in wherescape UI. 
        -- key_type 2 corresponds to dimension keys
        ws_fact_col.fc_key_type = '2'
    order by
        ws_dbc_target.dt_schema
        , ws_fact_tab.ft_table_name
    UNION
    select
        ws_dbc_target.dt_schema,
        ws_fact_tab.ft_table_name,
        null
    from
        dbo.ws_fact_tab
        left join dbo.ws_obj_object on oo_obj_key = ft_obj_key
        left join dbo.ws_dbc_target on dt_target_key = oo_target_key
    where
        not exists (
            select 1 
              from dbo.ws_fact_col
             where
                fc_obj_key = ft_obj_key
                and fc_key_type = '2'
        );
    """

    facts_with_dimensions = wherescape.query_meta(sql)

    # Loop through all fact tables and columns with dimension keys
    #   and count the (0-dimension key) records
    for fact_dimension in facts_with_dimensions:
        # create empty dict to store results
        result_row = {}
        # unpack tuple
        schema, table_name, column_name = fact_dimension
        # create qualified table name
        qualified_table_name = schema + "." + table_name

        result_row["date"] = date
        result_row["table"] = qualified_table_name
        result_row["attribute"] = column_name

        # First count all records in the fact table (unless already known)
        # check if table exists in result_rows
        #   if table exists, find the count_of_all_records and update the result_row
        if any(row["table"] == qualified_table_name for row in result_rows):
            # table exists in result_rows -->  count_of_all_records already known
            # --> from the result_rows, find the first row with the table name
            #   and put the count_of_all_records in the result_row
            for row in result_rows:
                if row["table"] == qualified_table_name:
                    result_row["count_of_all_records"] = row["count_of_all_records"]
                    break
        else:
            # table does not exists in result_rows
            #   --> count all records in the target table
            sql = f"""
            -- count fact table rows and fact table rows with 0-dimension key
            select
                count(*)                              as count_of_all_records
            from {qualified_table_name}
            """
            count_of_all_records = wherescape.query_target(sql)
            result_row["count_of_all_records"] = count_of_all_records[0][0]

        # secondly count the (0-dimension key) records
        #   if column_name is None, this is a fact table without dimension keys
        #   --> then we're ready
        if column_name is None:
            # this is a fact table without dimension keys
            #   --> skip
            result_row["count_of_0_key_records"] = None
            continue
        else:
            # this is a fact table with dimension keys
            # create sql statement to count (0-dimension key) records
            sql = f"""
            -- count fact table rows and fact table rows with 0-dimension key
            select
                count(*) filter (where {column_name} = 0) as count_of_0_key_records
            from {qualified_table_name}
            """
            count_of_0_key_records = wherescape.query_target(sql)
            result_row["count_of_0_key_records"] = count_of_0_key_records[0][1]

        # add the result_row to the result_rows
        result_rows.append(result_row)

    # write the results to the file
    keys = [
        "date",
        "table",
        "attribute",
        "count_of_all_records",
        "count_of_0_key_records",
    ]
    if result_rows:
        # create filename
        if output_file_location == "":
            output_file_location = wherescape.workdir
        filename = f"{wherescape.job_key}_fact_dimension_check_result_{datetime.now().strftime('%y%m%d')}.csv"
        logging.info(f"Writing output file {filename} to {output_file_location}")
        filename = os.path.join(output_file_location, filename)

        # write the results to the file
        with open(filename, "w", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(result_rows)


if __name__ == "__main__":
    # __main__ is executed when running the module standalone

    # set up the environment
    #   NB. ws_env.py can be created based on ../wherescape/ws_env_template.py
    #       and needs to live in the same directory as this file
    from ws_env import setup_env

    setup_env("not_relevant", schema="star")

    # call the main function
    check_fact_dimension_join(output_file_location=r"C:\Temp")
