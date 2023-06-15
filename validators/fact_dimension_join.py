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

    Creates a file in output_file_location (WSL_WORKDIR by default)
    """
    wherescape = WhereScape()
    
    # start logging
    start_time = datetime.now()
    logging.info(
        "Start time: %s for check_fact_dimension_join" % start_time.strftime(
            "%Y-%m-%d %H:%M:%S")
    )

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
        , ws_fact_tab.ft_table_name;
    """

    repository_results = wherescape.query_meta(sql)

    list_of_attributes = []
    for result in repository_results:
        schema, table_name, column_name = result
        qualified_table_name = schema + "." + table_name
        # store attribute, tablename in list_of_attributes
        list_of_attributes.append((column_name, qualified_table_name))

    logging.info(f"Checking {len(list_of_attributes)} dimension keys")
    rows = []
    date = datetime.now().strftime("%Y-%m-%d")
    for column_name, qualified_table_name in list_of_attributes:
        row = {}
        sql = f"""
        -- count fact table rows and fact table rows with 0-dimension key
        select
           count(*)                                  as count_of_all_records
        ,  count(*) filter (where {column_name} = 0) as count_of_0_key_records
        from {qualified_table_name}
        """
        result = wherescape.query_target(sql)

        row["date"] = date
        row["table"] = qualified_table_name
        row["attribute"] = column_name
        row["count_of_all_records"] = result[0][0]
        row["count_of_0_key_records"] = result[0][1]
        rows.append(row)

    keys = [
        "date",
        "table",
        "attribute",
        "count_of_all_records",
        "count_of_0_key_records",
    ]
    if rows:
        # write the results to the file
        if output_file_location == "":
            output_file_location = wherescape.workdir
        filename = f"{wherescape.job_key}_fact_dimension_check_result_{datetime.now().strftime('%y%m%d')}.csv"
        logging.info(
            f"Writing output file {filename} to {output_file_location}")
        filename = os.path.join(output_file_location, filename)

        with open(filename, "w", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(rows)


if __name__ == "__main__":
    # __main__ is executed when running the module standalone

    # set up the environment
    #   NB. ws_env.py can be created based on ../wherescape/ws_env_template.py
    #       and needs to live in the same directory as this file
    from ws_env import setup_env

    setup_env("not_relevant", schema="star")

    # call the main function
    check_fact_dimension_join(output_file_location=r"C:\Temp")
