"""Module with function to validate fact-dimension joins."""
import csv
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
    Does a count of all fact records

    Creates a file in output_file_location (WSL_WORKDIR by default)
    """
    wherescape = WhereScape()

    sql = """
    select dt_schema, ft_table_name, fc_col_name
      from dbo.ws_fact_col
        left join dbo.ws_fact_tab on fc_obj_key = ft_obj_key
        left join dbo.ws_obj_object on oo_obj_key = ft_obj_key
        left join dbo.ws_dbc_target on dt_target_key = oo_target_key
     where
       -- key_type is also shown in wherescape UI. Alternative would have been
       --   where fc_col_name like 'dim_%'
       fc_key_type = '2'
     order by 1,2
    """

    repository_results = wherescape.query_meta(sql)

    facts = set()
    list_of_attributes = []
    for result in repository_results:
        schema, table_name, column_name = result
        qualified_table_name = schema + "." + table_name
        # add tablename to set
        facts.add(qualified_table_name)
        # store attribute, tablename in list_of_attributes
        list_of_attributes.append((column_name, qualified_table_name))

    rows = []
    date = datetime.now().strftime("%Y-%m-%d")
    for column_name, qualified_table_name in list_of_attributes:
        row = {}
        sql = f"""
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
        filename = os.path.join(output_file_location, filename)

        with open(filename, "w", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(rows)


if __name__ == "__main__":
    # __main__ is executed when running the module standalone

    # set up the environment
    from wherescape_os.ws_env import setup_env

    setup_env("not_relevant", schema="star")

    # call the main function
    check_fact_dimension_join(output_file_location=r"C:\Temp")
