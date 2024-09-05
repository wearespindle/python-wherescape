import logging
from datetime import datetime, UTC

from ...helper_functions import create_column_names, create_display_names, prepare_metadata_query
from ...wherescape import WhereScape
from .gsheets_wrapper import Gsheet, set_gsheet_variables
from .gsheets_parsing import parse_gspread_arguments


def gsheet_create_metadata():
    """
    Function that creates a load table in Wherescape based on the data
    in a provided Google sheet file.
    """
    start_time = datetime.now(tz=UTC)
    # Initialize Wherescape 
    logging.info("Connecting to WhereScape")
    wherescape_instance = WhereScape()
    logging.info(
        "Start time: %s for gsheet_load_data_os." % start_time.strftime("%Y-%m-%d %H:%M:%S")
    )
    gsheet: Gsheet = Gsheet()

    load_table_name = wherescape_instance.table
    url = wherescape_instance.query_meta(
        "select lt_file_path from ws_load_tab where lt_table_name = ?",
        [load_table_name],
    )[0][0]
    workbook_details = wherescape_instance.query_meta(
        "select lt_file_name from ws_load_tab where lt_table_name = ?",
        [load_table_name],
    )[0][0]
    logging.info(f"Metadata. URL: {url} ; Details : {workbook_details}")

    args = parse_gspread_arguments(workbook_details)
    if args.debug:
        logging.warning("Debug mode on -> do not use for production.")

    set_gsheet_variables(gsheet, url, args)

    header_row = gsheet.get_header()
    column_types = gsheet.get_column_types()
    title = gsheet.get_worksheet().title
    lt_obj_key = wherescape_instance.object_key

    display_names = create_display_names(header_row)
    column_names = create_column_names(header_row)
    source_columns, comments = set_source_columns_and_comments(header_row)

    sql = prepare_metadata_query(
        lt_obj_key = lt_obj_key,
        src_table_name = title,
        columns=column_names,
        display_names=display_names,
        types=column_types,
        comments=comments,
        source_columns=source_columns,
    )
    logging.info(f"Stored details for {len(header_row)} columns")

    wherescape_instance.push_to_meta(sql)
    logging.info("--> Metadata updated. Table can be created.")

    end_time = datetime.now(tz=UTC)
    logging.info("End time: %s" % end_time.strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("Time elapsed: %s seconds" % (end_time - start_time).seconds)


def set_source_columns_and_comments(header_row: list):
    """
    Fuction to determine source_column and comments for metadata.

    Params:
        header_row (list): header values.
    
    Returns:
        - list: source_column values.
        - list: comment values.
    """
    comments = []
    source_columns = []

    for value in header_row:
        src_column_name = value.rstrip()

        comments.append(src_column_name[0:1023].replace("'", "''"))
        source_columns.append(src_column_name)
    
    return source_columns, comments
