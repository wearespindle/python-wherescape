import logging
import re

from datetime import datetime, UTC
from itertools import zip_longest

from .gsheets_wrapper import Gsheet, set_gsheet_variables
from .gsheets_parsing import parse_gspread_arguments
from ...helper_functions import set_date_to_ymd
from ...wherescape import WhereScape


def google_sheet_load_data():
    """
    Loads content of a google sheet file into a table from a google sheet.
    Relevant metadata must already be created.
    """
    start_time = datetime.now(tz=UTC)
    logging.info("Start time: %s" % start_time.strftime("%Y-%m-%d %H:%M:%S"))
    
    wherescape = WhereScape()
    gsheet = Gsheet()
    table_name = wherescape.table
    column_names, column_types = wherescape.get_columns()

    url = wherescape.query_meta(
            "select lt_file_path from ws_load_tab where lt_table_name = ?",
            [table_name],
        )[0][0]
    workbook_details = wherescape.query_meta(
            "select lt_file_name from ws_load_tab where lt_table_name = ?",
            [table_name],
        )[0][0]

    logging.info(f"Metadata. URL: {url} ; Details : {workbook_details}")

    args = parse_gspread_arguments(workbook_details)
    set_gsheet_variables(gsheet, url, args)
    content = gsheet.get_content(args.range)
    # For name consistency.
    gsheet_header = gsheet.get_header()

    # Missing from wherescape (added after metadata upload)
    added_columns, added_indexes = get_missing_columns(column_names, gsheet_header)
    content, gsheet_header = remove_extra_columns(content, gsheet_header, added_indexes)
    if len(added_columns) > 0:
        logging.warn(f"New columns in gsheet data: {added_columns}")

    # Missing from upload (removed after metadata upload)
    removed_columns, removed_indexes = get_missing_columns(gsheet_header, column_names)
    content, gsheet_header = add_empty_columns(content, gsheet_header, removed_indexes, column_names)
    if len(removed_columns) > 0:
        logging.warn(f"Colums missing from gsheet data: {removed_columns}")

    dss_title = gsheet.get_spreadsheet().title.replace(" ", "_")
    for row in content:
        # Add content for dss columns
        row.append(f"{dss_title}")
        row.append(f"{start_time.strftime("%Y-%m-%d %H:%M:%S.%f")}")

    for i in range(len(gsheet_header)):
        if column_types[i] == "timestamp":
            transp_content = [list(i) for i in zip_longest(*content, fillvalue=None)]
            for j in range(len(transp_content[i])):
                transp_content[i][j] = set_date_to_ymd(transp_content[i][j])
            content =  [list(i) for i in zip_longest(*transp_content, fillvalue=None)]

    column_names_string = ",".join(column for column in column_names)
    question_mark_string = ",".join("?" for _ in column_names)
    sql = f"INSERT INTO {table_name} ({column_names_string}) VALUES ({question_mark_string})"

    wherescape.push_many_to_target(sql, content)
    logging.info(f"{len(content)} rows successfully inserted in {table_name} from google data.")

    # Final logging
    end_time = datetime.now(tz=UTC)
    logging.info(
        "Time elapsed: %s seconds for gitlab_load_data"
        % (end_time - start_time).seconds
    )

def get_missing_columns(input_header: list, expected: list) -> tuple:
    """
    Returns columns that are in compare and not in input.
    dss_record_source and dss_load_date are considered expected as missing if not present.

    Args:
    - input_header (list): list of strings to check
    - expected (list): list of strings expected in input_header (larger if any are missing)

    Returns
    - columns (list): list of columns unexpectedly missing.
    - indexes (list): list of index values of missing columns.
    """
    # WS might end with digits. Remove those to compare.
    if re.search(r'_\d{3}$', input_header[0]) is not None:
        input_header = remove_final_digits(input_header)
    elif re.search(r'_\d{3}$', expected[0]) is not None:
        expected = remove_final_digits(expected)

    columns = []
    indexes = []
    for column in expected:
        if column not in input_header:
            # Columns missing in expected are considered
            if column not in ["dss_record_source", "dss_load_date"]:
                columns.append(column)
            indexes.append(expected.index(column))
    return columns, indexes

def add_empty_columns(content: list, header: list, indexes: list, full_header: list) -> tuple:
    """
    adds columns where columns are missing to both the header and the content.

    Params:
    - content (list): full content to add empty columns to.
    - header (list): header to add missing headers to.
    - indexes (list): indexes of missing columns.
    - full_header (list): expected containing correct header names.

    Returns:
    - content (list): content including new columns for missing fields.
    - header (list): header including new column names for missing fields.
    """
    transposed = [list(i) for i in zip_longest(*content, fillvalue=None)]
    for i in indexes:
        if full_header[i] in ["dss_record_source", "dss_load_date"]:
            continue
        transposed = transposed[:i] + [[None for _ in range(len(content))]] + transposed[i:]
        header = header[:i] + [full_header[i]] + header[i:]
    content = [list(i) for i in zip_longest(*transposed, fillvalue=None)]

    return content, header

def remove_extra_columns(content: list, header: list, indexes: list)-> tuple:
    """
    remove columns from content that aren't listed for it's destination.

    Params:
    - content (list): full content to add empty columns to.
    - header (list): header to add missing headers to.
    - indexes (list): indexes of missing columns.

    Returns:
    - content (list): content including new columns for missing fields.
    - header (list): header including new column names for missing fields.
    """
    transposed = [list(i) for i in zip_longest(*content, fillvalue=None)]
    indexes.reverse()
    for i in indexes:
        del transposed[i]
        del header[i]
    content = [list(i) for i in zip_longest(*transposed, fillvalue=None)]
    return content, header

def remove_final_digits(header: list) -> list:
    """
    Removes _000 (or other digits) from the end of words in a list if they are there.
    This method is to make comparing easier since the numbers might differ if the columns are not the same.

    Params:
    - header (list): header to remove extra digits from.

    Returns:
    - result (list): new header without the _000.
    """
    result = []
    for header in header:
        if re.search(r'_\d{3}$', header) is None:
            result.append(header)
        else:
            result.append(header[:-4])
    return result
