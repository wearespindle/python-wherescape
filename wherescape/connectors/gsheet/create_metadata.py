import datetime
import logging
from argparse import Namespace
from time import sleep
from typing import List

from gspread import (
    Client,
    Spreadsheet,
    SpreadsheetNotFound,
    Worksheet,
    WorksheetNotFound,
)
from gspread.utils import a1_range_to_grid_range

from ...wherescape import WhereScape
from .gheets_wrapper import Gsheet
from .gsheets_parsing import parse_gspread_arguments

TYPE_ORDER = ['int', 'float', 'bool', 'datetime', 'date', 'time']
CONVERTERS = {
    'int': int,
    'float': float,
    'text'
}

def gsheet_create_metadata():
    """
    Function that loads data from a gsheet into the LoadTable.
    """
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("connecting to WhereScape")

    gsheet: Gsheet = Gsheet()
    wherescape_instance = WhereScape()

    logging.info(f"Start time: {start_time} for jira_load_data")

    load_table_name = wherescape_instance.table
    url = wherescape_instance.query_meta(
        "select lt_file_path form ws_load_tab where lt_table_name = ?",
        [load_table_name],
    )[0][0]
    workbook_details = wherescape_instance.query_meta(
        "select lt_file_name from ws_load_tab where lt_table_name = ?",
        [load_table_name],
    )[0][0]

    logging.info(f"Metadata. URL: {url} ; Details : {workbook_details}")
    args = parse_gspread_arguments(workbook_details)
    if args.debug:
        logging.warning("Debug mode on -> do not use for production")

    workbook = gsheet.get_workbook(url, args.workbook_name)
    logging.info(f"Opened workbook: {workbook.title}")
    worksheet = gsheet.get_worksheet(args.sheet)

    opened

    start_cell_header = find_start_cell(args.header_range, args.range)

    column_names = get_column_names(args.no_header, start_cell_header)
    logging.info(
        f"Retrieved {len(column_names)} columns of worksheet: {worksheet.title}"
    )
    logging.info(f"column_names: {column_names}")

    column_types = []


def find_start_cell(header_range: str, range: str) -> str:
    """
    Returns thes tarting cell.
    """
    # determine the start cell for the header
    if header_range:
        # simple, start_cell is given
        start_cell_header = header_range
    elif range:
        # header range was not given, header could be first line of range
        start_cell_header = range
    else:
        # default A1:1
        start_cell_header = "A1:1"
    logging.info(f"start_cell_header: {start_cell_header}")
    return start_cell_header


def get_column_names(
    worksheet: Worksheet, no_header: str, start_cell_header: str
) -> List[str]:
    """
    Returns List of column Names.
    """
    first_line = worksheet.get(start_cell_header)
    if no_header:
        # make up column names if no_header
        column_names = ["column_" + str(i + 1) for i in range(len(first_line))]
    else:
        # fill empty_column headers
        column_names = [
            "column_" + str(i + 1) if value == "" else value
            for i, value in enumerate(first_line)
        ]
    return column_names

def get_type(value:str):


def get_first_row(
    start_cell_header: str, range: str, no_header: str, header_range: str
) -> int | None:
    """
    Returns the first row.
    """
    if range and no_header:
        # when there's no header, first row of values will be start row of args.range
        return a1_range_to_grid_range(range).get("startRowIndex")
    elif range and header_range:
        # when the header range is explicity given given,
        #   the first row of values will be start row of args.range
        return a1_range_to_grid_range(range).get("startRowIndex")
    elif range:
        # neither no_header nor header_range explcitly given,  first row of values will be start row of args.range + 1
        return a1_range_to_grid_range(start_cell_header).get("startRowIndex") + 1
    elif no_header:
        return 0
    else:
        return 1
