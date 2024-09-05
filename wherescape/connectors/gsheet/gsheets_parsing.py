import argparse
import logging
import shlex

from gspread.utils import a1_range_to_grid_range


def parse_gspread_arguments(argument: str) -> argparse.Namespace:
    """
    Converts an argument string into args object.

    Parameters:
    - argument (str): arguments for the parser collected in a string.

    Returns
    - args (Namespace): object with all arguments provided stored within.
    """
    if argument == "":
        logging.info("No arguments provided. Using defaults.")

    argument_list = shlex.split(argument)

    parser = create_parser()

    try:
        args = parser.parse_args(argument_list)
    except SystemExit as ex:
        logging.warning("There might be a mistake with the arguments. Ensure it's all correct.")
        logging.error(ex)

    if args.range:
        args.range = args.range.upper()
    if args.header_range:
        args.header_range = args.header_range.upper()

    logging.info(
        f"workbook_name: {args.workbook_name}, sheet: {args.sheet}, range: {args.range},  hr: {args.header_range}, no_header: {str(args.no_header)}, debug: {args.debug}"
    )

    if args.header_range and args.no_header:
        logging.error(
            "You cannot specify both a header_range and --no_header in the object source File Name."
        )
    if args.header_range and not args.range:
        logging.error(
            "A --header_range can not be specified without specifying a --range."
        )

    if args.header_range and args.range:
        row_index_header_range = a1_range_to_grid_range(args.header_range).get(
            "startRowIndex"
        )
        row_index_range = a1_range_to_grid_range(args.range).get("startRowIndex")
        if row_index_header_range != row_index_range:
            logging.warning(
                "If both a range and a header_range are specified, they should overlap."
            )
    return args


def create_parser():
    """
    Method to create parser with arguments for workbook_details.

    Return:
    - parser containing possible args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "workbook_name", help="Name of the Google Sheet/ workbook", default=None
    )  # positional argument
    parser.add_argument("--sheet", help="Name of the sheet in the workbook")
    parser.add_argument("--range", help="Cell range to retrieve")
    parser.add_argument("--header_range", help="Cell range to be used as header")
    parser.add_argument(
       "--no_header", action="store_true", help="Specify if table has no header"
    )
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Print debug messages"
    )
    return parser
