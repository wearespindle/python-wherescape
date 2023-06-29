import argparse
import gspread
from gspread.utils import a1_range_to_grid_range
import os
from oauth2client.service_account import ServiceAccountCredentials
import re
import sys

def split_arg_string(string):
    """Given an argument string this attempts to split it into small parts."""

    rv = []
    for match in re.finditer(r"('([^'\\]*(?:\\.[^'\\]*)*)'"
                             r'|"([^"\\]*(?:\\.[^"\\]*)*)"'
                             r'|\S+)\s*', string, re.S):
        arg = match.group().strip()
        if arg[:1] == arg[-1:] and arg[:1] in '"\'':
            arg = arg[1:-1].encode('ascii', 'backslashreplace').decode('unicode-escape')
        try:
            arg = type(string)(arg)
        except UnicodeError:
            pass
        rv.append(arg)
    return rv


def create_gsheet_client():  
    """
    Loads Security information and initialises gspread client  object

    Prerequisites:
    --------------
    This function expects a client secret that can be downloaded from the 
      Google API console (https://console.developers.google.com/) 
    Client secret should be located in:
    - (Windows) %%APPDATA%%\gspread
    - (Unix) ~/.gspread/

    References:
     https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html
     https://gspread.readthedocs.io/en/latest/index.html
    """
    
    # Read the secret-file (Location is os dependent)
    if sys.platform == "win32":
        datapath = os.getenv('APPDATA')
        json_keyfile = os.path.join(datapath, "gspread", "google-drive-client-secret.json")
    else:
        datapath = os.path.expanduser("~")
        json_keyfile = os.path.join(datapath, ".gspread", "google-drive-client-secret.json")

    #set scope 
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # use creds to create a client to interact with the Google Drive API
    creds = ServiceAccountCredentials.from_json_keyfile_name(
            json_keyfile, scopes
    )
    return gspread.authorize(creds)

def parse_gspread_arguments(argument_string, messages=[]):
    """
    Converts an argument string into args object

    """

    # split string
    argument_list = split_arg_string(argument_string)
    #print(argument_list)

    # use argparser to parse workbook_details
    parser = argparse.ArgumentParser()
    parser.add_argument('workbook_name', help='Name of the Google Sheet/ workbook') # positional argument
    parser.add_argument('--sheet', help = 'Name of the sheet in the workbook')
    parser.add_argument('--range', help = 'Cell range to retrieve')
    parser.add_argument('--header_range', help = 'Cell range to be used as header')
    parser.add_argument('--no_header', action='store_true', help='Specify if table has no header')
    parser.add_argument('-d', '--debug',  action='store_true', help='Print debug messages' )
    
    # need try/catch to get a decent error message in Wherescape
    try:
       args = parser.parse_args(argument_list)
    except SystemExit as ex:
       raise Exception(ex)

    # apply capitals to ranges
    if args.range:
        args.range = args.range.upper()
    if args.header_range:
        args.header_range = args.header_range.upper()

    messages.append(f"workbook_name: {args.workbook_name}, sheet: {args.sheet}, range: {args.range},  hr: {args.header_range}, no_header: {str(args.no_header)}, debug: {args.debug}" )

    # You cannot specify both a header_range and --no_header in the object source File Name 
    if args.header_range and args.no_header:
        #error_messages.append("You cannot specify both a --header_range and --no_header in the object source File Name"
        raise Exception("You cannot specify both a header_range and --no_header in the object source File Name")
    
    if args.header_range and not args.range:
        raise Exception("A --header_range can not be specified without specifying a --range")
    
    # If both a range and a header_range are specified, they can not overlap
    if args.header_range and args.range:
        row_index_header_range  = a1_range_to_grid_range(args.header_range).get('startRowIndex')
        row_index_range         = a1_range_to_grid_range(args.range).get('startRowIndex')
        if row_index_header_range == row_index_range:
            #error_messages.append("If both a range and a header_range are specified, they can not overlap")
            raise Exception("If both a range and a header_range are specified, they can not overlap")

    return args, messages