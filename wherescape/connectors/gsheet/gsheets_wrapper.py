import gspread
import logging
import os
import sys

from datetime import datetime
from gspread import (
    Client,
    Spreadsheet,
    Worksheet,
)
from gspread.exceptions import (
    SpreadsheetNotFound,
    WorksheetNotFound,
    APIError,
)



from ...helper_functions import convert_string, remove_empty_rows_and_columns, get_python_type


class Gsheet:
    def __init__(self):
        self._client: Client = _authorize()
        self.spreadsheet: Spreadsheet = None
        self.worksheet: Worksheet = None
        self.header: list = []
        self.content = None
        self.column_types = None

    def set_spreadsheet(self, url: str = "", name: str = ""):
        """
        Attempts to retreive the spreadsheet from google drive. 
        Requires either url or name.

        Params:
        - url (str): url to the spreadsheet document.
        - name (str): name of the spreadsheet document.
        """
        try:
            if url:
                try:
                    self.spreadsheet = self._client.open_by_url(url)
                    logging.info("spreadsheet file has been obtained.")
                    return
                except SpreadsheetNotFound as notFound:
                    logging.error("Invalid URL")

            if name:
                try:
                    self.spreadsheet = self._client.open(name)
                    logging.info("spreadsheet file has been obtained.")
                    return
                except SpreadsheetNotFound as notFound:
                    logging.error("Invalid workbook name")

            logging.error("Enter a valid workbook URL or workbook name")
            # Raised when both url and name don't find a spreadsheet.
            raise notFound

        except PermissionError as pe:
            logging.error("Invalid Permissions, make sure access is granted.")
            raise pe

    def get_spreadsheet(
        self, url: str = "", name: str = ""
    ) -> Spreadsheet:
        """
        Getter for Spreadsheet. Calls setter if not already present.

        Args:
        - url (str): (optional) link to the spreadsheet.
        - name (str): (optional) name of spreadsheet.

        Returns:
        - Spreadsheet.
        """
        if self.spreadsheet is not None:
            return self.spreadsheet
        else:
            self.set_spreadsheet(url, name)
            return self.spreadsheet

    def set_worksheet(self, title: str = None):
        """
        Sets the worksheet based on the given title.
        A spreadsheet needs to have already been set.

        Args:
        - title (str) : name of spreadsheet default value is standard among most
            spreadsheet applications
        """
        try:
            if self.spreadsheet is None:
                logging.info("No name was provided. Using the first sheet")
                self.spreadsheet = self.worksheet.get_worksheet(0)
            else:
                self.worksheet = self.spreadsheet.worksheet(title)
        except WorksheetNotFound as notFound:
            logging.warning("No worksheet was found")
            raise notFound

        logging.info(f"worksheet found")

    def get_worksheet(self, title: str = "") -> Worksheet:
        """
        Getter for worksheet. Calls setter if not already present.

        Params:
        - title (str) : title of the worksheet

        Returns:
        - Worksheet
        """
        if self.worksheet:
            pass
        elif title:
            self.set_worksheet(title)
        else:
            self.set_worksheet()
        return self.worksheet

    def set_content(self, range: str = None):
        """
        Retreives content from gsheet. removes empty rows.
        """
        if range:
            try:
                content = self.worksheet.get(range, )
            except APIError as e:
                logging.error(f"Invalid range: {range}")
                raise e
        else:
            content = self.worksheet.get_all_values()
        self.content = remove_empty_rows_and_columns(content)

    def get_content(self, range: str = None) -> list:
        """
        Getter for content. Calls setter if not already present.

        Returns:
        - List of lists containing the content of the sheet.
        """
        if self.content is None:
            self.set_content(range)
        return self.content

    def set_header(self, no_header: str = None, header_range: str = None):
        """
        Creates the header for the content. Takes it from content if no_header is False.

        Params:
        - no_header (str): (optional) input of args.no_header
        - header_Range (str): (optional) range of the header example: "A1:B5"
        """
        # Set content if none is set yet.
        if self.content is None:
            self.set_content()
        
        if not no_header:
            if header_range:
                try:
                    row = self.worksheet.get(header_range)[0]
                    self.header = ["column_" + str(i + 1) if value == "" else value for i, value in enumerate(row)]
                    self.content.pop(0)
                except APIError as e:
                    logging.error(f"Invalid Header range: {header_range}")
                    raise e
            else:
                row = self.content.pop(0)
                self.header = ["column_" + str(i + 1) if value == "" else value for i, value in enumerate(row)]
        else:
            self.header = ["column_" + str(i + 1) for i in range(len(self.content[0]))]

    def get_header(self, no_header: bool = False, range: str = None) -> list:
        """
        Getter for the header. Calls setter if not already present.

        Params:
        - no_header (str): (optional) True if no header is provided (legacy naming)
        - range (str): (optional) range of the header.

        Returns
        - List containing header values.
        """
        if self.header == []:
            self.set_header(no_header, range)
        return self.header

    def set_column_types(self):
        """
        Set a list with the postgrestype for each column in content.
        """
        if not self.content or self.header == []:
            self.set_header() # will also set content
        
        postgres_types = []
        
        for c in range(len(self.header)):
            if self.header[c] == "dss_record_source":
                postgres_types.append("varchar(256)")
            elif self.header[c] == "dss_load_date":
                postgres_types.append("timestamp")
            else:
                column_values = [self.content[r][c] for r in range(len(self.content))]
                if len(column_values) == 0:
                    postgres_types.append("text")
                else:
                    python_type = get_python_type(column_values)
                    if python_type == int or python_type == float:
                        postgres_types.append("numeric")
                    elif python_type == datetime:
                        postgres_types.append("timestamp")
                    elif python_type == bool:
                        postgres_types.append("bool")
                    else:
                        postgres_types.append("text")


        self.column_types = postgres_types

    def get_column_types(self) -> list:
        """
        Get column types. Calls setter if not yet set.

        Returns:
        - list of column types
        """
        if not self.column_types:
            self.set_column_types()
        return self.column_types

def set_gsheet_variables(gsheet: Gsheet, url: str, args):
    """
    Function to set all a bunch of variables for gsheet.
    """
    # Set spreadsheet and worksheet.
    gsheet.set_spreadsheet(url=url, name=args.workbook_name)
    gsheet.set_worksheet(title=args.sheet)

    # Set all content.
    gsheet.set_content(args.range)
    gsheet.set_header(args.no_header, args.header_range)
    gsheet.set_column_types()

def _authorize() -> Client:
    """
    Authorizes access for user.

    Returns:
    - Client for authorization when handling data
    """
    json_keyfile = _read_secret()
    return gspread.service_account(
        json_keyfile,
    )

def _read_secret() -> str:
    """
    Locates the json file with the secret depending on the OS.

    Returns:
    - path to secret file.
    """
    if sys.platform == "win32":
        datapath = os.getenv("APPDATA")
        return os.path.join(datapath, "gspread", "google-drive-client-secret.json")
    else:
        datapath = os.path.expanduser("~")
        return os.path.join(datapath, ".gspread", "google-drive-client-secret.json")

