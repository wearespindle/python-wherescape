import gspread
import logging
import os
import sys

from datetime import datetime
from gspread.client import Client
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet
from gspread.exceptions import (
    SpreadsheetNotFound,
    WorksheetNotFound,
    APIError,
)

from ...helper_functions import remove_empty_rows_and_columns, get_python_type


class Gsheet:
    def __init__(self, args = None, url: str = "", test: bool = False):
        """
        Init for Gsheet. set all variables.

        Args:
        - args: args provided for processing correct data
        - url (str): link to the spreadsheet being uploaded
        - test (bool): False by default. set True in tests to not require args
        """
        self._client: Client = _authorize()
        if test:
            logging.warning("Marked as TEST. no params will be set")
        elif args:
            self.set_gsheet_variables(url, args)
        else:
            logging.error("No args provided")
    
    def set_gsheet_variables(self, url: str, args):
        """
        Function to set all of the variables for gsheet.
        """
        # Set spreadsheet and worksheet.
        self._set_spreadsheet(url=url, name=args.workbook_name)
        self._set_worksheet(title=args.sheet)

        # Set all content.
        self._set_content(args.range)
        self._set_header(args.no_header, args.header_range)
        self._set_column_types()

    def _set_spreadsheet(self, url: str = "", name: str = ""):
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
                except SpreadsheetNotFound:
                    logging.error("Invalid URL")

            if name:
                try:
                    self.spreadsheet = self._client.open(name)
                    logging.info("spreadsheet file has been obtained.")
                    return
                except SpreadsheetNotFound:
                    logging.error("Invalid workbook name")

            logging.error("Enter a valid workbook URL or workbook name")
            # Raised when both url and name don't find a spreadsheet.
            raise SpreadsheetNotFound

        except PermissionError as pe:
            logging.error("Invalid Permissions, make sure access is granted.")
            raise pe

    def get_spreadsheet(self) -> Spreadsheet:
        """
        Getter for Spreadsheet.

        Returns:
        - Spreadsheet.
        """
        return self.spreadsheet

    def _set_worksheet(self, title: str | None = None):
        """
        Sets the worksheet based on the given title.
        A spreadsheet needs to have already been set.

        Args:
        - title (str) : name of spreadsheet default value is standard among most
            spreadsheet applications
        """
        try:
            if title is None:
                logging.info("No name was provided. Using the first sheet")
                self.worksheet = self.spreadsheet.get_worksheet(0)
            else:
                self.worksheet = self.spreadsheet.worksheet(title)
        except AttributeError as ae:
            logging.error("No spreadsheet was provided")
            raise SpreadsheetNotFound
        except WorksheetNotFound as notFound:
            logging.warning(f"No worksheet was found with {title}")
            raise notFound

        logging.info(f"worksheet found with title {self.worksheet.title}")

    def get_worksheet(self) -> Worksheet:
        """
        Getter for worksheet.

        Returns:
        - Worksheet
        """
        return self.worksheet

    def _set_content(self, range: str | None = None):
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

    def get_content(self) -> list:
        """
        Getter for content

        Returns:
        - List of lists containing the content of the sheet.
        """
        return self.content

    def _set_header(
        self, 
        no_header: bool | None = None, 
        header_range: str | None = None,
    ):
        """
        Creates the header for the content. Takes it from content if no_header is False or None.

        Params:
        - no_header (str): (optional) input of args.no_header
        - header_Range (str): (optional) range of the header example: "A1:B5"
        """
        # Set content if none is set yet.
        if self.content is None:
            self._set_content()
        
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

    def get_header(self) -> list:
        """
        Getter for the header.

        Returns
        - List containing header values.
        """
        return self.header

    def _set_column_types(self):
        """
        Set a list with the postgrestype for each column in content.
        """
        if not self.content or self.header == []:
            self._set_header() # will also set content
        
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
            self._set_column_types()
        return self.column_types



def _authorize() -> Client:
    """
    Authorizes access for user.

    Returns:
    - Client for authorization when handling data
    """
    json_keyfile = _read_secret()
    return gspread.auth.service_account(
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

