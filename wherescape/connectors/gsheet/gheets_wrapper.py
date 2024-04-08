import os
import sys

import gspread
from gspread import Client, Spreadsheet, Worksheet, SpreadsheetNotFound, WorksheetNotFound
import logging

# from google.oauth2 import service_account


class Gsheet:
    def __init__(self):
        self._client: Client = authorize()

    def _set_workbook_by_url(self, url: str) -> Spreadsheet:
        try:
            self.workbook = self._client.open_by_url(url)
        except SpreadsheetNotFound:
            logging.error("Invalid URL")

    def _set_workbook_by_name(self, name: str) -> Spreadsheet:
        try:
            self.workbook = self._client.open(name)
        except SpreadsheetNotFound:
            logging.error("Invalid workbook name")

    def get_workbook(self, url:str | None, name: str | None):
        if self.workbook:
            pass
        elif url:
            self.set_workbook_by_url(url)
        elif name:
            self.set_workbook_by_name(name)
        else:
            logging.error("Enter a valid workbook URL or workbook name")

        return self.workbook

    def get_worksheet(self, index: str | None) -> Worksheet:
        if index:
            try:
                return self.workbook.worksheet(index)
            except WorksheetNotFound:



def authorize() -> Client:
    """
    Authorizes access for user.

    Returns:
    - Client for authorization when handling data
    """
    json_keyfile = _read_secret()
    return gspread.service_account(
        json_keyfile,
    )


def _read_secret():
    """
    Locates the json file with the secret depending on the OS.

    Returns:
    - path to secret file
    """
    if sys.platform == "win32":
        datapath = os.getenv("APPDATA")
        return os.path.join(datapath, "gspread", "google-drive-client-secret.json")
    else:
        datapath = os.path.expanduser("~")
        return os.path.join(datapath, ".gspread", "google-drive-client-secret.json")
