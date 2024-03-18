import sys
import os
import logging
from google.oauth2 import service_account
from httplib2 import Credentials



_SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
]


class gsheet:
    def __init__(self):
        self.credentials = self.get_scoped_credentials()

    def get_credentials(self):
        json_keyfile = _read_secret()

        return service_account.Credentials.from_service_account_file(json_keyfile)

    def get_scoped_credentials(self):
        """
        Return scoped credentials.
        """
        return self.get_credentials.with_scopes(_SCOPES)


def _read_secret():
    """
    Locates the json file with the secret depending on the OS.

    Returns:
    - path to secret file
    """
    if sys.platform == "win32":
        datapath = os.getenv("APPDATA")
        return os.path.join(
            datapath, "gspread", "google-drive-client-secret.json"
        )
    else:
        datapath = os.path.expanduser("~")
        return os.path.join(
            datapath, ".gspread", "google-drive-client-secret.json"
        )

