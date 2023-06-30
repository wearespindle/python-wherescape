import logging
from google_sheets import google_sheet_load_data

try:
  google_sheet_load_data()

except Exception as e:
  logging.error("Unexpected Error")