
Default scopes for Gspread are

DEFAULT_SCOPES =[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
# Gsheet connector

Gsheet Connector for WhereScape. Takes care of creating metadata for a loading data and uploading the data from a gsheet file.

## WhereScape Parameters

## Connection Details
An authentication user is required from Wherescape. For this, a client secret has to be created in the Google API Console. 
Its secret should be stored in `%%APPDATAPP\gspread` for Windows or in `~/.gspread` for Unix.

The default scopes for this client secret are:

```
DEFAULT_SCOPES =[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
```

## Load table

Create 1 load table. with a script based load. 
Under Source. ensure a link is set to the file in the `Source Directory` and any desired arguments are provided in `Source File Name`.


## Host script
Create a new python host script and add it to the load table. Example code:

```
from wherescape_os.wherescape.connectors.gsheet.create_metadata import gsheet_create_metadata

gsheet_create_metadata()

```

Host scripts to create:
* python_gsheet_create_metadata
* python_gsheet_load_data

# Usage

First attach the metadata host script to the load table and ensure there's no pre-load action set. 
After creating the table, attach the load_data host script to the load table and set pre-load to struncate.
