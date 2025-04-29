from datetime import datetime
import logging
import pytest

from wherescape.connectors.gsheet.gsheets_wrapper import Gsheet, get_python_type
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound


NO_ACCESS_URL = "https://docs.google.com/spreadsheets/d/1lhrCYDeMpX8DUdoI_JC4hEmhhVqUUOkCIkTKgHpcN0o/edit?usp=drive_link"
NO_ACCESS_NAME = "no access"
BASIC_FILE_URL = "https://docs.google.com/spreadsheets/d/1O8BhaD385kPxxQGUeyU0DoGPLyBQITFCihje2av0POk/edit?usp=drive_link"
BASIC_FILE_NAME = "basic data file"
DIFF_START_CELL_URL = "https://docs.google.com/spreadsheets/d/15W15G9ERorhGT5QhvO8IJVTWvtbQC9Puzxi3txmU9ZM/edit?usp=drive_link"
DIFF_START_CELL_NAME = "middle_starter_cell_50_rows"
FAKE_URL = "https://docs.google.com/spreadsheets/d/1pdqdoPnhdTIjccXstqK7ReWzFT0rxez3gdJItO9j0Ng/edit?usp=drive_link"
BAD_URL = "https://docs.google.com/spreadsheets/d/1pdqdoPnhdTIjccXstqK7"

class TestGsheet:
    def setup_method(self, method):
        self.gsheet = Gsheet(test=True)

    def test_set_spreadsheet_on_url(self):
        """
        Test that a spreadsheet can be set using a valid url.
        """
        url = BASIC_FILE_URL
        self.gsheet._set_spreadsheet(url=url)
        
        assert self.gsheet.spreadsheet is not None

    def test_set_spreadsheet_on_name(self):
        """
        Test that a spreadsheet can be set using a valid name.
        """
        name = BASIC_FILE_NAME
        self.gsheet._set_spreadsheet(name=name)

        assert self.gsheet.spreadsheet is not None

    @pytest.mark.parametrize(
            ("url", "exception"), 
            (
                (NO_ACCESS_URL, PermissionError),
                (FAKE_URL, SpreadsheetNotFound),
                (BAD_URL, SpreadsheetNotFound),
                (FAKE_URL, SpreadsheetNotFound)
            )
    )
    def test_set_spreadsheet_errors(self, url, exception):
        """
        Test that the correct error is thrown given the url.
        """
        with pytest.raises(exception):
            self.gsheet._set_spreadsheet(url=url)

    def test_set_worksheet_no_input(self):
        """
        Test a worksheet is set without any inputs.
        """
        self.gsheet._set_spreadsheet(url=BASIC_FILE_URL)
        logging.warning(self.gsheet.spreadsheet)
        self.gsheet._set_worksheet()
        
        assert (self.gsheet.spreadsheet is not None)

    def test_set_worksheet_with_input(self):
        """
        Test worksheet gets sst using the title of the worksheet.
        """
        title = "Second"
        url = DIFF_START_CELL_URL
        self.gsheet._set_spreadsheet(url=url)
        self.gsheet._set_worksheet(title=title)

        assert (self.gsheet.spreadsheet is not None)

    def test_set_worksheet_no_spreadsheet(self):
        """
        Test SpreadsheetNotFound thrown when no spreadsheet is set when
        calling set_worksheet.
        """
        with pytest.raises(SpreadsheetNotFound):
            self.gsheet._set_worksheet()
    
    def test_set_worksheet_incorrect_input(self):
        """
        Test raises WorksheetNotFound if no worksheet was found with the given title.
        """
        self.gsheet._set_spreadsheet(url=BASIC_FILE_URL)
        with pytest.raises(WorksheetNotFound):
            self.gsheet._set_worksheet(title="sdfsffuysdkjfhsdkjf")
        
    @pytest.mark.parametrize(
            ("url", "rows", "columns"),
            (
                (BASIC_FILE_URL, 51, 7),
                (DIFF_START_CELL_URL, 50, 7),
            )
    )
    def test_set_content_has_all_content_of_sheet(self, url, rows, columns):
        """
        Check that set_content collects akk tge data
        """
        self.gsheet._set_spreadsheet(url=url)
        self.gsheet._set_worksheet()
        self.gsheet._set_content()
        content = self.gsheet.content

        assert len(content) == rows
        assert len(content[0]) == columns

    def test_set_header_gets_first_row_if_header(self):
        """
        Test to see if 
        """
        self.gsheet._set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet._set_worksheet()
        self.gsheet._set_content()
        content = self.gsheet.get_content()
        first_row = content[0]
        self.gsheet._set_header()

        assert self.gsheet.header == first_row
    
    def test_set_header_no_header_given(self):
        self.gsheet._set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet._set_worksheet()
        self.gsheet._set_content()
        content = self.gsheet.get_content()
        first_row = self.header = ["column_" + str(i + 1) for i in range(len(content[0]))]
        self.gsheet._set_header(no_header=True)

        assert self.gsheet.header == first_row

    def test_set_column_types(self):
        self.gsheet._set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet._set_worksheet()
        self.gsheet._set_content()
        self.gsheet._set_header()
        self.gsheet._set_column_types()

        expected = ["text", "numeric", "numeric", "text", "text", "text", "timestamp"]
        assert len(self.gsheet.column_types) == len(expected)
        assert self.gsheet.column_types == expected

@pytest.mark.parametrize(
            ("input", "expected_type"),
            (
                (["4","1","4562","4567","234634","34532"], int),
                (["dsjkfh", "skdjhf", "jkghsdf"], str),
                (["43.6", "4534.34", "457424.644"], float),
                (["2024-05-30", "2023-12-05", "2023-06-28"], datetime),
                (["5461", "534.687", "74849"], float),
                (["sdferw", "234234", "sdfjhs", "jghfd"], str),
                (["2024-05-30", "sdffsdfwe", "", ""], str),
                (["", "", ""], str),
            )
    )
def test_get_python_type(input, expected_type):
    result = get_python_type(input)
    
    assert result == expected_type
