from datetime import datetime
import pytest

from wherescape.connectors.gsheet.gsheets_wrapper import Gsheet, get_python_type
from gspread.exceptions import SpreadsheetNotFound


NO_ACCESS_URL = "https://docs.google.com/spreadsheets/d/1lhrCYDeMpX8DUdoI_JC4hEmhhVqUUOkCIkTKgHpcN0o/edit?usp=drive_link"
NO_ACCESS_NAME = "no access"
BASIC_FILE_URL = "https://docs.google.com/spreadsheets/d/1O8BhaD385kPxxQGUeyU0DoGPLyBQITFCihje2av0POk/edit?usp=drive_link"
BASIC_FILE_NAME = "basic data file"
DIFF_START_CELL_URL = "https://docs.google.com/spreadsheets/d/15W15G9ERorhGT5QhvO8IJVTWvtbQC9Puzxi3txmU9ZM/edit?usp=drive_link"
DIFF_START_CELL_NAME = "middle_starter_cell_50_rows"
NO_HEADER_URL = "https://docs.google.com/spreadsheets/d/1pdqdoPnkwTIjccXstqT6TeWzFT0rxez3KdJItO9j0Ng/edit?usp=drive_link"
NO_HEADER_NAME = "No header File"
FAKE_URL = "https://docs.google.com/spreadsheets/d/1pdqdoPnhdTIjccXstqK7ReWzFT0rxez3gdJItO9j0Ng/edit?usp=drive_link"
BAD_URL = "https://docs.google.com/spreadsheets/d/1pdqdoPnhdTIjccXstqK7"
NOT_URL = "skjdhfsjdfgfjsdhgfikrfgakerfdfggfd"

class TestGsheet:
    def setup_method(self, method):
        self.gsheet = Gsheet()

    def test_set_spreadsheet_on_url(self):
        """
        Test that a spreadsheet can be set using a valid url.
        """
        url = BASIC_FILE_URL
        self.gsheet.set_spreadsheet(url=url)
        
        assert self.gsheet.spreadsheet is not None

    def test_set_spreadsheet_on_name(self):
        """
        Test that a spreadsheet can be set using a valid name.
        """
        name = BASIC_FILE_NAME
        self.gsheet.set_spreadsheet(name=name)

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
            self.gsheet.set_spreadsheet(url=url)

    def test_get_spreadsheet_calls_setter_when_not_set(self, mocker):
        """
        Test that the setter is called when no spreadsheet is set before calling
        get spreadsheet.
        """
        mock_method = mocker.patch.object(self.gsheet, "set_spreadsheet")
        self.gsheet.set_spreadsheet()
        
        # Assertion
        mock_method.assert_called_once()

    def test_set_worksheet_no_input(self):
        """
        Test a worksheet is set without any inputs.
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        
        assert (self.gsheet.spreadsheet is not None)

    def test_set_worksheet_with_input(self):
        """
        Test worksheet gets sst using the title of the worksheet.
        """
        title = "First"
        url = DIFF_START_CELL_URL
        self.gsheet.set_spreadsheet(url=url)
        self.gsheet.set_worksheet(title=title)

        assert (self.gsheet.spreadsheet is not None)

    def test_set_worksheet_no_spreadsheet(self):
        """
        Test SpreadsheetNotFound thrown when no spreadsheet is set when
        calling set_worksheet.
        """
        with pytest.raises(SpreadsheetNotFound):
            self.gsheet.set_worksheet()
    
    def test_set_worksheet_incorrect_input(self):
        """
        Test to see if it uses the second title option if the first one fails.
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet(title="sdfsffuysdkjfhsdkjf")

        assert (self.gsheet.spreadsheet is not None)
        assert (self.gsheet.worksheet.title == "Table1")

    def test_get_worksheet_calls_setter_when_not_set(self, mocker):
        """
        Test to see if setter is called if worksheet is not yet set while
        calling its getter.
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        mock_method = mocker.patch.object(self.gsheet, "set_worksheet")
        self.gsheet.get_worksheet()
        
        # Assertion
        mock_method.assert_called_once()
        
    @pytest.mark.parametrize(
            ("url", "rows", "columns"),
            (
                (BASIC_FILE_URL, 51, 7),
                (DIFF_START_CELL_URL, 51, 9),
            )
    )
    def test_set_content_has_all_content_of_sheet(self, url, rows, columns):
        """
        Check that set_content collects akk tge data
        """
        self.gsheet.set_spreadsheet(url=url)
        self.gsheet.set_worksheet()
        self.gsheet.set_content()
        content = self.gsheet.content

        assert len(content) == rows
        assert len(content[0]) == columns

    def test_get_content_calls_setter_if_not_set(self, mocker):
        """
        Test to see if setter is called if content is not yet set while
        calling get_content.
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        mock_method = mocker.patch.object(self.gsheet, "set_content")
        self.gsheet.get_content()

        # Assertion
        mock_method.assert_called_once()

    def test_set_header_gets_first_row_if_header(self):
        """
        Test to see if 
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        content = self.gsheet.get_content()
        first_row = content[0]
        self.gsheet.set_header()

        assert self.gsheet.header == first_row
    
    def test_set_header_no_header_given(self):
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        content = self.gsheet.get_content()
        first_row = self.header = ["column_" + str(i + 1) for i in range(len(content[0]))]
        self.gsheet.set_header(no_header="true")

        assert self.gsheet.header == first_row

    def test_get_header_calls_setter_if_not_set(self, mocker):
        """
        Test to see if setter is called if content is not yet set while
        calling get_content.
        """
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        self.gsheet.set_content()
        mock_method = mocker.patch.object(self.gsheet, "set_header")
        self.gsheet.get_header()

        # Assertion
        mock_method.assert_called_once()

    def test_set_column_types(self):
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        self.gsheet.set_content()
        self.gsheet.set_header()
        self.gsheet.set_column_types()

        expected = ["text", "numeric", "numeric", "text", "text", "text", "timestamp"]
        assert len(self.gsheet.column_types) == len(expected)
        assert self.gsheet.column_types == expected

    def test_get_column_types_calls_setter_if_not_set(self, mocker):
        self.gsheet.set_spreadsheet(url=BASIC_FILE_URL)
        self.gsheet.set_worksheet()
        self.gsheet.set_content()
        self.gsheet.set_header()
        mock_method = mocker.patch.object(self.gsheet, "set_column_types")
        self.gsheet.get_column_types()

        mock_method.assert_called_once()

def test_set_gsheet_variables():
    pass

def test_remove_empy_rows_and_columns():
    pass

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
