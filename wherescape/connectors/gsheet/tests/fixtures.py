import pytest

from wherescape.connectors.gsheet.gsheets_wrapper import Gsheet


@pytest.fixture
def gsheet():
    return Gsheet()