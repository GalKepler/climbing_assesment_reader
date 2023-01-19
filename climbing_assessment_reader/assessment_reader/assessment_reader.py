from typing import Union

from pathlib import Path

import gspread as gs
import pandas as pd


class AssessmentReader:
    def __init__(
        self,
        google_sheet_url: str,
        google_sheet_name: str,
        service_account_file: Union[str, Path] = None,
    ):
        pass
