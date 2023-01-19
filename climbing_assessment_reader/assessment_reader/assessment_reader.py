from typing import Union

from pathlib import Path

import gspread as gs
import pandas as pd

from climbing_assessment_reader.assessment_reader.defaults import (
    GOOGLE_SHEET_NAME,
    GOOGLE_SHEET_URL,
)
from climbing_assessment_reader.static.converters import (
    convert_duplicated_columns,
    convert_grade,
    convert_questionnaire_id,
)
from climbing_assessment_reader.static.mappers import (
    CLIMBING_WALLS_MAPPER,
    COLUMNS_MAPPER,
)


class AssessmentReader:
    #: Columns to drop from the dataframe
    DROP_COLUMNS = ["color", "exit", "review"]

    #: Mappers
    MAPPERS = {
        "climbing_wall": CLIMBING_WALLS_MAPPER,
        "questionnaire_id": convert_questionnaire_id,
        "grade": convert_grade,
    }

    def __init__(
        self,
        service_account_file: Union[str, Path],
        google_sheet_url: str = GOOGLE_SHEET_URL,
        google_sheet_name: str = GOOGLE_SHEET_NAME,
    ):
        """
        Instantiate the AssessmentReader.

        Parameters
        ----------
        service_account_file : Union[str, Path]
            The path to the service account file.
        google_sheet_url : str, optional
            The URL of the Google Sheet, by default GOOGLE_SHEET_URL
        google_sheet_name : str, optional
            The name of the Google Sheet, by default GOOGLE_SHEET_NAME
        """
        self.service_account_file = service_account_file
        self.google_sheet_url = google_sheet_url
        self.google_sheet_name = google_sheet_name

    def read(self) -> pd.DataFrame:
        """
        Read the assessment data from the Google Sheet.

        Returns
        -------
        pd.DataFrame
            The assessment data.
        """
        gc = gs.service_account(filename=self.service_account_file)
        sh = gc.open_by_url(self.google_sheet_url)
        worksheet = sh.worksheet(self.google_sheet_name)
        df = pd.DataFrame(worksheet.get_all_values())
        df.columns = df.loc[0]
        return df.drop(0).rename(columns=COLUMNS_MAPPER).drop(columns=self.DROP_COLUMNS)

    def reformat(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reformat the assessment data.

        Parameters
        ----------
        df : pd.DataFrame
            The assessment data.

        Returns
        -------
        pd.DataFrame
            The cleaned assessment data.
        """
        df = df.drop(columns=self.DROP_COLUMNS)
        df = df.dropna(subset=["climbing_wall", "questionnaire_id"])
        df["climbing_wall"] = df["climbing_wall"].map(CLIMBING_WALLS_MAPPER)
        df["questionnaire_id"] = df["questionnaire_id"].map(convert_questionnaire_id)
        df["grade"] = df["grade"].map(convert_grade)
        df = convert_duplicated_columns(df)
        return df
