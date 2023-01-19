from typing import Union

from pathlib import Path

import gspread as gs
import numpy as np
import pandas as pd

from climbing_assessment_reader.assessment_reader.defaults import (
    GOOGLE_SHEET_NAME,
    GOOGLE_SHEET_URL,
)
from climbing_assessment_reader.assessment_reader.utils import fix_multiple_entries
from climbing_assessment_reader.static.converters import (
    convert_attempt,
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
    }
    CONVERTERS = {
        "questionnaire_id": convert_questionnaire_id,
        "grade*": convert_grade,
        "timestamp": pd.to_datetime,
        "attempt*": convert_attempt,
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

    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map the columns.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to map.

        Returns
        -------
        pd.DataFrame
            The mapped dataframe.
        """
        for column, mapper in self.MAPPERS.items():
            df[column] = df[column].map(mapper)
        return df

    def convert_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert the columns.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to convert.

        Returns
        -------
        pd.DataFrame
            The converted dataframe.
        """
        for regex, converter in self.CONVERTERS.items():
            for column in df.filter(regex=regex).columns:
                df[column] = df[column].apply(converter)
        return df

    def get_data(self) -> pd.DataFrame:
        """
        Get the assessment data.

        Returns
        -------
        pd.DataFrame
            The assessment data.
        """
        df = self.read()
        df = convert_duplicated_columns(df)
        df = self.map_columns(df)
        df = self.convert_columns(df)
        df["date"] = df["timestamp"].dt.date
        df["time"] = df["timestamp"].dt.time
        return (
            fix_multiple_entries(df.replace({"": np.nan}))
            .groupby(["questionnaire_id", "date", "time"])
            .first()
        ).reset_index()

    @property
    def data(self) -> pd.DataFrame:
        """
        Get the assessment data.

        Returns
        -------
        pd.DataFrame
            The assessment data.
        """
        return self.get_data()
