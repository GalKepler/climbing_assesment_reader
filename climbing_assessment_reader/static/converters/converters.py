from typing import Union

import numpy as np
import pandas as pd


def convert_duplicated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert duplicated columns

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to convert

    Returns
    -------
    pd.DataFrame
        Converted DataFrame
    """
    cols = pd.Series(df.columns)
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        cols[df.columns.get_loc(dup)] = [
            dup + "." + str(d_idx + 1) for d_idx in range(df.columns.get_loc(dup).sum())
        ]
    df.columns = cols
    return df


def convert_attempt(attempt: Union[str, int]) -> float:
    """
    Convert attempt

    Parameters
    ----------
    attempt: str or int
        Attempt to convert

    Returns
    -------
    str or int
        Converted attempt
    """
    if attempt == "":
        return np.nan
    if isinstance(attempt, str):
        return float(attempt)
    if attempt is None:
        return np.nan


def convert_grade(grade: Union[str, float]) -> float:
    """
    Convert grade to float

    Parameters
    ----------
    grade: str or float
        Grade to convert

    Returns
    -------
    float
        Converted grade
    """
    stripped_grade = "".join(
        [letter for letter in grade if letter.isnumeric() or letter in [".", "-"]]
    )
    if stripped_grade == "":
        return np.nan
    if "-" in stripped_grade:
        grade = np.mean([float(g) for g in stripped_grade.split("-")])
    else:
        grade = float(stripped_grade)
    return round(grade * 2) / 2


def convert_questionnaire_id(questionnaire_id: str) -> str:
    """
    Convert questionnaire id

    Parameters
    ----------
    questionnaire_id: str
        Questionnaire id to convert

    Returns
    -------
    str
        Converted questionnaire id
    """
    return questionnaire_id.zfill(4)
