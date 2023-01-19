import pandas as pd


def fix_multiple_entries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix multiple entries.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to fix.

    Returns
    -------
    pd.DataFrame
        The fixed dataframe.
    """
    new_df = (
        df.sort_values(by=["questionnaire_id", "timestamp"])
        .groupby(["questionnaire_id", "date"])
        .first()
        .reset_index()
    )  # Keep only the first entry of each questionnaire_id and date
    for q_id in new_df["questionnaire_id"].unique():  # For each questionnaire_id
        q_id_df = df[
            df["questionnaire_id"] == q_id
        ]  # Get the dataframe of the questionnaire_id
        for date in q_id_df["date"].unique():  # For each date
            target_row = new_df.loc[
                (new_df["questionnaire_id"] == q_id) & (new_df["date"] == date)
            ].index[
                0
            ]  # Get the index of the row to fill
            date_df = q_id_df[
                (q_id_df["date"] == date)
                & (q_id_df["timestamp"] != new_df.loc[target_row, "timestamp"])
            ]  # Get the other entries of the same date
            if date_df.shape[0] > 0:  # If there are other entries
                print("Multiple entries found:")
                print(f"Questionnaire ID: {q_id}, Date: {date}")
                fill_missing_values(
                    date_df, new_df, target_row
                )  # Fill the missing values
    return new_df


def fill_missing_values(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    target_row: int,
    columns_regex_list: list = ("grade*", "attempt*"),
) -> None:
    """
    Fill missing values.

    Parameters
    ----------
    source_df : pd.DataFrame
        The source dataframe.
    target_df : pd.DataFrame
        The target dataframe.
    target_row : int
        The target row.
    columns_regex_list : list, optional
        The columns regex list, by default ["grade*", "attempt*"]

    Returns
    -------
    None
    """
    for row in source_df.index:
        for column_regex in columns_regex_list:
            counter = 0
            values = source_df.loc[row].filter(regex=column_regex).values
            for column in target_df.filter(regex=column_regex).columns:
                if pd.notna(values[counter]) and pd.isna(
                    target_df.loc[target_row, column]
                ):
                    target_df.loc[target_row, column] = values[counter]
                counter += 1
