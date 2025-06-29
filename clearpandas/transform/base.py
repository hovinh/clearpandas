import pandas as pd


def remove_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna()