import pandas as pd


def filter_urgent_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['o_orderpriority'] == '1-URGENT'].reset_index(drop=True)
    return df