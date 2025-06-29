import pandas as pd

def round_up_acctbal(df: pd.DataFrame) -> pd.DataFrame:
    # Function to round up 'c_acctbal' values to the nearest integer
    def round_value(value):
        if pd.isna(value):
            return value
        return round(value, 2) if value >= 0 else round(value, 0)
    
    df['c_acctbal'] = df['c_acctbal'].apply(round_value)
    return df

def filter_customers(df: pd.DataFrame, max_custkey: int) -> pd.DataFrame:
    return df[df['c_custkey'] <= max_custkey]