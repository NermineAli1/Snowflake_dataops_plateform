def transform_ads_data(df):
    df["CTR"] = df["clicks"] / df["impressions"]
    df["CPC"] = df["cost"] / df["clicks"]
    return df
