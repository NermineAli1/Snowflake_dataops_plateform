import snowflake.connector
import os


def load_to_snowflake(df):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cs = conn.cursor()
    for _, row in df.iterrows():
        cs.execute(
            "INSERT INTO google_ads_metrics (campaign, clicks, impressions, cost, CTR, CPC) VALUES (%s, %s, %s, %s, %s, %s)",
            tuple(row),
        )
    cs.close()
    conn.close()
