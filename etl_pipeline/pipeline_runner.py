from etl_pipeline.extract.extract_google_ads1 import extract_google_ads
from transform.transform_data import transform_ads_data
from quality.data_quality_checks import check_data_quality
from load.load_to_snowflake import load_to_snowflake

if __name__ == "__main__":
    df = extract_google_ads()
    df = transform_ads_data(df)
    issues = check_data_quality(df)
    if issues:
        print("Problèmes de qualité:", issues)
    else:
        load_to_snowflake(df)
        print("Pipeline exécuté avec succès")