def check_data_quality(df):
    issues = []
    if df.isnull().sum().sum() > 0:
        issues.append("Présence de valeurs nulles")
    if (df['CTR'] > 1).any():
        issues.append("CTR > 1 détecté")
    if df.duplicated().any():
        issues.append("Doublons détectés")
    return issues