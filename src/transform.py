import pandas as pd

def process_data(records: list[dict]) -> pd.DataFrame:

    if not records:
        print("  [WARNING] No data to process")
        return pd.DataFrame()
 

    df = pd.DataFrame(records)
 

    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
 

    df["temp_diff"] = round(df["temperature"] - df["feels_like"], 1)
 

    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-10, 15, 25, 50],                  
        labels=["cold", "moderate", "hot"]         
    )
 
    return df
 

def print_summary(df: pd.DataFrame) -> None:
    if df.empty:
        return
 
    print("\n" + "=" * 60)
    print("  WEATHER SUMMARY")
    print("=" * 60)
 
    hottest = df.loc[df["temperature"].idxmax()]
    coldest = df.loc[df["temperature"].idxmin()]
    print(f"\n  Hottest:  {hottest['city']} — {hottest['temperature']}°C ({hottest['description']})")
    print(f"  Coldest:  {coldest['city']} — {coldest['temperature']}°C ({coldest['description']})")
 
    print(f"\n  Avg temperature:  {df['temperature'].mean():.1f}°C")
    print(f"  Avg humidity:     {df['humidity'].mean():.1f}%")
    print(f"  Avg wind speed:   {df['wind_speed'].mean():.1f} m/s")
 
    print(f"\n  Temperature categories:")
    for cat, count in df["temp_category"].value_counts().items():
        print(f"    {cat}: {count} cities")
 
    print(f"\n  Full data:")
    print(df[["city", "temperature", "humidity", "wind_speed", "description"]].to_string(index=False))
    print()
 