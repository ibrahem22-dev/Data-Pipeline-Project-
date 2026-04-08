import pandas as pd

def process_data(records: list[dict]) -> pd.DataFrame:
    """
    Takes the raw list of city dictionaries from the extraction step
    and turns it into a clean, enriched pandas DataFrame.
    Also adds a couple of derived columns that are useful for analysis.
    """
    if not records:
        print("  [WARNING] No data to process")
        return pd.DataFrame()
 
    df = pd.DataFrame(records)
 
    # Make sure recorded_at is a proper datetime — it comes in as an ISO string
    # from the extract step and pandas won't aggregate it correctly otherwise
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
 
    # temp_diff shows how much warmer or colder it "feels" vs the actual reading.
    # Useful for spotting high-wind or high-humidity days where the gap is large.
    df["temp_diff"] = round(df["temperature"] - df["feels_like"], 1)
 
    # Bucketing temperatures into three categories for quick filtering in the dashboard.
    # Thresholds are based on what feels reasonable for Israeli climate:
    # below 15°C is genuinely cold here, above 25°C is when it starts getting uncomfortable
    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-10, 15, 25, 50],                  
        labels=["cold", "moderate", "hot"]         
    )
 
    return df
 

def print_summary(df: pd.DataFrame) -> None:
    """
    Prints a quick human-readable summary of the current batch to the console.
    I use this to sanity-check that the pipeline ran correctly without having
    to open the database or the dashboard every time.
    """
    if df.empty:
        return
 
    print("\n" + "=" * 60)
    print("  WEATHER SUMMARY")
    print("=" * 60)
 
    # idxmax/idxmin give me the row index of the extreme values,
    # then I use .loc to pull the full row for that city
    hottest = df.loc[df["temperature"].idxmax()]
    coldest = df.loc[df["temperature"].idxmin()]
    
    print(f"\n  Hottest:  {hottest['city']} — {hottest['temperature']}°C ({hottest['description']})")
    print(f"  Coldest:  {coldest['city']} — {coldest['temperature']}°C ({coldest['description']})")
 
    # Averages across all 5 cities in this batch
    print(f"\n  Avg temperature:  {df['temperature'].mean():.1f}°C")
    print(f"  Avg humidity:     {df['humidity'].mean():.1f}%")
    print(f"  Avg wind speed:   {df['wind_speed'].mean():.1f} m/s")
 
    # Show how many cities fell into each temperature bucket
    print(f"\n  Temperature categories:")
    for cat, count in df["temp_category"].value_counts().items():
        print(f"    {cat}: {count} cities")
 
    # Full table at the end — easier to read than individual print statements
    print(f"\n  Full data:")
    print(df[["city", "temperature", "humidity", "wind_speed", "description"]].to_string(index=False))
    print()
