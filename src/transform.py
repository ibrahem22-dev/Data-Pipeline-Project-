import pandas as pd

def process_data(records: list[dict]) -> pd.DataFrame:
    """
    Transforms raw dictionary data into a structured pandas DataFrame.
    Adds calculated columns like temperature difference and categories.
    """
    # Handle empty input gracefully
    if not records:
        print("  [WARNING] No data to process")
        return pd.DataFrame()
 
    # Convert the list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(records)
 
    # Ensure the recorded_at column is a proper datetime object
    df["recorded_at"] = pd.to_datetime(df["recorded_at"])
 
    # Calculate the difference between actual temperature and "feels like"
    # Round to 1 decimal place for cleaner display
    df["temp_diff"] = round(df["temperature"] - df["feels_like"], 1)
 
    # Categorize temperatures into cold, moderate, and hot
    # This is useful for quick analysis and dashboard filtering later
    df["temp_category"] = pd.cut(
        df["temperature"],
        bins=[-10, 15, 25, 50],                  
        labels=["cold", "moderate", "hot"]         
    )
 
    return df
 

def print_summary(df: pd.DataFrame) -> None:
    """
    Prints a formatted summary of the processed weather data to the console.
    This helps verify the pipeline ran correctly without checking the database.
    """
    if df.empty:
        return
 
    # Print a nice header for the summary
    print("\n" + "=" * 60)
    print("  WEATHER SUMMARY")
    print("=" * 60)
 
    # Find the hottest and coldest cities in the current batch
    hottest = df.loc[df["temperature"].idxmax()]
    coldest = df.loc[df["temperature"].idxmin()]
    
    print(f"\n  Hottest:  {hottest['city']} — {hottest['temperature']}°C ({hottest['description']})")
    print(f"  Coldest:  {coldest['city']} — {coldest['temperature']}°C ({coldest['description']})")
 
    # Calculate and print average metrics across all cities
    print(f"\n  Avg temperature:  {df['temperature'].mean():.1f}°C")
    print(f"  Avg humidity:     {df['humidity'].mean():.1f}%")
    print(f"  Avg wind speed:   {df['wind_speed'].mean():.1f} m/s")
 
    # Show the distribution of temperature categories
    print(f"\n  Temperature categories:")
    for cat, count in df["temp_category"].value_counts().items():
        print(f"    {cat}: {count} cities")
 
    # Print the core data table for a quick overview
    print(f"\n  Full data:")
    print(df[["city", "temperature", "humidity", "wind_speed", "description"]].to_string(index=False))
    print()
