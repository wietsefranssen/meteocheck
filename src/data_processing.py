import polars as pl
import pandas as pd


def process_data(data_df, sensorinfo_df):
    """Process and clean the data by removing empty columns"""
    
    # Remove all sensor_ids from data_df which only contain NaN values
    # Convert to pandas for dropna operation, then back to polars
    data_pd = data_df.to_pandas()
    if 'datetime' in data_pd.columns:
        data_pd = data_pd.set_index('datetime')
    data_pd = data_pd.dropna(axis=1, how='all')
    data_df = pl.from_pandas(data_pd.reset_index())

    # Remove all sensor_ids from sensorinfo_df which are not in data_df
    remaining_sensors = [col for col in data_df.columns if col != 'datetime']
    sensorinfo_df = sensorinfo_df.filter(
        pl.col('sensor_id').cast(pl.Utf8).is_in([str(s) for s in remaining_sensors])
    )
    
    return data_df, sensorinfo_df


def create_pivot_table(nan_table):
    """Create pivot table from nan_table for the AgGrid display"""
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='NaN_Percentage')
    pivot_table = pivot_table.fillna(0.0)  # Fill any missing values with 0% data availability
    
    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()
    
    return pivot_table

def create_pivot_table_reason(nan_table):
    """Create pivot table from nan_table for the AgGrid display"""
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='Reason')
    pivot_table = pivot_table.fillna("")  # Fill any missing values with empty strings

    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()

    return pivot_table
    
    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()
    
    return pivot_table
