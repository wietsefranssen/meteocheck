import os
import pandas as pd
import polars as pl
from datetime import datetime

def check_dates_last_retrieval(lastrun_info_file, start_dt, end_dt):
    # Read start and end dates from the text file
    if os.path.exists(lastrun_info_file):
        with open(lastrun_info_file, 'r') as f:
            lines = f.readlines()
            start_dt_retrieved = lines[0].strip().split(': ')[1]
            end_dt_retrieved = lines[1].strip().split(': ')[1]
            
            # Convert the retrieved dates to datetime objects for comparison (format: 2025-06-13 00:00:00+00:00)
            start_dt_retrieved = pd.to_datetime(start_dt_retrieved)
            end_dt_retrieved = pd.to_datetime(end_dt_retrieved)
            # compare the retrieved start and end dates with the original start and end dates

        if start_dt == start_dt_retrieved and end_dt == end_dt_retrieved:
            print("Start and end dates retrieved successfully and match the original dates.")
            dates_match = True
        else:
            print("Start and/or end dates retrieved do not match the original dates. There might be an issue with the retrieval process.")
            dates_match = False
    else:
        dates_match = False
    return dates_match

def check_checktable_last_retrieval(check_table_file, check_table):
    if os.path.exists(check_table_file):
        print(f"Check table file found: {check_table_file}")
        # Read check_table from the text file and compare it with the original check_table
        try:
            check_table_retrieved = pd.read_csv(check_table_file, sep=r'\s+')
        except:
            # Try with different separators if whitespace doesn't work
            try:
                check_table_retrieved = pd.read_csv(check_table_file, sep='\t')
            except:
                check_table_retrieved = pd.read_csv(check_table_file)
        
        # Compare the retrieved check_table with the original check_table
        if check_table.equals(check_table_retrieved):
            print("Check table retrieved successfully and matches the original check table.")
            check_table_match = True
        else:
            print("Check table retrieved does not match the original check table. There might be an issue with the retrieval process.")
            check_table_match = False
    else:
        print(f"Check table file not found: {check_table_file}. Proceeding to fetch data from the database.")
        check_table_match = False
    return check_table_match

# Check if the data files exist and if the dates and check_table match
def check_if_download_data_needed(last_retrieval_info_file, start_dt, end_dt, last_retrieval_checktable_file, check_table, data_df_file, sensorinfo_df_file):
    dates_match = check_dates_last_retrieval(last_retrieval_info_file, start_dt, end_dt)
    check_table_match = check_checktable_last_retrieval(last_retrieval_checktable_file, check_table)

    if dates_match and check_table_match:
        if os.path.exists(data_df_file) and os.path.exists(sensorinfo_df_file):
            print(f"Data files found: {data_df_file} and {sensorinfo_df_file}")
            return False  # No need to download data
        else:
            print(f"Data files not found: {data_df_file} and {sensorinfo_df_file}. Proceeding to fetch data from the database.")
            return True  # Need to download data
    else:
        print("Dates or check table do not match. Proceeding to fetch data from the database.")
        return True  # Need to download data
    
# Add extra info to sensorinfo_df
def add_extra_info_to_sensorinfo(sensorinfo_df, variable_info_file):
    """
    Add extra info to sensorinfo_df. Works with both Pandas and Polars DataFrames.
    """
    # Handle Polars DataFrame
    variable_df = pl.read_csv(variable_info_file, separator=';')
    
    # Create mapping dictionary from variable_df
    variable_mapping = dict(zip(variable_df['variable'], variable_df['long_name']))
    
    # Map variable from variable_df to variable_name in sensorinfo_df and add the long_name
    sensorinfo_df = sensorinfo_df.with_columns([
        pl.col('variable_name').map_elements(
            lambda x: variable_mapping.get(x, None),
            return_dtype=pl.Utf8
        ).alias('long_name')
    ])

    return sensorinfo_df
  
def save_last_retrieval_info(check_table, start_dt, end_dt, last_retrieval_info_file, last_retrieval_checktable_file):
    """
    Save the start and end dates to a text file.
    """
    
    # Get the directory of the last retrieval info file
    temp_path = os.path.dirname(last_retrieval_info_file)
    
    # Create the directory if it doesn't exist
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    # Get the directory of the last retrieval check table file
    last_retrieval_checktable_path = os.path.dirname(last_retrieval_checktable_file)
    
    # Create the directory if it doesn't exist
    if not os.path.exists(last_retrieval_checktable_path):
        os.makedirs(last_retrieval_checktable_path)    

    with open(last_retrieval_info_file, 'w') as f:
        f.write(f"Start date: {start_dt}\n")
        f.write(f"End date: {end_dt}\n")
        
    # Save the check_table to a text file
    # Handle both Pandas and Polars DataFrames
    with open(last_retrieval_checktable_file, 'w') as f:
        if hasattr(check_table, 'height'):  # It's Polars
            # Convert to string representation
            f.write(str(check_table))
        else:  # It's Pandas
            f.write(check_table.to_string(index=False))
