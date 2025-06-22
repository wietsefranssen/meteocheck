import os
import pandas as pd

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
        check_table_retrieved = pd.read_csv(check_table_file, sep=r'\s+')
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
def check_if_download_data_needed(dates_match, check_table_match, data_df_file, sensorinfo_df_file):
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