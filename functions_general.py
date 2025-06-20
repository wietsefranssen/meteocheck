import os
from config import load_config
from get_dbstring import get_dbstring
import pandas as pd

def fix_start_end_dt(start_dt=None, end_dt=None, tz='UTC'):
    # if the time of start_dt is not provided, set it to 00:00:00
    if len(start_dt) == 10:
        start_dt = f"{start_dt} 00:00:00"

    # if the time of end_dt is not provided, set it to 23:59:59
    if len(end_dt) == 10:
        end_dt = f"{end_dt} 23:59:00"
        
    # Make sure both start_dt and end_dt are tz aware
    # Check if start_dt and end_dt are already timezone-aware
    if pd.to_datetime(start_dt).tzinfo is None:
        start_dt = pd.to_datetime(start_dt).tz_localize(tz)
        
    if pd.to_datetime(end_dt).tzinfo is None:
        end_dt = pd.to_datetime(end_dt).tz_localize(tz)
        
    return start_dt, end_dt

def adapt_start_dt_to_existing_dataset(start_dt, end_dt, file, tz):

    if os.path.exists(file):
        # Read the file with special handling for the first three lines
        with open(file, 'r') as f:
            column_names = f.readline().strip().split(',')
            units = f.readline().strip().split(',')
        
        # Read only the last line of the file to get the last date
        with open(file, 'rb') as f:
            f.seek(-2, os.SEEK_END)  # Move to the second-to-last byte
            while f.read(1) != b'\n':  # Find the last newline character
                f.seek(-2, os.SEEK_CUR)
            last_line = f.readline().decode()
        
        # Convert the last line into a dictionary using column names
        last_line_data = dict(zip(column_names, last_line.strip().split(',')))
        
        # Extract the last date from the datetime column
        last_date = pd.to_datetime(last_line_data.get("datetime")).tz_convert(tz)
        
        # Determine the start date
        start_dt = last_date + pd.Timedelta(minutes=1)

        print(f"Previous data exists with last date: {last_date}.")
    
        # Check if the start date is before the end date
        if pd.to_datetime(last_date) == pd.to_datetime(end_dt):
            print(f"This is the same as the defined end_dt {end_dt}. Skipping...")
            return None, None
        elif pd.to_datetime(last_date) > pd.to_datetime(end_dt):
            print(f"This is later than the defined end_dt {end_dt}. Skipping...")
            return None, None

    if pd.to_datetime(start_dt) >= pd.to_datetime(end_dt):
        print(f"Start date {start_dt} is after end date {end_dt}. Skipping...")
        return None, None
    
    # print(f"Start date set to: {start_dt}. End date set to: {end_dt}.")
    
    return start_dt

def get_check_table(filename='check_table.csv'):
    """ 
    This function reads the check_table.csv file and returns a DataFrame with the columns and index names
    where the value is 'x'.
    """
    # Read in check_table.csv file 
    check_table = pd.read_csv(filename, index_col=0, sep=';')
    
    # Remove any leading or trailing whitespace from the column names
    check_table.columns = check_table.columns.str.strip()
    # Remove any leading or trailing whitespace from the index names
    check_table.index = check_table.index.str.strip()
    # Remove any leading or trailing whitespace from the values
    # check_table = check_table.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    result = []
    for idx in check_table.index:
        # for each station index, check each column except the first one
        for col in check_table.columns[1:]:
            # Check if not empty or NaN or None or 0
            if pd.notna(check_table.loc[idx, col]) and check_table.loc[idx, col] != '' and check_table.loc[idx, col] != 0:
            # if check_table.loc[idx, col] == 'x':
                result.append({'Station': idx, 'Variable': check_table.loc[idx, col],'Variable_name': col,'source': check_table.loc[idx, 'source']})

    result_df = pd.DataFrame(result)
    
    if result_df.empty:
        return None

    return result_df
