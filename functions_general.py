import psycopg2
from config import load_config
from get_dbstring import get_dbstring
from psycopg2.extras import RealDictCursor
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

def get_stations_table(filename='stations.csv'):
    """ 
    This function reads the .csv file and returns a DataFrame with the columns and index names
    """
    # Read in .csv file 
    data = pd.read_csv(filename, sep=';')
    
    # Remove any leading or trailing whitespace from the column names
    data.columns = data.columns.str.strip()

    result_df = pd.DataFrame(data)
    return result_df

def get_check_table(filename='check_table.csv'):
    """ 
    This function reads the check_table.csv file and returns a DataFrame with the columns and index names
    where the value is 'x'.
    """
    # Read in check_table.csv file 
    check_table = pd.read_csv(filename, index_col=0)
    
    # Remove any leading or trailing whitespace from the column names
    check_table.columns = check_table.columns.str.strip()
    # Remove any leading or trailing whitespace from the index names
    check_table.index = check_table.index.str.strip()
    
    result = []
    for idx in check_table.index:
        for col in check_table.columns:
            if check_table.loc[idx, col] == 'x':
                result.append({'Station': col, 'Variable': idx})

    result_df = pd.DataFrame(result)
    return result_df

def get_check_table2(filename='check_table.csv'):
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
    
    result = []
    for idx in check_table.index:
        for col in check_table.columns:
            # Check if not empty or NaN or None or 0
            if pd.notna(check_table.loc[idx, col]) and check_table.loc[idx, col] != '' and check_table.loc[idx, col] != 0:
            # if check_table.loc[idx, col] == 'x':
                result.append({'Station': idx, 'Variable': check_table.loc[idx, col],'Variable_name': col})

    result_df = pd.DataFrame(result)
    return result_df


def get_check_table_db(stations_table, source = 'wur_db', check_table_filename='check_table_base.csv'):
    # Get the check_table
    check_table = get_check_table2(check_table_filename)

    # Select the rows from check_table that match the Stations column with the 'name' from column from stations_table and match 'source' in the Variable column from stations_table
    matching_names = stations_table.loc[stations_table['source'] == source, 'name'].unique()
    check_table_db = check_table[check_table['Station'].isin(matching_names)]

    # reset the index of check_table_vudb
    check_table_db = check_table_db.reset_index(drop=True)

    return check_table_db