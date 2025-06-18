import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from vudb import get_site_id_by_name, get_sensorinfo_by_siteid_and_sensorname, get_data
from vudb import read_csv_with_header, select_variables
from vudb import fix_start_end_dt
from config import load_config

def load_biomet_vudb(site, names, start_dt, end_dt, tz):
    """
    This function would connect to a database and retrieve the data.
    """
  
    # Remove spaces from names
    if isinstance(names, str):
        names = names.replace(" ", "")  # Remove all spaces
    elif isinstance(names, list):
        names = [s.replace(" ", "") for s in names]  # Remove spaces in each list element
 
    # Handle case where all sensors in a group want to be returned
    if not names:  # Check if names is empty or None
        names = ''

    names = names
    # Ensure `names` is a list of strings
    if isinstance(names, list):
        # Create a comma-separated string of quoted names
        names_str = ",".join(f"'{name}'" for name in names)
    else:
        # If `names` is already a string, ensure it is properly quoted
        names_str = f"'{names}'"
        
  
    """
    Connect to PostgreSQL database and retrieve data.
    """
    # Database connection parameters
    db_config = load_config(filename='database.ini', section='postgresql_vu')

    try:
        # Establish connection
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # GetSensor_id by site_id and sensor_group
        site_id = get_site_id_by_name(cursor, site)
        
        # Get sensor_id and unit_id
        sensor_info = get_sensorinfo_by_siteid_and_sensorname(cursor, site_id, names_str)
        
        if not sensor_info:
            print(f"No sensor information found for site '{site}'.")
            return None, None
        
        # Get sensor_ids
        sensor_ids = [row['sensor_id'] for row in sensor_info]
        
        # Get the data
        df = get_data(cursor, sensor_ids, start_dt, end_dt)           
        if df is None:
            return None, None
        
        # Convert 'dt' to datetime for proper handling
        df['dt'] = pd.to_datetime(df['dt'], utc=True).dt.tz_convert(tz)

        # Pivot the DataFrame
        data_df = df.pivot(index='dt', columns='logicid', values='value')

        # Add columns that are not in the data_df but are in the sensor_info
        for row in sensor_info:
            logicid = row['sensor_id']
            if logicid not in data_df.columns:
                # Create a new column with NaN values
                data_df[logicid] = np.nan

        # Put the columns in the same order as in sensor_info
        data_df = data_df[sensor_ids]

        # Reset the column names (optional, to remove the MultiIndex)
        data_df.columns.name = None
        
        # Make index of data_df into a column
        data_df.reset_index(inplace=True)
        
        # Rename the first column to 'datetime'
        data_df.rename(columns={data_df.columns[0]: 'datetime'}, inplace=True)
        
        # Create a dictionary mapping sensor_id to sensor_name from sensor_info
        sensor_id_to_name = {row['sensor_id']: row['sensor_name'] for row in sensor_info}

        # Rename the columns in data_df using the mapping
        data_df.rename(columns=sensor_id_to_name, inplace=True)

        # Make a df with the sensor names as columns and the first row the units and the second row the aggregation method
        # Create a new DataFrame with the same columns as data_df and two rows
        df_header = pd.DataFrame(index=['units', 'aggmethod'], columns=data_df.columns)
        # Fill the new DataFrame with the units and aggregation methods
        for i, row in enumerate(sensor_info):
            df_header.iloc[0, i+1] = row['sensor_units']
            df_header.iloc[1, i+1] = row['aggmethod']

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None, None

    finally:
        if conn:
            cursor.close()
            conn.close()
            
    return data_df, df_header
  
  
  
  
def download_and_append_vu_data(path, varfile, start_dt, end_dt, site, tz):
    """
    Download and append VU data, handling column names, units, and aggregation.
    """
    file = f"{path}/{site}.csv"
    
    # Fix the start and end datetime strings
    start_dt, end_dt = fix_start_end_dt(start_dt, end_dt, file, tz)
    
    if start_dt is None or end_dt is None:
        return None
        
    # Readin variables
    selected_variables = select_variables(varfile, site)

    # Download the data (placeholder for actual implementation)
    selected_variables_db = selected_variables['varname_db'].tolist()
    
    # Make output directory if it does not exist
    if not os.path.exists(path):
        os.makedirs(path)
    
    # Load the data from the database
    print(f"Loading data from {site} from {start_dt} to {end_dt}...")
    sensor_data, header_data = load_biomet_vudb(site = site,
                                  names = selected_variables_db,
                                  # Comment out sensor type to get all data; HEFL 1 en HEFL 2?
                                  start_dt= start_dt,
                                  end_dt = end_dt, 
                                  tz = tz)
    if sensor_data is None or header_data is None:
        return None

    # Convert selected_variables to a dictionary for renaming columns
    selected_variables_dict = selected_variables.set_index('varname_db')['variable_name'].to_dict()
    
    # Rename the columns in sensor_data using the mapping
    sensor_data.rename(columns=selected_variables_dict, inplace=True)
    
    # Rename the columns in header_data using the mapping
    header_data.rename(columns=selected_variables_dict, inplace=True)

    # print(sensor_data)                  
    # if file exists
    if os.path.exists(file):
        
        # Append the new data to the file add enter at the if needed
        sensor_data.to_csv(file, mode='a', header=False, index=False, na_rep='NA')

    else:
        # Write the new data to a new file the header is written first
        # the header contains 3 lines with the column names, units and aggregation
        # the first column is the datetime column
        with open(file, 'w') as f:

            # Write the header
            header_data.to_csv(f, index=False, header=True, na_rep='NA')
            
            # Write the data
            sensor_data.to_csv(f, index=False, header=False, na_rep='NA')

if __name__ == "__main__":

    sites=['ALB_MS', 'ALB_RF','ZEG_RF','MOB_X1','MOB_X2','LAW_MS','DEM_NT','ASD_MP','ANK_PT','ZEG_MP','ZEG_PT','ILP_PT','WRW_SR','WRW_OW','ZEG_MOB']
    
    # Loop through the sites and download the data for each site
    # Meteo
    for site in sites:
        print(f"\nDownloading data for {site}...")
        download_and_append_vu_data(
            path="./data/",
            varfile="./vars_meteo.csv",
            start_dt="2019-01-01",
            end_dt=datetime.now().strftime("%Y-%m-%d"),
            site=site,
            tz="Etc/GMT-1"
        )
    
    ndays = 1
    end_dt = datetime.now().strftime("%Y-%m-%d"),    
    start_dt = (datetime.now() - pd.DateOffset(days=ndays)).strftime("%Y-%m-%d")
    site = 'ALB_RF'
    sensor_data, header_data = load_biomet_vudb(site = site,
                            names = "MT1_SWIN_1_H_180",
                            # Comment out sensor type to get all data; HEFL 1 en HEFL 2?
                            start_dt= start_dt,
                            end_dt = end_dt,
                            tz = "Etc/GMT-1")