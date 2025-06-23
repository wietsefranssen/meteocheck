import os
import pandas as pd

from src.general import fix_start_end_dt, adapt_start_dt_to_existing_dataset
from src.general import get_check_table
from src.db import get_data
 
if __name__ == "__main__":

    start_dt_init=(pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d')
    start_dt_init="2019-01-01"
    start_dt_init="2025-06-01"
    end_dt_init=pd.to_datetime('today').strftime('%Y-%m-%d')
    tz="Etc/GMT-1"
    check_table_filename='check_table_base.csv'
    path="./data_full"
    
    # Get the variables_table
    check_table = get_check_table(filename=check_table_filename)

    # Loop through the stations and get the data
    for station in check_table['Station'].unique():
        print(f"\nDownloading data for {station}...")
        
        file = f"{path}/{station}.csv"
        
        # Select the current station from the check_table
        check_table_tmp = check_table[check_table['Station'] == station]
        
        limit = 100000
        # Loop till data_df is None or empty
        while True:
            # Fix the start and end datetime strings
            start_dt, end_dt = fix_start_end_dt(start_dt_init, end_dt_init, tz)

            # Adapt start_dt to last line in existing dataset/file
            start_dt = adapt_start_dt_to_existing_dataset(start_dt, end_dt, file, tz)
            print(f"Start date set to: {start_dt}. End date set to: {end_dt}.")

            # If 'source' is 'vu_db', get the check_table for the vu_db
            if check_table_tmp['source'].values[0] == 'vu_db':
                # Get data from the database
                sensorinfo_df, data_df = get_data(check_table_tmp, start_dt, end_dt, source='vu_db', limit=limit)
            elif check_table_tmp['source'].values[0] == 'wur_db':    
                # Get data from the database
                sensorinfo_df, data_df = get_data(check_table_tmp, start_dt, end_dt, source='wur_db', limit=limit)
            else:
                print(f"Unknown source for station {station}. Skipping...")
                continue
            # If data_df is not None and not empty, break the loop
            if data_df is None or data_df.empty:
                break
            
            if sensorinfo_df is None or data_df is None:
                print(f"No data found for {station} in the specified date range.")
                continue

            # Make header data 
            # Line 1: first column is datetime, the rest are the variable names
            header_data = pd.DataFrame(columns=['datetime'] + sensorinfo_df['sensor_name'].tolist())
            # Line 2: first column is '-', the rest are the units of the variables
            header_data.loc[0] = ['-'] + sensorinfo_df['unit'].tolist()
            
            ##### TODO: be sure that the order of columns in header_data matches the order of columns in data_df
            
            ##### TODO: be sure that the order and the number of columns in the file matches the order of columns in header_data and data_df
            
            # if file exists
            if os.path.exists(file):
                
                # Append the new data to the file add enter at the if needed
                data_df.to_csv(file, mode='a', header=False, index=True, na_rep='NA')
            else:
                # Write the new data to a new file the header is written first
                # the header contains 3 lines with the column names, units and aggregation
                # the first column is the datetime column
                with open(file, 'w') as f:

                    # Write the header
                    header_data.to_csv(f, index=False, header=True, na_rep='NA')
                    
                    # Write the data
                    data_df.to_csv(f, index=True, header=False, na_rep='NA')

            
                        
        # # If 'source' is 'vu_db', get the check_table for the vu_db
        # if check_table_tmp['source'].values[0] == 'vu_db':
        #     # Get data from the database
        #     sensorinfo_df, data_df = get_data(check_table_tmp, start_dt, end_dt, source='vu_db', limit=limit)
        # elif check_table_tmp['source'].values[0] == 'wur_db':    
        #     # Get data from the database
        #     sensorinfo_df, data_df = get_data(check_table_tmp, start_dt, end_dt, source='wur_db', limit=limit)
        # else:
        #     print(f"Unknown source for station {station}. Skipping...")
        #     continue
        # if sensorinfo_df is None or data_df is None:
        #     print(f"No data found for {station} in the specified date range.")
        #     continue
        
        # # Make header data 
        # # Line 1: first column is datetime, the rest are the variable names
        # header_data = pd.DataFrame(columns=['datetime'] + sensorinfo_df['sensor_name'].tolist())
        # # Line 2: first column is '-', the rest are the units of the variables
        # header_data.loc[0] = ['-'] + sensorinfo_df['unit'].tolist()
        
        # ##### TODO: be sure that the order of columns in header_data matches the order of columns in data_df
        
        # ##### TODO: be sure that the order and the number of columns in the file matches the order of columns in header_data and data_df
        
        # # if file exists
        # if os.path.exists(file):
            
        #     # Append the new data to the file add enter at the if needed
        #     data_df.to_csv(file, mode='a', header=False, index=True, na_rep='NA')
        # else:
        #     # Write the new data to a new file the header is written first
        #     # the header contains 3 lines with the column names, units and aggregation
        #     # the first column is the datetime column
        #     with open(file, 'w') as f:

        #         # Write the header
        #         header_data.to_csv(f, index=False, header=True, na_rep='NA')
                
        #         # Write the data
        #         data_df.to_csv(f, index=True, header=False, na_rep='NA')
