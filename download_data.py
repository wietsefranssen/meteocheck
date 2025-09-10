from curses.ascii import BS
from src.data_processing import create_pivot_table, create_pivot_table_reason
# from src.table import get_cell_values_and_colors, get_datatable #, generate_color_rules_and_css
from data_manager import DataManager
import polars as pl
import pandas as pd
import numpy as np
import os

def download_data():

    basepath = os.path.dirname(os.path.abspath(__file__))
    dm = DataManager()
    dm.set_meta_path(os.path.join(basepath, 'meta'))
    dm.set_data_path(os.path.join(basepath, 'data'))
    dm.set_temp_path(os.path.join(basepath, 'temp'))

    dm.set_dates(days_back=7, offset=1)
    # dm.set_dates(start_dt='2025-07-25', end_dt='2025-08-01 23:59:00')
    print(f"start_dt: {dm.start_dt}, end_dt: {dm.end_dt}")

    # dm.set_load_from_disk(True)

    dm.download_or_load_data()
    data_df, sensorinfo_df = dm.get_data()


    # # Detect and correct air pressure sensors with wrong units
    # incorrect_sensors = find_incorrect_airpressure_sensors(sensorinfo_df, data_df)
    # if incorrect_sensors:
    #     print("Correcting air pressure sensors:", incorrect_sensors)
    #     data_df, sensorinfo_df = correct_airpressure_units(data_df, sensorinfo_df, incorrect_sensors)

    
    

if __name__ == "__main__":
    download_data()
