import os
import pandas as pd
import polars as pl
from src.general import fix_start_end_dt
from src.db import get_data_from_db
from src.last_retrieval import (
    check_if_download_data_needed,
    add_extra_info_to_sensorinfo,
    save_last_retrieval_info
)

class DataManager:
    def __init__(
        self,
        data_path='data',
        meta_path='meta',
        temp_path='temp',
        days_back=6,
        offset=2,
        tz='UTC',
        start_dt=None,
        end_dt=None
    ):
        self.data_path = data_path
        self.meta_path = meta_path
        self.temp_path = temp_path
        self.data_df_file = os.path.join(data_path, 'data.parquet')
        self.sensorinfo_df_file = os.path.join(data_path, 'sensorinfo.parquet')
        self.check_table_filename = os.path.join(meta_path, 'check_table_base.csv')
        self.variable_info_file = os.path.join(meta_path, 'variables.csv')
        self.last_retrieval_info_file = os.path.join(temp_path, 'last_run_config.txt')
        self.last_retrieval_checktable_file = os.path.join(temp_path, 'check_table.txt')
        self.data_df = None
        self.sensorinfo_df = None
        self.check_table = None

        # Always use set_dates to initialize dates
        self.set_dates(
            start_dt=start_dt,
            end_dt=end_dt,
            days_back=days_back,
            offset=offset,
            tz=tz
        )

    def set_load_from_disk(self, load_from_disk=False):
        self.load_from_disk = load_from_disk

    def set_meta_path(self, path):
        self.meta_path = path
        self.check_table_filename = os.path.join(path, 'check_table_base.csv')
        self.variable_info_file = os.path.join(path, 'variables.csv')
        
    def set_data_path(self, path):
        self.data_path = path
        if not os.path.exists(path):
            os.makedirs(path)

        self.data_df_file = os.path.join(path, 'data.parquet')
        self.sensorinfo_df_file = os.path.join(path, 'sensorinfo.parquet')
        
    def set_temp_path(self, path):
        self.temp_path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.last_retrieval_info_file = os.path.join(path, 'last_run_config.txt')
        self.last_retrieval_checktable_file = os.path.join(path, 'check_table.txt')
        
    def set_dates(self, start_dt=None, end_dt=None, days_back=7, offset=2, tz='UTC'):
        """
        Set start and end dates as timezone-aware datetimes.
        You can provide explicit start_dt and end_dt, or use days_back/offset.
        """
        if start_dt is not None and end_dt is not None:
            start_dt = pd.to_datetime(start_dt)
            end_dt = pd.to_datetime(end_dt)
        else:
            today = pd.to_datetime('today')
            start_dt = (today - pd.DateOffset(days=days_back + offset)).strftime('%Y-%m-%d 00:00:00')
            end_dt = (today - pd.DateOffset(days=offset)).strftime('%Y-%m-%d 23:59:00')
            start_dt = pd.to_datetime(start_dt)
            end_dt = pd.to_datetime(end_dt)
        # Localize or convert to timezone
        if start_dt.tzinfo is None:
            start_dt = start_dt.tz_localize(tz)
        else:
            start_dt = start_dt.tz_convert(tz)
        if end_dt.tzinfo is None:
            end_dt = end_dt.tz_localize(tz)
        else:
            end_dt = end_dt.tz_convert(tz)
        self.start_dt = start_dt
        self.end_dt = end_dt
            
    def load_check_table(self):
        self.check_table = pd.read_csv(self.check_table_filename, sep=';')

    def download_or_load_data(self):
        self.load_check_table()
        download_data = check_if_download_data_needed(
            self.last_retrieval_info_file,
            self.start_dt,
            self.end_dt,
            self.last_retrieval_checktable_file,
            self.check_table,
            self.data_df_file,
            self.sensorinfo_df_file
        )
        if not self.load_from_disk:
            if download_data:
                self.sensorinfo_df, self.data_df = get_data_from_db(
                    start_dt=self.start_dt,
                    end_dt=self.end_dt,
                    check_table_filename=self.check_table_filename
            )
            # Save as Polars parquet files
            if self.data_df is not None and self.data_df.height > 0:
                self.data_df.write_parquet(self.data_df_file)
            if self.sensorinfo_df is not None and self.sensorinfo_df.height > 0:
                self.sensorinfo_df.write_parquet(self.sensorinfo_df_file)
        else:
            # Load as Polars DataFrames
            if os.path.exists(self.data_df_file):
                self.data_df = pl.read_parquet(self.data_df_file)
            else:
                self.data_df = pl.DataFrame()
            if os.path.exists(self.sensorinfo_df_file):
                self.sensorinfo_df = pl.read_parquet(self.sensorinfo_df_file)
            else:
                self.sensorinfo_df = pl.DataFrame()
                
        # Convert sensorinfo_df to pandas for add_extra_info_to_sensorinfo function
        if self.sensorinfo_df is not None and self.sensorinfo_df.height > 0:
            self.sensorinfo_df = add_extra_info_to_sensorinfo(self.sensorinfo_df, self.variable_info_file)
        
        save_last_retrieval_info(
            self.check_table,
            self.start_dt,
            self.end_dt,
            self.last_retrieval_info_file,
            self.last_retrieval_checktable_file
        )

    def get_data(self):
        return self.data_df, self.sensorinfo_df
    
    def is_check_table_value(self, site_name, variable_name):
        """
        Returns the value in check_table for the given site_name (station) and variable_name.
        Returns None if not found.
        """
        if self.check_table is None:
            self.load_check_table()
        # Find the row where 'station' == site_name
        row = self.check_table[self.check_table['station'] == site_name]
        if not row.empty and variable_name in self.check_table.columns[2:]:            
            result = row.iloc[0][variable_name]
            # Check if the result is not NaN or empty
            if pd.notna(result) and result != '':
                return True
            else:
                return False
        # If not found or result is NaN/empty, return None
        return None
