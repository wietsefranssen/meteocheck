import os
import pandas as pd
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
        self.data_df_file = os.path.join(data_path, 'data.pkl')
        self.sensorinfo_df_file = os.path.join(data_path, 'sensorinfo.pkl')
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
        # self.set_dates()
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
        if download_data:
            self.sensorinfo_df, self.data_df = get_data_from_db(
                start_dt=self.start_dt,
                end_dt=self.end_dt,
                check_table_filename=self.check_table_filename
            )
            self.data_df.to_pickle(self.data_df_file)
            self.sensorinfo_df.to_pickle(self.sensorinfo_df_file)
        else:
            self.data_df = pd.read_pickle(self.data_df_file)
            self.sensorinfo_df = pd.read_pickle(self.sensorinfo_df_file)
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

# Usage in your main script:
# from data_manager import DataManager
# dm = DataManager()
# dm.download_or_load_data()
# data_df, sensorinfo_df = dm.get_data()