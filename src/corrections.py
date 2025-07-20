import polars as pl

### Fix PAIR units for selected Stations ###
# The PAIR variable is in mbar, but we want it in Pa for consistency with other variables.
# The conversion is done by multiplying the values by 10.
# This is only done for the selected sensor_ids, which are defined in sel_names.
def fix_pair_units(sensorinfo_df, data_df, station_names):
    """
    Fix the units of the PAIR variable in the sensorinfo_df and data_df.
    The PAIR variable is in mbar, but we want it in Pa for consistency with other variables.
    The conversion is done by multiplying the values by 1000.
    """
    # Select sensor_ids from the sensorinfo_df with variable_name 'PAIR'
    sensorinfo_df_sel = sensorinfo_df[sensorinfo_df['variable_name'] == 'PAIR']
    
    # Multiply the values in data_df by 10 for the selected sensor_ids
    data_df[sensorinfo_df_sel['sensor_id'].tolist()] *= -10000
    
    # Set values below 1000 to NaN
    data_df[sensorinfo_df_sel['sensor_id'].tolist()] = data_df[sensorinfo_df_sel['sensor_id'].tolist()].where(
        data_df[sensorinfo_df_sel['sensor_id'].tolist()] >= 1000, other=pd.NA
    )
    return data_df

# if select sensor_ids from the sensorinfo_df with variable_name 'PAIR' and site_name 'GOB_44_MT', 'GOI_38_MT', 'GOB_45_MT'
sel_names = ['GOB_44_MT', 'BUO_31_MT', 'BUW_32_MT', 'HOH_33_MT', 'HOC_34_MT', 'LDH_35_MT',
 'LDC_36_MT', 'AMM_37_MT', 'POH_39_MT', 'POG_40_MT', 'HOD_41_MT', 'MOB_42_MT',
 'HEW_43_MT', 'HEH_42_MT', 'MOB_01_MT', 'MOB_02_MT', 'MOB_21_EC', 'GOI_38_MT',
 'WIE_41_MT', 'ONL_22_MT', 'CAM_21_MT', 'BPB_31_MT', 'BPC_32_MT', 'PPA_42_MT',
 'BRO_43_MT', 'BLO_36_MT', 'BLR_35_MT', 'HWG_37_MT', 'HWR_34_MT',
 'HWN_45_MT', 'HWH_46_MT', 'WRW_SR', 'ZEG_PT']

sel_names = ['GOB_44_MT', 'BUO_31_MT', 'BUW_32_MT', 'HOH_33_MT', 'HOC_34_MT', 'LDH_35_MT',
 'LDC_36_MT', 'AMM_37_MT', 'POH_39_MT', 'POG_40_MT', 'HOD_41_MT', 'MOB_42_MT',
 'HEW_43_MT', 'HEH_42_MT', 'MOB_01_MT', 'MOB_02_MT', 'MOB_21_EC', 'GOI_38_MT',
 'WIE_41_MT', 'ONL_22_MT', 'CAM_21_MT', 'BPB_31_MT', 'BPC_32_MT', 'PPA_42_MT',
 'BRO_43_MT', 'BLO_36_MT', 'BLR_35_MT', 'HWG_37_MT', 'HWR_34_MT',
 'HWN_45_MT', 'HWH_46_MT', 'WRW_SR', 'ZEG_PT']

sel_names = ['PPA_42_MT', 'BRO_43_MT', 'MOB_42_MT', 'CAM_21_MT', 'ONL_22_MT']
# sel_names = ['ZEG_PT', 'ZEG_RF', 'ALB_RF',
#  'LAW_MS']

def correct_airpressure_units(data_df, sensorinfo_df, sensor_ids):
    """
    Multiplies the values of the given sensor_ids by 10 in data_df and updates the unit in sensorinfo_df.
    """
    for sensor in sensor_ids:
        sensor_str = str(sensor)
        if sensor_str in data_df.columns:
            # Update data_df using Polars
            data_df = data_df.with_columns([
                (pl.col(sensor_str) * 10).alias(sensor_str)
            ])
        
        if 'unit' in sensorinfo_df.columns:
            # Update sensorinfo_df using Polars
            sensorinfo_df = sensorinfo_df.with_columns([
                pl.when(pl.col('sensor_id') == sensor)
                .then(pl.lit('hPa'))
                .otherwise(pl.col('unit'))
                .alias('unit')
            ])
    return data_df, sensorinfo_df

def find_incorrect_airpressure_sensors(sensorinfo_df, data_df, threshold=200):
    """
    Returns a list of sensor_ids where air pressure values are likely a factor 10 too low.
    Also checks the unit in sensorinfo_df if available.
    """
    # Find air pressure sensors using Polars
    airpressure_sensors = sensorinfo_df.filter(
        pl.col('variable_name').str.contains('PAIR')
    )['sensor_id'].to_list()
    
    incorrect_sensors = []
    for sensor in airpressure_sensors:
        sensor_str = str(sensor)
        if sensor_str in data_df.columns:
            # Get values using Polars
            vals = data_df.select(sensor_str).to_series().drop_nulls()
            
            # Check for low median or wrong unit
            sensor_info = sensorinfo_df.filter(pl.col('sensor_id') == sensor)
            if sensor_info.height > 0 and 'unit' in sensorinfo_df.columns:
                unit = sensor_info['unit'].item()
            else:
                unit = 'hPa'
                
            if (vals.len() > 0 and vals.median() < threshold) or (unit not in ['hPa', 'hpa']):
                incorrect_sensors.append(sensor)
    return incorrect_sensors

