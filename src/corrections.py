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

