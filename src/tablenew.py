import pandas as pd
import polars as pl

# Create data availability percentage table
def create_nan_percentage_table(data_df, sensorinfo_df, check_table):
    """Create a table showing percentage of available data (non-NaN) per location and sensor type"""
    # Calculate total number of data points
    total_rows = len(data_df)

    
    # Initialize results list
    results = []
    
    # Iterate through each station in check_table
    for _, row in check_table.iterrows():
        station = row['station']
        
        # Select sensor_ids from sensorinfo_df for this station
        sensorinfo_df_station = sensorinfo_df.filter(pl.col('site_name') == station)
        if sensorinfo_df_station.height == 0:
            print(f"Warning: No sensor info found for station {station}. Skipping.")
            continue
        
        # Create mapping from sensor_name to sensor_id
        sensor_name_to_id = {}
        for sensor_row in sensorinfo_df_station.iter_rows(named=True):
            sensor_name_to_id[sensor_row['sensor_name']] = str(sensor_row['sensor_id'])

        # Get all sensor columns for this station (excluding station and source columns)
        for var_name in check_table.columns[2:]:  # Skip 'station' and 'source' columns
            sensor_name = row[var_name]  # This is actually sensor_name from check_table
            
            if pd.isna(sensor_name) or sensor_name == '':
                # No sensor for this variable at this station
                data_availability = 0.0
                actual_sensor_id = ''
                reason = 'No_sensor'
            else:
                # Map sensor_name to sensor_id
                actual_sensor_id = sensor_name_to_id.get(sensor_name, '')
                
                if actual_sensor_id and actual_sensor_id in data_df.columns:
                    # Calculate data availability percentage (100 - NaN percentage)
                    sensor_data = data_df.select(pl.col(actual_sensor_id))

                    # Join datetime to sensor_data
                    sensor_data = data_df.select([pl.col("datetime"), pl.col(actual_sensor_id)])
                    # Filter out nulls
                    non_null_data = sensor_data.filter(pl.col(actual_sensor_id).is_not_null())

                    # Check if all minutes are 0 or 30
                    is_30min = non_null_data["datetime"].dt.minute().is_in([0, 30]).all()
                    if is_30min:
                        # Only count expected 30-min intervals
                        expected = sensor_data.filter(
                            pl.col("datetime").dt.minute().is_in([0, 30])
                        )
                        nan_count = expected.select(pl.col(actual_sensor_id).is_null()).sum().item()
                        data_availability = ((total_rows - nan_count) / total_rows) * 100
                        reason = 'Data_available'

                    else:
                        nan_count = sensor_data.select(pl.col(actual_sensor_id).is_null()).sum().item()
                        data_availability = ((total_rows - nan_count) / total_rows) * 100
                        reason = 'Data_available'

                else:
                    # Sensor not found in data
                    data_availability = 0.0
                    reason = 'Sensor_not_found'
            
            results.append({
                'Station': station,
                'Variable': var_name,
                'Sensor_Name': sensor_name if not pd.isna(sensor_name) else '',
                'Sensor_ID': actual_sensor_id,
                'NaN_Percentage': round(data_availability, 1),
                'Reason': reason,
            })

    return pd.DataFrame(results)
