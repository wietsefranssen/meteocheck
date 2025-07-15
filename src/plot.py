import plotly.graph_objects as go
import plotly.colors
import polars as pl
import pandas as pd
print(plotly.__version__)

# for sensor in sensor_names:
def make_figure(data_df, sensorinfo_df, sensor_groups, sensor, x_range=None, y_range=None):
    # Handle Polars DataFrames
    if hasattr(sensorinfo_df, 'to_pandas'):  # It's Polars
        # Convert to pandas for easier operations
        sensorinfo_pd = sensorinfo_df.to_pandas()
        sitenames = sorted(sensorinfo_pd['site_name'].unique())
    else:
        # It's already pandas
        sensorinfo_pd = sensorinfo_df
        sitenames = sorted(sensorinfo_df['site_name'].unique())
    
    # Assign colors dynamically using Plotly's color sequence
    color_seq = plotly.colors.qualitative.Plotly
    sensor_color_map = {sensor: color_seq[i % len(color_seq)] for i, sensor in enumerate(sitenames)}

    downsample_factor = 30  # Adjust this value to control the downsampling rate

    # Create a mapping from sensor_id to various attributes using pandas
    sensor_id_to_unit = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['unit']))
    sensor_id_to_sensor_name = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['sensor_name']))
    sensor_id_to_site_name = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['site_name']))
    sensor_id_to_variable_name = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['variable_name']))
    sensor_id_to_source = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['source']))
    sensor_id_to_longname = dict(zip(sensorinfo_pd['sensor_id'], sensorinfo_pd['long_name']))

    # Handle data_df - convert to pandas for plotting
    if hasattr(data_df, 'to_pandas'):  # It's Polars
        data_pd = data_df.to_pandas()
        if 'datetime' in data_pd.columns:
            data_pd = data_pd.set_index('datetime')
    else:
        # It's already pandas
        data_pd = data_df

    # Downsample the data
    shared_x = data_pd.index[::downsample_factor]
    data_pd_downsampled = data_pd[::downsample_factor]

    fig = go.Figure()
    
    # Get the sensor_ids for the current sensor group
    sensor_ids = sensor_groups[sensor]
    
    # Sort sensor_ids based on site_name and convert to strings
    sensor_ids_str = [str(sid) for sid in sensor_ids]
    sensor_ids_str = sorted(
        sensor_ids_str,
        key=lambda sensor_id: sensor_id_to_site_name.get(int(sensor_id) if sensor_id.isdigit() else sensor_id, "")
    )

    for sensor_id_str in sensor_ids_str:
        # Convert back to original type for lookup
        sensor_id = int(sensor_id_str) if sensor_id_str.isdigit() else sensor_id_str
        
        # Check if column exists in data
        if sensor_id_str not in data_pd_downsampled.columns:
            print(f"Warning: Sensor {sensor_id_str} not found in data columns")
            continue
            
        # Get metadata
        unit = sensor_id_to_unit.get(sensor_id, "")
        sitename = sensor_id_to_site_name.get(sensor_id, "")
        sensor_name = sensor_id_to_sensor_name.get(sensor_id, "")
        var_name = sensor_id_to_variable_name.get(sensor_id, "")
        long_name = sensor_id_to_longname.get(sensor_id, "")
        source = sensor_id_to_source.get(sensor_id, "")
        
        # Add trace
        fig.add_trace(
            go.Scattergl(
                x=shared_x,
                y=data_pd_downsampled[sensor_id_str],
                mode='markers+lines',
                name=f"{sitename} ({source}: {sensor_name})",
                line=dict(width=1, color=sensor_color_map.get(sitename, '#1f77b4')),
                marker=dict(size=3, color=sensor_color_map.get(sitename, '#1f77b4'))
            )
        )
    
    # Get variable name and long name for title (use first sensor)
    if sensor_ids:
        first_sensor = sensor_ids[0]
        var_name = sensor_id_to_variable_name.get(first_sensor, sensor)
        long_name = sensor_id_to_longname.get(first_sensor, "")
        unit = sensor_id_to_unit.get(first_sensor, "")
    else:
        var_name = sensor
        long_name = ""
        unit = ""
    
    fig.update_layout(
        title=f"{var_name} - {long_name}",
        yaxis_title=unit if unit else "Value",
        legend_title="Measurement",
    )

    if x_range:
        fig.update_xaxes(range=x_range)
    else:
        fig.update_xaxes(autorange=True)

    # # Set y_range to the min and max of all sensor_ids in the current sensor group
    # try:
    #     # Get valid sensor columns that exist in data
    #     valid_sensors = [str(sid) for sid in sensor_ids if str(sid) in data_pd_downsampled.columns]
    #     if valid_sensors:
    #         y_data = data_pd_downsampled[valid_sensors]
    #         y_min = y_data.min().min()
    #         y_max = y_data.max().max()
            
    #         if not (pd.isna(y_min) or pd.isna(y_max)):
    #             # Add some padding
    #             y_range_calc = [y_min * 0.95, y_max * 1.05]
    #             print(f"Setting y-axis range for {var_name}: {y_range_calc}")
    #             fig.update_yaxes(range=y_range_calc, autorange=False)
    #         else:
    #             print(f"No valid y-axis range for {var_name}, using autorange")
    #             fig.update_yaxes(autorange=True)
    #     else:
    #         print(f"No valid sensors found for {var_name}, using autorange")
    #         fig.update_yaxes(autorange=True)
    # except Exception as e:
    #     print(f"Error setting y-axis range for {var_name}: {e}, using autorange")
    #     fig.update_yaxes(autorange=True)

    return fig
