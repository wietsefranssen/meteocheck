import plotly.graph_objects as go
import plotly.colors

# for sensor in sensor_names:
def make_figure(data_df, sensorinfo_df, sensor_groups, sensor, x_range=None, y_range=None):
    # Assign colors dynamically using Plotly's color sequence
    sitenames = sorted(sensorinfo_df['site_name'].unique())
    color_seq = plotly.colors.qualitative.Plotly
    sensor_color_map = {sensor: color_seq[i % len(color_seq)] for i, sensor in enumerate(sitenames)}

    downsample_factor = 30  # Adjust this value to control the downsampling rate
    # shared_x = data_df.index[::downsample_factor]

    # Create a mapping from sensor_id to various attributes
    # sensor_id_to_fullname = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['fullname']))
    sensor_id_to_unit = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['unit']))
    sensor_id_to_sensor_name = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['sensor_name']))
    sensor_id_to_site_name = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['site_name']))
    sensor_id_to_variable_name = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['variable_name']))
    sensor_id_to_source = dict(zip(sensorinfo_df['sensor_id'], sensorinfo_df['source']))

    shared_x = data_df.index[::downsample_factor]
    # shared_x = data_df.index[::downsample_factor]
    data_df = data_df[::downsample_factor]

    fig = go.Figure()
    # Get the sensor_ids for the current sensor group
    sensor_ids = sensor_groups[sensor]
    
    # Sort sensor_ids based on site_name
    sensor_ids = sorted(
        sensor_ids,
        key=lambda sensor_id: sensor_id_to_site_name.get(sensor_id, "")
    )
# data_df = data_df[sorted_columns]
    for sensor_id in sensor_ids:
        # unit = units[line]
        # data_df[sensor_id].min()
        unit = sensor_id_to_unit.get(sensor_id, "")
#         fullname = sensor_id_to_fullname.get(sensor_id, "")
        sitename = sensor_id_to_site_name.get(sensor_id, "")
        sensor_name = sensor_id_to_sensor_name.get(sensor_id, "")
        var_name = sensor_id_to_variable_name.get(sensor_id, "")
        source = sensor_id_to_source.get(sensor_id, "")
        fig.add_trace(
            go.Scattergl(
                x=shared_x,  # Plot every 10th point for performance
                # x=data_df.index,
                y=data_df[sensor_id],
                mode='markers+lines',
                name=f"{sitename} ({source}: {sensor_name})",
                line=dict(width=1,
                          color=sensor_color_map[sitename]),
                marker=dict(size=3,
                            color=sensor_color_map[sitename])  # Adjust marker size for better visibility
            )
        )
    fig.update_layout(
        # title=f"{sensor.capitalize()} Data",
        title=f"{var_name}",
        # xaxis_title="timedate",
        yaxis_title=unit if unit else "Value",
        legend_title="Measurement",
        # legend=dict(
        #     orientation="h",
        #     yanchor="bottom",
        #     y=-0.6,      # Move legend below the plot
        #     xanchor="center",
        #     x=0.5
        # )
    )

    if x_range:
        fig.update_xaxes(range=x_range)
    else:
        fig.update_xaxes(autorange=True)


    # # Set y_range to the min and max of all sensor_ids in the current sensor group
    # y_range = [data_df[sensor_ids].min().min(), data_df[sensor_ids].max().max()]
    # if y_range:
    #     # print(f"Setting y-axis range for {var_name}: {y_range}")
    #     print(f"Setting y-axis: {y_range}"  )
    #     fig.update_yaxes(autorange=False)
    #     fig.update_yaxes(range=y_range)
    # else:
    #     print(f"No y-axis range set for {var_name}, using autorange")
    #     fig.update_yaxes(autorange=True)

    return fig
