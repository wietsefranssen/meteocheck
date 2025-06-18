from functions_general import fix_start_end_dt, get_stations_table, get_check_table_db
from functions_db import get_siteids_vu, get_data_vu, get_sensorinfo_vu, get_sensorinfo_siteid_name_combo_vu, get_sensorinfo_by_site_and_varname_vu
from functions_db import get_data_wur
from functions_plot import make_figure   

import pandas as pd
import numpy as np
import dash
from dash import dcc, html, Input, Output, State, ctx

# Set the start and end dates for the data retrieval
start_dt, end_dt = fix_start_end_dt(start_dt=(pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d'), 
                                    end_dt=pd.to_datetime('today').strftime('%Y-%m-%d'))

# Get the variables_table
stations_table = get_stations_table("stations.csv")

####### WUR DB DATA RETRIEVAL #######
# Get the check_table
check_table_wurdb = get_check_table_db(stations_table, source = 'wur_db')

# Get data from the database
sensor_info_df_wur, data_df_wur = get_data_wur(check_table_wurdb, start_dt, end_dt)

####### VU DB DATA RETRIEVAL #######
# Get the check_table
check_table_vudb = get_check_table_db(stations_table, source = 'vu_db')

# Get data from the database
sensor_info_df_vu, data_df_vu = get_data_vu(check_table_vudb, start_dt, end_dt)

####### Combine VU and WUR data #######
# Combine the two DataFrames
pivoted_df = pd.concat([data_df_wur, data_df_vu], axis=1)

# Combine the two sensor_info DataFrames
sensor_info_df = pd.concat([sensor_info_df_wur, sensor_info_df_vu], ignore_index=True)

####### Group the data into parts to separate plots #######
# make groups of sensor_ids by variable_name
sensor_groups = sensor_info_df.groupby('variable_name')['sensor_id'].apply(list).to_dict()

# # make groups of sensor_ids by variable_name and source
# sensor_groups = sensor_info_df.groupby(['variable_name', 'source'])['sensor_id'].apply(list).to_dict()

# # make groups of sensor_ids by variable_name and source. I the group reaches more than 6 sensor_ids, split it into smaller groups
# sensor_groups = {}
# for (var_name, source), group in sensor_info_df.groupby(['variable_name', 'source']):
#     sensor_ids = group['sensor_id'].tolist()
#     # Split into smaller groups if the number of sensor_ids is greater than 6
#     for i in range(0, len(sensor_ids), 6):
#         group_name = f"{var_name} ({source},{i}/{len(sensor_ids)})"
#         if group_name not in sensor_groups:
#             sensor_groups[group_name] = []
#         sensor_groups[group_name].extend(sensor_ids[i:i+6])
  
# List of sensor names
sensor_names = list(sensor_groups.keys())

# Number of sensor names/subplots
nfigs = len(sensor_names)

####### Make dash app #######
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Store(id='zoom-store', data={'x_range': None}),
    html.Div([
        dcc.Graph(id=f'graph-{i}', figure=make_figure(pivoted_df, sensor_info_df, sensor_groups, sensor_names[i]))
        for i in range(nfigs)
    ])
])

# @app.callback(
#     Output('zoom-store', 'data'),
#     [Input(f'graph-{i}', 'relayoutData') for i in range(nfigs)],
#     State('zoom-store', 'data'),
#     prevent_initial_call=True
# )
# def update_zoom_store(*args):
#     relayout_datas = args[:-1]
#     store = args[-1] or {}
#     x_range = store.get('x_range')
#     new_x_range = x_range

#     # Use dash.callback_context (ctx) to find which graph triggered
#     triggered = ctx.triggered_id
#     if triggered is None:
#         return dash.no_update

#     # Extract the index from the triggered id
#     if triggered.startswith('graph-'):
#         idx = int(triggered.split('-')[1])
#         relayout = relayout_datas[idx]
#         if relayout:
#             # Zoom in
#             if 'xaxis.range[0]' in relayout and 'xaxis.range[1]' in relayout:
#                 new_x_range = [relayout['xaxis.range[0]'], relayout['xaxis.range[1]']]
#             elif 'xaxis' in relayout and 'range' in relayout['xaxis']:
#                 new_x_range = relayout['xaxis']['range']
#             # Double-click zoom out
#             elif relayout.get('xaxis.autorange') is True:
#                 new_x_range = None

#     # Only update if the range actually changed
#     if new_x_range != x_range:
#         return {'x_range': new_x_range}
#     else:
#         return dash.no_update

# @app.callback(
#     [Output(f'graph-{i}', 'figure') for i in range(nfigs)],
#     Input('zoom-store', 'data')
# )
# def update_graphs(zoom_data):
#     x_range = zoom_data.get('x_range') if zoom_data else None
#     return [make_figure(pivoted_df, sensor_info_df, sensor_groups, sensor_names[i], x_range) for i in range(nfigs)]

if __name__ == "__main__":
    app.run_server(debug=True)