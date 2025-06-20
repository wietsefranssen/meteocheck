import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, ctx

from functions_general import fix_start_end_dt
from functions_db import get_data_from_db
from functions_plot import make_figure   

print(dash.__version__)
# Set the start and end dates for the data retrieval
check_table_filename='check_table_base.csv'

start_dt, end_dt = fix_start_end_dt(start_dt=(pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d'), 
                                    end_dt=pd.to_datetime('today').strftime('%Y-%m-%d'))


# read check_table_filename
check_table = pd.read_csv(check_table_filename)

lastrun_info_path = 'lastrun_info'
lastrun_info_file = 'last_run_config.txt'
data_df_file = 'data.pkl'
sensorinfo_df_file = 'sensorinfo.pkl'    

# Create the directory if it doesn't exist
import os
if not os.path.exists(lastrun_info_path):
    os.makedirs(lastrun_info_path)

# Save the start and end dates to a text file
lastrun_info_file = os.path.join(lastrun_info_path, lastrun_info_file)
# Save the start and end dates to a text file
with open(lastrun_info_file, 'w') as f:
    f.write(f"Start date: {start_dt}\n")
    f.write(f"End date: {end_dt}\n")
# Save the check_table to a text file
check_table_file = os.path.join(lastrun_info_path, 'check_table.txt')
with open(check_table_file, 'w') as f:
    f.write(check_table.to_string(index=False))

# Read start and end dates from the text file
with open(lastrun_info_file, 'r') as f:
    lines = f.readlines()
    start_dt_retrieved = lines[0].strip().split(': ')[1]
    end_dt_retrieved = lines[1].strip().split(': ')[1]
    
    # Convert the retrieved dates to datetime objects for comparison (format: 2025-06-13 00:00:00+00:00)
    start_dt_retrieved = pd.to_datetime(start_dt_retrieved)
    end_dt_retrieved = pd.to_datetime(end_dt_retrieved)
    # compare the retrieved start and end dates with the original start and end dates
if start_dt == start_dt_retrieved and end_dt == end_dt_retrieved:
    print("Start and end dates retrieved successfully and match the original dates.")
    dates_match = True
else:
    print("Start and/or end dates retrieved do not match the original dates. There might be an issue with the retrieval process.")
    dates_match = False

# Read check_table from the text file and compare it with the original check_table
check_table_retrieved = pd.read_csv(check_table_file, sep=r'\s+')
# Compare the retrieved check_table with the original check_table
if check_table.equals(check_table_retrieved):
    print("Check table retrieved successfully and matches the original check table.")
    check_table_match = True
else:
    print("Check table retrieved does not match the original check table. There might be an issue with the retrieval process.")
    check_table_match = False


# If the dates and check_table match, read the data and sensorinfo from the pickle files
data_df_file = os.path.join(lastrun_info_path, data_df_file)
sensorinfo_df_file = os.path.join(lastrun_info_path, sensorinfo_df_file)
# read pickle file data_df_file

if os.path.exists(data_df_file) and os.path.exists(sensorinfo_df_file):
    print(f"Data files found: {data_df_file} and {sensorinfo_df_file}")
    download_data = False
else:
    print(f"Data files not found: {data_df_file} and {sensorinfo_df_file}. Proceeding to fetch data from the database.")
    download_data = True
    

if download_data:
    print("Dates or check table do not match. Proceeding to fetch data from the database.")
    sensorinfo_df, data_df = get_data_from_db(start_dt=start_dt, end_dt=end_dt, check_table_filename=check_table_filename)

    # Save the data to CSV files and save a pickle file for faster loading next time
    # data_df.to_csv('data.csv', index=True)
    # sensorinfo_df.to_csv('sensorinfo.csv', index=True)
    data_df.to_pickle(data_df_file)
    sensorinfo_df.to_pickle(sensorinfo_df_file)
else:
    data_df = pd.read_pickle(data_df_file)
    sensorinfo_df = pd.read_pickle(sensorinfo_df_file) 







####### Group the data into parts to separate plots #######
# make groups of sensor_ids by variable_name
sensor_groups = sensorinfo_df.groupby('variable_name')['sensor_id'].apply(list).to_dict()

# # make groups of sensor_ids by variable_name and source
# sensor_groups = sensorinfo_df.groupby(['variable_name', 'source'])['sensor_id'].apply(list).to_dict()

# # make groups of sensor_ids by variable_name and source. I the group reaches more than 6 sensor_ids, split it into smaller groups
# sensor_groups = {}
# for (var_name, source), group in sensorinfo_df.groupby(['variable_name', 'source']):
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

# show per group the sensor_ids which contain no data
for group_name, sensor_ids in sensor_groups.items():
    # Get the sensorinfo for the current group
    group_sensorinfo = sensorinfo_df[sensorinfo_df['sensor_id'].isin(sensor_ids)]
    # Get the data for the current group
    group_data = data_df[group_sensorinfo['sensor_id'].tolist()]
    # Check if there are any NaN values in the data
    # if group_data.isnull().values.any():
    #     print(f"Group '{group_name}' has NaN values in the following sensor_ids: {group_sensorinfo[group_data.isnull().any(axis=0)]['sensor_id'].tolist()}")



# print min and max and average of data_df[23724]
# print(f"Min: {data_df[23724].min()}, Max: {data_df[23724].max()}, Avg: {data_df[23724].mean()}")


####### Make dash app #######
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Store(id='zoom-store', data={'x_range': None}),
    html.Div([
        dcc.Graph(id=f'graph-{i}', figure=make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i]))
        for i in range(nfigs)
    ])
    # ], style={'height': '1000px', 'overflowY': 'scroll'})
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
#     return [make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i], x_range) for i in range(nfigs)]

if __name__ == "__main__":
    app.run(debug=True)