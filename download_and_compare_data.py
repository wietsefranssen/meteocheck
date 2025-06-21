import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, ctx, dash_table

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
check_table_file = os.path.join(lastrun_info_path, 'check_table.txt')

# Read start and end dates from the text file
if os.path.exists(lastrun_info_file):
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
else:
    dates_match = False

if os.path.exists(check_table_file):
    print(f"Check table file found: {check_table_file}")
    # Read check_table from the text file and compare it with the original check_table
    check_table_retrieved = pd.read_csv(check_table_file, sep=r'\s+')
    # Compare the retrieved check_table with the original check_table
    if check_table.equals(check_table_retrieved):
        print("Check table retrieved successfully and matches the original check table.")
        check_table_match = True
    else:
        print("Check table retrieved does not match the original check table. There might be an issue with the retrieval process.")
        check_table_match = False
else:
    print(f"Check table file not found: {check_table_file}. Proceeding to fetch data from the database.")
    check_table_match = False

# If the dates and check_table match, read the data and sensorinfo from the pickle files
data_df_file = os.path.join(lastrun_info_path, data_df_file)
sensorinfo_df_file = os.path.join(lastrun_info_path, sensorinfo_df_file)
# read pickle file data_df_file

if dates_match and check_table_match:
    if os.path.exists(data_df_file) and os.path.exists(sensorinfo_df_file):
        print(f"Data files found: {data_df_file} and {sensorinfo_df_file}")
        download_data = False
    else:
        print(f"Data files not found: {data_df_file} and {sensorinfo_df_file}. Proceeding to fetch data from the database.")
        download_data = True
else:
    print("Dates or check table do not match. Proceeding to fetch data from the database.")
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

# Read variable.csv file
variable_df = pd.read_csv('variables.csv', sep=';')
# Map variable from variable_df to variable_name in sensorinfo_df and add the long_name from variable_df to sensorinfo_df
sensorinfo_df['long_name'] = sensorinfo_df['variable_name'].map(variable_df.set_index('variable')['long_name'])

# Save the start and end dates to a text file
with open(lastrun_info_file, 'w') as f:
    f.write(f"Start date: {start_dt}\n")
    f.write(f"End date: {end_dt}\n")
# Save the check_table to a text file
with open(check_table_file, 'w') as f:
    f.write(check_table.to_string(index=False))


# if select sensor_ids from the sensorinfo_df with variable_name 'RAIR' and site_name 'GOB_44_MT', 'GOI_38_MT', 'GOB_45_MT'
sel_names = ['GOB_44_MT', 'BUO_31_MT', 'BUW_32_MT', 'HOH_33_MT', 'HOC_34_MT', 'LDH_35_MT',
 'LDC_36_MT', 'AMM_37_MT', 'POH_39_MT', 'POG_40_MT', 'HOD_41_MT', 'MOB_42_MT',
 'HEW_43_MT', 'HEH_42_MT', 'MOB_01_MT', 'MOB_02_MT', 'MOB_21_EC', 'GOI_38_MT',
 'WIE_41_MT', 'ONL_22_MT', 'CAM_21_MT', 'BPB_31_MT', 'BPC_32_MT', 'PPA_42_MT',
 'BRO_43_MT', 'BLO_36_MT', 'BLR_35_MT', 'HWG_37_MT', 'HWR_34_MT',
 'HWN_45_MT', 'HWH_46_MT', 'WRW_SR', 'ZEG_PT']
# sel_names = ['ZEG_PT', 'ZEG_RF', 'ALB_RF',
#  'LAW_MS']
sensorinfo_df_sel = sensorinfo_df[(sensorinfo_df['variable_name'] == 'RAIR') & (sensorinfo_df['site_name'].isin(sel_names))]

# multiply the values in data_df by 1000 for the selected sensor_ids
data_df[sensorinfo_df_sel['sensor_id'].tolist()] *= 10

sensorinfo_df_sel = sensorinfo_df[(sensorinfo_df['variable_name'] == 'RAIR')]

data_df[sensorinfo_df_sel['sensor_id'].tolist()] = data_df[sensorinfo_df_sel['sensor_id'].tolist()].where(
    data_df[sensorinfo_df_sel['sensor_id'].tolist()] >= 1000, other=pd.NA
)
import pandas as pd
import dash
from dash import dcc, html, Input, Output
import matplotlib.colors as mcolors
import plotly.graph_objects as go
import os

# ... (your data loading and preprocessing code here) ...

# Prepare names
var_names = sensorinfo_df['variable_name'].unique().tolist()
site_names = sensorinfo_df['site_name'].unique().tolist()

# Helper for color gradient
def nan_to_color(frac):
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "nan_gradient", ["#00cc96", "#ffa600", "#ef553b"]
    )
    frac = min(max(frac, 0), 1)
    rgb = cmap(frac)[:3]
    return f'rgb({int(rgb[0]*255)}, {int(rgb[1]*255)}, {int(rgb[2]*255)})'

# Build cell values and colors
cell_values = []
cell_colors = []
for site in site_names:
    row_vals = []
    row_colors = []
    for var in var_names:
        sensors = sensorinfo_df[(sensorinfo_df['site_name'] == site) & (sensorinfo_df['variable_name'] == var)]['sensor_id'].tolist()
        if sensors:
            sensor_data = data_df[sensors]
            total = sensor_data.size
            nans = sensor_data.isna().sum().sum()
            if total > 0:
                frac_nan = nans / total
                row_vals.append(f"{frac_nan:.0%} NaN")
                row_colors.append(nan_to_color(frac_nan))
            else:
                row_vals.append('')
                row_colors.append('#f0f0f0')
        else:
            row_vals.append('')
            row_colors.append('#f0f0f0')
    cell_values.append(row_vals)
    cell_colors.append(row_colors)

# Prepare DataTable data and styles
table_data = []
for i, site in enumerate(site_names):
    row = {'Site Name': site}
    for j, var in enumerate(var_names):
        row[var] = cell_values[i][j]
    table_data.append(row)

style_data_conditional = []
for i, site in enumerate(site_names):
    for j, var in enumerate(var_names):
        color = cell_colors[i][j]
        style_data_conditional.append({
            'if': {'row_index': i, 'column_id': var},
            'backgroundColor': color
        })

datatable = dash_table.DataTable(
    id='nan-table',
    columns=[{'name': 'Site Name', 'id': 'Site Name'}] + [{'name': v, 'id': v} for v in var_names],
    data=table_data,
    style_data_conditional=style_data_conditional,
    style_cell={'textAlign': 'center'},
    style_header={'fontWeight': 'bold'},
)

# remove all sensor_ids from data_df which only contain NaN values
data_df = data_df.dropna(axis=1, how='all')
# remove all sensor_ids from sensorinfo_df which are not in data_df
sensorinfo_df = sensorinfo_df[sensorinfo_df['sensor_id'].isin(data_df.columns)]

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


#######################
import numpy as np

# Get all ATMP sensor_ids
atmp_sensors = sensorinfo_df[sensorinfo_df['variable_name'] == 'ATMP']['sensor_id'].tolist()

# Extract ATMP data
atmp_data = data_df[atmp_sensors]

# Calculate z-scores for each timestamp (row-wise)
atmp_zscores = (atmp_data - atmp_data.mean(axis=1, skipna=True).values[:, None]) / atmp_data.std(axis=1, skipna=True).values[:, None]
zscore_outlier_mask = np.abs(atmp_zscores) > 4

# IQR method
Q1 = atmp_data.quantile(0.25)
Q3 = atmp_data.quantile(0.75)
IQR = Q3 - Q1
iqr_outlier_mask = (atmp_data < (Q1 - 1.5 * IQR)) | (atmp_data > (Q3 + 1.5 * IQR))

# Modified Z-score
median = atmp_data.median(axis=1, skipna=True)
mad = (np.abs(atmp_data.sub(median, axis=0))).median(axis=1, skipna=True)
modz = 0.6745 * (atmp_data.sub(median, axis=0)).div(mad, axis=0)
modz_outlier_mask = np.abs(modz) > 3.5

# Percentile (1-99%)
lower = atmp_data.quantile(0.01)
upper = atmp_data.quantile(0.99)
percentile_outlier_mask = (atmp_data < lower) | (atmp_data > upper)

# Rolling Window Z-score (window size can be adjusted, e.g., 24 for 24 hours)
window = 24
rolling_mean = atmp_data.rolling(window, min_periods=1, center=True).mean()
rolling_std = atmp_data.rolling(window, min_periods=1, center=True).std()
rolling_zscore = (atmp_data - rolling_mean) / rolling_std
rolling_outlier_mask = np.abs(rolling_zscore) > 4

# wind speed sensor_ids
wind_speed_sensor_ids = sensorinfo_df[sensorinfo_df['variable_name'] == 'WINS']['sensor_id'].tolist()
wind_direction_sensor_ids = sensorinfo_df[sensorinfo_df['variable_name'] == 'WIND']['sensor_id'].tolist()


import numpy as np
import plotly.graph_objects as go

# Collect all wind speed and direction data into a single array
wind_speeds = []
wind_dirs = []

for speed_id, dir_id in zip(wind_speed_sensor_ids, wind_direction_sensor_ids):
    if speed_id in data_df.columns and dir_id in data_df.columns:
        # Drop rows where either is NaN
        df = data_df[[speed_id, dir_id]].dropna()
        wind_speeds.extend(df[speed_id].values)
        wind_dirs.extend(df[dir_id].values)

wind_speeds = np.array(wind_speeds)
wind_dirs = np.array(wind_dirs)

if len(wind_speeds) > 0 and len(wind_dirs) > 0:
    # Bin directions (e.g., every 30 degrees)
    bins = np.arange(0, 361, 30)
    labels = (bins[:-1] + bins[1:]) / 2
    dir_bins = pd.cut(wind_dirs, bins, labels=labels, include_lowest=True)
    # For each direction bin, compute mean wind speed (or count, as you prefer)
    rose = pd.DataFrame({'speed': wind_speeds, 'dir_bin': dir_bins})
    rose_counts = rose.groupby('dir_bin')['speed'].count()
    rose_means = rose.groupby('dir_bin')['speed'].mean()

    windrose_fig = go.Figure()
    windrose_fig.add_trace(go.Barpolar(
        r=rose_counts.values,
        theta=rose_counts.index.astype(float),
        width=30,
        marker_color=rose_means.values,
        marker_colorscale='Viridis',
        opacity=0.8,
        name='Windrose'
    ))
    windrose_fig.update_layout(
        title='Windrose (all WIND sensors)',
        polar=dict(
            angularaxis=dict(direction='clockwise', rotation=90)
        )
    )
else:
    windrose_fig = go.Figure()
    windrose_fig.update_layout(title='No wind data available')

#############
# Number of sensor names/subplots
nfigs = len(sensor_names)




# print min and max and average of data_df[23724]
# print(f"Min: {data_df[23724].min()}, Max: {data_df[23724].max()}, Avg: {data_df[23724].mean()}")


####### Make dash app #######
# Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(figure=windrose_fig),
    html.H3("NaN Overview Table (click a cell to highlight below)"),
    datatable,
    dcc.Checklist(
        id='show-outliers',
        options=[
            {'label': 'Show outliers', 'value': 'show'},
            {'label': 'Remove outliers', 'value': 'remove'}
        ],
        value=['show'],
        style={'margin': '10px'}
    ),
    dcc.Dropdown(
        id='outlier-method',
        options=[
            {'label': 'Z-score', 'value': 'zscore'},
            {'label': 'IQR', 'value': 'iqr'},
            {'label': 'Modified Z-score', 'value': 'modz'},
            {'label': 'Percentile (1-99%)', 'value': 'percentile'},
            {'label': 'Rolling Window Z-score', 'value': 'rolling'}
        ],
        value='zscore',
        clearable=False,
        style={'width': '200px', 'margin': '10px'}
    ),  
    dcc.Graph(id='highlight-graph'),
    html.Div([
        dcc.Graph(id=f'graph-{i}', figure=make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i]))
        for i in range(len(sensor_names))
    ])
])

@app.callback(
    Output('highlight-graph', 'figure'),
    [Input('nan-table', 'active_cell'),
     Input('show-outliers', 'value'),
     Input('outlier-method', 'value')]
)
def update_highlight_graph(active_cell, show_outliers, outlier_method):
    if active_cell is None or active_cell['column_id'] == 'Site Name':
        return go.Figure()
    row = active_cell['row']
    col = active_cell['column_id']
    site = site_names[row]
    var = col
    sensors = sensorinfo_df[(sensorinfo_df['site_name'] == site) & (sensorinfo_df['variable_name'] == var)]['sensor_id'].tolist()
    if not sensors:
        return go.Figure()
    # Select outlier mask
    if outlier_method == 'iqr':
        outlier_mask_used = iqr_outlier_mask
    elif outlier_method == 'modz':
        outlier_mask_used = modz_outlier_mask
    elif outlier_method == 'percentile':
        outlier_mask_used = percentile_outlier_mask
    elif outlier_method == 'rolling':
        outlier_mask_used = rolling_outlier_mask
    else:
        outlier_mask_used = zscore_outlier_mask
    fig = go.Figure()
    for sensor in sensors:
        y = data_df[sensor].copy()
        # Remove outliers if requested and this is ATMP
        if var == 'ATMP' and sensor in atmp_data.columns and 'remove' in show_outliers:
            y = y.mask(outlier_mask_used[sensor])
        fig.add_trace(go.Scatter(
            x=data_df.index,
            y=y,
            mode='lines+markers',
            name=f"{sensorinfo_df[sensorinfo_df['sensor_id'] == sensor]['site_name'].iloc[0]} ({sensor})"
        ))
        # Highlight outliers if this is an ATMP sensor and user wants to see them
        if var == 'ATMP' and sensor in atmp_data.columns and 'show' in show_outliers:
            outlier_idx = outlier_mask_used[sensor]
            fig.add_trace(go.Scatter(
                x=atmp_data.index[outlier_idx],
                y=atmp_data[sensor][outlier_idx],
                mode='markers',
                marker=dict(color='red', size=12, symbol='x'),
                name=f"{sensorinfo_df[sensorinfo_df['sensor_id'] == sensor]['site_name'].iloc[0]} Outlier"
            ))
    fig.update_layout(title=f"{site} - {var}")
    return fig
if __name__ == "__main__":
    app.run(debug=True)
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

# if __name__ == "__main__":
#     app.run(debug=True)