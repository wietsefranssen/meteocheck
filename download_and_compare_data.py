import dash
from dash import dcc, html, Input, Output, State, ctx, dash_table
from src.plot import make_figure   
from data_manager import DataManager
import matplotlib.colors as mcolors
import plotly.graph_objects as go

dm = DataManager()
dm.set_dates(days_back=4, offset=2)
dm.set_dates(start_dt='2025-06-20', end_dt='2025-06-24 23:59:00')
print(f"start_dt: {dm.start_dt}, end_dt: {dm.end_dt}")

dm.download_or_load_data()
data_df, sensorinfo_df = dm.get_data()

# Fix PAIR units for selected Stations
# sel_names = ['GOB_44_MT', 'BUO_31_MT']
# data_df = fix_pair_units(sensorinfo_df, data_df, sel_names)

# Prepare names
check_table = dm.check_table
var_names = check_table.columns[2:].tolist()
# var_names = sensorinfo_df['variable_name'].unique().tolist()

site_names = check_table['station'].unique().tolist()
# site_names = sensorinfo_df['site_name'].unique().tolist()
site_names.sort()  # Sort site names for consistent order

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
        if dm.is_check_table_value(site, var):
            if not sensors:
                row_vals.append('')
                row_colors.append("red")
                continue

            sensor_data = data_df[sensors]
            total = 0
            nans = 0
            for sensor in sensors:
                series = data_df[sensor]
                # Detect if this sensor is 30-min data
                is_30min = series.dropna().index.minute.isin([0, 30]).all()
                if is_30min:
                    # Only count expected 30-min intervals
                    expected = series.index[(series.index.minute == 0) | (series.index.minute == 30)]
                    total += len(expected)
                    nans += series.loc[expected].isna().sum()
                else:
                    total += series.size
                    nans += series.isna().sum()
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

# Sort table_data by 'Site Name'
# table_data = sorted(table_data, key=lambda x: x['Site Name'])
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

#############
# Number of sensor names/subplots
nfigs = len(sensor_names)

# print min and max and average of data_df[23724]
# print(f"Min: {data_df[23724].min()}, Max: {data_df[23724].max()}, Avg: {data_df[23724].mean()}")


####### Make dash app #######
# Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.A(
        "Open SharePoint Document",
        href="https://wageningenur4.sharepoint.com/:x:/r/sites/Veenweiden/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7B3d741ab0-36f1-4687-953b-902b0a009582%7D&wdOrigin=TEAMS-MAGLEV.undefined_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1750680875439&web=1",
        target="_blank"  # Opens in new tab
    ),
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
            name=f"{sensorinfo_df[sensorinfo_df['sensor_id'] == sensor]['site_name'].iloc[0]} ({sensor})",
            line=dict(width=1),
            marker=dict(size=3)
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