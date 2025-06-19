import pandas as pd
import dash
from dash import dcc, html, Input, Output, State, ctx

from functions_general import fix_start_end_dt
from functions_db import get_data_from_db
from functions_plot import make_figure   


# Set the start and end dates for the data retrieval
start_dt, end_dt = fix_start_end_dt(start_dt=(pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d'), 
                                    end_dt=pd.to_datetime('today').strftime('%Y-%m-%d'))

sensorinfo_df, data_df = get_data_from_db(stationsfile="stations.csv", start_dt=start_dt, end_dt=end_dt, check_table_filename='check_table_base.csv')

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

####### Make dash app #######
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Store(id='zoom-store', data={'x_range': None}),
    html.Div([
        dcc.Graph(id=f'graph-{i}', figure=make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i]))
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
#     return [make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i], x_range) for i in range(nfigs)]

if __name__ == "__main__":
    app.run_server(debug=True)