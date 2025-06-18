import psycopg2
from config import load_config
from get_dbstring import get_dbstring
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from vudb import get_sensor_units
from functions_db import get_siteids_vu, get_data_vu

def fix_start_end_dt(start_dt=None, end_dt=None, tz='UTC'):
    # if the time of start_dt is not provided, set it to 00:00:00
    if len(start_dt) == 10:
        start_dt = f"{start_dt} 00:00:00"

    # if the time of end_dt is not provided, set it to 23:59:59
    if len(end_dt) == 10:
        end_dt = f"{end_dt} 23:59:00"
        
    # Make sure both start_dt and end_dt are tz aware
    # Check if start_dt and end_dt are already timezone-aware
    if pd.to_datetime(start_dt).tzinfo is None:
        start_dt = pd.to_datetime(start_dt).tz_localize(tz)
        
    if pd.to_datetime(end_dt).tzinfo is None:
        end_dt = pd.to_datetime(end_dt).tz_localize(tz)
        
    return start_dt, end_dt

def get_sensorinfo(siteid, names):
    names_db_string = get_dbstring(names)      
    siteid_db_string = get_dbstring(siteid)      

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_vu')
    try:
        with psycopg2.connect(**config) as conn:
            # with conn.cursor() as cur:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = f"""
                SELECT id AS sensor_id, unit AS unit_id, name AS sensor_name, aggmethod AS aggmethod, site AS site_id
                FROM cdr.logvalproviders
                WHERE site IN ({siteid_db_string})
                    AND name IN ({names_db_string})
                """
                
                cur.execute(query, ())
                sensor_info_result = cur.fetchall()
                if not sensor_info_result:
                    # print(f"No sensor_id found for site_id: {site_id}")
                    # print(f"No sensor_id found for site_id: {site_id}, names: {names}")
                    return None
                # sensor_ids = [row['sensor_id'] for row in sensor_info_result]
                unit_ids = [row['unit_id'] for row in sensor_info_result]

                # data_result = cur.fetchall()
              
                  # Get sensor units
                sensor_units = get_sensor_units(cur, unit_ids)  

                # Add sensor units to sensor_info_result
                for i, row in enumerate(sensor_info_result):
                    unit_id = row['unit_id']
                    # Find the corresponding unit from sensor_units
                    unit_row = next((u for u in sensor_units if u['unit_id'] == unit_id), None)
                    if unit_row:
                        sensor_info_result[i]['sensor_units'] = unit_row['unit']
                    else:
                        sensor_info_result[i]['sensor_units'] = None  # or some default value

                # Get sensor units
                sensor_units = get_sensor_units(cur, unit_ids)  

                # Add sensor units to sensor_info_result
                for i, row in enumerate(sensor_info_result):
                    unit_id = row['unit_id']
                    # Find the corresponding unit from sensor_units
                    unit_row = next((u for u in sensor_units if u['unit_id'] == unit_id), None)
                    if unit_row:
                        sensor_info_result[i]['sensor_units'] = unit_row['unit']
                    else:
                        sensor_info_result[i]['sensor_units'] = None  # or some default value

                return sensor_info_result

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_sensorinfo_siteid_name_combo(siteid_names_combo):
 
    
    # Convert siteid_names_combo to a string by removing the [ and ] characters
    siteid_names_combo = str(siteid_names_combo).replace('[', '').replace(']', '')
    
    
    # Make a string for the siteid_names_combo     
    # siteid_names_combo_string = get_dbstring(siteid_names_combo)

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_vu')
    try:
        with psycopg2.connect(**config) as conn:
            # with conn.cursor() as cur:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = f"""
                SELECT id AS sensor_id, unit AS unit_id, name AS sensor_name, aggmethod AS aggmethod, site AS site_id
                FROM cdr.logvalproviders
                WHERE (site, name) IN ({siteid_names_combo})
                """
                
                cur.execute(query, ())
                sensor_info_result = cur.fetchall()
                if not sensor_info_result:
                    # print(f"No sensor_id found for site_id: {site_id}")
                    # print(f"No sensor_id found for site_id: {site_id}, names: {names}")
                    return None
                # sensor_ids = [row['sensor_id'] for row in sensor_info_result]
                unit_ids = [row['unit_id'] for row in sensor_info_result]

                # Get sensor units
                sensor_units = get_sensor_units(cur, unit_ids)  

                # Add sensor units to sensor_info_result
                for i, row in enumerate(sensor_info_result):
                    unit_id = row['unit_id']
                    # Find the corresponding unit from sensor_units
                    unit_row = next((u for u in sensor_units if u['unit_id'] == unit_id), None)
                    if unit_row:
                        sensor_info_result[i]['unit'] = unit_row['unit']
                    else:
                        sensor_info_result[i]['unit'] = None  # or some default value

                return sensor_info_result

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_stations_table(filename='stations.csv'):
    """ 
    This function reads the .csv file and returns a DataFrame with the columns and index names
    """
    # Read in .csv file 
    data = pd.read_csv(filename, sep=';')
    
    # Remove any leading or trailing whitespace from the column names
    data.columns = data.columns.str.strip()

    result_df = pd.DataFrame(data)
    return result_df

def get_check_table(filename='check_table.csv'):
    """ 
    This function reads the check_table.csv file and returns a DataFrame with the columns and index names
    where the value is 'x'.
    """
    # Read in check_table.csv file 
    check_table = pd.read_csv(filename, index_col=0)
    
    # Remove any leading or trailing whitespace from the column names
    check_table.columns = check_table.columns.str.strip()
    # Remove any leading or trailing whitespace from the index names
    check_table.index = check_table.index.str.strip()
    
    result = []
    for idx in check_table.index:
        for col in check_table.columns:
            if check_table.loc[idx, col] == 'x':
                result.append({'Station': col, 'Variable': idx})

    result_df = pd.DataFrame(result)
    return result_df

def get_check_table2(filename='check_table.csv'):
    """ 
    This function reads the check_table.csv file and returns a DataFrame with the columns and index names
    where the value is 'x'.
    """
    # Read in check_table.csv file 
    check_table = pd.read_csv(filename, index_col=0, sep=';')
    
    # Remove any leading or trailing whitespace from the column names
    check_table.columns = check_table.columns.str.strip()
    # Remove any leading or trailing whitespace from the index names
    check_table.index = check_table.index.str.strip()
    
    result = []
    for idx in check_table.index:
        for col in check_table.columns:
            # Check if not empty or NaN or None or 0
            if pd.notna(check_table.loc[idx, col]) and check_table.loc[idx, col] != '' and check_table.loc[idx, col] != 0:
            # if check_table.loc[idx, col] == 'x':
                result.append({'Station': idx, 'Variable': check_table.loc[idx, col],'Variable_name': col})

    result_df = pd.DataFrame(result)
    return result_df

# def get sensorinfo by site and varname combination
def get_sensorinfo_by_site_and_varname(check_table):
    
    # get sites by selecting all unique values in the 'Station' column of the check_table
    sites = check_table['Station'].unique().tolist()
    
    # Get the siteid and siteid_name from the database
    siteid, siteid_name = get_siteids_vu(sites)
    
    # Match the Station names in check_table with the siteid_name and add a new column 'siteid' integer to check_table as integer values        
    check_table['siteid'] = check_table['Station'].map(dict(zip([s[1] for s in siteid_name], siteid)))

    # Conert NaN values in the siteid column to -9999 and make in integer
    check_table['siteid'] = check_table['siteid'].fillna(-9999).astype(int)
    
    # siteid and Variable columns from check_table
    siteid_varname = check_table[['siteid', 'Variable']].drop_duplicates()
    
    # make a list of tuples (siteid, varname) from siteid_varname
    siteid_varname = [tuple(x) for x in siteid_varname.to_numpy()]
    
    sensor_info = get_sensorinfo_siteid_name_combo(siteid_varname)
    # print(sensor_info)
    # add a sitename column to sensor_info by using siteid_name as mapping (column 0 is site_id, and column 1 is site_name)
    for i, row in enumerate(sensor_info):
        site_id = row['site_id']
        # Find the corresponding site name from siteid_name
        site_row = next((s for s in siteid_name if s[0] == site_id), None)
        if site_row:
            sensor_info[i]['site_name'] = site_row[1]
        else:
            sensor_info[i]['site_name'] = None
    
    # Add a fullname column to sensor_info by combining site_name and sensor_name
    for i, row in enumerate(sensor_info):
        site_name = row['site_name']
        sensor_name = row['sensor_name']
        # Combine site_name and sensor_name
        sensor_info[i]['fullname'] = f"{site_name}_{sensor_name}"
        
    # Add Variable_name column to sensor_info by using the Variable column from check_table
    for i, row in enumerate(sensor_info):
        varname = check_table.loc[check_table['Variable'] == row['sensor_name'], 'Variable_name'].values
        if len(varname) > 0:
            sensor_info[i]['variable_name'] = varname[0]
        else:
            sensor_info[i]['variable_name'] = None
        
    # Add source to sensor_info
    for i, row in enumerate(sensor_info):
        sensor_info[i]['source'] = 'vu_db'
    
    return sensor_info


def get_check_table_db(source = 'wur_db', check_table_filename='check_table_base.csv'):
    # Get the check_table
    check_table = get_check_table2(check_table_filename)

    # Select the rows from check_table that match the Stations column with the 'name' from column from stations_table and match 'source' in the Variable column from stations_table
    matching_names = stations_table.loc[stations_table['source'] == source, 'name'].unique()
    check_table_db = check_table[check_table['Station'].isin(matching_names)]

    # reset the index of check_table_vudb
    check_table_db = check_table_db.reset_index(drop=True)

    return check_table_db

def get_vu_data(check_table, start_dt, end_dt):
    # Get the sensor_info by site and varname combination
    sensor_info = get_sensorinfo_by_site_and_varname(check_table)
        
    # make dataframe of sensor_info
    sensor_info_df = pd.DataFrame(sensor_info)

    # Check if all items od check_table are in sensor_info_df
    missing_items = check_table[~check_table.set_index(['Station', 'Variable']).index.isin(sensor_info_df.set_index(['site_name', 'sensor_name']).index)]
    if not missing_items.empty:
        print("The following items in the check_table are not found in the sensor_info:")
        print(missing_items[['Station', 'Variable']])

    # Get the sensor_ids from sensor_info_df
    sensorids = sensor_info_df['sensor_id'].tolist()
    
    # Get the sensor data from the database
    data = get_data_vu(sensorids, start_dt, end_dt)

    # Pivot the DataFrame
    pivoted_df = data.pivot(index='dt', columns='logicid', values='value')

    # Add columns that are not in the pivoted_df but are in the sensor_info
    for row in sensor_info:
        logicid = row['sensor_id']
        if logicid not in pivoted_df.columns:
            # Create a new column with NaN values
            pivoted_df[logicid] = np.nan

    # Put the columns in the same order as in sensor_info
    pivoted_df = pivoted_df[sensorids]

    # Reset the column names (optional, to remove the MultiIndex)
    pivoted_df.columns.name = None
    
    # Make index of pivoted_df into a column
    pivoted_df.reset_index(inplace=True)
    
    # Rename the first column to 'datetime'
    pivoted_df.rename(columns={pivoted_df.columns[0]: 'datetime'}, inplace=True)

    # # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    # sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # # Rename the columns in pivoted_df using the mapping
    # pivoted_df.rename(columns=sensor_id_to_name, inplace=True)

    # Make index of datetime culumn
    # pivoted_df['datetime'] = pd.to_datetime(pivoted_df['datetime'])
    pivoted_df.set_index('datetime', inplace=True)
    # print(sensor_info_df)
    # print(pivoted_df)
    return sensor_info_df, pivoted_df

# if __name__ == '__main__':
  
# Define end_date as today
end_dt = pd.to_datetime('today').strftime('%Y-%m-%d')

# Define start_date as 7 days before today
start_dt = (pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d')
start_dt, end_dt = fix_start_end_dt(start_dt=start_dt, end_dt=end_dt)

# Get the variables_table
stations_table = get_stations_table("stations.csv")

####### WUR DB DATA RETRIEVAL #######
# Get the check_table
check_table_wurdb = get_check_table_db(source = 'wur_db')


#############################
from wurdb import get_sensorinfo_by_site_and_varname_wur
from wurdb import get_data_wur
def get_wur_data(check_table, start_dt, end_dt):
    
    # Get the sensor_info by site and varname combination
    sensor_info_df = get_sensorinfo_by_site_and_varname_wur(check_table)
        
    # Check if all items od check_table are in sensor_info_df
    missing_items = check_table[~check_table.set_index(['Station', 'Variable']).index.isin(sensor_info_df.set_index(['site_name', 'sensor_name']).index)]
    if not missing_items.empty:
        print("The following items in the check_table are not found in the sensor_info:")
        print(missing_items[['Station', 'Variable']])

    # Get the sensor_ids from sensor_info_df
    sensorids = sensor_info_df['sensor_id'].tolist()
    
    # Get the sensor data from the database
    data = get_data_wur(sensorids, start_dt, end_dt)

    # Remove duplicates based on 'dt' and 'logicid' to ensure unique entries
    data_nodup = data.drop_duplicates(subset=['dt', 'logicid'])
    pivoted_df = data_nodup.pivot(index='dt', columns='logicid', values='value')

    # Pivot the DataFrame
    pivoted_df = data_nodup.pivot(index='dt', columns='logicid', values='value')
    # pivoted_df = data.pivot(index='dt', columns='logicid', values='value')

    # Add columns that are not in the pivoted_df but are in the sensor_info
    for i, row in sensor_info_df.iterrows():
        # Get the logicid from sensor_info_df
        logicid = row['sensor_id']
        if logicid not in pivoted_df.columns:
            # Create a new column with NaN values
            pivoted_df[logicid] = np.nan

    # Put the columns in the same order as in sensor_info
    pivoted_df = pivoted_df[sensorids]

    # Reset the column names (optional, to remove the MultiIndex)
    pivoted_df.columns.name = None
    
    # Make index of pivoted_df into a column
    pivoted_df.reset_index(inplace=True)
    
    # Rename the first column to 'datetime'
    pivoted_df.rename(columns={pivoted_df.columns[0]: 'datetime'}, inplace=True)

    # # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    # sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # # Rename the columns in pivoted_df using the mapping
    # pivoted_df.rename(columns=sensor_id_to_name, inplace=True)

    # Make index of datetime culumn
    # pivoted_df['datetime'] = pd.to_datetime(pivoted_df['datetime'])
    pivoted_df.set_index('datetime', inplace=True)
    # print(sensor_info_df)
    # print(pivoted_df)
    return sensor_info_df, pivoted_df
#############################
# Get data from the database
sensor_info_df_wur, pivoted_df_wur = get_wur_data(check_table_wurdb, start_dt, end_dt)

####### VU DB DATA RETRIEVAL #######
# Get the check_table
check_table_vudb = get_check_table_db(source = 'vu_db')

# Get data from the vu_db database
sensor_info_df_vu, pivoted_df_vu = get_vu_data(check_table_vudb, start_dt, end_dt)

# Combine the two DataFrames
pivoted_df = pd.concat([pivoted_df_wur, pivoted_df_vu], axis=1)
# Combine the two sensor_info DataFrames
sensor_info_df = pd.concat([sensor_info_df_wur, sensor_info_df_vu], ignore_index=True)

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Create a mapping from sensor_id to various attributes
# sensor_id_to_fullname = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['fullname']))
sensor_id_to_unit = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['unit']))
sensor_id_to_sensor_name = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['sensor_name']))
sensor_id_to_site_name = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['site_name']))
sensor_id_to_variable_name = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['variable_name']))
sensor_id_to_source = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['source']))


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

downsample_factor = 30  # Adjust this value to control the downsampling rate
shared_x = pivoted_df.index[::downsample_factor]
pivoted_df = pivoted_df[::downsample_factor]
# Set x_range to the full range of the index
# x_range = [shared_x.min(), shared_x.max()]

import plotly.colors
# Assign colors dynamically using Plotly's color sequence

sitenames = sorted(sensor_info_df['site_name'].unique())
color_seq = plotly.colors.qualitative.Plotly
sensor_color_map = {sensor: color_seq[i % len(color_seq)] for i, sensor in enumerate(sitenames)}

# for sensor in sensor_names:
def make_figure(sensor, x_range=None, y_range=None):
    fig = go.Figure()
    # Get the sensor_ids for the current sensor group
    sensor_ids = sensor_groups[sensor]
    
    # Sort sensor_ids based on site_name
    sensor_ids = sorted(
        sensor_ids,
        key=lambda sensor_id: sensor_id_to_site_name.get(sensor_id, "")
    )
# pivoted_df = pivoted_df[sorted_columns]
    for sensor_id in sensor_ids:
        # unit = units[line]
        # pivoted_df[sensor_id].min()
        unit = sensor_id_to_unit.get(sensor_id, "")
#         fullname = sensor_id_to_fullname.get(sensor_id, "")
        sitename = sensor_id_to_site_name.get(sensor_id, "")
        sensor_name = sensor_id_to_sensor_name.get(sensor_id, "")
        var_name = sensor_id_to_variable_name.get(sensor_id, "")
        source = sensor_id_to_source.get(sensor_id, "")
        fig.add_trace(
            go.Scattergl(
                x=shared_x,  # Plot every 10th point for performance
                # x=pivoted_df.index,
                y=pivoted_df[sensor_id],
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
    # y_range = [pivoted_df[sensor_ids].min().min(), pivoted_df[sensor_ids].max().max()]
    # if y_range:
    #     # print(f"Setting y-axis range for {var_name}: {y_range}")
    #     print(f"Setting y-axis: {y_range}"  )
    #     fig.update_yaxes(autorange=False)
    #     fig.update_yaxes(range=y_range)
    # else:
    #     print(f"No y-axis range set for {var_name}, using autorange")
    #     fig.update_yaxes(autorange=True)

    return fig

    

###############################
import dash
from dash import dcc, html, Input, Output, State, ctx

# Number of sensor names/subplots
nfigs = len(sensor_names)

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Store(id='zoom-store', data={'x_range': None}),
    html.Div([
        dcc.Graph(id=f'graph-{i}', figure=make_figure(sensor_names[i]))
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
#     return [make_figure(sensor_names[i], x_range) for i in range(nfigs)]

if __name__ == "__main__":
    app.run_server(debug=True)