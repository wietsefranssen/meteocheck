import psycopg2
from config import load_config
from get_dbstring import get_dbstring
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np

from vudb import get_sensor_units
# from vudb import fix_start_end_dt
def get_siteids(shortname):
    db_string = get_dbstring(shortname)    

    """ Retrieve data from the vendors table """
    config  = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
              query = f"""
              SELECT id AS site_id, shortname AS name
              FROM cdr.sites
              WHERE shortname IN ({db_string})
              """
              cur.execute(query, ())
              rows = cur.fetchall()
            #   print("The number of parts: ", cur.rowcount)
            #   for row in rows:
            #       print(row)
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    # Return the site IDs    
    return [row[0] for row in rows], rows

def get_sensorids(siteid, varname=None):
    siteid_db_string = get_dbstring(siteid)    
    varname_db_string = get_dbstring(varname)    

    """ Retrieve data from the vendors table """
    config  = load_config()
    checkk = 0
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
            # with conn.cursor(cursor_factory=RealDictCursor) as cur:
              query = f"""
              SELECT id AS sensor_id, unit AS unit_id, name AS sensor_name
              FROM cdr.logvalproviders
              WHERE site IN ({siteid_db_string})
                AND name IN ({varname_db_string})
              """
              cur.execute(query, (checkk,))
              rows = cur.fetchall()
              # Get columnnr 0 from rows
              sensor_ids = [row[0] for row in rows]
              print("The number of parts: ", cur.rowcount)
              for row in rows:
                  print(row)
              return sensor_ids
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_data(sensorid, start_dt, end_dt):
    sensorid_db_string = get_dbstring(sensorid)      

    """ Retrieve data from the vendors table """
    config  = load_config()
    try:
        with psycopg2.connect(**config) as conn:
            # with conn.cursor() as cur:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
              query = f"""
              SELECT dt, logicid, value
              FROM cdr.pointdata
              WHERE logicid IN ({sensorid_db_string})
                AND dt BETWEEN %s AND %s
              """
              cur.execute(query, (start_dt, end_dt))
              data_result = cur.fetchall()
              if not data_result:
                print(f"No data found for period {start_dt} - {end_dt}")
                return None
              # Convert the result to a DataFrame
              df = pd.DataFrame(data_result)
              # # Get columnnr 0 from rows
              # sensor_ids = [row[0] for row in rows]
              # print("The number of parts: ", cur.rowcount)
              
              # for row in rows:
              #     print(row)
              # return sensor_ids
              return df  
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
       
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
    config  = load_config()
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
    config  = load_config()
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

def get_stations_table(filename='stations.csv'):
    """ 
    This function reads the .csv file and returns a DataFrame with the columns and index names
    """
    # Read in .csv file 
    data = pd.read_csv(filename)
    
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

# def get sensorinfo by site and varname combination
def get_sensorinfo_by_site_and_varname(check_table):
    
    # get sites by selecting all unique values in the 'Station' column of the check_table
    sites = check_table['Station'].unique().tolist()
    
    # Get the siteid and siteid_name from the database
    siteid, siteid_name = get_siteids(sites)
    
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
        
    return sensor_info

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

    # Get the sensor_ids from sensor_info
    sensorids = [row['sensor_id'] for row in sensor_info]
    
    # Get the sensor data from the database
    data = get_data(sensorids, start_dt, end_dt)

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

if __name__ == '__main__':
  
    # Define end_date as today
    end_dt = pd.to_datetime('today').strftime('%Y-%m-%d')
    
    # Define start_date as 7 days before today
    start_dt = (pd.to_datetime('today') - pd.DateOffset(days=14)).strftime('%Y-%m-%d')
    start_dt, end_dt = fix_start_end_dt(start_dt=start_dt, end_dt=end_dt)
    
    # Get the variables_table
    stations_table = get_stations_table("stations.csv")
    
    # Get the check_table
    check_table = get_check_table()
    
    # Select the rows from check_table that match the Stations column with the 'name' from column from stations_table and match 'vu_db' in the Variable column from stations_table
    matching_names = stations_table.loc[stations_table['source'] == 'vu_db', 'name'].unique()
    check_table_vudb = check_table[check_table['Station'].isin(matching_names)]
    
    # reset the index of check_table_vudb
    check_table_vudb = check_table_vudb.reset_index(drop=True)

    # Get data from the vu_db database
    sensor_info_df, pivoted_df = get_vu_data(check_table_vudb, start_dt, end_dt)

    
    # # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    # sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # # Rename the columns in pivoted_df using the mapping
    # pivoted_df.rename(columns=sensor_id_to_name, inplace=True)

    # make a plotly plot of the data
    # import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    # import plotly.io as pio
    # pio.renderers.default = "browser"
    
    # Create a mapping from fullname to unit
    # fullname_to_unit = dict(zip(sensor_info_df['fullname'], sensor_info_df['sensor_units']))
    
    # Create a mapping from sensor_id to fullname
    sensor_id_to_fullname = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['fullname']))
    sensor_id_to_unit = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['sensor_units']))
    sensor_id_to_sensor_name = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['sensor_name']))
    # sensor_id_to_aggmethod = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['aggmethod']))
    # sensor_id_to_site_id = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['site_id']))
    sensor_id_to_site_name = dict(zip(sensor_info_df['sensor_id'], sensor_info_df['site_name']))
    # make groups of sensor_ids by sensor_name
    sensor_groups = sensor_info_df.groupby('sensor_name')['sensor_id'].apply(list).to_dict()
    
    # List of sensor names
    sensor_names = list(sensor_groups.keys())
    
    downsample_factor = 1  # Adjust this value to control the downsampling rate
    shared_x = pivoted_df.index[::downsample_factor]
    figures = []
    for sensor in sensor_names:
        fig = go.Figure()
        sensor_group = {k: v for k, v in sensor_groups.items() if k == 'MT1_PAR_1_H_180'}
        # Get the sensor_ids for the current sensor group
        sensor_ids = sensor_groups[sensor]

        for sensor_id in sensor_ids:
            # unit = units[line]
            unit = sensor_id_to_unit.get(sensor_id, "")
    #         fullname = sensor_id_to_fullname.get(sensor_id, "")
            sitename = sensor_id_to_site_name.get(sensor_id, "")
    #         legend_name = f"{sitename} [{unit}]" if unit else col
    #         
            fig.add_trace(
                go.Scattergl(
                    x=shared_x,  # Plot every 10th point for performance
                    y=pivoted_df[sensor_id],
                    mode='markers+lines',
                    name=f"{sitename} ({unit})",
                    line=dict(width=1),
                    marker=dict(size=3)  # Adjust marker size for better visibility
                )
            )
        fig.update_layout(
            title=f"{sensor.capitalize()} Data",
            xaxis_title="timedate",
            yaxis_title=unit if unit else "Value",
            legend_title="Measurement",
            xaxis=dict(range=[shared_x.min(), shared_x.max()])
        )
        figures.append(fig)

        
    
    ###############################
    import dash
    from dash import dcc, html  
    # Dash app
    app = dash.Dash(__name__)

    app.layout = html.Div([
        html.H2("Sensor Data (Three Separate Plots)"),
        *[dcc.Graph(figure=fig) for fig in figures]
    ])

    if __name__ == "__main__":
        app.run_server(debug=True)
