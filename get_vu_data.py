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
              print("The number of parts: ", cur.rowcount)
              # for row in rows:
              #     print(row)
                
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



if __name__ == '__main__':
  
    # Define end_date as today
    end_dt = pd.to_datetime('today').strftime('%Y-%m-%d')
    
    # Define start_date as 7 days before today
    start_dt = (pd.to_datetime('today') - pd.DateOffset(days=7)).strftime('%Y-%m-%d')
    start_dt, end_dt = fix_start_end_dt(start_dt=start_dt, end_dt=end_dt)


    sites = ['ALB_MS', 'ALB_RF','ZEG_RF','MOB_X1','MOB_X2','LAW_MS','DEM_NT','ASD_MP','ANK_PT','ZEG_MP','ZEG_PT','ILP_PT','WRW_SR','WRW_OW','ZEG_MOB']
    # sites='ALB_MS'
          
    varname = ['MT1_SWIN_1_H_180','MT1_PAR_1_H_180']
    varname = ['MT1_SWIN_1_H_180']
    # varname = ['SWOUT']
    # read checks.csv file to get the internal varname related to the database varname
    checks_df = pd.read_csv('checks.csv')
    
    # select only the row with name 'SWIN'
    # checks_df = checks_df[checks_df['variable'].str.contains('SWIN')]
    print(checks_df)

    # make a dict of column names as location and values as variable names from the checks_df and skip the first column
    checks_dict = {row.iloc[0]: row.iloc[1:] for index, row in checks_df.iterrows()}

    # def get sensorinfo by site and varname combination
    def get_sensorinfo_by_site_and_varname(sites, varname):
    
        # Get the siteid and siteid_name from the database
        siteid, siteid_name = get_siteids(sites)
        print(siteid_name)
        
        # Get sensor_id and unit_id
        sensor_info = get_sensorinfo(siteid, varname)
        
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
    
    
    # Get the sensor_info by site and varname combination
    sensor_info = get_sensorinfo_by_site_and_varname(sites, varname)
        
    # make dataframe of sensor_info
    sensor_info_df = pd.DataFrame(sensor_info)

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

    # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # Rename the columns in pivoted_df using the mapping
    pivoted_df.rename(columns=sensor_id_to_name, inplace=True)

    print(sensor_info_df)
    print(pivoted_df)

    # make a plotly plot of the data
    import plotly.express as px
    import plotly.graph_objects as go
    import plotly.io as pio
    pio.renderers.default = "browser"
    
    # Create a mapping from fullname to unit
    fullname_to_unit = dict(zip(sensor_info_df['fullname'], sensor_info_df['sensor_units']))

    fig = go.Figure()
    for col in pivoted_df.columns[1:]:
        unit = fullname_to_unit.get(col, "")
        legend_name = f"{col} [{unit}]" if unit else col
        fig.add_trace(go.Scatter(
            x=pivoted_df['datetime'],
            y=pivoted_df[col],
            mode='lines',
            name=legend_name,
            line=dict(width=1)
        ))
    fig.update_layout(title='Sensor Data', xaxis_title='Datetime', yaxis_title='Value')

    fig.show()
    