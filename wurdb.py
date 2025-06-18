from config import load_config
from get_dbstring import get_dbstring
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

def get_data_wur2(sensorid, start_dt, end_dt):
    sensorid_db_string = get_dbstring(sensorid)      

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_wur')
    try:
        with psycopg2.connect(**config) as conn:
            # with conn.cursor() as cur:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
              query = f"""
              SELECT time AS dt, sensor_id AS logicid, value
              FROM sensor_data
              WHERE sensor_id IN ({sensorid_db_string})
                AND time BETWEEN %s AND %s
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

def get_sensorinfo_wur(shortname):
    db_string = get_dbstring(shortname)    

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_wur')
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
              query = f"""
              SELECT id AS sensor_id, name AS sensor_name, unit AS unit, stationname AS site_name
              FROM sensors
              WHERE stationname IN ({db_string})
              """
              cur.execute(query, ())
              rows = cur.fetchall()
            #   print("The number of parts: ", cur.rowcount)
            #   for row in rows:
            #       print(row)
            # Conver rows to dataframe
        df = pd.DataFrame(rows, columns=['sensor_id', 'sensor_name', 'unit', 'site_name'])
                
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        
    # Return
    return df
    # return [row[0] for row in rows], rows

def get_siteids_wur(shortname):
    db_string = get_dbstring(shortname)    

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_wur')
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
              query = f"""
              SELECT id AS site_id, stationname AS name
              FROM sensors
              WHERE stationname IN ({db_string})
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

# def get sensorinfo by site and varname combination
def get_sensorinfo_by_site_and_varname_wur(check_table):
    
    # get sites by selecting all unique values in the 'Station' column of the check_table
    sites = check_table['Station'].unique().tolist()
    
    # Get the siteid and siteid_name from the database
    sensor_info = get_sensorinfo_wur(sites)
        
    # Only keep the rows where the sensor_name is in the check_table
    sensor_info = sensor_info[sensor_info['sensor_name'].isin(check_table['Variable'].tolist())] 

    # Add Variable_name column from check_table to sensor_info by matching the sensor_name with the Variable column in check_table
    for i, row in sensor_info.iterrows():
        varname = check_table.loc[check_table['Variable'] == row['sensor_name'], 'Variable_name'].values
        if len(varname) > 0:
            sensor_info.loc[i, 'variable_name'] = varname[0]
        else:
            sensor_info.loc[i, 'variable_name'] = None

    # Add source column to sensor_info
    sensor_info['source'] = 'wur_db'
  
    return sensor_info
