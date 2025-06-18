from get_dbstring import get_dbstring
from config import load_config
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np

def get_siteids_vu(shortname):
    db_string = get_dbstring(shortname)    

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_vu')
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

def get_sensorids_vu(siteid, varname=None):
    siteid_db_string = get_dbstring(siteid)    
    varname_db_string = get_dbstring(varname)    

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_vu')
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

def get_sensorinfo_vu(siteid, names):
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

def get_sensorinfo_siteid_name_combo_vu(siteid_names_combo):
 
    
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

def get_sensorinfo_by_site_and_varname_vu(check_table):
    
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
    
    sensor_info = get_sensorinfo_siteid_name_combo_vu(siteid_varname)
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

def get_data_vudb(sensorid, start_dt, end_dt):
    sensorid_db_string = get_dbstring(sensorid)      

    """ Retrieve data from the vendors table """
    config  = load_config(filename='database.ini', section='postgresql_vu')
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
              return df  
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def get_data_vu(check_table, start_dt, end_dt):
    # Get the sensor_info by site and varname combination
    sensor_info = get_sensorinfo_by_site_and_varname_vu(check_table)
        
    # make dataframe of sensor_info
    sensorinfo_df = pd.DataFrame(sensor_info)

    # Check if all items od check_table are in sensorinfo_df
    missing_items = check_table[~check_table.set_index(['Station', 'Variable']).index.isin(sensorinfo_df.set_index(['site_name', 'sensor_name']).index)]
    if not missing_items.empty:
        print("The following items in the check_table are not found in the sensor_info:")
        print(missing_items[['Station', 'Variable']])

    # Get the sensor_ids from sensorinfo_df
    sensorids = sensorinfo_df['sensor_id'].tolist()
    
    # Get the sensor data from the database
    data = get_data_vudb(sensorids, start_dt, end_dt)

    # Pivot the DataFrame
    data_df = data.pivot(index='dt', columns='logicid', values='value')

    # Add columns that are not in the data_df but are in the sensor_info
    for row in sensor_info:
        logicid = row['sensor_id']
        if logicid not in data_df.columns:
            # Create a new column with NaN values
            data_df[logicid] = np.nan

    # Put the columns in the same order as in sensor_info
    data_df = data_df[sensorids]

    # Reset the column names (optional, to remove the MultiIndex)
    data_df.columns.name = None
    
    # Make index of data_df into a column
    data_df.reset_index(inplace=True)
    
    # Rename the first column to 'datetime'
    data_df.rename(columns={data_df.columns[0]: 'datetime'}, inplace=True)

    # # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    # sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # # Rename the columns in data_df using the mapping
    # data_df.rename(columns=sensor_id_to_name, inplace=True)

    # Make index of datetime culumn
    # data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df.set_index('datetime', inplace=True)
    # print(sensorinfo_df)
    # print(data_df)
    return sensorinfo_df, data_df

def get_data_wur(check_table, start_dt, end_dt):
    
    # Get the sensor_info by site and varname combination
    sensorinfo_df = get_sensorinfo_by_site_and_varname_wur(check_table)
        
    # Check if all items od check_table are in sensorinfo_df
    missing_items = check_table[~check_table.set_index(['Station', 'Variable']).index.isin(sensorinfo_df.set_index(['site_name', 'sensor_name']).index)]
    if not missing_items.empty:
        print("The following items in the check_table are not found in the sensor_info:")
        print(missing_items[['Station', 'Variable']])

    # Get the sensor_ids from sensorinfo_df
    sensorids = sensorinfo_df['sensor_id'].tolist()
    
    # Get the sensor data from the database
    data = get_data_wurdb(sensorids, start_dt, end_dt)

    # Remove duplicates based on 'dt' and 'logicid' to ensure unique entries
    data_nodup = data.drop_duplicates(subset=['dt', 'logicid'])
    data_df = data_nodup.pivot(index='dt', columns='logicid', values='value')

    # Pivot the DataFrame
    data_df = data_nodup.pivot(index='dt', columns='logicid', values='value')
    # data_df = data.pivot(index='dt', columns='logicid', values='value')

    # Add columns that are not in the data_df but are in the sensor_info
    for i, row in sensorinfo_df.iterrows():
        # Get the logicid from sensorinfo_df
        logicid = row['sensor_id']
        if logicid not in data_df.columns:
            # Create a new column with NaN values
            data_df[logicid] = np.nan

    # Put the columns in the same order as in sensor_info
    data_df = data_df[sensorids]

    # Reset the column names (optional, to remove the MultiIndex)
    data_df.columns.name = None
    
    # Make index of data_df into a column
    data_df.reset_index(inplace=True)
    
    # Rename the first column to 'datetime'
    data_df.rename(columns={data_df.columns[0]: 'datetime'}, inplace=True)

    # # Create a dictionary mapping sensor_id to sensor_name from sensor_info
    # sensor_id_to_name = {row['sensor_id']: row['fullname'] for row in sensor_info}

    # # Rename the columns in data_df using the mapping
    # data_df.rename(columns=sensor_id_to_name, inplace=True)

    # Make index of datetime culumn
    # data_df['datetime'] = pd.to_datetime(data_df['datetime'])
    data_df.set_index('datetime', inplace=True)
    # print(sensorinfo_df)
    # print(data_df)
    return sensorinfo_df, data_df




############### WUR DB
from config import load_config
from get_dbstring import get_dbstring
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

def run_pg_query(query, params=None, config_file='database.ini', config_section='postgresql_wur'):
    """
    Run a query on the PostgreSQL database and return the results.
    
    Parameters:
    - query: SQL query string to execute.
    - params: Optional parameters for the query.
    
    Returns:
    - List of rows returned by the query.
    """
    config = load_config(filename=config_file, section=config_section)
    
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return []
    
def get_sensorinfo_wur(shortname):
    db_string = get_dbstring(shortname)    

    query = f"""
    SELECT id AS sensor_id, name AS sensor_name, unit AS unit, stationname AS site_name
    FROM sensors
    WHERE stationname IN ({db_string})
    """
              
    result = run_pg_query(query, config_section='postgresql_wur')
    
    df = pd.DataFrame(result, columns=['sensor_id', 'sensor_name', 'unit', 'site_name'])

    return df


def get_data_wurdb(sensorid, start_dt, end_dt):
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


########## VU DB


import pandas as pd
import os

def select_variables(varfile, site):
    vardata = read_csv_with_header(varfile)
    vardata.columns = vardata.columns.str.strip()

    # Select specific column name
    if site not in vardata.columns:
        print(f"Using the default variables and naming conventions for {site} from file '{os.path.basename(varfile)}'.")
        site_col = 'default'
    else:
        # Select the site column
        site_col = site
    
    # Select variable_names where site_col is 1 or true
    # Select 'variable_name' where 'default' is 1
    selected_variables = vardata.loc[vardata[site_col] == 1, ['varname_db', 'variable_name']]

    # Remove leading and trailing spaces from the variable names
    selected_variables['variable_name'] = selected_variables['variable_name'].str.strip()
    selected_variables['varname_db'] = selected_variables['varname_db'].str.strip()

    return selected_variables

def read_csv_with_header(file_path):
    """
    Reads a CSV file with a header and returns a DataFrame.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path, header=0)  # `header=0` assumes the first row is the header
        # print(f"Successfully read the CSV file: {file_path}")
        return df
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

def get_site_id_by_name(cursor, shortname):
    # Query to get the site_id
    site_id_query = """
    SELECT id AS site_id
    FROM cdr.sites
    WHERE shortname = %s
    """
    cursor.execute(site_id_query, (shortname,))
    site_id_result = cursor.fetchone()

    if not site_id_result:
        print(f"No site_id found for shortname: {shortname}")
        return None

    site_id = site_id_result['site_id']
    return site_id

def get_sensorinfo_by_siteid_and_sensorname(cursor, site_id, names):
    # Query to get the sensor_id
    query = f"""
    SELECT id AS sensor_id, unit AS unit_id, name AS sensor_name, aggmethod AS aggmethod
    FROM cdr.logvalproviders
    WHERE site = %s
        AND name IN ({names})
    """
    
    cursor.execute(query, (site_id,))
    
    sensor_info_result = cursor.fetchall()
    if not sensor_info_result:
        # print(f"No sensor_id found for site_id: {site_id}")
        # print(f"No sensor_id found for site_id: {site_id}, names: {names}")
        return None
    # sensor_ids = [row['sensor_id'] for row in sensor_info_result]
    unit_ids = [row['unit_id'] for row in sensor_info_result]
    # print(f"Retrieved sensor_ids: {sensor_ids}, unit_ids: {unit_ids}")
    
    # Get sensor units
    sensor_units = get_sensor_units(cursor, unit_ids)  

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

# Get the sensor units
def get_sensor_units(cursor, unit_ids):
    if isinstance(unit_ids, list) and len(unit_ids) > 1:
        # unit_ids = ",".join(map(str, unit_ids))  # Join list elements with '|'
        unit_ids = "{" + ",".join(map(str, unit_ids)) + "}"

    # Query to get the sensor units
    unit_query = """
    SELECT id AS unit_id, abbreviation AS unit
    FROM cdr.units
    WHERE id = ANY(%s)
    """
    cursor.execute(unit_query, (unit_ids,))
    unit_result = cursor.fetchall()
    if not unit_result:
        print(f"No units found for unit_ids: {unit_ids}")
        return None

    return unit_result


# Get the data
def get_data(cursor, sensor_ids, start_dt, end_dt):
    if isinstance(sensor_ids, list) and len(sensor_ids) > 1:
        # sensor_ids = ",".join(map(str, sensor_ids))  # Join list elements with '|'
        sensor_ids_str = "{" + ",".join(map(str, sensor_ids)) + "}"

    # Query to get the data
    data_query = """
    SELECT dt, logicid, value
    FROM cdr.pointdata
    WHERE logicid = ANY(%s)
        AND dt BETWEEN %s AND %s
    """
    
    cursor.execute(data_query, (sensor_ids_str, start_dt, end_dt))
    data_result = cursor.fetchall()
    if not data_result:
        print(f"No data found for period {start_dt} - {end_dt}")
        return None
    # Convert the result to a DataFrame
    df = pd.DataFrame(data_result)
    return df
