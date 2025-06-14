import pandas as pd
import os

def fix_start_end_dt(start_dt, end_dt, file, tz):
    """
    Fix the start and end datetime strings to ensure they are in the correct format.
    """
    
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
    
    if os.path.exists(file):
        # Read the file with special handling for the first three lines
        with open(file, 'r') as f:
            column_names = f.readline().strip().split(',')
            units = f.readline().strip().split(',')
            aggregation = f.readline().strip().split(',')
        
        # Read only the last line of the file to get the last date
        with open(file, 'rb') as f:
            f.seek(-2, os.SEEK_END)  # Move to the second-to-last byte
            while f.read(1) != b'\n':  # Find the last newline character
                f.seek(-2, os.SEEK_CUR)
            last_line = f.readline().decode()
        
        # Convert the last line into a dictionary using column names
        last_line_data = dict(zip(column_names, last_line.strip().split(',')))
        
        # Extract the last date from the datetime column
        last_date = pd.to_datetime(last_line_data.get("datetime")).tz_convert(tz)
        
        # Determine the start date
        start_dt = last_date + pd.Timedelta(minutes=1)

        print(f"Previous data exists with last date: {last_date}.")
    
        # Check if the start date is before the end date
        if pd.to_datetime(last_date) == pd.to_datetime(end_dt):
            print(f"This is the same as the defined end_dt {end_dt}. Skipping...")
            return None, None
        elif pd.to_datetime(last_date) > pd.to_datetime(end_dt):
            print(f"This is later than the defined end_dt {end_dt}. Skipping...")
            return None, None

    if pd.to_datetime(start_dt) >= pd.to_datetime(end_dt):
        print(f"Start date {start_dt} is after end date {end_dt}. Skipping...")
        return None, None
    
    # print(f"Start date set to: {start_dt}. End date set to: {end_dt}.")
    
    return start_dt, end_dt

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
