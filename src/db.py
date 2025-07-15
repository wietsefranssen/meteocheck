from get_dbstring import get_dbstring
from config import load_config

from datetime import datetime
from src.general import get_check_table
import polars as pl
from sqlalchemy import create_engine, Engine
from urllib.parse import quote  

def get_engine(config: dict, config_file='database.ini', config_section='postgresql_wur') -> Engine:
    config = load_config(filename='database.ini', section=config_section)

    """Create a SQLAlchemy engine."""
    # db_url = f"postgresql://{configg['user']}:{configg['password']}@{configg['host']}:{configg['port']}/{configg['database']}"
    db_url = f"postgresql://{config['user']}:%s@{config['host']}:{config['port']}/{config['database']}"
    engine = create_engine(db_url % quote(config['password']))
    # engine = create_engine(db_url)
    return engine

def run_pg_query(query: str, config_file='database.ini', config_section='postgresql_wur',params=None, **kwargs) -> pl.DataFrame:
    """Execute a SQL query and read the results using polars."""
    try:
        config = load_config(filename=config_file, section=config_section)

        # Create a SQLAlchemy engine
        engine = get_engine(config=config, config_section=config_section)

        # Execute the query
        df = pl.read_database(query, connection=engine, **kwargs)

        return df

    except Exception as error:
        print(f"Error: {error}")
        return pl.DataFrame()
    
def get_data_from_db(start_dt=None, end_dt=None, check_table_filename='check_table.csv'):
    """
    Function to retrieve data from the WUR and VU databases.
    """
    
    # Get the variables_table
    check_table = get_check_table(filename=check_table_filename)

    # initialize sensorinfo_df_vu and data_df_vu
    sensorinfo_df_vu = pl.DataFrame()
    data_df_vu = pl.DataFrame()
    sensorinfo_df_wur = pl.DataFrame()
    data_df_wur = pl.DataFrame()
    
    # Get data from the database
    sensorinfo_df_wur, data_df_wur = get_data(check_table[check_table['source'] == 'wur_db'], start_dt, end_dt, source='wur_db')
    
    # Get data from the database
    sensorinfo_df_vu, data_df_vu = get_data(check_table[check_table['source'] == 'vu_db'], start_dt, end_dt, source='vu_db')

    # Check if data_df_wur and data_df_vu are None or empty
    if data_df_wur is None or data_df_wur.height == 0:
        print("No data found for WUR database.")
        data_df_wur = pl.DataFrame()
    if data_df_vu is None or data_df_vu.height == 0:      
        print("No data found for VU database.")
        data_df_vu = pl.DataFrame()

    # Combine the two DataFrames using Polars join
    if data_df_wur.height > 0 and data_df_vu.height > 0:
        # Join on datetime column
        data_df = data_df_wur.join(data_df_vu, on='datetime', how='outer')
    elif data_df_wur.height > 0:
        data_df = data_df_wur
    elif data_df_vu.height > 0:
        data_df = data_df_vu
    else:
        data_df = pl.DataFrame()
    
    # Check if sensorinfo_df_wur and sensorinfo_df_vu are None or empty
    if sensorinfo_df_wur is None or sensorinfo_df_wur.height == 0:
        print("No sensor information found for WUR database.")
        sensorinfo_df_wur = pl.DataFrame()
    if sensorinfo_df_vu is None or sensorinfo_df_vu.height == 0:
        print("No sensor information found for VU database.")
        sensorinfo_df_vu = pl.DataFrame()

    # Combine the two sensor_info DataFrames using Polars concat
    if sensorinfo_df_wur.height > 0 and sensorinfo_df_vu.height > 0:
        # Find overlapping columns
        common_cols = list(set(sensorinfo_df_wur.columns) & set(sensorinfo_df_vu.columns))
        # Select only common columns from both DataFrames
        sensorinfo_df_wur_aligned = sensorinfo_df_wur.select(common_cols)
        sensorinfo_df_vu_aligned = sensorinfo_df_vu.select(common_cols)
        sensorinfo_df = pl.concat([sensorinfo_df_wur_aligned, sensorinfo_df_vu_aligned])
    elif sensorinfo_df_wur.height > 0:
        sensorinfo_df = sensorinfo_df_wur
    elif sensorinfo_df_vu.height > 0:
        sensorinfo_df = sensorinfo_df_vu
    else:
        sensorinfo_df = pl.DataFrame()

    return sensorinfo_df, data_df

def get_sensorinfo_wur(shortname):
    
    db_string = get_dbstring(shortname)    

    query = f"""
    SELECT id AS sensor_id, name AS sensor_name, unit AS unit, stationname AS site_name
    FROM sensors
    WHERE stationname IN ({db_string})
    """
              
    result = run_pg_query(query, config_section='postgresql_wur')
    
    return result

def get_siteids_vu(shortname):

    db_string = get_dbstring(shortname)    

    query = f"""
    SELECT id AS site_id, shortname AS name
    FROM cdr.sites
    WHERE shortname IN ({db_string})
    """
  
    result = run_pg_query(query, config_section='postgresql_vu')
    
    return result

def get_sensor_units_vu(shortname):

    db_string = get_dbstring(shortname)    

    # Query to get the sensor units
    query = f"""
    SELECT id AS unit_id, abbreviation AS unit
    FROM cdr.units
    WHERE id IN ({db_string})
    """

    result = run_pg_query(query, config_section='postgresql_vu')

    return result


def get_sensorinfo_siteid_name_combo_vu(siteid_names_combo):
  
    # Convert siteid_names_combo to a string by removing the [ and ] characters
    siteid_names_combo = str(siteid_names_combo).replace('[', '').replace(']', '')
    
    query = f"""
    SELECT id AS sensor_id, unit AS unit_id, name AS sensor_name, site AS site_id
    FROM cdr.logvalproviders
    WHERE (site, name) IN ({siteid_names_combo})
    """
                
    sensor_info = run_pg_query(query, config_section='postgresql_vu')

    # get vector of unit_ids from result
    unit_ids = sensor_info['unit_id'].to_list()

    # Get sensor units
    sensor_units = get_sensor_units_vu(unit_ids)  
    
    # Add 'unit' from sensor_units to sensor_info using Polars join
    sensor_info = sensor_info.join(
        sensor_units,
        on='unit_id',
        how='left'
    )

    return sensor_info

def get_data_vudb(sensorid, start_dt, end_dt, limit=None):
    
    sensorid_db_string = get_dbstring(sensorid)      

    # Check if start_dt and end_dt are datetime objects, if convert to strings take timezone into account
    if isinstance(start_dt, datetime):
        start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S%z')
    if isinstance(end_dt, datetime):
        end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S%z')
 
    if limit is None:
        query = f"""
        SELECT dt, logicid, value
        FROM cdr.pointdata
        WHERE logicid IN ({sensorid_db_string})
        AND dt BETWEEN '{start_dt}' AND '{end_dt}'
        """
    else:
        query = f"""
        SELECT dt, logicid, value
        FROM cdr.pointdata
        WHERE logicid IN ({sensorid_db_string})
        AND dt BETWEEN '{start_dt}' AND '{end_dt}'
        LIMIT {limit}
        """

    result = run_pg_query(query, config_section='postgresql_vu')

    return result

def get_data_wurdb(sensorid, start_dt, end_dt, limit=None):
    
    sensorid_db_string = get_dbstring(sensorid)      

    # Check if start_dt and end_dt are datetime objects, if convert to strings take timezone into account
    if isinstance(start_dt, datetime):
        start_dt = start_dt.strftime('%Y-%m-%d %H:%M:%S%z')
    if isinstance(end_dt, datetime):
        end_dt = end_dt.strftime('%Y-%m-%d %H:%M:%S%z')

    if limit is None:     
        query = f"""
        SELECT time AS dt, sensor_id AS logicid, value
        FROM sensor_data
        WHERE sensor_id IN ({sensorid_db_string})
        AND time BETWEEN '{start_dt}' AND '{end_dt}'
        """
    else:
        query = f"""
        SELECT time AS dt, sensor_id AS logicid, value
        FROM sensor_data
        WHERE sensor_id IN ({sensorid_db_string})
        AND time BETWEEN '{start_dt}' AND '{end_dt}'
        LIMIT {limit}
        """

    result = run_pg_query(query, config_section='postgresql_wur')

    return result
       
def get_sensorinfo_by_site_and_varname_vu(check_table):
    
    # get sites by selecting all unique values in the 'Station' column of the check_table
    sites = check_table['Station'].unique().tolist()
    
    # Get the siteid and siteid_name from the database
    siteid_name = get_siteids_vu(sites)
    
    # Convert check_table to Polars if it's pandas
    if hasattr(check_table, 'index'):  # It's pandas
        check_table_pl = pl.from_pandas(check_table)
    else:
        check_table_pl = check_table
    
    # Create mapping for site_id using join
    check_table_with_siteid = check_table_pl.join(
        siteid_name,
        left_on='Station',
        right_on='name',
        how='left'
    ).with_columns([
        pl.col('site_id').fill_null(-9999).cast(pl.Int64).alias('siteid')
    ])
    
    # Get unique siteid and Variable combinations
    siteid_varname = check_table_with_siteid.select(['siteid', 'Variable']).unique()
    
    # Convert to list of tuples for the database query
    siteid_varname_tuples = [
        (row['siteid'], row['Variable']) 
        for row in siteid_varname.to_dicts()
    ]
    
    sensor_info = get_sensorinfo_siteid_name_combo_vu(siteid_varname_tuples)
    
    if sensor_info is None or sensor_info.height == 0:
        return pl.DataFrame()
    
    # Add site_name using join
    sensor_info = sensor_info.join(
        siteid_name.select(['site_id', 'name']).rename({'name': 'site_name'}),
        on='site_id',
        how='left'
    )
    
    # Add variable_name using join
    variable_mapping = check_table_pl.select(['Variable', 'Variable_name']).unique()
    sensor_info = sensor_info.join(
        variable_mapping,
        left_on='sensor_name',
        right_on='Variable',
        how='left'
    ).rename({'Variable_name': 'variable_name'})
    
    # Drop the extra 'Variable' column if it exists
    if 'Variable' in sensor_info.columns:
        sensor_info = sensor_info.drop('Variable')
    
    # Add source column
    sensor_info = sensor_info.with_columns([
        pl.lit('vu_db').alias('source')
    ])
        
    return sensor_info

def get_sensorinfo_by_site_and_varname_wur(check_table):
    
    # get sites by selecting all unique values in the 'Station' column of the check_table
    sites = check_table['Station'].unique().tolist()
    
    # Get the siteid and siteid_name from the database
    sensor_info = get_sensorinfo_wur(sites)
    # sensor_info = sensor_info.to_pandas()
    
    if sensor_info is None or sensor_info.height == 0:
        print("No sensor information found for the specified sites.")
        return None
    
    # Only keep the rows where the sensor_name is in the check_table    
    # Convert to Polars syntax
    variables_list = check_table['Variable'].tolist()
    sensor_info = sensor_info.filter(
        pl.col('sensor_name').is_in(variables_list)
    )

    # Add Variable_name column from check_table to sensor_info by matching the sensor_name with the Variable column in check_table
    # Convert check_table to polars for easier joining
    check_table_pl = pl.from_pandas(check_table) if hasattr(check_table, 'index') else check_table
    
    # Create mapping using Polars join
    variable_mapping = check_table_pl.select(['Variable', 'Variable_name']).unique()
    
    # Join sensor_info with variable_mapping
    sensor_info = sensor_info.join(
        variable_mapping,
        left_on='sensor_name',
        right_on='Variable',
        how='left'
    ).rename({'Variable_name': 'variable_name'})
    
    # Drop the extra 'Variable' column if it exists
    if 'Variable' in sensor_info.columns:
        sensor_info = sensor_info.drop('Variable')

    # Add source column to sensor_info using Polars
    sensor_info = sensor_info.with_columns([
        pl.lit('wur_db').alias('source')
    ])
  
    return sensor_info

def get_data(check_table, start_dt, end_dt, source='wur_db', limit=None):
    
    # Convert check_table to polars if it's pandas
    if hasattr(check_table, 'to_pandas'):
        check_table_pl = check_table
    else:
        check_table_pl = pl.from_pandas(check_table)
    
    # Check if check_table is None or empty
    if check_table_pl is None or check_table_pl.height == 0:
        return None, None
    
    # Get the sensor_info by site and varname combination
    if source == 'vu_db':
        sensorinfo_df = get_sensorinfo_by_site_and_varname_vu(check_table)
    elif source == 'wur_db':
        sensorinfo_df = get_sensorinfo_by_site_and_varname_wur(check_table)
    else:
        raise ValueError(f"Unknown source: {source}. Supported sources are 'vu_db' and 'wur_db'.")
    
    # Check if sensorinfo_df is None or empty
    if sensorinfo_df is None or sensorinfo_df.height == 0:
        return None, None
    
    # Get the sensor_ids from sensorinfo_df
    sensorids = sensorinfo_df['sensor_id'].to_list()
    
    # Get the sensor data from the database
    if source == 'vu_db':
        data = get_data_vudb(sensorids, start_dt, end_dt, limit=limit)
    elif source == 'wur_db':
        data = get_data_wurdb(sensorids, start_dt, end_dt, limit=limit)
    else:
        raise ValueError(f"Unknown source: {source}. Supported sources are 'vu_db' and 'wur_db'.")

    # Check if data is None or empty
    if data is None or data.height == 0:
        print(f"No data found for period {start_dt} - {end_dt}")
        return sensorinfo_df, pl.DataFrame()

    # Remove duplicates based on 'dt' and 'logicid' to ensure unique entries
    data_nodup = data.unique(subset=['dt', 'logicid'])
    
    # Pivot the DataFrame using Polars
    data_df = data_nodup.pivot(index='dt', columns='logicid', values='value')

    # Sort the columns based on dt
    data_df = data_df.sort('dt')
    
    # Get existing columns (excluding 'dt')
    existing_cols = set(data_df.columns) - {'dt'}
    missing_cols = set(str(sid) for sid in sensorids) - existing_cols
    
    # Add missing columns with null values
    for col in missing_cols:
        data_df = data_df.with_columns([
            pl.lit(None, dtype=pl.Float64).alias(col)
        ])

    # Reorder columns to match sensorids order (add 'dt' first)
    column_order = ['dt'] + [str(sid) for sid in sensorids if str(sid) in data_df.columns]
    data_df = data_df.select(column_order)

    # Rename 'dt' column to 'datetime'
    data_df = data_df.rename({'dt': 'datetime'})
    
    return sensorinfo_df, data_df
