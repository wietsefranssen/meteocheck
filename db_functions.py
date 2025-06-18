from get_dbstring import get_dbstring
from config import load_config
import psycopg2

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
