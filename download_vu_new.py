import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from vudb import get_site_id_by_name, get_sensorinfo_by_siteid_and_sensorname, get_data
from vudb import read_csv_with_header, select_variables
from vudb import fix_start_end_dt
from download_vu import get_sensorinfo_by_siteid_and_sensorname, get_data
from config import load_config

# Make connection tp postgresql database
def connect_to_db():
    """
    Connect to PostgreSQL database and retrieve data.
    """
    # Database connection parameters
    db_config = load_config()

    try:
        # Establish connection
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        return conn, cursor
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None, None
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        if cursor:
            cursor.close()
            print("Cursor closed.")
    return conn, cursor

connect_to_db()