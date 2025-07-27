from curses.ascii import BS
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx
from src.plot import make_figure  
from src.corrections import find_incorrect_airpressure_sensors, correct_airpressure_units 
from src.tablenew import create_nan_percentage_table
from src.callbacks import register_callbacks
from src.aggrid_table import create_aggrid_datatable
from src.layout import create_app_layout
from src.data_processing import process_data, create_pivot_table
# from src.table import get_cell_values_and_colors, get_datatable #, generate_color_rules_and_css
from data_manager import DataManager
import plotly.graph_objects as go
import polars as pl
import pandas as pd
import numpy as np
import os
import dash_bootstrap_components as dbc
# from dash_bootstrap_templates import load_figure_template
from dash_bootstrap_templates import ThemeChangerAIO, template_from_url
import dash_ag_grid as dag

def download_and_compare_data(application='standalone'):
    dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"

    basepath = os.path.dirname(os.path.abspath(__file__))
    dm = DataManager()
    dm.set_meta_path(os.path.join(basepath, 'meta'))
    dm.set_data_path(os.path.join(basepath, 'data'))
    dm.set_temp_path(os.path.join(basepath, 'temp'))

    dm.set_dates(days_back=7, offset=1)
    # dm.set_dates(start_dt='2025-07-03', end_dt='2025-07-10 23:58:00')
    print(f"start_dt: {dm.start_dt}, end_dt: {dm.end_dt}")

    dm.download_or_load_data()
    data_df, sensorinfo_df = dm.get_data()


    # Detect and correct air pressure sensors with wrong units
    incorrect_sensors = find_incorrect_airpressure_sensors(sensorinfo_df, data_df)
    if incorrect_sensors:
        print("Correcting air pressure sensors:", incorrect_sensors)
        data_df, sensorinfo_df = correct_airpressure_units(data_df, sensorinfo_df, incorrect_sensors)

    # Prepare names
    check_table = dm.check_table  # This stays as pandas
    var_names = check_table.columns[2:].tolist()
    site_names = check_table['station'].unique().tolist()
    site_names.sort()
    
    # Process and clean data
    data_df, sensorinfo_df = process_data(data_df, sensorinfo_df)

    # Create the data availability percentage table
    nan_table = create_nan_percentage_table(data_df, sensorinfo_df, check_table)
    
    # Create pivot table for AgGrid display
    pivot_table = create_pivot_table(nan_table)

    # Dash app
    if application=='standalone':
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
    elif application=='django':
        from django_plotly_dash import DjangoDash
        app = DjangoDash(name='dash_meteo', external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
    else:
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])

    # Create AgGrid DataTable
    aggrid_datatable = create_aggrid_datatable(pivot_table, check_table)

    # Create app layout
    app.layout = create_app_layout(dm, data_df, aggrid_datatable)
    
    # Register callbacks from separate file
    register_callbacks(app, pivot_table, check_table, nan_table, data_df)
    
    return app

if __name__ == "__main__":
    app = download_and_compare_data(application='standalone')
    app.run(debug=True)
