from curses.ascii import BS
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx, dash_table
from src.plot import make_figure  
from src.corrections import find_incorrect_airpressure_sensors, correct_airpressure_units 
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
    
    # Remove all sensor_ids from data_df which only contain NaN values
    # Convert to pandas for dropna operation, then back to polars
    data_pd = data_df.to_pandas()
    if 'datetime' in data_pd.columns:
        data_pd = data_pd.set_index('datetime')
    data_pd = data_pd.dropna(axis=1, how='all')
    data_df = pl.from_pandas(data_pd.reset_index())

    # Remove all sensor_ids from sensorinfo_df which are not in data_df
    remaining_sensors = [col for col in data_df.columns if col != 'datetime']
    sensorinfo_df = sensorinfo_df.filter(
        pl.col('sensor_id').cast(pl.Utf8).is_in([str(s) for s in remaining_sensors])
    )

    # Create NaN percentage table
    def create_nan_percentage_table(data_df, sensorinfo_df, check_table):
        """Create a table showing percentage of NaNs per location and sensor type"""
        # Calculate total number of data points
        total_rows = len(data_df)
        
        # Create mapping from sensor_name to sensor_id
        sensor_name_to_id = {}
        for row in sensorinfo_df.iter_rows(named=True):
            sensor_name_to_id[row['sensor_name']] = str(row['sensor_id'])
        
        # Initialize results list
        results = []
        
        # Iterate through each station in check_table
        for _, row in check_table.iterrows():
            station = row['station']
            
            # Get all sensor columns for this station (excluding station and source columns)
            for var_name in check_table.columns[2:]:  # Skip 'station' and 'source' columns
                sensor_name = row[var_name]  # This is actually sensor_name from check_table
                
                if pd.isna(sensor_name) or sensor_name == '':
                    # No sensor for this variable at this station
                    nan_percentage = 100.0
                    actual_sensor_id = ''
                else:
                    # Map sensor_name to sensor_id
                    actual_sensor_id = sensor_name_to_id.get(sensor_name, '')
                    
                    if actual_sensor_id and actual_sensor_id in data_df.columns:
                        # Calculate NaN percentage
                        sensor_data = data_df.select(pl.col(actual_sensor_id))
                        nan_count = sensor_data.null_count().item(0, 0)
                        nan_percentage = (nan_count / total_rows) * 100
                    else:
                        # Sensor not found in data
                        nan_percentage = 100.0
                
                results.append({
                    'Station': station,
                    'Variable': var_name,
                    'Sensor_Name': sensor_name if not pd.isna(sensor_name) else '',
                    'Sensor_ID': actual_sensor_id,
                    'NaN_Percentage': round(nan_percentage, 1)
                })

        return pd.DataFrame(results)

    # Create the NaN percentage table
    nan_table = create_nan_percentage_table(data_df, sensorinfo_df, check_table)
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='NaN_Percentage')
    pivot_table = pivot_table.fillna(100.0)  # Fill any missing values with 100% NaN
    
    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()

    # Dash app
    if application=='standalone':
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
    elif application=='django':
        from django_plotly_dash import DjangoDash
        app = DjangoDash(name='dash_meteo', external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
    else:
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])

    theme_change = ThemeChangerAIO(aio_id="theme")

    # Create DataTable for NaN percentages
    def create_nan_datatable(pivot_table):
        """Create a Dash DataTable for NaN percentages with color coding"""
        
        # Prepare columns for DataTable
        columns = [{"name": "Station", "id": "Station", "type": "text"}]
        for col in pivot_table.columns[1:]:  # Skip 'Station' column
            columns.append({
                "name": col, 
                "id": col, 
                "type": "numeric",
                "format": {"specifier": ".1f"}
            })
        
        # Convert DataFrame to records for DataTable
        data = pivot_table.to_dict('records')
        
        return dash_table.DataTable(
            id='nan-percentage-table',
            columns=columns,
            data=data,
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'fontFamily': 'Arial, sans-serif',
                'fontSize': '12px',
                'minWidth': '80px',
                'width': '80px',
                'maxWidth': '120px',
            },
            style_header={
                'backgroundColor': '#343a40',
                'color': 'white',
                'fontWeight': 'bold',
                'textAlign': 'center'
            },
            style_data={
                'backgroundColor': 'white',
                'color': 'black'
            },
            style_table={'overflowX': 'auto'},
            page_size=25,
            sort_action="native",
            filter_action="native",
            editable=False
        )
    
    datatable = create_nan_datatable(pivot_table)
    print(f"Created datatable with {len(pivot_table)} rows and {len(pivot_table.columns)} columns")

    badge = dbc.Button(
        [
            "Notifications",
            dbc.Badge("4", color="light", text_color="primary", className="ms-1"),
        ],
        color="primary",
    )
 
    app.layout = html.Div([
        theme_change,
        dbc.Container(
            [
                html.H3("NaN Overview Table (click a cell to highlight below)", className="mb-4"),
                html.P([
                    "This table shows the percentage of missing data (NaN) for each sensor at each station. ",
                    html.Span("Green", style={'color': '#28a745', 'fontWeight': 'bold'}), " indicates good data availability (0-10% missing), ",
                    html.Span("Yellow", style={'color': '#ffc107', 'fontWeight': 'bold'}), " indicates moderate missing data (10-50%), ",
                    html.Span("Orange", style={'color': '#fd7e14', 'fontWeight': 'bold'}), " indicates high missing data (50-99%), and ",
                    html.Span("Red", style={'color': '#dc3545', 'fontWeight': 'bold'}), " indicates no data available (100% missing)."
                ], className="mb-3"),
                datatable,
                html.Div(id="cell-click-output", className="mt-3")
            ],
            fluid=True,
            className="dbc dbc-ag-grid"
        ),
    ])
    
    # Callback for handling cell clicks
    @app.callback(
        Output('cell-click-output', 'children'),
        Input('nan-percentage-table', 'active_cell'),
        State('nan-percentage-table', 'data')
    )
    def display_click_data(active_cell, data):
        if active_cell:
            row_idx = active_cell['row']
            col_id = active_cell['column_id']
            
            if col_id != 'Station' and row_idx < len(data):
                station = data[row_idx]['Station']
                nan_percentage = data[row_idx][col_id]
                
                # Find the corresponding sensor information
                sensor_name = ''
                sensor_id = ''
                try:
                    station_row = check_table[check_table['station'] == station]
                    if not station_row.empty and col_id in station_row.columns:
                        sensor_name = station_row.iloc[0][col_id]
                        if pd.isna(sensor_name):
                            sensor_name = 'Not assigned'
                        else:
                            # Find matching sensor_id from nan_table
                            matching_row = nan_table[
                                (nan_table['Station'] == station) & 
                                (nan_table['Variable'] == col_id)
                            ]
                            if not matching_row.empty:
                                sensor_id = matching_row.iloc[0]['Sensor_ID']
                except Exception as e:
                    sensor_name = 'Unknown'
                    sensor_id = 'Unknown'
                
                return dbc.Alert([
                    html.H5(f"Selected: {station} - {col_id}", className="alert-heading"),
                    html.P([
                        f"Station: {station}",
                        html.Br(),
                        f"Variable: {col_id}",
                        html.Br(),
                        f"Sensor Name: {sensor_name}",
                        html.Br(),
                        f"Sensor ID: {sensor_id}",
                        html.Br(),
                        f"Missing data: {nan_percentage:.1f}%",
                        html.Br(),
                        f"Available data: {100 - nan_percentage:.1f}%"
                    ])
                ], color="info", dismissable=True)
        
        return ""
    
    return app

if __name__ == "__main__":
    app = download_and_compare_data(application='standalone')
    app.run(debug=True)
