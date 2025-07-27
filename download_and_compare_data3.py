from curses.ascii import BS
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx
from src.plot import make_figure  
from src.corrections import find_incorrect_airpressure_sensors, correct_airpressure_units 
from src.tablenew import create_nan_percentage_table
from src.callbacks import register_callbacks
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


    # Create the data availability percentage table
    nan_table = create_nan_percentage_table(data_df, sensorinfo_df, check_table)
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='NaN_Percentage')
    pivot_table = pivot_table.fillna(0.0)  # Fill any missing values with 0% data availability
    
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

    # Create AgGrid DataTable for data availability percentages
    def create_aggrid_datatable(pivot_table):
        """Create an ag-Grid DataTable for data availability percentages"""
        
        # Prepare column definitions for AgGrid
        columnDefs = [
            {"headerName": "Station", "field": "Station", "sortable": True, "filter": True, "pinned": "left"},
        ]
        
        # Add variable columns with conditional formatting in check_table order
        for col in check_table.columns[2:]:  # Use check_table column order
            if col in pivot_table.columns:  # Only add if column exists in pivot_table
                columnDefs.append({
                    "headerName": col, 
                    "field": col, 
                    "sortable": True, 
                    "filter": True,
                    "type": "numericColumn",
                    "cellStyle": {
                        "styleConditions": [
                            {
                                "condition": "params.value > 80",
                                "style": {"backgroundColor": "#d4edda", "color": "black"}
                            },
                            {
                                "condition": "params.value >= 30 && params.value <= 80", 
                                "style": {"backgroundColor": "#fff3cd", "color": "black"}
                            },
                            {
                                "condition": "params.value < 30",
                                "style": {"backgroundColor": "#f8d7da", "color": "black"}
                            }
                        ]
                    }
                })
        
        return dag.AgGrid(
            enableEnterpriseModules=True,
            id='nan-percentage-aggrid',
            columnDefs=columnDefs,
            rowData=pivot_table.to_dict('records'),
            defaultColDef={
                "flex": 1,
                "minWidth": 100,
                "editable": False,
                "resizable": True
            },
            dashGridOptions={
                "pagination": False,
                "animateRows": True,
                "rowSelection": "single",
                "suppressRowClickSelection": False,
                "enableRangeSelection": True,
                "suppressCellFocus": False,
                "rowHeight": 30,
                "domLayout": "autoHeight"

            },
            style={"width": "100%"},
        )
    aggrid_datatable = create_aggrid_datatable(pivot_table)

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
                html.H3("Data Availability Table (click a cell to highlight below)", className="mb-4"),
                html.P([
                    f"Data period: {dm.start_dt.strftime('%Y-%m-%d %H:%M')} to {dm.end_dt.strftime('%Y-%m-%d %H:%M')} "
                    f"({len(data_df)} data points)"
                ], className="mb-2", style={"fontSize": "14px", "color": "gray"}),
                html.P([
                    "This table shows the percentage of available data for each sensor at each station. ",
                    html.Span("Green", style={'color': '#28a745', 'fontWeight': 'bold'}), " indicates good data availability (>80%), ",
                    html.Span("Yellow", style={'color': '#ffc107', 'fontWeight': 'bold'}), " indicates moderate data availability (30-80%), and ",
                    html.Span("Red", style={'color': '#dc3545', 'fontWeight': 'bold'}), " indicates poor data availability (<30%)."
                ], className="mb-3"),
                # datatable,
                aggrid_datatable,
                html.Div(id="cell-click-output", className="mt-3"),
                html.Div(id="debug-output", className="mt-2", style={"fontSize": "12px", "color": "gray"})
            ],
            fluid=True,
            className="dbc dbc-ag-grid"
        ),
    ])
    
    # Register callbacks from separate file
    register_callbacks(app, pivot_table, check_table, nan_table)
    
    return app

if __name__ == "__main__":
    app = download_and_compare_data(application='standalone')
    app.run(debug=True)
