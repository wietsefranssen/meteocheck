from curses.ascii import BS
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx, dash_table
from src.plot import make_figure  
from src.corrections import find_incorrect_airpressure_sensors, correct_airpressure_units 
from src.table import get_cell_values_and_colors, get_datatable #, generate_color_rules_and_css
from data_manager import DataManager
import plotly.graph_objects as go
import polars as pl
import numpy as np
import os
import dash_bootstrap_components as dbc
# from dash_bootstrap_templates import load_figure_template
from dash_bootstrap_templates import ThemeChangerAIO, template_from_url


def download_and_compare_data(application='standalone'):
    """
    Main function to download and compare data.
    Initializes DataManager, sets dates, downloads data, and processes it.
    """

    dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"


    # loads the "sketchy" template and sets it as the default
    # load_figure_template(["sketchy", "cyborg", "minty"])


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
    
    # Get cell values and colors
    cell_values, cell_colors = get_cell_values_and_colors(dm, sensorinfo_df, data_df, site_names, var_names)

    datatable, css_content, color_rules = get_datatable(cell_values, cell_colors, site_names, var_names)

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

    # Group the data into parts to separate plots using Polars
    sensor_groups_df = sensorinfo_df.group_by('variable_name').agg([
        pl.col('sensor_id').alias('sensor_ids')
    ])

    # Sort sensor_groups_df by variable_name
    sensor_groups_df = sensor_groups_df.sort('variable_name')

    sensor_groups = dict(zip(
        sensor_groups_df['variable_name'].to_list(),
        sensor_groups_df['sensor_ids'].to_list()
    ))

    # Dash app
    BS = "https://cdn.jsdelivr.net/npm/bootstrap@5.3.6/dist/css/bootstrap.min.css"
    # app = dash.Dash(external_stylesheets=[BS])  
    if application=='standalone':
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
        # app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
    elif application=='django':
        from django_plotly_dash import DjangoDash
        # app = DjangoDash(name='dash_meteo', external_stylesheets=[dbc.themes.BOOTSTRAP])
        # app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
        app = DjangoDash(name='dash_meteo', external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
    else:
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
        # app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

    theme_change = ThemeChangerAIO(aio_id="theme")

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
                badge,
                html.A(
                    "Open SharePoint Document",
                    href="https://wageningenur4.sharepoint.com/:x:/r/sites/Veenweiden/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7B3d741ab0-36f1-4687-953b-902b0a009582%7D&wdOrigin=TEAMS-MAGLEV.undefined_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1750680875439&web=1",
                    target="_blank"
                ),
                html.H3("NaN Overview Table (click a cell to highlight below)"),
                datatable, 
            ],
            fluid=True,
            className="dbc dbc-ag-grid"
                #   className="m-4 dbc"),
        ),
        dcc.Checklist(
            id='show-outliers',
            options=[
                {'label': 'Show outliers', 'value': 'show'},
                {'label': 'Remove outliers', 'value': 'remove'}
            ],
            value=['show'],
            style={'margin': '10px'}
        ),
    ])
    
    app.index_string = f'''
    <!DOCTYPE html>
    <html>
        <head>
            {{%metas%}}
            <title>{{%title%}}</title>
            {{%favicon%}}
            {{%css%}}
            <style>
                .low-value {{
                    background-color: rgb(255, 0, 0) !important;
                    color: white !important;
                    font-weight: bold !important;
                    text-align: center !important;
                }}
                .medium-low-value {{
                    background-color: rgb(255, 130, 0) !important;
                    color: black !important;
                    font-weight: bold !important;
                    text-align: center !important;
                }}
                .medium-value {{
                    background-color: rgb(255, 165, 0) !important;
                    color: black !important;
                    font-weight: bold !important;
                    text-align: center !important;
                }}
                .medium-high-value {{
                    background-color: rgb(200, 220, 0) !important;
                    color: black !important;
                    font-weight: bold !important;
                    text-align: center !important;
                }}
                .high-value {{
                    background-color: rgb(0, 255, 0) !important;
                    color: white !important;
                    font-weight: bold !important;
                    text-align: center !important;
                }}
                .ag-cell {{
                    text-align: center !important;
                    display: flex !important;
                    align-items: center !important;
                    justify-content: center !important;
                }}
            </style>
        </head>
        <body>
            {{%app_entry%}}
            <footer>
                {{%config%}}
                {{%scripts%}}
                {{%renderer%}}
            </footer>
        </body>
    </html>
    '''
    return app

if __name__ == "__main__":
    app = download_and_compare_data(application='standalone')
    app.run(debug=True)
