import dash
from dash import html, Input, Output, ctx
import dash_bootstrap_components as dbc
import pandas as pd


def register_callbacks(app, pivot_table, check_table, nan_table):
    """Register all callbacks for the data availability table application"""
    
    # Callback for handling cell clicks
    @app.callback(
        Output('cell-click-output', 'children'),
        [
         Input('nan-percentage-aggrid', 'cellClicked'),
         Input('nan-percentage-aggrid', 'selectedRows')],
    )
    def display_click_data(aggrid_clicked, aggrid_selected):
        ctx_trigger = ctx.triggered[0]['prop_id'] if ctx.triggered else None
                
        if ('nan-percentage-aggrid' in str(ctx_trigger)):
            # Handle both cellClicked and selectedRows events
            if aggrid_clicked and aggrid_clicked.get('colId') != 'Station':
                # Handle the actual structure of cellClicked event
                if 'rowData' in aggrid_clicked:
                    station = aggrid_clicked['rowData']['Station']
                elif 'data' in aggrid_clicked:
                    station = aggrid_clicked['data']['Station']
                else:
                    # Try to get station from rowIndex and aggrid data
                    row_index = aggrid_clicked.get('rowIndex', 0)
                    pivot_records = pivot_table.to_dict('records')
                    if row_index < len(pivot_records):
                        station = pivot_records[row_index]['Station']
                    else:
                        station = 'Unknown'
                
                col_id = aggrid_clicked['colId']
                availability_percentage = aggrid_clicked.get('value', 0)
                
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
                    html.H5(f"AgGrid Selected: {station} - {col_id}", className="alert-heading"),
                    html.P([
                        f"Station: {station}",
                        html.Br(),
                        f"Variable: {col_id}",
                        html.Br(),
                        f"Sensor Name: {sensor_name}",
                        html.Br(),
                        f"Sensor ID: {sensor_id}",
                        html.Br(),
                        f"Data available: {availability_percentage:.1f}%",
                        html.Br(),
                        f"Missing data: {100 - availability_percentage:.1f}%"
                    ])
                ], color="success", dismissable=True)
            
            elif aggrid_selected and len(aggrid_selected) > 0:
                # Fallback to selectedRows if cellClicked doesn't work
                selected_row = aggrid_selected[0]
                station = selected_row['Station']
                
                return dbc.Alert([
                    html.H5(f"AgGrid Row Selected: {station}", className="alert-heading"),
                    html.P([
                        f"Station: {station}",
                        html.Br(),
                        f"Click on a specific data cell to see detailed sensor information."
                    ])
                ], color="warning", dismissable=True)
        
        return ""
    
    # Debug callback to see what AgGrid events are firing
    @app.callback(
        Output('debug-output', 'children'),
        [Input('nan-percentage-aggrid', 'cellClicked'),
         Input('nan-percentage-aggrid', 'selectedRows')]
    )
    def debug_aggrid_events(cell_clicked, selected_rows):
        if cell_clicked:
            return html.Div([
                html.P(f"Cell clicked event structure:"),
                html.Pre(f"{cell_clicked}"),
                html.Hr()
            ])
        if selected_rows:
            return html.Div([
                html.P(f"Rows selected: {len(selected_rows)} rows"),
                html.Pre(f"First row: {selected_rows[0] if selected_rows else 'None'}"),
                html.Hr()
            ])
        return ""
