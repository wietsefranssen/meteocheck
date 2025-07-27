import dash
from dash import html, Input, Output, State, ctx, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from dash_bootstrap_templates import ThemeChangerAIO
from .timeline_plot import create_timeline_plot


def register_callbacks(app, pivot_table, check_table, nan_table, data_df):
    """Register all callbacks for the data availability table application"""
    
    # Callback to track cell selections
    @app.callback(
        Output('selected-cells-store', 'data'),
        [Input('nan-percentage-aggrid', 'cellClicked'),
         Input('clear-cells-btn', 'n_clicks'),
         Input('clear-all-btn', 'n_clicks')],
        [State('selected-cells-store', 'data'),
         State('pivot-table-store', 'data')]
    )
    def track_cell_selections(cell_clicked, clear_cells_clicks, clear_all_clicks, current_selections, pivot_data):
        ctx_trigger = ctx.triggered[0]['prop_id'] if ctx.triggered else None
        
        # Clear buttons
        if 'clear-cells-btn' in str(ctx_trigger) or 'clear-all-btn' in str(ctx_trigger):
            return []
        
        if not cell_clicked:
            return current_selections or []
        
        # Extract cell information
        if cell_clicked.get('colId') == 'Station':
            return current_selections or []  # Don't track Station column clicks
        
        # Use the same logic as the main callback to extract station
        if 'rowData' in cell_clicked:
            station = cell_clicked['rowData']['Station']
        elif 'data' in cell_clicked:
            station = cell_clicked['data']['Station']
        else:
            # Try to get station from rowIndex and pivot data
            row_index = cell_clicked.get('rowIndex', 0)
            if pivot_data and row_index < len(pivot_data):
                station = pivot_data[row_index]['Station']
            else:
                station = 'Unknown'
            
        cell_info = {
            'station': station,
            'variable': cell_clicked.get('colId', 'Unknown'),
            'value': cell_clicked.get('value', 'N/A'),
            'rowIndex': cell_clicked.get('rowIndex', 0)
        }
        
        # Check if this cell is already selected
        current_selections = current_selections or []
        cell_key = f"{cell_info['station']}_{cell_info['variable']}"
        
        # Check if cell is already in selection
        is_already_selected = any(f"{cell['station']}_{cell['variable']}" == cell_key 
                                for cell in current_selections)
        
        if is_already_selected:
            # Remove the cell (toggle off)
            current_selections = [cell for cell in current_selections 
                                if f"{cell['station']}_{cell['variable']}" != cell_key]
        else:
            # Add the new cell (toggle on)
            current_selections.append(cell_info)
            
            # Keep only last 10 selections to avoid excessive memory usage
            if len(current_selections) > 10:
                current_selections = current_selections[-10:]
            
        return current_selections
    
    # Callback to handle clear all selections (for AgGrid row deselection)
    @app.callback(
        Output('nan-percentage-aggrid', 'deselectAll'),
        [Input('clear-all-btn', 'n_clicks')]
    )
    def clear_all_selections(n_clicks):
        if n_clicks:
            return True
        return False
    
    # Main callback for handling selection changes and updating info and graph
    @app.callback(
        [Output('cell-click-output', 'children'),
         Output('timeline-graph', 'figure'),
         Output('timeline-graph', 'style'),
         Output('selection-info', 'children')],
        [
         Input('nan-percentage-aggrid', 'cellClicked'),
         Input('nan-percentage-aggrid', 'selectedRows'),
         Input('selected-cells-store', 'data'),
         Input(ThemeChangerAIO.ids.radio("theme"), "value")],
    )
    def display_selection_data(aggrid_clicked, aggrid_selected, selected_cells, theme_url):
        ctx_trigger = ctx.triggered[0]['prop_id'] if ctx.triggered else None
        
        # Default returns
        empty_fig = {"data": [], "layout": {}}
        hidden_style = {"display": "none"}
        visible_style = {"display": "block"}
        
        # Show current selection info based on selectedRows and selected cells
        selection_info = []
        
        # Show selected rows
        if aggrid_selected and len(aggrid_selected) > 0:
            selection_info.append(html.H6("📋 Selected Rows:", className="mb-2"))
            selected_count = len(aggrid_selected)
            selection_info.append(html.P(f"Count: {selected_count}"))
            for i, row in enumerate(aggrid_selected[:3]):  # Show first 3 rows
                station = row.get('Station', 'Unknown')
                selection_info.append(html.P(f"  • {station}", className="mb-1", style={"marginLeft": "20px"}))
            if selected_count > 3:
                selection_info.append(html.P(f"  ... and {selected_count - 3} more", className="mb-1", style={"marginLeft": "20px"}))
        
        # Show selected cells
        if selected_cells and len(selected_cells) > 0:
            if selection_info:  # Add separator if we already have row info
                selection_info.append(html.Hr(className="my-2"))
            selection_info.append(html.H6("🎯 Selected Cells:", className="mb-2"))
            cell_count = len(selected_cells)
            selection_info.append(html.P(f"Count: {cell_count} (click cells again to deselect)"))
            for i, cell in enumerate(selected_cells[-5:]):  # Show last 5 cells
                station = cell.get('station', 'Unknown')
                variable = cell.get('variable', 'Unknown')
                value = cell.get('value', 'N/A')
                if isinstance(value, (int, float)):
                    value_str = f"{value:.1f}%"
                else:
                    value_str = str(value)
                selection_info.append(html.P(f"  • {station} - {variable}: {value_str}", 
                                           className="mb-1", style={"marginLeft": "20px"}))
            if cell_count > 5:
                selection_info.append(html.P(f"  ... showing last 5 of {cell_count} selected", 
                                           className="mb-1", style={"marginLeft": "20px", "fontStyle": "italic"}))
        
        # If no selections, show helpful message
        if not selection_info:
            selection_info = [html.P("No selections yet. Click on rows to select them, or click on data cells to toggle selection.", 
                                   style={"color": "#6c757d", "fontStyle": "italic"})]
                
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
                
                # Create timeline plot
                timeline_fig = create_timeline_plot(data_df, sensor_id, station, col_id, sensor_name, theme_url)
                
                alert = dbc.Alert([
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
                        f"Data available: {availability_percentage:.1f}%",
                        html.Br(),
                        f"Missing data: {100 - availability_percentage:.1f}%"
                    ])
                ], color="success", dismissable=True)
                
                return alert, timeline_fig, visible_style, selection_info
            
            elif aggrid_selected and len(aggrid_selected) > 0:
                # Fallback to selectedRows if cellClicked doesn't work
                selected_row = aggrid_selected[0]
                station = selected_row['Station']
                
                alert = dbc.Alert([
                    html.H5(f"Row Selected: {station}", className="alert-heading"),
                    html.P([
                        f"Station: {station}",
                        html.Br(),
                        f"Click on a specific data cell to see detailed sensor information and timeline."
                    ])
                ], color="warning", dismissable=True)
                
                return alert, empty_fig, hidden_style, selection_info
        
        return "", empty_fig, hidden_style, selection_info if selection_info else ""
    
    # Debug callback to see what AgGrid events are firing
    @app.callback(
        Output('debug-output', 'children'),
        [Input('nan-percentage-aggrid', 'cellClicked'),
         Input('nan-percentage-aggrid', 'selectedRows'),
         Input('selected-cells-store', 'data')]
    )
    def debug_aggrid_events(cell_clicked, selected_rows, selected_cells):
        debug_info = []
        
        if cell_clicked:
            debug_info.append(html.Div([
                html.P(f"Cell clicked event:"),
                html.Pre(f"{cell_clicked}"),
            ]))
            
        if selected_rows:
            debug_info.append(html.Div([
                html.P(f"Rows selected: {len(selected_rows)} rows"),
                html.Pre(f"Selected rows data: {selected_rows}"),
            ]))
        
        if selected_cells:
            debug_info.append(html.Div([
                html.P(f"Cells tracked: {len(selected_cells)} cells"),
                html.Pre(f"Selected cells: {selected_cells}"),
            ]))
            
        if debug_info:
            return html.Div(debug_info + [html.Hr()])
        return ""
