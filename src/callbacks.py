import dash
from dash import html, Input, Output, State, ctx, dcc, clientside_callback
import dash_bootstrap_components as dbc
import pandas as pd
from dash_bootstrap_templates import ThemeChangerAIO
from .timeline_plot import create_timeline_plot


def register_callbacks(app, pivot_table, check_table, nan_table, data_df):
    """Register all callbacks for the data availability table application"""
    
import dash
from dash import html, Input, Output, State, ctx, dcc, clientside_callback
import dash_bootstrap_components as dbc
import pandas as pd
from dash_bootstrap_templates import ThemeChangerAIO
from .timeline_plot import create_timeline_plot


def register_callbacks(app, pivot_table, check_table, nan_table, data_df):
    """Register all callbacks for the data availability table application"""
    
    # Clientside callback to continuously check for selected cells using AG Grid API
    clientside_callback(
        """async (n_intervals) => {
            try {
                const gridApi = await dash_ag_grid.getApiAsync("nan-percentage-aggrid");
                
                let cellData = [];
                
                // Get cell ranges (for Ctrl+click and drag selections)
                const selectedRanges = gridApi.getCellRanges();
                if (selectedRanges && selectedRanges.length > 0) {
                    selectedRanges.forEach(range => {
                        const startRow = Math.min(range.startRow.rowIndex, range.endRow.rowIndex);
                        const endRow = Math.max(range.startRow.rowIndex, range.endRow.rowIndex);
                        
                        for (let rowIndex = startRow; rowIndex <= endRow; rowIndex++) {
                            const rowNode = gridApi.getDisplayedRowAtIndex(rowIndex);
                            if (rowNode) {
                                range.columns.forEach(column => {
                                    const colKey = column.getColId();
                                    // Skip the Station column for selection
                                    if (colKey !== 'Station') {
                                        const value = rowNode.data[colKey];
                                        const station = rowNode.data['Station'];
                                        cellData.push({
                                            row: rowIndex,
                                            station: station,
                                            variable: colKey,
                                            value: value,
                                            type: 'range'
                                        });
                                    }
                                });
                            }
                        }
                    });
                }
                
                return cellData;
            } catch (error) {
                console.log('Grid not ready yet or error:', error);
                return [];
            }
        }""",
        Output("selected-cells-store", "data"),
        Input("selection-interval", "n_intervals"),
    )

    # Main callback for handling selection changes and updating info and graph
    @app.callback(
        [Output('selection-info', 'children'),
         Output('timeline-graph', 'figure'),
         Output('timeline-graph', 'style')],
        [Input('selected-cells-store', 'data'),
         Input(ThemeChangerAIO.ids.radio("theme"), "value")],
    )
    def display_selection_data(selected_cells, theme_url):
        # Default returns
        empty_fig = {"data": [], "layout": {}}
        hidden_style = {"display": "none"}
        visible_style = {"display": "block"}
        
        # Show current selection info based on selected cells
        if selected_cells and len(selected_cells) > 0:
            selection_info = []
            selection_info.append(html.H6("🎯 Selected Cells:", className="mb-2"))
            cell_count = len(selected_cells)
            selection_info.append(html.P(f"Count: {cell_count} cells selected"))
            
            # Show details of selected cells
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
            
            # Show timeline for the most recent selection
            if selected_cells:
                latest_cell = selected_cells[-1]
                station = latest_cell.get('station', '')
                col_id = latest_cell.get('variable', '')
                availability_percentage = latest_cell.get('value', 0)
                
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
                
                # Create timeline plot for latest selected cell
                timeline_fig = create_timeline_plot(data_df, sensor_id, station, col_id, sensor_name, theme_url)
                
                # Add info about the displayed timeline
                selection_info.append(html.Hr(className="my-2"))
                selection_info.append(html.P([
                    html.Strong("Timeline showing: "),
                    f"{station} - {col_id} ({sensor_name})"
                ], className="mb-1", style={"color": "#007bff"}))
                
                return selection_info, timeline_fig, visible_style
        
        # If no selections, show helpful message
        selection_info = [html.P("No cells selected. Use Ctrl+Click or drag to select cells in the table above.", 
                               style={"color": "#6c757d", "fontStyle": "italic"})]
                
        return selection_info, empty_fig, hidden_style
