import dash
from dash import html, Input, Output, State, ctx, dcc, clientside_callback
import dash_bootstrap_components as dbc
import pandas as pd
from dash_bootstrap_templates import ThemeChangerAIO
from .timeline_plot import create_timeline_plot, create_multi_timeline_plot


def register_callbacks(app, pivot_table, check_table, nan_table, data_df):
    """Register all callbacks for the data availability table application"""
    
    """Register all callbacks for the data availability table application"""
    
    # Clientside callback to continuously check for selected cells using AG Grid API with simple debouncing
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
                
                // Create a unique identifier for the current selection to detect changes
                const currentSelection = JSON.stringify(cellData.map(cell => `${cell.station}-${cell.variable}`).sort());
                
                // Initialize if not exists
                if (typeof window.lastSelectionHash === 'undefined') {
                    window.lastSelectionHash = '';
                    window.lastChangeTime = 0;
                }
                
                const now = Date.now();
                
                // Check if selection has changed
                if (window.lastSelectionHash !== currentSelection) {
                    window.lastSelectionHash = currentSelection;
                    window.lastChangeTime = now;
                    window.pendingSelection = cellData;
                    // Don't update immediately - wait for debounce period
                    return dash_clientside.no_update;
                } else {
                    // Selection hasn't changed - check if enough time has passed since last change
                    if (window.pendingSelection && (now - window.lastChangeTime) >= 800) {
                        // Debounce period has passed, send the update
                        const result = window.pendingSelection;
                        window.pendingSelection = null;
                        return result;
                    }
                    
                    // Either no pending selection or debounce period not yet passed
                    return dash_clientside.no_update;
                }
                
            } catch (error) {
                console.log('Grid not ready yet or error:', error);
                return dash_clientside.no_update;
            }
        }""",
        Output("selected-cells-store", "data"),
        Input("selection-interval", "n_intervals"),
        prevent_initial_call=True
    )

    # Optional: Add callback to track pending selections and show visual feedback
    clientside_callback(
        """
        function(n_intervals) {
            // Check if there's a pending selection update
            if (typeof window.pendingSelection !== 'undefined' && window.pendingSelection !== null) {
                return true;  // Selection is pending
            }
            return false;  // No pending selection
        }
        """,
        Output("selection-pending-store", "data"),
        Input("selection-interval", "n_intervals"),
        prevent_initial_call=True
    )

    # Main callback for handling selection changes and updating info and graph
    @app.callback(
        [Output('selection-info', 'children'),
         Output('selection-info', 'style'),
         Output('timeline-graph', 'figure'),
         Output('timeline-graph', 'style')],
        [Input('selected-cells-store', 'data'),
         Input('selection-pending-store', 'data'),
         Input(ThemeChangerAIO.ids.radio("theme"), "value")],
    )
    def display_selection_data(selected_cells, is_pending, theme_url):
        # Base style for selection info
        base_style = {
            "backgroundColor": "#f8f9fa", 
            "padding": "10px", 
            "borderRadius": "5px",
            "border": "1px solid #dee2e6"
        }
        
        # Show pending state if selection is being debounced
        if is_pending and (not selected_cells or len(selected_cells) == 0):
            base_style.update({
                "backgroundColor": "#fff3cd",
                "border": "1px solid #ffeaa7",
                "transition": "all 0.3s ease"
            })
        
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
            
            # Create multi-timeline plot for all selected cells
            timeline_fig = create_multi_timeline_plot(data_df, selected_cells, check_table, nan_table, theme_url)
            
            # Add info about the displayed timeline
            selection_info.append(html.Hr(className="my-2"))
            unique_sensors = set()
            for cell in selected_cells:
                station = cell.get('station', '')
                variable = cell.get('variable', '')
                unique_sensors.add(f"{station} - {variable}")
            
            selection_info.append(html.P([
                html.Strong("Timeline showing: "),
                f"{len(unique_sensors)} unique sensors from selected cells"
            ], className="mb-1", style={"color": "#007bff"}))
            
            return selection_info, base_style, timeline_fig, visible_style
        
        # If no selections, show helpful message with debounce info
        selection_info = [html.P([
            "No cells selected. Use Ctrl+Click or drag to select cells in the table above. ",
            html.Small("(Updates are debounced by 800ms for smooth performance)", 
                      style={"color": "#9ea1a5", "fontStyle": "italic"})
        ], style={"color": "#6c757d", "fontStyle": "italic"})]
                
        return selection_info, base_style, empty_fig, hidden_style
