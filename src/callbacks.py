import dash
from dash import html, Input, Output, State, ctx, dcc, clientside_callback
import dash_bootstrap_components as dbc
import pandas as pd
from dash_bootstrap_templates import ThemeChangerAIO
from .timeline_plot import create_timeline_plot, create_multi_timeline_plot

# Define theme configurations
LIGHT_THEME = {
    'background': '#ffffff',
    'paper': '#ffffff', 
    'text': '#212529',
    'grid': '#dee2e6',
    'primary': '#0d6efd',
    'font_family': 'system-ui, -apple-system, "Segoe UI", Roboto'
}

DARK_THEME = {
    'background': '#212529',
    'paper': '#343a40', 
    'text': '#ffffff',
    'grid': '#495057',
    'primary': '#0d6efd',
    'font_family': 'system-ui, -apple-system, "Segoe UI", Roboto'
}

def get_plotly_theme(is_dark=False):
    """Get Plotly layout theme based on dark/light mode"""
    theme = DARK_THEME if is_dark else LIGHT_THEME
    
    return {
        'plot_bgcolor': theme['background'],
        'paper_bgcolor': theme['paper'],
        'font': {'color': theme['text'], 'family': theme['font_family']},
        'xaxis': {
            'gridcolor': theme['grid'],
            'zerolinecolor': theme['grid'],
            'tickcolor': theme['text'],
            'linecolor': theme['grid']
        },
        'yaxis': {
            'gridcolor': theme['grid'],
            'zerolinecolor': theme['grid'],
            'tickcolor': theme['text'],
            'linecolor': theme['grid']
        }
    }


def register_callbacks(app, pivot_table, check_table, nan_table, data_df):
    """Register all callbacks for the data availability table application"""
    
    # Clientside callback to detect Django theme changes
    app.clientside_callback(
        """
        function(n_intervals) {
            const htmlElement = document.documentElement;
            const currentTheme = htmlElement.getAttribute('data-bs-theme') || 'light';
            return currentTheme;
        }
        """,
        Output('django-theme-store', 'data'),
        [Input('theme-interval', 'n_intervals')]
    )
    
    # Remove the clientside callback and use this server-side callback instead
    @app.callback(
        Output("selected-cells-store", "data"),
        [Input("nan-percentage-aggrid", "cellClicked"),
         Input("nan-percentage-aggrid", "selectedCells")],
        prevent_initial_call=True
    )
    def handle_cell_selection(cell_clicked, selected_cells):
        # Handle both single clicks and multi-selection
        all_cells = []
        
        # Add single clicked cell
        if cell_clicked and cell_clicked.get('colId') != 'Station':
            station = ''
            if 'rowData' in cell_clicked:
                station = cell_clicked['rowData'].get('Station', '')
            elif 'rowIndex' in cell_clicked and cell_clicked['rowIndex'] < len(pivot_table):
                station = pivot_table.iloc[cell_clicked['rowIndex']]['Station']
            
            all_cells.append({
                'station': station,
                'variable': cell_clicked.get('colId', ''),
                'value': cell_clicked.get('value', 0),
                'type': 'single'
            })
        
        # Add selected range cells
        if selected_cells:
            for cell in selected_cells:
                if cell.get('colId') != 'Station':
                    station = ''
                    if 'rowData' in cell:
                        station = cell['rowData'].get('Station', '')
                    elif 'rowIndex' in cell and cell['rowIndex'] < len(pivot_table):
                        station = pivot_table.iloc[cell['rowIndex']]['Station']
                    
                    all_cells.append({
                        'station': station,
                        'variable': cell.get('colId', ''),
                        'value': cell.get('value', 0),
                        'type': 'range'
                    })
        
        return all_cells

    # Main callback for handling selection changes and updating info and graph
    @app.callback(
        [Output('selection-info', 'children'),
         Output('selection-info', 'style'),
         Output('timeline-graph', 'figure'),
         Output('timeline-graph', 'style')],
        [Input('selected-cells-store', 'data'),
         Input('django-theme-store', 'data')],
    )
    def display_selection_data(selected_cells, theme_data):
        # Get current theme
        current_theme = 'light'  # default
        if theme_data:
            if isinstance(theme_data, dict):
                current_theme = theme_data.get('theme', 'light')
            elif isinstance(theme_data, str):
                current_theme = theme_data
        
        # Base style for selection info (theme-aware)
        if current_theme == 'dark':
            base_style = {
                "backgroundColor": "#343a40", 
                "color": "#ffffff",
                "padding": "10px", 
                "borderRadius": "5px",
                "border": "1px solid #495057"
            }
        else:
            base_style = {
                "backgroundColor": "#f8f9fa", 
                "padding": "10px", 
                "borderRadius": "5px",
                "border": "1px solid #dee2e6"
            }
        
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
            timeline_fig = create_multi_timeline_plot(data_df, selected_cells, check_table, nan_table, current_theme)
            
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
        
        # If no selections, show helpful message
        selection_info = [html.P("No cells selected. Use Ctrl+Click or drag to select cells in the table above.", 
                               style={"color": "#6c757d", "fontStyle": "italic"})]
                
        return selection_info, base_style, empty_fig, hidden_style
