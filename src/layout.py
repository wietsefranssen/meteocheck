from dash import html, dcc
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import ThemeChangerAIO


def create_app_layout(dm, data_df, aggrid_datatable, pivot_table):
    """Create the main application layout"""
    
    # theme_change = ThemeChangerAIO(aio_id="theme")
    
    return html.Div([
        # theme_change,
        dcc.Store(id='selected-cells-store', data=[]),  # Store for tracking selected cells
        dcc.Store(id='pivot-table-store', data=pivot_table.to_dict('records')),  # Store pivot table data
        dbc.Container(
            [
                html.H3("Data Availability Table", className="mb-4"),
                html.P([
                    "💡 Tip: Use Ctrl+click to select multiple cells, or drag to select ranges. ",
                    "Selection updates are event-driven for instant response without browser polling. ",
                    "Selected cells and timeline will appear below immediately."
                ], className="mb-3", style={"fontSize": "14px", "color": "#6c757d", "fontStyle": "italic"}),
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
                aggrid_datatable,
                html.Div(id="selection-info", className="mt-3 mb-3"),
                dcc.Graph(id="timeline-graph", style={"display": "none"}),
                html.Div(id="cell-click-output", className="mt-3"),
                html.Div(id="debug-output", className="mt-2", style={"fontSize": "12px", "color": "gray"})
            ],
            fluid=True,
            className="dbc dbc-ag-grid"
        ),
    ])
