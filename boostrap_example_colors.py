##https://www.ag-grid.com/javascript-data-grid/cell-styles/
from dash import Dash, html, dcc, Input, Output, Patch, clientside_callback, callback, dash_table
import plotly.express as px
import plotly.io as pio
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template, ThemeChangerAIO
import pandas as pd     
import dash_ag_grid as dag

# Create a simple test dataset with values around 1
test_data = {
    'name': ['A', 'B', 'C', 'D', 'E'],
    'value1': [0.5, 1.0, 1.5, 0.8, 2.0],
    'value2': [0.9, 1.0, 1.2, 0.7, 1.8],
    'value3': [1.1, 1.0, 0.6, 1.3, 0.4]
}
df = pd.DataFrame(test_data)
dff = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/ag-grid/space-mission-data.csv")

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = Dash(__name__, external_stylesheets=[dbc.themes.SPACELAB, dbc.icons.FONT_AWESOME])

# app = Dash(__name__, external_stylesheets=[dbc.themes.COSMO])

theme_change = ThemeChangerAIO(aio_id="theme")

color_mode_switch =  html.Span(
    [
        dbc.Label(className="fa fa-moon", html_for="switch"),
        dbc.Switch( id="switch-theme", value=True, className="d-inline-block ms-1", persistence=True),
        dbc.Label(className="fa fa-sun", html_for="switch"),
    ]
)


# Function to generate color based on value
def value_to_color(value):
    """Convert value (0-2 range) to RGB color"""
    if value <= 1:
        # From red (255,0,0) to orange (255,165,0) for values 0-1
        ratio = value
        red = 255
        green = int(165 * ratio)
        blue = 0
    else:
        # From orange (255,165,0) to green (0,255,0) for values 1-2
        ratio = min((value - 1), 1)
        red = int(255 * (1 - ratio))
        green = int(165 + (255 - 165) * ratio)
        blue = 0
    
    return f"rgb({red}, {green}, {blue})"

# Generate more granular color classes
def generate_color_rules_and_css():
    rules = {}
    css_classes = []
    
    # Create 20 color steps for smooth gradient
    for i in range(21):  # 0 to 20
        value = i * 0.1  # 0.0 to 2.0
        class_name = f"color-step-{i}"
        color = value_to_color(value)
        
        # Determine text color based on brightness
        if value <= 0.5:
            text_color = "white"
        elif value <= 1.5:
            text_color = "black"
        else:
            text_color = "white"
        
        # Add CSS class
        css_classes.append(f"""
        .{class_name} {{
            background-color: {color} !important;
            color: {text_color} !important;
            font-weight: bold !important;
            text-align: center !important;
        }}""")
        
        # Create rule conditions
        if i == 0:
            condition = f"params.data.{{col}} < {value + 0.05}"
        elif i == 20:
            condition = f"params.data.{{col}} >= {value - 0.05}"
        else:
            condition = f"params.data.{{col}} >= {value - 0.05} && params.data.{{col}} < {value + 0.05}"
        
        rules[class_name] = condition
    
    return rules, css_classes

# Generate the rules and CSS
color_rules, css_classes = generate_color_rules_and_css()

# Column definitions using cellStyle approach
columnDefs = [
    {
        "headerName": "Name", 
        "field": "name", 
        "width": 100
    }
]

# Add columns with granular color rules
for i, col in enumerate(['value1', 'value2', 'value3']):
    columnDefs.append({
        "headerName": f"Value {i+1}",
        "field": col,
        "width": 120,
        "type": "numericColumn",
        "cellClassRules": {
            "low-value": f"params.data.{col} <= 0.5",
            "medium-low-value": f"params.data.{col} > 0.5 && params.data.{col} <= 0.8",
            "medium-value": f"params.data.{col} > 0.8 && params.data.{col} <= 1.2",
            "medium-high-value": f"params.data.{col} > 1.2 && params.data.{col} <= 1.5",
            "high-value": f"params.data.{col} > 1.5"
        }
    })

table = html.Div(
    
    dag.AgGrid(
        id="data-table",
        defaultColDef={"flex": 1, "minWidth": 150, "sortable": True, "resizable": True, "filter": True},
        rowData=df.to_dict("records"),
        columnDefs=columnDefs,
        className="ag-theme-alpine",
        dashGridOptions={
            "domLayout": "autoHeight",
            "headerHeight": 40,
            "rowHeight": 35,
        },
        style={"height": "400px", "width": "100%"}
    ),
    # className="m-4"
)

# Generate CSS with all color classes
css_content = "\n".join(css_classes)

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

app.layout = html.Div([
    dbc.Container([
        # theme_change, 
        ThemeChangerAIO(aio_id="theme"),
        html.H3("Test AG Grid with Smooth Gradient Colors"),
        html.P("Values: 0 = Red → 1 = Orange → 2 = Green (smooth gradient)"),
        table,
    ], className="dbc dbc-ag-grid")
])

if __name__ == "__main__":
    print("Starting app...")
    print("Data:")
    print(df)
    print(df.to_dict("records"))
    print("Generated color rules:")
    for i, (class_name, _) in enumerate(list(color_rules.items())[:5]):
        print(f"  {class_name}: {value_to_color(i * 0.1)}")
    app.run(debug=True, port=8050)