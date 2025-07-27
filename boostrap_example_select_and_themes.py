from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import ThemeChangerAIO, template_from_url
import pandas as pd
import dash_ag_grid as dag

df = px.data.stocks()

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css])
# app = Dash(__name__, external_stylesheets=[dbc.themes.COSMO])

theme_change = ThemeChangerAIO(aio_id="theme")
graph = html.Div(dcc.Graph(id="theme-change-graph"), className="m-4")

dff = pd.read_csv(
    "https://raw.githubusercontent.com/plotly/datasets/master/ag-grid/olympic-winners.csv"
)

columnDefs = [
    # {"field": "athlete", "checkboxSelection": True, "headerCheckboxSelection": True},
    {"field": "age", "maxWidth": 100},
    {"field": "country"},
    {"field": "year", "maxWidth": 120},
    {"field": "date"},
    {"field": "sport"},
    {"field": "gold"},
    {"field": "silver"},
    {"field": "bronze"},
    {"field": "total"},
]

grid = dag.AgGrid(
    enableEnterpriseModules=True,
    # licenseKey=<your_license_key>,
    id="selection-checkbox-grid",
    columnDefs=columnDefs,
    rowData=dff.to_dict("records"),
    defaultColDef={"flex": 1, "minWidth": 150, "sortable": True, "resizable": True, "filter": True},
    dashGridOptions={
        # "rowSelection":"multiple",
        # 'pagination':True,
        "enableRangeSelection": True,
        # "enableCellSelection": True,
        "suppressCellFocus": False
    },
    
    
)

app.layout = dbc.Container(
    [
       theme_change, 
       graph,
       grid,
    ],
    fluid=True,
    className="dbc dbc-ag-grid"
)
# app.layout = dbc.Container([theme_change, graph], className="m-4 dbc")


@app.callback(
    Output("theme-change-graph", "figure"),
    Input(ThemeChangerAIO.ids.radio("theme"), "value"),
)
def update_graph_theme(theme):
    return px.line(df, x="date", y="GOOG", template=template_from_url(theme))


if __name__ == "__main__":
    app.run(debug=True)
 