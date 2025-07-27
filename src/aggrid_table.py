import dash_ag_grid as dag


def create_aggrid_datatable(pivot_table, check_table):
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
            "rowSelection": "multiple",
            "rowMultiSelectWithClick": True,
            "suppressRowClickSelection": False,
            "enableRangeSelection": True,
            "enableCellSelection": True,
            "suppressCellFocus": False,
            "rowHeight": 30,
            "domLayout": "autoHeight",
            "suppressMovableColumns": True,
            "enableBrowserTooltips": True
        },
        style={"width": "100%"},
    )
