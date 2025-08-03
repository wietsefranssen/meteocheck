import dash_ag_grid as dag
import numpy as np  # Add this import at the top if not present


def create_aggrid_datatable(pivot_table, check_table, pivot_table_reason=None):
    """Create an ag-Grid DataTable for data availability percentages"""
    
    # Prepare column definitions for AgGrid
    columnDefs = [
        {"headerName": "Station", "field": "Station", "sortable": True, "filter": False, "suppressMenu": True, "pinned": "left", "width": 120},  # Keep Station column wider
    ]
    
    # Add variable columns with conditional formatting in check_table order
    for col in check_table.columns[2:]:  # Use check_table column order
        if col in pivot_table.columns:  # Only add if column exists in pivot_table
            style_conditions = [
                # Specific color for 'No_sensor'
                {
                    "condition": f"params.data['{col}_reason'] === 'No_sensor'",
                    "style": {"backgroundColor": "#C2C2C2", "color": "black"}  # Gray for No_sensor
                },
                # Specific color for 'Sensor_not_found'
                {
                    "condition": f"params.data['{col}_reason'] === 'Sensor_not_found'",
                    "style": {"backgroundColor": "#7A7A7A", "color": "black"}  # Light red for Sensor_not_found
                },
                # Generic color for any other reason (except Data_available)
                {
                    "condition": f"params.data['{col}_reason'] && params.data['{col}_reason'] !== 'Data_available'",
                    "style": {"backgroundColor": "#ff27a9", "color": "black"}  # Light blue for other reasons
                },
                # Normal coloring for data availability
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
            # Add extra color for reason if pivot_table_reason is provided
            # if pivot_table_reason is not None:
            #     style_conditions.insert(0, {
            #         # If there is a reason, color blue
            #         "condition": f"params.data['{col}_reason'] && params.data['{col}_reason'].length > 0",
            #         "style": {"backgroundColor": "#cce5ff", "color": "black"}  # Light blue for reason
            #     })
            columnDefs.append({
                "headerName": col, 
                "field": col, 
                "sortable": True, 
                "filter": False,
                "suppressMenu": True,
                "type": "numericColumn",
                "width": 80,
                "cellStyle": {
                    "styleConditions": style_conditions
                # },
                # "valueFormatter": {
                #     "function": f"""
                #         function(params) {{
                #             if (params.data['{col}_reason'] === 'No_sensor' || params.data['{col}_reason'] === 'Sensor_not_found') {{
                #                 return '';
                #             }}
                #             return params.value;
                #         }}
                #     """
                }
            })
    
    # Before passing to AgGrid, merge reason columns into rowData if pivot_table_reason is provided
    rowData = pivot_table.to_dict('records')
    if pivot_table_reason is not None:
        reason_dicts = pivot_table_reason.to_dict('records')
        for row, reason_row in zip(rowData, reason_dicts):
            for key in reason_row:
                reason = reason_row[key]
                row[f"{key}_reason"] = reason
                # Remove value for No_sensor and Sensor_not_found
                if reason in ['No_sensor', 'Sensor_not_found']:
                    row[key] = None  # or np.nan

    return dag.AgGrid(
        enableEnterpriseModules=True,
        licenseKey="[TRIAL]_this_{AG_Charts_and_AG_Grid}_Enterprise_key_{AG-090575}_is_granted_for_evaluation_only___Use_in_production_is_not_permitted___Please_report_misuse_to_legal@ag-grid.com___For_help_with_purchasing_a_production_key_please_contact_info@ag-grid.com___You_are_granted_a_{Single_Application}_Developer_License_for_one_application_only___All_Front-End_JavaScript_developers_working_on_the_application_would_need_to_be_licensed___This_key_will_deactivate_on_{14 August 2025}____[v3]_[0102]_MTc1NTEyNjAwMDAwMA==2bf724e243e12a2673a0da27840ab6db",
        id='nan-percentage-aggrid',
        columnDefs=columnDefs,
        rowData=rowData,
        defaultColDef={
            "minWidth": 60,     # Smaller minimum for data columns
            "editable": False,
            "resizable": True
        },
        dashGridOptions={
            "pagination": False,
            "animateRows": True,
            # "rowSelection": "none",  # Disable row selection
            # "cellSelection.handle": True,  # Enable cell selection handles
            # "enableRangeSelection": True,
            "cellSelection.handles": True,  # Enable cell selection handles
            # "enableCellSelection": True,
            # "cellSelection": True,
            "suppressCellFocus": False,
            "rowHeight": 30,
            "domLayout": "autoHeight",
            "suppressMovableColumns": True,
            "enableBrowserTooltips": True,
            # "suppressRowClickSelection": True,  # Prevent row selection on click
            "rowSelection.enableClickSelection": False,  # Disable click selection
            # "suppressMultiRangeSelection": False,  # Allow multiple range selections
            "cellSelection.suppressMultiRanges": False,  # Allow multiple ranges in cell selection
            # "enableRangeHandle": True,  # Enable drag handle for range selection
            "cellSelection.handle": True,  # Enable cell selection handles
            "enableRangeSelection": True,
            "enableCellSelection": True,
            "suppressRowClickSelection": True,
            "suppressMenuHide": True,  # Hide column menu icons
            "suppressColumnMenus": True,  # Remove column options icons completely
        },
        style={"width": "100%"},
    )
