import polars as pl
import matplotlib.colors as mcolors
from dash import html
import dash_ag_grid as dag

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

# # Generate the rules and CSS
# color_rules, css_classes = generate_color_rules_and_css()



def get_cell_values_and_colors(dm, sensorinfo_df, data_df, site_names, var_names):
    
    # Helper for color gradient
    def nan_to_color(frac):
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "nan_gradient", ["#00cc96", "#ffa600", "#ef553b"]
        )
        frac = min(max(frac, 0), 1)
        rgb = cmap(frac)[:3]
        return f'rgb({int(rgb[0]*255)}, {int(rgb[1]*255)}, {int(rgb[2]*255)})'

    # Build cell values and colors
    cell_values = []
    cell_colors = []
    for site in site_names:
        row_vals = []
        row_colors = []
        for var in var_names:
            # Use Polars filtering
            sensors = sensorinfo_df.filter(
                (pl.col('site_name') == site) & (pl.col('variable_name') == var)
            )['sensor_id'].to_list()
            
            if dm.is_check_table_value(site, var):
                if not sensors:
                    row_vals.append('A')
                    row_colors.append("red")
                    continue

                total = 0
                nans = 0
                for sensor in sensors:
                    sensor_str = str(sensor)
                    if sensor_str in data_df.columns:
                        # Convert to pandas temporarily for datetime operations
                        sensor_data = data_df.select(['datetime', sensor_str]).to_pandas().set_index('datetime')[sensor_str]
                        
                        # Detect if this sensor is 30-min data
                        non_null_data = sensor_data.dropna()
                        if len(non_null_data) > 0:
                            is_30min = non_null_data.index.minute.isin([0, 30]).all()
                            if is_30min:
                                # Only count expected 30-min intervals
                                expected = sensor_data.index[(sensor_data.index.minute == 0) | (sensor_data.index.minute == 30)]
                                total += len(expected)
                                nans += sensor_data.loc[expected].isna().sum()
                            else:
                                total += sensor_data.size
                                nans += sensor_data.isna().sum()
                                
                if total > 0:
                    frac_nan = nans / total
                    frac = 1 - frac_nan
                    row_vals.append(f"{frac:.0%}")
                    row_colors.append(nan_to_color(frac_nan))
                else:
                    row_vals.append('X')
                    row_colors.append('#f0f0f0')
            else:
                row_vals.append('Y')
                row_colors.append('#f0f0f0')
        cell_values.append(row_vals)
        cell_colors.append(row_colors)
    return cell_values, cell_colors

def get_datatable(cell_values, cell_colors, site_names, var_names):

    # Remove %   
    cell_values = [[str(value).replace('%', '') for value in row] for row in cell_values]
    
    # convert to integer if possible
    for i in range(len(cell_values)):
        for j in range(len(cell_values[i])):
            try:
                cell_values[i][j] = int(cell_values[i][j])
            except ValueError:
                pass
        
    
    # Generate the rules and CSS
    color_rules, css_classes = generate_color_rules_and_css()

    # Prepare DataTable data and styles
    table_data = []
    for i, site in enumerate(site_names):
        row = {'Site Name': site}
        for j, var in enumerate(var_names):
            row[var] = cell_values[i][j]
        table_data.append(row)

    style_data_conditional = []
    for i, site in enumerate(site_names):
        for j, var in enumerate(var_names):
            color = cell_colors[i][j]
            style_data_conditional.append({
                'if': {'row_index': i, 'column_id': var},
                'backgroundColor': color
            })

    # Prepare AG Grid data - convert to list of dictionaries
    ag_grid_data = []
    for i, site in enumerate(site_names):
        row = {'Site Name': site}
        for j, var in enumerate(var_names):
            row[var] = cell_values[i][j]
        ag_grid_data.append(row)

    # Create column definitions for AG Grid
    columnDefs = [
        {
            "headerName": "Site Name",
            "field": "Site Name",
            "pinned": "left",
            "width": 150,
            "cellStyle": {"fontWeight": "bold"}
        }
    ]

    # Add variable columns with conditional styling
    for var in var_names:
        columnDefs.append({
            "headerName": var,
            "field": var,
            "width": 120,
            "cellClassRules": {
                "low-value": f"params.data.{var} <= 20",
                "medium-low-value": f"params.data.{var} > 20 && params.data.{var} <= 40",
                "medium-value": f"params.data.{var} > 40 && params.data.{var} <= 60",
                "medium-high-value": f"params.data.{var} > 60 && params.data.{var} <= 80",
                "high-value": f"params.data.{var} > 80"
            }
            # "cellStyle": {
            #     "function": f"""
            #     function(params) {{
            #         const siteIndex = {site_names}.indexOf(params.data['Site Name']);
            #         const varIndex = {var_names}.indexOf('{var}');
            #         if (siteIndex >= 0 && varIndex >= 0) {{
            #             const colors = {cell_colors};
            #             return {{backgroundColor: colors[siteIndex][varIndex]}};
            #         }}
            #         return {{}};
            #     }}
            #     """
            # }
        })

    # Create the AG Grid component
    datatable = html.Div([
        dag.AgGrid(
            enableEnterpriseModules=True,
            id='nan-table',
            columnDefs=columnDefs,
            rowData=ag_grid_data,
            # className="ag-theme-alpine-dark",  # Use dark theme
            # defaultColDef={"flex": 1, "minWidth": 150, "sortable": True, "resizable": True, "filter": True},
            defaultColDef={"flex": 1, "minWidth": 100, "sortable": True, "resizable": True, "filter": True},
            dashGridOptions={
                "enableRangeSelection": True,
                "suppressCellFocus": False,
                # "rowSelection": "multiple",
                # "suppressRowClickSelection": False,
                "domLayout": "autoHeight",
                # "headerHeight": 40,
                # "rowHeight": 35,
            },
            # style={"height": "auto", "width": "100%"}
        )
    ],
    #  className="ag-grid-container", style={"margin": "20px 0"}
    )
    # Generate CSS with all color classes
    # css_content = "\n".join(css_classes)

    print(ag_grid_data)
    print("Generated color rules:")
    for i, (class_name, _) in enumerate(list(color_rules.items())[:5]):
        print(f"  {class_name}: {value_to_color(i * 0.1)}")
    
    return datatable, css_classes, color_rules
