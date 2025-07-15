import dash
from dash import dcc, html, Input, Output, State, ctx, dash_table
from src.plot import make_figure   
from data_manager import DataManager
import matplotlib.colors as mcolors
import plotly.graph_objects as go
import polars as pl
import numpy as np
import os

def download_and_compare_data(application='standalone'):
    """
    Main function to download and compare data.
    Initializes DataManager, sets dates, downloads data, and processes it.
    """
    basepath = os.path.dirname(os.path.abspath(__file__))
    dm = DataManager()
    dm.set_meta_path(os.path.join(basepath, 'meta'))
    dm.set_data_path(os.path.join(basepath, 'data'))
    dm.set_temp_path(os.path.join(basepath, 'temp'))

    dm.set_dates(days_back=6, offset=1)
    # dm.set_dates(start_dt='2025-07-03', end_dt='2025-07-10 23:58:00')
    print(f"start_dt: {dm.start_dt}, end_dt: {dm.end_dt}")

    dm.download_or_load_data()
    data_df, sensorinfo_df = dm.get_data()

    def find_incorrect_airpressure_sensors(sensorinfo_df, data_df, threshold=200):
        """
        Returns a list of sensor_ids where air pressure values are likely a factor 10 too low.
        Also checks the unit in sensorinfo_df if available.
        """
        # Find air pressure sensors using Polars
        airpressure_sensors = sensorinfo_df.filter(
            pl.col('variable_name').str.contains('PAIR')
        )['sensor_id'].to_list()
        
        incorrect_sensors = []
        for sensor in airpressure_sensors:
            sensor_str = str(sensor)
            if sensor_str in data_df.columns:
                # Get values using Polars
                vals = data_df.select(sensor_str).to_series().drop_nulls()
                
                # Check for low median or wrong unit
                sensor_info = sensorinfo_df.filter(pl.col('sensor_id') == sensor)
                if sensor_info.height > 0 and 'unit' in sensorinfo_df.columns:
                    unit = sensor_info['unit'].item()
                else:
                    unit = 'hPa'
                    
                if (vals.len() > 0 and vals.median() < threshold) or (unit not in ['hPa', 'hpa']):
                    incorrect_sensors.append(sensor)
        return incorrect_sensors

    def correct_airpressure_units(data_df, sensorinfo_df, sensor_ids):
        """
        Multiplies the values of the given sensor_ids by 10 in data_df and updates the unit in sensorinfo_df.
        """
        for sensor in sensor_ids:
            sensor_str = str(sensor)
            if sensor_str in data_df.columns:
                # Update data_df using Polars
                data_df = data_df.with_columns([
                    (pl.col(sensor_str) * 10).alias(sensor_str)
                ])
            
            if 'unit' in sensorinfo_df.columns:
                # Update sensorinfo_df using Polars
                sensorinfo_df = sensorinfo_df.with_columns([
                    pl.when(pl.col('sensor_id') == sensor)
                    .then(pl.lit('hPa'))
                    .otherwise(pl.col('unit'))
                    .alias('unit')
                ])
        return data_df, sensorinfo_df

    # Detect and correct air pressure sensors with wrong units
    incorrect_sensors = find_incorrect_airpressure_sensors(sensorinfo_df, data_df)
    if incorrect_sensors:
        print("Correcting air pressure sensors:", incorrect_sensors)
        data_df, sensorinfo_df = correct_airpressure_units(data_df, sensorinfo_df, incorrect_sensors)

    # Prepare names
    check_table = dm.check_table  # This stays as pandas
    var_names = check_table.columns[2:].tolist()
    site_names = check_table['station'].unique().tolist()
    site_names.sort()

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
                    row_vals.append('')
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
                    row_vals.append(f"{frac_nan:.0%} NaN")
                    row_colors.append(nan_to_color(frac_nan))
                else:
                    row_vals.append('')
                    row_colors.append('#f0f0f0')
            else:
                row_vals.append('')
                row_colors.append('#f0f0f0')
        cell_values.append(row_vals)
        cell_colors.append(row_colors)

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

    datatable = dash_table.DataTable(
        id='nan-table',
        columns=[{'name': 'Site Name', 'id': 'Site Name'}] + [{'name': v, 'id': v} for v in var_names],
        data=table_data,
        style_data_conditional=style_data_conditional,
        style_cell={'textAlign': 'center'},
        style_header={'fontWeight': 'bold'},
        selected_cells=[],
    )

    # Remove all sensor_ids from data_df which only contain NaN values
    # Convert to pandas for dropna operation, then back to polars
    data_pd = data_df.to_pandas()
    if 'datetime' in data_pd.columns:
        data_pd = data_pd.set_index('datetime')
    data_pd = data_pd.dropna(axis=1, how='all')
    data_df = pl.from_pandas(data_pd.reset_index())

    # Remove all sensor_ids from sensorinfo_df which are not in data_df
    remaining_sensors = [col for col in data_df.columns if col != 'datetime']
    sensorinfo_df = sensorinfo_df.filter(
        pl.col('sensor_id').cast(pl.Utf8).is_in([str(s) for s in remaining_sensors])
    )

    # Group the data into parts to separate plots using Polars
    sensor_groups_df = sensorinfo_df.group_by('variable_name').agg([
        pl.col('sensor_id').alias('sensor_ids')
    ])

    # Sort sensor_groups_df by variable_name
    sensor_groups_df = sensor_groups_df.sort('variable_name')

    sensor_groups = dict(zip(
        sensor_groups_df['variable_name'].to_list(),
        sensor_groups_df['sensor_ids'].to_list()
    ))


    sensor_names = list(sensor_groups.keys())

    # Get all ATMP sensor_ids using Polars
    atmp_sensors_df = sensorinfo_df.filter(pl.col('variable_name') == 'ATMP')
    atmp_sensors = atmp_sensors_df['sensor_id'].to_list()

    # Convert to pandas for outlier calculations (easier with current numpy operations)
    atmp_columns = [str(s) for s in atmp_sensors if str(s) in data_df.columns]
    if atmp_columns:
        # Convert datetime column to index for outlier calculations
        atmp_data = data_df.select(['datetime'] + atmp_columns).to_pandas().set_index('datetime')
        
        # Calculate outlier masks using existing numpy operations
        with np.errstate(divide='ignore', invalid='ignore'):
            atmp_zscores = (atmp_data - atmp_data.mean(axis=1, skipna=True).values[:, None]) / atmp_data.std(axis=1, skipna=True).values[:, None]
            atmp_zscores = atmp_zscores.fillna(0)
            zscore_outlier_mask = np.abs(atmp_zscores) > 4

            Q1 = atmp_data.quantile(0.25)
            Q3 = atmp_data.quantile(0.75)
            IQR = Q3 - Q1
            iqr_outlier_mask = (atmp_data < (Q1 - 1.5 * IQR)) | (atmp_data > (Q3 + 1.5 * IQR))

            median = atmp_data.median(axis=1, skipna=True)
            mad = (np.abs(atmp_data.sub(median, axis=0))).median(axis=1, skipna=True)
            modz = 0.6745 * (atmp_data.sub(median, axis=0)).div(mad, axis=0)
            modz = modz.fillna(0)
            modz_outlier_mask = np.abs(modz) > 3.5

            lower = atmp_data.quantile(0.01)
            upper = atmp_data.quantile(0.99)
            percentile_outlier_mask = (atmp_data < lower) | (atmp_data > upper)

            window = 24
            rolling_mean = atmp_data.rolling(window, min_periods=1, center=True).mean()
            rolling_std = atmp_data.rolling(window, min_periods=1, center=True).std()
            rolling_zscore = (atmp_data - rolling_mean) / rolling_std
            rolling_zscore = rolling_zscore.fillna(0)
            rolling_outlier_mask = np.abs(rolling_zscore) > 4
    else:
        # Create empty masks if no ATMP data
        atmp_data = None
        zscore_outlier_mask = None
        iqr_outlier_mask = None
        modz_outlier_mask = None
        percentile_outlier_mask = None
        rolling_outlier_mask = None

    # Dash app
    if application=='standalone':
        app = dash.Dash(__name__)
    elif application=='django':
        from django_plotly_dash import DjangoDash
        app = DjangoDash(name='dash_meteo')
    else:
        app = dash.Dash(__name__)
 
    app.layout = html.Div([
        html.A(
            "Open SharePoint Document",
            href="https://wageningenur4.sharepoint.com/:x:/r/sites/Veenweiden/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7B3d741ab0-36f1-4687-953b-902b0a009582%7D&wdOrigin=TEAMS-MAGLEV.undefined_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1750680875439&web=1",
            target="_blank"
        ),
        html.H3("NaN Overview Table (click a cell to highlight below)"),
        datatable,
        dcc.Checklist(
            id='show-outliers',
            options=[
                {'label': 'Show outliers', 'value': 'show'},
                {'label': 'Remove outliers', 'value': 'remove'}
            ],
            value=['show'],
            style={'margin': '10px'}
        ),
        dcc.Dropdown(
            id='outlier-method',
            options=[
                {'label': 'Z-score', 'value': 'zscore'},
                {'label': 'IQR', 'value': 'iqr'},
                {'label': 'Modified Z-score', 'value': 'modz'},
                {'label': 'Percentile (1-99%)', 'value': 'percentile'},
                {'label': 'Rolling Window Z-score', 'value': 'rolling'}
            ],
            value='zscore',
            clearable=False,
            style={'width': '200px', 'margin': '10px'}
        ),  
        dcc.Graph(id='highlight-graph'),
        html.Div([
            dcc.Graph(id=f'graph-{i}', figure=make_figure(data_df, sensorinfo_df, sensor_groups, sensor_names[i]))
            for i in range(len(sensor_names))
        ])
    ])

    @app.callback(
        Output('highlight-graph', 'figure'),
        [Input('nan-table', 'selected_cells'),
        Input('show-outliers', 'value'),
        Input('outlier-method', 'value')]
    )
    def update_highlight_graph(selected_cells, show_outliers, outlier_method):
        import plotly.graph_objects as go
        fig = go.Figure()
        if not selected_cells:
            return fig

        print("Selected cells:", selected_cells)

        # Collect all (site, var) pairs from selected cells
        pairs = []
        for cell in selected_cells:
            row = cell['row']
            col = cell['column_id']
            if col == 'Site Name':
                continue
            site = site_names[row]
            var = col
            pairs.append((site, var))

        print("Pairs to plot:", pairs)

        # Plot all selected (site, var) pairs
        for site, var in pairs:
            # Use Polars filtering
            sensors = sensorinfo_df.filter(
                (pl.col('site_name') == site) & (pl.col('variable_name') == var)
            )['sensor_id'].to_list()
            
            if not sensors:
                continue
                
            # Select outlier mask for this variable and method
            mask = None
            if var == 'ATMP' and atmp_data is not None:
                if outlier_method == 'iqr':
                    mask = iqr_outlier_mask
                elif outlier_method == 'modz':
                    mask = modz_outlier_mask
                elif outlier_method == 'percentile':
                    mask = percentile_outlier_mask
                elif outlier_method == 'rolling':
                    mask = rolling_outlier_mask
                else:
                    mask = zscore_outlier_mask
                    
            for sensor in sensors:
                sensor_str = str(sensor)
                if sensor_str not in data_df.columns:
                    continue
                    
                # Convert to pandas for plotting (easier with current plotly integration)
                plot_data = data_df.select(['datetime', sensor_str]).to_pandas().set_index('datetime')
                y = plot_data[sensor_str].copy()
                
                if var == 'ATMP' and mask is not None and sensor_str in atmp_data.columns and 'remove' in show_outliers:
                    y = y.mask(mask[sensor_str])
                    
                fig.add_trace(go.Scatter(
                    x=plot_data.index,
                    y=y,
                    mode='lines+markers',
                    name=f"{site} ({sensor})",
                    line=dict(width=1),
                    marker=dict(size=3)
                ))
                
                if var == 'ATMP' and mask is not None and sensor_str in atmp_data.columns and 'show' in show_outliers:
                    outlier_idx = mask[sensor_str]
                    fig.add_trace(go.Scatter(
                        x=atmp_data.index[outlier_idx],
                        y=atmp_data[sensor_str][outlier_idx],
                        mode='markers',
                        marker=dict(color='red', size=12, symbol='x'),
                        name=f"{site} ({sensor}) Outlier"
                    ))
                    
        fig.update_layout(title="Selected sensors")
        return fig
    
    return app

if __name__ == "__main__":
    app = download_and_compare_data(application='standalone')
    app.run(debug=True)
