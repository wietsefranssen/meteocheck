import polars as pl
import matplotlib.colors as mcolors


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
    return cell_values, cell_colors
