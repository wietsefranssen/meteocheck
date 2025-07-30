import plotly.graph_objects as go
import plotly.express as px
import polars as pl
import pandas as pd
from datetime import datetime
from dash_bootstrap_templates import template_from_url


def create_timeline_plot(data_df, sensor_id, station, variable, sensor_name):
    """Create a timeline plot for the selected sensor data"""
    
    # Determine the template based on theme URL
    # if theme_url:
    #     try:
    #         template = template_from_url(theme_url)
    #     except:
    #         template = "plotly_white"
    # else:
    #     template = "plotly_white"
    template = "plotly_white"  # Default template if theme_url is not provided
    if not sensor_id or sensor_id == '' or sensor_id not in data_df.columns:
        # Return empty figure if no valid sensor_id
        fig = go.Figure()
        fig.add_annotation(
            text="No data available for this sensor",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title=f"No Data: {station} - {variable}",
            xaxis_title="Time",
            yaxis_title="Value",
            height=400,
            template=template
        )
        return fig
    
    # Extract the sensor data
    try:
        # Get datetime and sensor columns
        plot_data = data_df.select(['datetime', sensor_id]).to_pandas()
        
        # Remove rows where sensor data is null
        plot_data = plot_data.dropna(subset=[sensor_id])
        
        if plot_data.empty:
            # Return empty figure if no data points
            fig = go.Figure()
            fig.add_annotation(
                text="No valid data points for this sensor",
                xref="paper", yref="paper",
                x=0.5, y=0.5, xanchor='center', yanchor='middle',
                showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(
                title=f"No Valid Data: {station} - {variable}",
                xaxis_title="Time",
                yaxis_title="Value",
                height=400,
                template=template
            )
            return fig
        
        # Create the timeline plot
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=plot_data['datetime'],
            y=plot_data[sensor_id],
            mode='lines+markers',
            name=f'{variable}',
            line=dict(width=2),
            marker=dict(size=4),
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Time: %{x}<br>' +
                         'Value: %{y}<br>' +
                         '<extra></extra>'
        ))
        
        # Update layout
        fig.update_layout(
            title=f"Timeline: {station} - {variable} ({sensor_name})",
            xaxis_title="Time",
            yaxis_title=f"{variable} Value",
            height=400,
            showlegend=True,
            hovermode='x unified',
            template=template
        )
        
        # Add range selector
        fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(count=7, label="7d", step="day", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=True),
                type="date"
            )
        )
        
        return fig
        
    except Exception as e:
        # Return error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="red")
        )
        fig.update_layout(
            title=f"Error: {station} - {variable}",
            xaxis_title="Time",
            yaxis_title="Value",
            height=400,
            template=template
        )
        return fig


def create_multi_timeline_plot(data_df, selected_cells, check_table, nan_table):
    """Create a timeline plot showing multiple selected sensors"""
    
    # Determine the template based on theme URL
    # if theme_url:
    #     try:
    #         template = template_from_url(theme_url)
    #     except:
    #         template = "plotly_white"
    # else:
    template = "plotly_white"
    
    if not selected_cells or len(selected_cells) == 0:
        # Return empty figure if no selections
        fig = go.Figure()
        fig.add_annotation(
            text="No cells selected. Use Ctrl+Click or drag to select cells in the table above.",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Multi-Sensor Timeline",
            xaxis_title="Time",
            yaxis_title="Value",
            height=400,
            template=template
        )
        return fig
    
    # Create the timeline plot
    fig = go.Figure()
    
    # Color palette for different sensors
    colors = px.colors.qualitative.Set3
    
    # Track added sensors to avoid duplicates
    added_sensors = set()
    
    try:
        for i, cell in enumerate(selected_cells):
            station = cell.get('station', '')
            variable = cell.get('variable', '')
            
            # Skip if we've already added this sensor
            sensor_key = f"{station}_{variable}"
            if sensor_key in added_sensors:
                continue
            
            # Find sensor information
            sensor_name = ''
            sensor_id = ''
            try:
                station_row = check_table[check_table['station'] == station]
                if not station_row.empty and variable in station_row.columns:
                    sensor_name = station_row.iloc[0][variable]
                    if pd.isna(sensor_name):
                        sensor_name = 'Not assigned'
                    else:
                        # Find matching sensor_id from nan_table
                        matching_row = nan_table[
                            (nan_table['Station'] == station) & 
                            (nan_table['Variable'] == variable)
                        ]
                        if not matching_row.empty:
                            sensor_id = matching_row.iloc[0]['Sensor_ID']
            except Exception as e:
                continue  # Skip this sensor if we can't find info
            
            # Skip if no valid sensor_id
            if not sensor_id or sensor_id == '' or sensor_id not in data_df.columns:
                continue
            
            # Extract the sensor data
            plot_data = data_df.select(['datetime', sensor_id]).to_pandas()
            plot_data = plot_data.dropna(subset=[sensor_id])
            
            if plot_data.empty:
                continue  # Skip if no data
            
            # Add trace for this sensor
            color = colors[i % len(colors)]
            
            fig.add_trace(go.Scatter(
                x=plot_data['datetime'],
                y=plot_data[sensor_id],
                mode='lines+markers',
                name=f'{station} - {variable}',
                line=dict(width=2, color=color),
                marker=dict(size=4, color=color),
                hovertemplate=f'<b>{station} - {variable}</b><br>' +
                             'Time: %{x}<br>' +
                             'Value: %{y}<br>' +
                             '<extra></extra>'
            ))
            
            added_sensors.add(sensor_key)
        
        # Update layout
        if len(added_sensors) > 0:
            fig.update_layout(
                title=f"Multi-Sensor Timeline ({len(added_sensors)} sensors selected)",
                xaxis_title="Time",
                yaxis_title="Sensor Values",
                height=500,
                showlegend=True,
                hovermode='x unified',
                template=template,
                legend=dict(
                    orientation="v",
                    yanchor="top",
                    y=1,
                    xanchor="left",
                    x=1.01
                )
            )
            
            # Add range selector
            fig.update_layout(
                xaxis=dict(
                    rangeselector=dict(
                        buttons=list([
                            dict(count=1, label="1d", step="day", stepmode="backward"),
                            dict(count=7, label="7d", step="day", stepmode="backward"),
                            dict(step="all")
                        ])
                    ),
                    rangeslider=dict(visible=True),
                    type="date"
                )
            )
        else:
            # No valid sensors found
            fig.add_annotation(
                text="No valid sensors found for selected cells",
                xref="paper", yref="paper",
                x=0.5, y=0.5, xanchor='center', yanchor='middle',
                showarrow=False,
                font=dict(size=16, color="orange")
            )
            fig.update_layout(
                title="Multi-Sensor Timeline - No Valid Data",
                xaxis_title="Time",
                yaxis_title="Value",
                height=400,
                template=template
            )
        
        return fig
        
    except Exception as e:
        # Return error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating multi-sensor plot: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            showarrow=False,
            font=dict(size=16, color="red")
        )
        fig.update_layout(
            title="Error: Multi-Sensor Timeline",
            xaxis_title="Time",
            yaxis_title="Value",
            height=400,
            template=template
        )
        return fig
