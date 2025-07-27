import plotly.graph_objects as go
import plotly.express as px
import polars as pl
import pandas as pd
from datetime import datetime
from dash_bootstrap_templates import template_from_url


def create_timeline_plot(data_df, sensor_id, station, variable, sensor_name, theme_url=None):
    """Create a timeline plot for the selected sensor data"""
    
    # Determine the template based on theme URL
    if theme_url:
        try:
            template = template_from_url(theme_url)
        except:
            template = "plotly_white"
    else:
        template = "plotly_white"
    
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
