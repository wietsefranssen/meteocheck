import polars as pl
import pandas as pd

def create_pivot_table(nan_table):
    """Create pivot table from nan_table for the AgGrid display"""
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='NaN_Percentage')
    pivot_table = pivot_table.fillna(0.0)  # Fill any missing values with 0% data availability
    
    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()
    
    return pivot_table

def create_pivot_table_reason(nan_table):
    """Create pivot table from nan_table for the AgGrid display"""
    
    # Pivot table to have variables as columns and stations as rows
    pivot_table = nan_table.pivot(index='Station', columns='Variable', values='Reason')
    pivot_table = pivot_table.fillna("")  # Fill any missing values with empty strings

    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()

    return pivot_table
    
    # Reset index to make Station a column again
    pivot_table = pivot_table.reset_index()
    
    return pivot_table
