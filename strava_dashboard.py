#!/usr/bin/env python3
"""
Simple Strava Activities Dashboard

A Streamlit dashboard to visualize Strava activity data from the Parquet file.
Shows miles per day with activity type filtering.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import logging
from datetime import datetime, timedelta
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_date_range(period, max_date):
    """
    Calculate start date based on period and max date.
    
    Args:
        period (str): Time period ('1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '5Y', '10Y', 'ALL')
        max_date (datetime): Latest date in the data
        
    Returns:
        datetime or None: Start date for filtering, None for ALL
    """
    if period == 'ALL':
        return None
    elif period == '1W':
        return max_date - timedelta(weeks=1)
    elif period == '1M':
        return max_date - timedelta(days=30)
    elif period == '3M':
        return max_date - timedelta(days=90)
    elif period == '6M':
        return max_date - timedelta(days=180)
    elif period == 'YTD':
        return datetime(max_date.year, 1, 1)
    elif period == '1Y':
        return max_date - timedelta(days=365)
    elif period == '2Y':
        return max_date - timedelta(days=730)
    elif period == '5Y':
        return max_date - timedelta(days=1825)
    elif period == '10Y':
        return max_date - timedelta(days=3650)
    else:
        return None

def load_strava_data(parquet_path="data/strava_activities_latest.parquet"):
    """
    Load and process Strava data from Parquet file.
    
    Args:
        parquet_path (str): Path to the Parquet file
        
    Returns:
        pd.DataFrame: Processed DataFrame with date and miles columns
    """
    try:
        # Load the data
        df = pd.read_parquet(parquet_path)
        
        # Convert distance from meters to miles (1 meter = 0.000621371 miles)
        df['miles'] = df['distance'] * 0.000621371
        
        # Convert start_date to just date (no time)
        df['date'] = pd.to_datetime(df['start_date']).dt.date
        
        # Filter out activities with no distance or invalid dates
        df = df.dropna(subset=['miles', 'date', 'type'])
        df = df[df['miles'] > 0]
        
        logger.info(f"Loaded {len(df)} activities with valid distance data")
        return df
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        logger.error(f"Error loading data: {e}")
        return pd.DataFrame()

def process_daily_miles(df, activity_type="All"):
    """
    Process data to calculate total miles per day with moving averages.
    
    Args:
        df (pd.DataFrame): Raw activity data
        activity_type (str): Activity type to filter by
        
    Returns:
        pd.DataFrame: Daily aggregated data with moving averages
    """
    if df.empty:
        return pd.DataFrame()
    
    # Filter by activity type if specified
    if activity_type != "All":
        df_filtered = df[df['type'] == activity_type].copy()
    else:
        df_filtered = df.copy()
    
    # Group by date and sum miles
    daily_miles = df_filtered.groupby('date')['miles'].sum().reset_index()
    
    # Sort by date and convert date to datetime for proper ordering
    daily_miles = daily_miles.sort_values('date')
    daily_miles['date'] = pd.to_datetime(daily_miles['date'])
    
    # Create a complete date range from earliest data to today
    if len(daily_miles) > 0:
        today = pd.Timestamp.now().normalize()
        start_date = daily_miles['date'].min()
        
        date_range = pd.date_range(
            start=start_date,
            end=today,
            freq='D'
        )
        full_date_df = pd.DataFrame({'date': date_range})
        daily_miles = full_date_df.merge(daily_miles, on='date', how='left')
        daily_miles['miles'] = daily_miles['miles'].fillna(0)
    else:
        # If no data, create just today with 0 miles
        today = pd.Timestamp.now().normalize()
        daily_miles = pd.DataFrame({
            'date': [today],
            'miles': [0]
        })
    
    # Calculate trailing moving averages
    daily_miles['7_day_avg'] = daily_miles['miles'].rolling(window=7, min_periods=1).mean()
    daily_miles['30_day_avg'] = daily_miles['miles'].rolling(window=30, min_periods=1).mean()
    
    logger.info(f"Processed {len(daily_miles)} days of data for {activity_type}")
    return daily_miles

def create_miles_chart(daily_data, activity_type):
    """
    Create a Plotly chart for daily miles with bar chart and moving averages.
    
    Args:
        daily_data (pd.DataFrame): Daily miles data with moving averages
        activity_type (str): Selected activity type
        
    Returns:
        plotly figure: Chart object
    """
    if daily_data.empty:
        st.warning("No data available for the selected activity type.")
        return None
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Add bar chart for daily miles
    fig.add_trace(
        go.Bar(
            x=daily_data['date'],
            y=daily_data['miles'],
            name='Daily Miles',
            marker_color='lightblue',
            opacity=0.7,
            hovertemplate='<b>%{x}</b><br>Daily Miles: %{y:.1f}<extra></extra>'
        )
    )
    
    # Add 7-day trailing average line
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['7_day_avg'],
            mode='lines',
            name='7-Day Trailing Avg',
            line=dict(color='orange', width=2),
            hovertemplate='<b>%{x}</b><br>7-Day Trailing Avg: %{y:.1f}<extra></extra>'
        )
    )
    
    # Add 30-day trailing average line
    fig.add_trace(
        go.Scatter(
            x=daily_data['date'],
            y=daily_data['30_day_avg'],
            mode='lines',
            name='30-Day Trailing Avg',
            line=dict(color='red', width=2),
            hovertemplate='<b>%{x}</b><br>30-Day Trailing Avg: %{y:.1f}<extra></extra>'
        )
    )
    
    # Update layout
    fig.update_layout(
        title=f'Daily Miles with Trailing Averages - {activity_type}',
        xaxis_title='Date',
        yaxis_title='Miles',
        template='plotly_white',
        height=500,
        hovermode='x unified',
        xaxis=dict(showgrid=True, gridcolor='lightgray'),
        yaxis=dict(showgrid=True, gridcolor='lightgray'),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    return fig

def create_calendar_heatmap(daily_data, activity_type):
    """
    Create calendar heatmap with separate traces for each year.
    
    Args:
        daily_data (pd.DataFrame): Daily miles data
        activity_type (str): Selected activity type
        
    Returns:
        plotly figure: Heatmap chart object
    """
    if daily_data.empty:
        return None
    
    # Filter to data with non-zero activities for start date
    non_zero_data = daily_data[daily_data['miles'] > 0]
    if non_zero_data.empty:
        return None
    
    start_date = non_zero_data['date'].min()
    end_date = daily_data['date'].max()
    
    # Create a copy for processing
    heatmap_data = daily_data[(daily_data['date'] >= start_date) & (daily_data['date'] <= end_date)].copy()
    
    # Add calendar fields
    heatmap_data['year'] = heatmap_data['date'].dt.year
    heatmap_data['week_of_year'] = heatmap_data['date'].dt.isocalendar().week
    heatmap_data['day_of_week'] = heatmap_data['date'].dt.day_of_week  # Monday=0, Sunday=6
    
    # Get unique years (sorted descending so most recent years appear first)
    years = sorted(heatmap_data['year'].unique(), reverse=True)
    
    # Find the global week range to align all years
    all_weeks = sorted(heatmap_data['week_of_year'].unique())
    min_week = min(all_weeks)
    max_week = max(all_weeks)
    week_range = list(range(min_week, max_week + 1))
    
    # Create subplot structure - no subplot titles since we'll add year labels on y-axis
    fig = make_subplots(
        rows=len(years), 
        cols=1,
        vertical_spacing=0.01
    )
    
    # Day of week labels (Monday=0 to Sunday=6)
    day_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    
    for i, year in enumerate(years):
        year_data = heatmap_data[heatmap_data['year'] == year].copy()
        
        # Create pivot table for heatmap with consistent week range
        pivot_data = year_data.pivot_table(
            values='miles', 
            index='day_of_week', 
            columns='week_of_year', 
            fill_value=0
        )
        
        # Ensure all days of week are present
        for day in range(7):
            if day not in pivot_data.index:
                pivot_data.loc[day] = 0
        
        # Ensure all weeks are present for alignment
        for week in week_range:
            if week not in pivot_data.columns:
                pivot_data[week] = 0
        
        # Sort by index and columns to ensure proper ordering
        pivot_data = pivot_data.sort_index()
        pivot_data = pivot_data.reindex(columns=sorted(pivot_data.columns))
        
        # Create display values for color mapping (clamp to 20 for color scale)
        pivot_data_display = pivot_data.clip(upper=20.0)
        
        # Create hover text with dates and actual miles
        hover_text = []
        for day_idx in pivot_data.index:
            day_row = []
            for week_num in pivot_data.columns:
                # Find the actual date for this week/day combination
                matching_dates = year_data[
                    (year_data['week_of_year'] == week_num) & 
                    (year_data['day_of_week'] == day_idx)
                ]['date']
                
                if len(matching_dates) > 0:
                    actual_date = matching_dates.iloc[0].strftime('%Y-%m-%d')
                else:
                    # Calculate approximate date for week/day
                    import datetime
                    jan_1 = datetime.date(year, 1, 1)
                    week_1_monday = jan_1 - datetime.timedelta(days=jan_1.weekday())
                    target_date = week_1_monday + datetime.timedelta(weeks=week_num-1, days=day_idx)
                    actual_date = target_date.strftime('%Y-%m-%d')
                
                actual_mile_value = pivot_data.loc[day_idx, week_num]
                day_row.append(f"{actual_date}<br>{day_labels[day_idx]}<br>Miles: {actual_mile_value:.1f}")
            hover_text.append(day_row)
        
        # Create heatmap for this year
        fig.add_trace(
            go.Heatmap(
                z=pivot_data_display.values,
                x=pivot_data.columns,
                y=[day_labels[j] for j in pivot_data.index],
                colorscale='thermal',
                zmin=0,
                zmax=20.0,
                colorbar=dict(
                    title="Miles"
                ) if i == 0 else None,
                showscale=(i == 0),  # Only show colorbar for first trace
                # Use hover text with full information
                text=hover_text,
                hovertemplate='%{text}<extra></extra>',
                name=f"{year}"
            ),
            row=i+1, col=1
        )
    
    # Update layout
    fig.update_layout(
        title=f'Calendar Heatmap - {activity_type}',
        height=120 * len(years),
        template='plotly_white',
        margin=dict(l=80)  # Increase left margin for year labels
    )
    
    # Update axes for all subplots
    for i in range(len(years)):
        # Only show x-axis title on bottom subplot
        fig.update_xaxes(
            title="Week of Year" if i == len(years)-1 else "",
            showticklabels=(i == len(years)-1),  # Only show tick labels on bottom
            row=i+1, col=1
        )
        # Add year label on the left and show day labels
        fig.update_yaxes(
            title=dict(text=f"{years[i]}", font=dict(size=14)),
            side='left',
            row=i+1, col=1
        )
    
    return fig

def main():
    """Main dashboard function."""
    
    # Page configuration
    st.set_page_config(
        page_title="Strava Dashboard",
        page_icon="🏃‍♂️",
        layout="wide"
    )
    
    # Title and description
    st.title("🏃‍♂️ Strava Activities Dashboard")
    st.markdown("Track your daily activity miles over time")
    
    # Load data
    with st.spinner("Loading Strava data..."):
        df = load_strava_data()
    
    if df.empty:
        st.error("Could not load data. Please make sure the Parquet file exists.")
        st.stop()
    
    # Sidebar for controls
    st.sidebar.header("Filters")
    
    # Get unique activity types
    activity_types = ["All"] + sorted(df['type'].unique().tolist())
    
    # Activity type selector - default to "Run" if available
    default_index = 0
    if "Run" in activity_types:
        default_index = activity_types.index("Run")
    
    selected_type = st.sidebar.selectbox(
        "Select Activity Type:",
        activity_types,
        index=default_index
    )
    
    # Display summary stats
    st.sidebar.subheader("Data Summary")
    total_activities = len(df)
    date_range = f"{df['date'].min()} to {df['date'].max()}"
    total_miles = df['miles'].sum()
    
    st.sidebar.metric("Total Activities", f"{total_activities:,}")
    st.sidebar.metric("Total Miles", f"{total_miles:,.1f}")
    st.sidebar.text(f"Date Range:\n{date_range}")
    
    # Process data for selected activity type
    daily_data = process_daily_miles(df, selected_type)
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["Timeline Analysis", "Calendar Heatmap"])
    
    with tab1:
        # Date range selector
        if not daily_data.empty:
            st.subheader("Date Range")
            
            # Create columns for date range buttons
            date_ranges = ['1W', '1M', '3M', '6M', 'YTD', '1Y', '2Y', '5Y', '10Y', 'ALL']
            cols = st.columns(len(date_ranges))
            
            # Initialize session state for selected range - default to '3M'
            if 'selected_range' not in st.session_state:
                st.session_state.selected_range = '3M'
            
            # Create buttons for each date range
            for i, period in enumerate(date_ranges):
                with cols[i]:
                    if st.button(period, key=f"btn_{period}"):
                        st.session_state.selected_range = period
            
            # Filter data based on selected date range
            max_date = daily_data['date'].max()
            start_date = get_date_range(st.session_state.selected_range, max_date)
            
            if start_date:
                filtered_data = daily_data[daily_data['date'] >= start_date].copy()
                range_label = st.session_state.selected_range
            else:
                filtered_data = daily_data.copy()
                range_label = 'ALL'
            
            st.write(f"**Showing:** {range_label}")
        
        # Create and display the chart
        if not daily_data.empty and not filtered_data.empty:
            fig = create_miles_chart(filtered_data, f"{selected_type} ({range_label})")
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            
            # Display some summary stats for the filtered data
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                days_with_activity = len(filtered_data[filtered_data['miles'] > 0])
                st.metric("Days with Activity", days_with_activity)
            
            with col2:
                avg_daily = filtered_data['miles'].mean()
                st.metric("Avg Miles/Day", f"{avg_daily:.1f}")
            
            with col3:
                max_daily = filtered_data['miles'].max()
                st.metric("Max Miles/Day", f"{max_daily:.1f}")
            
            with col4:
                filtered_total = filtered_data['miles'].sum()
                st.metric(f"Total Miles ({range_label})", f"{filtered_total:.1f}")
        
        else:
            st.warning(f"No activities found for {selected_type}")
    
    with tab2:
        # Calendar Heatmap Tab
        st.subheader("Calendar Heatmap")
        
        if not daily_data.empty:
            heatmap_fig = create_calendar_heatmap(daily_data, selected_type)
            if heatmap_fig:
                st.plotly_chart(heatmap_fig, use_container_width=True)
            else:
                st.warning("No activity data available for heatmap")
        else:
            st.warning(f"No activities found for {selected_type}")

if __name__ == "__main__":
    main()
