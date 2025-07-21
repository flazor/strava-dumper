#!/usr/bin/env python3
"""
Convert Strava activities JSON dump to queryable Parquet format.

This script processes the JSON data dump from Strava API and converts it 
into a structured Parquet file for efficient querying and analysis.
"""

import json
import pandas as pd
import argparse
import sys
from pathlib import Path
from datetime import datetime
import logging

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('json_to_parquet.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def flatten_nested_data(data):
    """
    Flatten nested JSON data for tabular format.
    
    Args:
        data: Raw JSON data from Strava activities
        
    Returns:
        dict: Flattened data dictionary
    """
    flattened = {}
    
    for key, value in data.items():
        if isinstance(value, dict):
            # Flatten nested dictionaries (e.g., athlete, map)
            for nested_key, nested_value in value.items():
                flattened[f"{key}_{nested_key}"] = nested_value
        elif isinstance(value, list):
            # Convert lists to string representation for now
            flattened[key] = str(value) if value else None
        else:
            flattened[key] = value
    
    return flattened

def convert_json_to_parquet(json_file_path, output_path=None):
    """
    Convert JSON data dump to Parquet format.
    
    Args:
        json_file_path (str): Path to the input JSON file
        output_path (str, optional): Path for the output Parquet file
        
    Returns:
        str: Path to the created Parquet file
    """
    logger = logging.getLogger(__name__)
    
    # Determine output path
    if output_path is None:
        json_path = Path(json_file_path)
        output_path = json_path.parent / f"{json_path.stem}.parquet"
    
    logger.info(f"Loading JSON data from: {json_file_path}")
    
    try:
        # Load JSON data - handle potential concatenated JSON arrays
        with open(json_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Fix malformed JSON if it contains concatenated arrays or empty arrays
        if '][' in content:
            logger.info("Fixing concatenated JSON arrays")
            content = content.replace('][', ',')
        
        # Remove empty arrays and trailing commas at the end
        content = content.rstrip('[]').rstrip(',')
        
        # Ensure proper array closing bracket
        if not content.endswith(']'):
            content += ']'
        
        activities_data = json.loads(content)
        
        logger.info(f"Loaded {len(activities_data)} activities")
        
        # Flatten each activity record
        flattened_activities = []
        for activity in activities_data:
            flattened = flatten_nested_data(activity)
            flattened_activities.append(flattened)
        
        # Create DataFrame
        df = pd.DataFrame(flattened_activities)
        
        # Convert date columns to proper datetime format
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"Could not convert {col} to datetime: {e}")
        
        # Convert numeric columns
        numeric_columns = [
            'distance', 'moving_time', 'elapsed_time', 'total_elevation_gain',
            'average_speed', 'max_speed', 'average_watts', 'kilojoules',
            'average_heartrate', 'max_heartrate', 'elev_high', 'elev_low',
            'kudos_count', 'comment_count', 'athlete_count', 'photo_count',
            'achievement_count', 'pr_count', 'total_photo_count'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Save to Parquet
        logger.info(f"Saving to Parquet format: {output_path}")
        df.to_parquet(output_path, engine='pyarrow', compression='snappy')
        
        # Log summary statistics
        logger.info(f"Successfully created Parquet file with {len(df)} records and {len(df.columns)} columns")
        logger.info(f"Date range: {df['start_date'].min()} to {df['start_date'].max()}")
        logger.info(f"Activity types: {df['type'].value_counts().to_dict()}")
        
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise

def main():
    """Main function to handle command line execution."""
    parser = argparse.ArgumentParser(description='Convert Strava JSON dump to Parquet format')
    parser.add_argument('json_file', help='Path to the JSON data dump file')
    parser.add_argument('-o', '--output', help='Output Parquet file path (optional)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    try:
        output_file = convert_json_to_parquet(args.json_file, args.output)
        logger.info(f"Conversion completed successfully: {output_file}")
        
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()