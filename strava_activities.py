#!/usr/bin/python3

import requests
import json
import logging
import os
import sys
import gzip
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strava_backup.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def fetch_strava_accesstoken(clientid: str, secret: str, refreshtoken: str) -> Optional[str]:
    """Fetch access token using refresh token"""
    try:
        resp = requests.post(
            'https://www.strava.com/api/v3/oauth/token',
            params={
                'client_id': clientid,
                'client_secret': secret,
                'grant_type': 'refresh_token',
                'refresh_token': refreshtoken,
                'scope': 'activity:read_all'
            },
            timeout=30
        )
        resp.raise_for_status()
        
        response = resp.json()
        logger.info('Successfully retrieved access token')
        return response['access_token']
    except requests.exceptions.RequestException as e:
        logger.error(f'Failed to fetch access token: {e}')
        return None
    except KeyError as e:
        logger.error(f'Unexpected response format: {e}')
        return None


def fetch_strava_activities(token: str) -> Optional[list]:
    """Fetch all activities from Strava API"""
    activities = []
    page = 1
    
    logger.info('Starting to fetch activities from Strava API')
    
    while True:
        try:
            resp = requests.get(
                'https://www.strava.com/api/v3/athlete/activities',
                headers={'Authorization': f'Bearer {token}'},
                params={'page': page, 'per_page': 200},
                timeout=30
            )
            resp.raise_for_status()
            
            data = resp.json()
            
            if not data:
                break
                
            activities.extend(data)
            logger.info(f'Fetched page {page} with {len(data)} activities')
            
            if len(data) < 200:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to fetch activities on page {page}: {e}')
            return None
        except json.JSONDecodeError as e:
            logger.error(f'Invalid JSON response on page {page}: {e}')
            return None
    
    logger.info(f'Successfully fetched {len(activities)} total activities')
    return activities


def load_config() -> Dict[str, str]:
    """Load configuration from environment variables or config file"""
    config = {}
    
    # Try environment variables first
    config['client_id'] = os.getenv('STRAVA_CLIENT_ID')
    config['client_secret'] = os.getenv('STRAVA_CLIENT_SECRET')
    config['refresh_token'] = os.getenv('STRAVA_REFRESH_TOKEN')
    
    # If not found in env vars, try config file
    if not all(config.values()):
        config_file = Path('strava.conf')
        if config_file.exists():
            with open(config_file, 'r') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    line = line.strip()
                    if line == 'Client Secret' and i + 1 < len(lines):
                        config['client_secret'] = lines[i + 1].strip()
                    elif line == 'Refresh Token' and i + 1 < len(lines):
                        config['refresh_token'] = lines[i + 1].strip()
                    elif line == 'Client ID' and i + 1 < len(lines):
                        config['client_id'] = lines[i + 1].strip()
    
    # Validate config
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Missing configuration: {', '.join(missing)}")
    
    return config


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


def create_parquet_file(activities: list, backup_dir: str = 'data') -> str:
    """
    Create Parquet file from activities data.
    
    Args:
        activities (list): List of activity data
        backup_dir (str): Directory to save the file
        
    Returns:
        str: Path to created Parquet file
    """
    backup_path = Path(backup_dir)
    backup_path.mkdir(exist_ok=True)
    
    parquet_filepath = backup_path / 'strava_activities_latest.parquet'
    
    try:
        logger.info(f"Processing {len(activities)} activities for Parquet conversion")
        
        # Flatten each activity record
        flattened_activities = []
        for activity in activities:
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
        
        # Save to Parquet (overwrites if exists)
        logger.info(f"Saving to Parquet format: {parquet_filepath}")
        df.to_parquet(parquet_filepath, engine='pyarrow', compression='snappy')
        
        # Log summary statistics
        logger.info(f"Successfully created Parquet file with {len(df)} records and {len(df.columns)} columns")
        if 'start_date' in df.columns:
            logger.info(f"Date range: {df['start_date'].min()} to {df['start_date'].max()}")
        if 'type' in df.columns:
            logger.info(f"Activity types: {df['type'].value_counts().to_dict()}")
        
        return str(parquet_filepath)
        
    except Exception as e:
        logger.error(f"Failed to create Parquet file: {e}")
        raise


def save_activities(activities: list, backup_dir: str = 'data') -> tuple[str, str]:
    """
    Save activities to compressed timestamped JSON file and create latest Parquet file.
    
    Args:
        activities (list): List of activity data
        backup_dir (str): Directory to save files
        
    Returns:
        tuple: (json_gz_filepath, parquet_filepath)
    """
    backup_path = Path(backup_dir)
    backup_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save compressed JSON file
    json_gz_filename = f'strava_activities_{timestamp}.json.gz'
    json_gz_filepath = backup_path / json_gz_filename
    
    try:
        # Save compressed JSON (for long-term storage)
        with gzip.open(json_gz_filepath, 'wt', encoding='utf-8') as f:
            json.dump(activities, f, indent=2)
        logger.info(f'Saved {len(activities)} activities to compressed file: {json_gz_filepath}')
        
        # Create/update latest Parquet file
        parquet_filepath = create_parquet_file(activities, backup_dir)
        
        return str(json_gz_filepath), str(parquet_filepath)
        
    except Exception as e:
        logger.error(f'Failed to save activities: {e}')
        raise


def main():
    """Main backup function"""
    try:
        logger.info('Starting Strava backup process')
        
        # Load configuration
        config = load_config()
        
        # Get access token
        access_token = fetch_strava_accesstoken(
            config['client_id'],
            config['client_secret'], 
            config['refresh_token']
        )
        
        if not access_token:
            logger.error('Failed to obtain access token')
            return False
        
        # Fetch activities
        activities = fetch_strava_activities(access_token)
        
        if not activities:
            logger.error('Failed to fetch activities')
            return False
        
        # Save to files (compressed JSON and Parquet)
        json_gz_filepath, parquet_filepath = save_activities(activities)
        
        logger.info(f'Backup completed successfully.')
        logger.info(f'Compressed JSON file: {json_gz_filepath}')
        logger.info(f'Parquet file: {parquet_filepath}')
        return True
        
    except Exception as e:
        logger.error(f'Backup failed: {e}')
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)