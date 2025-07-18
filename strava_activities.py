#!/usr/bin/python3

import requests
import json
import logging
import os
import sys
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


def save_activities(activities: list, backup_dir: str = 'data') -> str:
    """Save activities to JSON file with timestamp"""
    backup_path = Path(backup_dir)
    backup_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'strava_activities_{timestamp}.json'
    filepath = backup_path / filename
    
    try:
        with open(filepath, 'w') as f:
            json.dump(activities, f, indent=2)
        
        logger.info(f'Saved {len(activities)} activities to {filepath}')
        return str(filepath)
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
        
        # Save to file
        filepath = save_activities(activities)
        
        logger.info(f'Backup completed successfully. File saved: {filepath}')
        return True
        
    except Exception as e:
        logger.error(f'Backup failed: {e}')
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)