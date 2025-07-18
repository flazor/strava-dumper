# Strava Data Backup Tool

A Python tool for backing up your Strava activity data using the Strava API.

## Features

- Secure credential management (environment variables or config file)
- Comprehensive error handling and logging
- Timestamped backup files
- Proper JSON parsing and data storage
- Production-ready code structure

## Setup

1. Create a Strava API application at https://www.strava.com/settings/api
2. Get your Client ID, Client Secret, and Refresh Token

### Authentication

Run the authentication script to get your refresh token:

```bash
python3 strava_click_auth.py
```

### Configuration

Option 1: Environment variables (recommended)
```bash
export STRAVA_CLIENT_ID="your_client_id"
export STRAVA_CLIENT_SECRET="your_client_secret"
export STRAVA_REFRESH_TOKEN="your_refresh_token"
```

Option 2: Config file
Create `strava.conf` with:
```
Client ID
your_client_id

Client Secret
your_client_secret

Refresh Token
your_refresh_token
```

## Usage

Run a one-time backup:
```bash
python3 strava_activities.py
```

Backup files are saved to `data/strava_activities_YYYYMMDD_HHMMSS.json`

## Logs

Check `strava_backup.log` for detailed execution logs.

## Security

- Never commit `strava.conf` to version control
- Use environment variables in production
- Refresh tokens expire and need periodic renewal