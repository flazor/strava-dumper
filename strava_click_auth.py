#!/usr/bin/python3

from requests_oauthlib import OAuth2Session
import requests
import pprint



def run():
    strava_clientid     = '1376' 
    strava_clientsecret = '1a5dc76c04787c4c8d49eabd45009db5635a8810'

    strava_refreshtoken = 'b34e0a85e080379e10bd2ce5ec6a201eb7c4353c'

    redirect_url = "https://flazor.github.io/galactic-compass/smash/"
#    redirect_url = "https://s3.eu-west-1.amazonaws.com/rideyourbike.org/tim/index.html"
    session = OAuth2Session(client_id=strava_clientid, redirect_uri=redirect_url)
    auth_base_url = "https://www.strava.com/oauth/authorize"
    session.scope = ["activity:read_all"]
    auth_link = session.authorization_url(auth_base_url)

    print(f"Click Here!! {auth_link[0]}")

    redirect_response = input(f"Paste redirect url here: ")
    
    token_url = "https://www.strava.com/api/v3/oauth/token"
    token_response = session.fetch_token(
        token_url=token_url,
        client_id=strava_clientid,
        client_secret=strava_clientsecret,
        authorization_response=redirect_response,
        include_client_id=True
    )
## Token response - need to save the refresh token from this response (see below) and use it for next requests
##
## or can hopefully just use the '7f4...' one because it's set with the right scope
##
##  {'token_type': 'Bearer', 'expires_at': 1706995690.5403814, 'expires_in': 19765, 'refresh_token': '7f40a6919bc739ce106c8217fe27411607e92ca3', 'access_token': '375f4a00714d117f8f36768ec6e937553d64ff80', 'athlete': {'id': 299529, 'username': 'flazor', 'resource_state': 2, 'firstname': 'Tim', 'lastname': 'Charnecki', 'bio': '', 'city': 'Stepaside', 'state': 'County Dublin', 'country': 'Ireland', 'sex': 'M', 'premium': True, 'summit': True, 'created_at': '2012-03-01T21:19:16Z', 'updated_at': '2024-01-15T15:31:16Z', 'badge_type_id': 1, 'weight': 72.5747, 'profile_medium': 'https://dgalywyr863hv.cloudfront.net/pictures/athletes/299529/171446/1/medium.jpg', 'profile': 'https://dgalywyr863hv.cloudfront.net/pictures/athletes/299529/171446/1/large.jpg', 'friend': None, 'follower': None}}



    response = session.get("https://www.strava.com/api/v3/athlete/activities")
    pprint.pprint(response)
    print(response.text)
    return f"Strava API Job completed."

run()
