"""
This script contains functions that help extract data from the Clash Royale API
"""

# Libraries

import requests
import json

# Global variables

# base_url = "https://proxy.royaleapi.dev/v1/"

with open("./access_token.json", "r") as file:
    access_token = json.load(file)
token = access_token["token"]

# Functions

def get_headers(token: str):
    """
    This function will be responsible for creating the headers,
    which will be used in the get_response function

    Args:
        token (str): token to communicate with Clash Royale API

    Returns:
        header (dict)
    """
    
    header = {"Authorization": f"Bearer {token}"}
    return header

def get_response(url_path: str, payload: dict = None):
    """
    This function will be responsible for making request to Clash Royale API
    If the request was successful, the function will return the response
    If the request was unsuccessful, the function will return empty string

    Args:
        url_path (str): The specif url path desired
        payload (dict, optional): Parameters for url, if the payload is None, these parameters will not be sent

    Returns:
        response if successful or empty string if unsuccessful
    """

    base_url = "https://proxy.royaleapi.dev/v1"
    url = base_url + url_path
    headers = get_headers(token)

    if payload is None:
        response = requests.get(url, headers=headers)
    else:
        response = requests.get(url, headers=headers, params=payload)

    status_code = response.status_code

    if status_code == 404:
        print("Error 404")
        return ""
    else:
        return response

def player_tag_correction(player_tag: str):
    """
    This function will be responsible for correcting the player tag
    The player tag starts with the character "#", but this character cannot be used in the url
    This function will make a simple correction to the player tag, changing "#" to "%23"

    Args:
        player_tag (str): Player tag in original form

    Returns:
        player_tag corrected to be utilized in url
    """

    return player_tag.replace("#", "%23")

def get_player_info(player_tag: str):
    """
    This function will be responsible for request information about a specif player

    Args:
        player_tag (str): Player tag in original form

    Returns:
        response content if successful or empty string if unsuccessful
    """

    player_tag = player_tag_correction(player_tag)
    url_path = f"/players/{player_tag}"

    response = get_response(url_path)
    successful_response = response != ""

    if successful_response:
        return response.text
    else:
        print("Unsuccess response")
        return ""

def get_top_path_of_legend_players(season_id: str, limit: int = None):
    """
    This function will be responsible for get top Path of Legend players for given season

    Args:
        season_id (str): Year and month of the season in the format YYYY-MM
        limit (int, optional): Number of players to be returned

    Returns:
        response content if successful or empty string if unsuccessful
    """

    payload = {'limit': limit}
    url_path = f"/locations/global/pathoflegend/{season_id}/rankings/players"

    if limit is not None:
        response = get_response(url_path, payload)
    else:
        response = get_response(url_path)
    
    successful_response = response != ""

    if successful_response:
        return response.text
    else:
        print("Unsuccess response")
        return ""
