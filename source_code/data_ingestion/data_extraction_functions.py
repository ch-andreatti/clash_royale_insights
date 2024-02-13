import requests
import json

# Global variables

base_url = "https://proxy.royaleapi.dev/v1/"

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

def get_response(url_path: str):
    """
    This function will be responsible for making request to Clash Royale API
    If the request was successful, the function will return the response
    If the request was unsuccessful, the function will return empty string

    Args:
        url_path (str): The specif url path desired

    Returns:
        response if successful or empty string if unsuccessful
    """

    url = base_url + url_path
    headers = get_headers(token)

    response = requests.get(url, headers)
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
    url_path = f"players/{player_tag}"

    response = get_response(url_path)
    successful_response = response != ""

    if successful_response:
        return response.text
    
    else:
        print("Unsuccess response")
        return ""
