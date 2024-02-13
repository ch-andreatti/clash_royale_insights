import requests
import json

# Load access token from access_token.json
with open("./access_token.json", "r") as file:
    access_token = json.load(file)
token = access_token["token"]

# Load personal tag from players_tag.json
with open("./players_tag.json", "r") as file:
    players_tag = json.load(file)
personal_tag = players_tag["personal_tag"]

headers = {"Authorization": f"Bearer {token}"}
response = requests.get(f"https://proxy.royaleapi.dev/v1/players/{personal_tag}", headers=headers)

print(response.text)
