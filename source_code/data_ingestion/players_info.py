"""
This script is responsible for creating and updating the players_info table in the data lake
"""

# Libraries

import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from data_extraction_functions import *

# Data info
target_bucket = "support"
target_table = "players_info"

# Start spark session
spark = SparkSession.builder.appName('players_info').getOrCreate()

# Resources of players_info table

players_info_schema = StructType([ \
    StructField("tag", StringType(), True), \
    StructField("name", StringType(), True), \
    StructField("expLevel", IntegerType(), True), \
    StructField("trophies", IntegerType(), True), \
    StructField("best_trophies", IntegerType(), True), \
    StructField("wins", IntegerType(), True), \
    StructField("losses", IntegerType(), True), \
    StructField("battle_count", IntegerType(), True), \
    StructField("three_crown_wins", IntegerType(), True), \
    StructField("challenge_max_wins", IntegerType(), True), \
    StructField("role", StringType(), True), \
    StructField("total_donations", IntegerType(), True), \
    StructField("arena", StringType(), True) \
])

players_info_data = []

# Get data from fixed players

with open("./players_tag.json", "r") as file:
    fixed_players = json.load(file)

for key, tag in fixed_players.items():
    
    player_info = get_player_info(tag)

    if player_info != "":

        player_info = json.loads(player_info)

        row = (
            player_info.get("tag", ""),
            player_info.get("name", ""),
            player_info.get("expLevel", 0),
            player_info.get("trophies", 0),
            player_info.get("bestTrophies", 0),
            player_info.get("wins", 0),
            player_info.get("losses", 0),
            player_info.get("battleCount", 0),
            player_info.get("threeCrownWins", 0),
            player_info.get("challengeMaxWins", 0),
            player_info.get("role", ""),
            player_info.get("totalDonations", 0),
            player_info["arena"]["name"]
        )
        players_info_data.append(row)
    
    else:

        print(f"Problem to get data from fixed player {key} with tag {tag}, at players_tag.json")
        continue

# Get players_info_df

players_info_df = spark.createDataFrame(data=players_info_data, schema=players_info_schema)

# Get data from temporary players

top_path_of_legend_players = get_top_path_of_legend_players("2024-01", 3)
top_path_of_legend_players = json.loads(top_path_of_legend_players)

print(top_path_of_legend_players["items"])