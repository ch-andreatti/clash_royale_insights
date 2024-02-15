"""
This script is responsible for creating and updating the players_info table in the data lake
"""

# Libraries

import datetime
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from delta import *
from data_extraction_functions import *

# Helper functions

def get_last_season_id():

    first_day_of_current_month = datetime.datetime.today().replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - datetime.timedelta(days=1)
    last_season_id = last_day_of_previous_month.strftime('%Y-%m')
    return last_season_id

# Data info
target_bucket = "support"
target_table = "players_info"

# Config SparkSession
builder = (
    SparkSession.builder.appName("players_info")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
    if player_info is not None:

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

# Get data from temporary players

fixed_players_tags = [tag for tag in fixed_players.values()]

top_path_of_legend_players = get_top_path_of_legend_players(get_last_season_id(), 3)
if top_path_of_legend_players is not None:

    top_path_of_legend_players = json.loads(top_path_of_legend_players)

    for player in top_path_of_legend_players["items"]:

        player_tag = player.get("tag")
        player_name = player.get("name")
        
        # To avoid duplicate data, only players who are not in the players_tag.json file will be considered
        if player_tag in fixed_players_tags:
            continue

        player_info = get_player_info(player_tag)
        if player_info is not None:

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
            print(f"Problem to get data from temporary player {player_name} with tag {player_tag}")

# Get and save players_info_df
players_info_df = spark.createDataFrame(data=players_info_data, schema=players_info_schema)
# players_info_df.write.format("delta").save("players_info_df")
players_info_df.show()