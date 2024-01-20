from sqlalchemy import types
import datetime
import os
import json
import pandas as pd
import constants


class DataSchema:
    def __init__(self) -> None:
        None

    def players_table_schema(self):
        players_table_sql_types = {
            "player_name": types.VARCHAR,
            "team_id": types.INTEGER,
            "team_name": types.VARCHAR,
            "date_of_birth": types.Date,
            "batting_style": types.VARCHAR,
            "bowling_style": types.VARCHAR,
            "player_role": types.VARCHAR,
            "player_type": types.VARCHAR,
        }

        return players_table_sql_types

    def teams_table_schema(self):
        teams_table_sql_types = {
            "team_name": types.VARCHAR,
            "team_short_name": types.VARCHAR,
            "team_logo_url": types.VARCHAR,
            "team_captain": types.Integer,
            "team_coach": types.VARCHAR,
        }
        return teams_table_sql_types

    def matches_innings_table_schema(self):
        matches_innings_table_sql_types = {
            "match_id": types.Integer,
            "innings_number": types.Integer,
            "team_batted": types.VARCHAR,
        }
        return matches_innings_table_sql_types

    def innings_table_schema(self):
        innings_table_sql_types = {
            "innings_id": types.Integer,
            "innings_number": types.Integer,
            "batting_team_id": types.Integer,
            "bowling_id": types.Integer,
            "total_runs": types.Integer,
            "wicket_lost": types.Integer,
        }

        return innings_table_sql_types

    def series_table_schema(self):
        series_table_sql_types = {
            "series_name": types.VARCHAR,
            "season": types.VARCHAR,
            "match_id_set_text": types.JSON,
            "series_start_date": types.Date,
            "series_end_date": types.Date,
            "winner_id": types.Integer,
        }
        return series_table_sql_types

    def series_matches_table_schema(self):
        series_table_sql_types = {
            "series_id": types.Integer,
            "match_id": types.Integer,
            "team_1": types.Integer,
            "team_2": types.Integer,
            "match_number": types.Integer,
        }
        return series_table_sql_types

    def matches_table_schema(self):
        matches_table_sql_types = {
            "match_id": types.Integer,
            "match_type": types.VARCHAR,
            "series_id": types.Integer,
            "match_date": types.Date,
            "venue": types.VARCHAR,
            "city": types.VARCHAR,
            "toss_winner": types.VARCHAR,
            "toss_decision": types.VARCHAR,
            "result": types.VARCHAR,
            "winning_team": types.Integer,
        }
        return matches_table_sql_types

    def deliveries_table_schema(self):
        deliveries_table_sql_types = {
            "delivery_id": types.Integer,
            "match_id": types.Integer,
            "inning_id": types.Integer,
            "batsman_id": types.Integer,
            "bowler_id": types.Integer,
            "non_striker_id": types.Integer,
            "ball_number": types.Integer,
            "over_number": types.Integer,
            "runs_scored": types.Integer,
            "extra_runs": types.Integer,
            "is_wicket": types.Boolean,
            "player_out": types.VARCHAR,
            "dismissal_type": types.VARCHAR,
            "fielder_id": types.Integer,
        }
        return deliveries_table_sql_types


if __name__ == "__main__":
    ds = DataSchema()
