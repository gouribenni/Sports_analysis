import os.path, sys

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.append("/Users/nyzy/allianceware_2/sports_analysis_website/beyond_the_numbers_project")
from sqlalchemy import types
import datetime
import os
import json
import pandas as pd
import constants
from data_access_layer.flexible_data_read import FlexDataRead
from transform.schemas import DataSchema
import psycopg2
from sqlalchemy import create_engine
import numpy as np
from data_access_layer.read_sports_analysis_tables_from_postgres import (
    ReadSportsAnalysisTablesPostgre,
)
from data_access_layer.write_sports_analysis_tables_from_postgres import (
    WriteSportsAnalysisTablesPostgre,
)
from ast import literal_eval


class S3ToSQL:
    def __init__(self) -> None:
        self.directory_of_files = constants.JSON_FILE_DIR
        self.fdr = FlexDataRead()
        self.engine = create_engine(
            "postgresql+psycopg2://{0}:{1}@{2}:5432/sports_analysis".format(
                constants.POSTGRES_USER,
                constants.POSTGRES_PASSWORD,
                constants.POSTGRES_HOST,
            )
        )
        self.conn = self.engine.connect()
        self.data_schema = DataSchema()
        self.read_postgre_tables = ReadSportsAnalysisTablesPostgre()
        self.write_postgre_tables = WriteSportsAnalysisTablesPostgre()

    def create_players_table(self):
        df_teams = self.read_postgre_tables.read_teams_table()
        team_name_id = dict(zip(df_teams["team_name"], df_teams.team_id))
        print(team_name_id)
        list_files = os.listdir(self.directory_of_files)
        team_players = {}
        for ff in list_files:
            if "json" in ff:
                # print(os.path.join(directory_of_files,ff))
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                for each in data["info"]["players"]:
                    for xx in data["info"]["players"][each]:
                        if xx not in team_players.keys():
                            team_players[xx] = each
        df = pd.DataFrame(team_players.items(), columns=["player_name", "team_name"])
        df["team_id"] = df["team_name"].apply(lambda x: team_name_id[x])
        df = df[["player_name", "team_id", "team_name"]]
        date_string = "01-01-1980"
        df["date_of_birth"] = datetime.datetime.strptime(date_string, "%m-%d-%Y").date()
        df["batting_style"] = "B"
        df["bowling_style"] = "B"
        df["player_role"] = "B"
        df["player_type"] = "B"
        # df.to_sql('players', con=self.conn, if_exists='append',schema='raw_tables',index=False,dtype=self.data_schema.players_table_schema)
        self.write_postgre_tables.write_players_table(df)
        return df

    def create_teams_table(self):
        list_files = os.listdir(self.directory_of_files)
        team_list_set = set()

        for ff in list_files:
            if "json" in ff:
                # print(os.path.join(self.directory_of_files,ff))

                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                for team_list in data["info"]["teams"]:
                    team_list_set.add(team_list)
                # print(team_list_set)
        df = pd.DataFrame(team_list_set, columns=["team_name"])
        df["team_short_name"] = df["team_name"].apply(lambda x: x[:3])
        df["team_logo_url"] = "logo"
        df["team_captain"] = 8
        df["team_coach"] = ""
        print(df)

        self.write_postgre_tables.write_teams_table(df)
        print("Table Created with shape", df.shape)
        return df

    def create_series_table(self):
        list_files = os.listdir(self.directory_of_files)

        list_series_dict = []
        for ff in list_files:
            if "json" in ff:
                # print(os.path.join(directory_of_files,ff))
                series_dict = {
                    "series_name": "",
                    "series_date": "",
                    "season": "",
                    "match_id": "",
                }
                match_number = ff.split(".")[0]
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                for each in data["info"]["dates"]:
                    # print(data["info"]["event"])
                    series_dict["series_date"] = each
                    try:
                        series_dict["series_name"] = data["info"]["event"]["name"]
                    except:
                        # print(data["info"])
                        series_dict["series_name"] = (
                            data["info"]["teams"][0]
                            + " tour of "
                            + data["info"]["teams"][1]
                        )
                    series_dict["season"] = data["info"]["season"]
                    series_dict["match_id"] = match_number
                    list_series_dict.append(series_dict)
        df = pd.DataFrame(list_series_dict)
        df["match_id"] = df["match_id"].astype("int")
        df["series_date"] = pd.to_datetime(df["series_date"])
        df_dates = (
            df.groupby(["series_name", "season"])["series_date"]
            .aggregate(["min", "max"])
            .reset_index()
        )
        df_matches = df.groupby(["series_name", "season"])["match_id"].agg(
            match_id_set_text=lambda x: set(x)
        )
        df_final = df_matches.merge(
            df_dates,
            how="inner",
            left_on=["series_name", "season"],
            right_on=["series_name", "season"],
        )
        df_final.columns = [
            "series_name",
            "season",
            "match_id_set_text",
            "series_start_date",
            "series_end_date",
        ]
        # pd.DataFrame.from_dict(series_dict,orient='index').transpose()
        df_final["winner_id"] = None
        df_final["match_id_set_text"] = df_final["match_id_set_text"].apply(
            lambda x: list(x)
        )
        df_final["match_id_set_text"] = df_final["match_id_set_text"].apply(json.dumps)
        # df = df.drop_duplicates()
        print(df_final)
        # df.to_sql('series', con=self.conn, if_exists='append',schema='raw_tables',index=False,dtype=self.data_schema.series_table_schema())
        self.write_postgre_tables.write_series_table(df_final)
        print("Table Created with shape", df_final.shape)
        return df

    def create_series_matches_table(self):
        df_teams = self.read_postgre_tables.read_teams_table()
        team_name_id = dict(zip(df_teams["team_name"], df_teams.team_id))

        df_series = self.read_postgre_tables.read_series_table()
        df_series["match_id_set_text"] = df_series["match_id_set_text"].apply(
            literal_eval
        )
        df_series_exploded = df_series.explode("match_id_set_text")

        series_matches_list = []
        list_files = os.listdir(self.directory_of_files)

        for ff in list_files:
            series_matches_dict = {
                "match_id": 0,
                "team_1": 0,
                "team_2": 0,
                "match_number": 0,
            }
            match_number = ff.split(".")[0]
            if "json" in ff:
                # print(os.path.join(directory_of_files,ff))
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                series_matches_dict["match_id"] = match_number
                series_matches_dict["team_1"] = team_name_id[data["info"]["teams"][0]]
                series_matches_dict["team_2"] = team_name_id[data["info"]["teams"][1]]
                try:
                    series_matches_dict["match_number"] = data["info"]["event"][
                        "match_number"
                    ]
                except:
                    series_matches_dict["match_number"] = 1

            series_matches_list.append(series_matches_dict)
        df_matches = pd.DataFrame(series_matches_list)

        df_matches = df_matches.drop_duplicates()
        df_series_exploded["match_id_set_text"] = df_series_exploded[
            "match_id_set_text"
        ].astype("int")
        df_matches["match_id"] = df_matches["match_id"].astype("int")
        print(df_matches.shape, "matches _shape")
        print(df_series_exploded.shape, "series shape")

        df_merge_series_match = df_series_exploded.merge(
            df_matches, how="left", left_on=["match_id_set_text"], right_on=["match_id"]
        )
        df_merge_series_match = df_merge_series_match[
            ["series_id", "match_id", "team_1", "team_2", "match_number"]
        ]
        # df.to_sql('series_matches', con=self.conn, if_exists='append',schema='raw_tables',index=False,dtype=self.data_schema.series_matches_table_schema())
        # print("Table Created with shape",df.shape)
        print(df_merge_series_match)
        self.write_postgre_tables.write_series_matches_table(df_merge_series_match)
        return df_merge_series_match

    def create_matches_table(self):
        df_teams = self.read_postgre_tables.read_teams_table()
        team_name_id = dict(zip(df_teams["team_name"], df_teams.team_id))

        list_files = os.listdir(self.directory_of_files)
        list_of_matches = []
        for ff in list_files:
            if "json" in ff:
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                match_number = ff.split(".")[0]
                match = {
                    "match_id": 0,
                    "match_type": "",
                    "series_id": "",
                    "match_date": "",
                    "venue": "",
                    "city": "",
                    "toss_winner": "",
                    "toss_decision": "",
                    "result": "",
                    "winning_team": "",
                }
                match["match_id"] = match_number
                if match_number == "433605":
                    print(ff)
                match["match_type"] = data["info"]["match_type"]
                match["series_id"] = 4
                match["match_date"] = data["info"]["dates"][0]
                match["venue"] = data["info"]["venue"]
                try:
                    match["city"] = data["info"]["city"]
                except:
                    match["city"] = "Not Mentioned"

                match["toss_winner"] = team_name_id[data["info"]["toss"]["winner"]]

                match["toss_decision"] = data["info"]["toss"]["decision"]
                try:

                    match["result"] = str(data["info"]["outcome"]["by"])

                except:
                    match["result"] = None
                try:
                    match["winning_team"] = team_name_id[
                        data["info"]["outcome"]["winner"]
                    ]
                except:
                    match["winning_team"] = None
            list_of_matches.append(match)

        df = pd.DataFrame(list_of_matches)
        df.drop_duplicates(inplace=True)
        self.write_postgre_tables.write_matches_table(df)
        print("Table Created with shape", df.shape)
        return df

    def create_matches_innings_table(self):
        matches_id = {}
        list_files = os.listdir(self.directory_of_files)
        for ff in list_files:
            match_number = ff.split(".")[0]
            if "json" in ff:
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                for each in data["innings"]:
                    matches_id[match_number] = []
                    for team in data["innings"]:
                        matches_id[match_number].append(team["team"])
        df = pd.DataFrame(
            dict([(key, pd.Series(value)) for key, value in matches_id.items()])
        ).transpose()
        df = df.stack().reset_index()
        df.columns = ["match_id", "innings_number", "team_batted"]
        # df.to_sql('matches_innings', con=self.conn, if_exists='append',schema='raw_tables',index=False,dtype=self.data_schema.matches_innings_table_schema())
        self.write_postgre_tables.write_matches_innings_table(df)
        return df

    def create_innings_table(self):
        df_teams = self.read_postgre_tables.read_teams_table()
        team_name_id = dict(zip(df_teams["team_name"], df_teams.team_id))
        df_matches_innings = self.read_postgre_tables.read_matches_innings_table()
        construct_match_id_innings_number_dict = {}
        for k, v in df_matches_innings.iterrows():
            construct_match_id_innings_number_dict[v["inning_id"]] = (
                v["match_id"],
                v["innings_number"],
            )
        list_files = os.listdir(self.directory_of_files)
        innings_list = []
        for inningid, (
            match_id,
            innings_number,
        ) in construct_match_id_innings_number_dict.items():
            data = self.fdr.read_json(
                os.path.join(self.directory_of_files, str(match_id) + ".json")
            )
            innings = {
                "innings_id": "",
                "innings_number": "",
                "batting_team_id": "",
                "bowling_team_id": "",
                "total_runs": "",
                "wickets_lost": "",
            }
            total_runs = 0
            total_wickets = 0
            innings["innings_id"] = inningid
            innings["innings_number"] = innings_number
            innings["batting_team_id"] = data["innings"][innings_number]["team"]
            innings["batting_team_id"] = team_name_id[innings["batting_team_id"]]
            # print(list(set(data["info"]["teams"]).symmetric_difference(data["innings"][innings_number]["team"]))[0])
            innings["bowling_team_id"] = list(
                set(data["info"]["teams"]).symmetric_difference(
                    (data["innings"][innings_number]["team"]).split("-")
                )
            )[0]
            innings["bowling_team_id"] = team_name_id[innings["bowling_team_id"]]
            for over in data["innings"][innings_number]["overs"]:
                for delivery in over["deliveries"]:
                    total_runs += delivery["runs"]["total"]
                    if "wickets" in delivery.keys():
                        total_wickets += 1
            innings["total_runs"] = total_runs
            innings["wickets_lost"] = total_wickets
            innings_list.append(innings)
        df = pd.DataFrame(innings_list)
        print(df)
        self.write_postgre_tables.write_innings_table(df)
        print("Table Created with shape", df.shape)
        return df

    def create_deliveries_table(self):
        df_matches_innings_table = self.read_postgre_tables.read_matches_innings_table()
        df_players = self.read_postgre_tables.read_players_table()
        player_name_id = dict(zip(df_players["player_name"], df_players.player_id))
        matches_innings_dict_list = (
            df_matches_innings_table.groupby("match_id")["inning_id"]
            .apply(list)
            .to_dict()
        )
        list_files = os.listdir(self.directory_of_files)
        delivery_list = []
        for ff in list_files:

            if "json" in ff:
                match_number = ff.split(".")[0]
                data = self.fdr.read_json(os.path.join(self.directory_of_files, ff))
                for inning_number, inning in enumerate(data["innings"]):
                    for over in inning["overs"]:
                        ball_number = 0
                        for delivery in over["deliveries"]:
                            ball_number += 1
                            deliveries = {
                                "match_id": "",
                                "inning_id": "",
                                "batsman_id": "",
                                "bowler_id": "",
                                "non_striker_id": "",
                                "ball_number": 0,
                                "over_number": "",
                                "runs_scored": "",
                                "is_wicket": "",
                                "player_out": "",
                                "dismissal_type": "",
                                "fielder_id": "",
                            }
                            deliveries["match_id"] = match_number
                            deliveries["inning_id"] = matches_innings_dict_list[
                                int(match_number)
                            ][inning_number]
                            deliveries["batsman_id"] = player_name_id[
                                delivery["batter"]
                            ]
                            deliveries["bowler_id"] = player_name_id[delivery["bowler"]]
                            deliveries["non_striker_id"] = player_name_id[
                                delivery["non_striker"]
                            ]
                            deliveries["ball_number"] += ball_number
                            deliveries["over_number"] = over["over"]
                            deliveries["runs_scored"] = delivery["runs"]["batter"]
                            deliveries["extra_runs"] = delivery["runs"]["extras"]
                            if "wickets" in delivery.keys():
                                deliveries["is_wicket"] = True
                                deliveries["player_out"] = delivery["wickets"][0][
                                    "player_out"
                                ]
                                deliveries["dismissal_type"] = delivery["wickets"][0][
                                    "kind"
                                ]
                                if "fielders" in delivery["wickets"][0].keys():
                                    try:
                                        deliveries["fielder_id"] = player_name_id[
                                            delivery["wickets"][0]["fielders"][0][
                                                "name"
                                            ]
                                        ]
                                    except:
                                        deliveries["fielder_id"] = None
                                else:
                                    deliveries["fielder_id"] = None

                            else:
                                deliveries["player_out"] = None
                                deliveries["is_wicket"] = False
                                deliveries["dismissal_type"] = None
                                deliveries["fielder_id"] = None
                            delivery_list.append(deliveries)
        df = pd.DataFrame(delivery_list)
        print(df)
        self.write_postgre_tables.write_deliveries_table(df)
        # df.to_sql('deliveries', con=self.conn, if_exists='append',schema='raw_tables',index=False,dtype=self.data_schema.deliveries_table_schema())
        print("Table Created with shape", df.shape)
        return df

    def start_transformation(self):
        ## read json files in a loop

        teams_table = self.create_teams_table()
        matches_table = self.create_matches_table()
        series_table = self.create_series_table()
        players_table = self.create_players_table()
        matches_innings = self.create_matches_innings_table()
        series_matches_table = self.create_series_matches_table()
        innings_table  = self.create_innings_table()
        deliveries_table = self.create_deliveries_table()
        return None


if __name__ == "__main__":
    sc = S3ToSQL()
    df = sc.start_transformation()
