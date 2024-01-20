from data_access_layer.postgres_data_read import PostgresSqlReader


class ReadSportsAnalysisTablesPostgre:
    def __init__(self) -> None:
        self.postgre_read = PostgresSqlReader()

    def read_teams_table(self):
        df = PostgresSqlReader().read_table("teams", "raw_tables")
        return df

    def read_matches_table(self):
        df = PostgresSqlReader().read_table("matches", "raw_tables")
        return df

    def read_players_table(self):
        df = PostgresSqlReader().read_table("players", "raw_tables")
        return df

    def read_series_table(self):
        df = PostgresSqlReader().read_table("series", "raw_tables")
        return df

    def read_team_stats_table(self):
        df = PostgresSqlReader().read_table("team_stats", "raw_tables")
        return df

    def read_matches_innings_table(self):
        df = PostgresSqlReader().read_table("matches_innings", "raw_tables")
        return df

    def read_player_stats_table(self):
        df = PostgresSqlReader().read_table("player_stats", "raw_tables")
        return df

    def read_series_matches_table(self):
        df = PostgresSqlReader().read_table("series_matches", "raw_tables")
        return df

    def read_innings_table(self):
        df = PostgresSqlReader().read_table("innings", "raw_tables")
        return df

    def read_deliveries_table(self):
        df = PostgresSqlReader().read_table("deliveries", "raw_tables")
        return df
