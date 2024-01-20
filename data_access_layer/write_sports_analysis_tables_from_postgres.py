from data_access_layer.postgres_data_write import PostgresSqlWriter
from transform.schemas import DataSchema


class WriteSportsAnalysisTablesPostgre:
    def __init__(self) -> None:
        self.postgre_write = PostgresSqlWriter()
        self.data_schema = DataSchema()

    def write_teams_table(self, df):
        PostgresSqlWriter().write_table(
            df, "teams", "raw_tables", dtype=self.data_schema.teams_table_schema()
        )
        return True

    def write_matches_table(self, df):
        PostgresSqlWriter().write_table(
            df, "matches", "raw_tables", dtype=self.data_schema.matches_table_schema()
        )
        return True

    def write_players_table(self, df):
        PostgresSqlWriter().write_table(
            df, "players", "raw_tables", dtype=self.data_schema.players_table_schema()
        )
        return True

    def write_series_table(self, df):
        PostgresSqlWriter().write_table(
            df, "series", "raw_tables", dtype=self.data_schema.series_table_schema()
        )
        return True

    def write_team_stats_table(self, df):
        PostgresSqlWriter().write_table(
            df, "team_stats", "raw_tables", dtype=self.data_schema.teams_table_schema()
        )
        return True

    def write_matches_innings_table(self, df):
        PostgresSqlWriter().write_table(
            df,
            "matches_innings",
            "raw_tables",
            dtype=self.data_schema.matches_innings_table_schema(),
        )
        return True

    def write_player_stats_table(self, df):
        PostgresSqlWriter().write_table(
            df,
            "player_stats",
            "raw_tables",
            dtype=self.data_schema.teams_table_schema(),
        )
        return True

    def write_series_matches_table(self, df):
        PostgresSqlWriter().write_table(
            df,
            "series_matches",
            "raw_tables",
            dtype=self.data_schema.series_matches_table_schema(),
        )
        return True

    def write_innings_table(self, df):
        PostgresSqlWriter().write_table(
            df, "innings", "raw_tables", dtype=self.data_schema.innings_table_schema()
        )
        return True

    def write_deliveries_table(self, df):
        PostgresSqlWriter().write_table(
            df,
            "deliveries",
            "raw_tables",
            dtype=self.data_schema.deliveries_table_schema(),
        )
        return True
