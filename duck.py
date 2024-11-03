import json
from typing import Any, Dict, List

import duckdb
import pandas
from numpy import bool, float64, int64, object_


class Duck:
    def __init__(self, db_name: str) -> None:
        self.conn = duckdb.connect("data/" + db_name + ".db")
        self.newline = "\n"
        self.tab = "\t"
        self.types = {
            int64: "INTEGER",
            float64: "FLOAT",
            bool: "BOOLEAN",
            object_: "STRING",
        }

    def truncate(self, table_name: str) -> None:
        try:
            self.conn.sql(f"TRUNCATE {table_name}")
        except Exception as e:
            print(e)

    def create_sql(
        self, table_name: str, df: pandas.DataFrame, replace: bool = True
    ) -> str:
        dtypes = df.dtypes.apply(lambda x: x.type).to_dict()
        if not replace:
            return f"""CREATE TABLE IF NOT EXISTS {table_name} ({self.newline}{("," + self.newline).join([self.tab + c + " " + self.types[dtypes[c]] for c in list(df.columns)])}{self.newline})"""
        return f"""CREATE OR REPLACE TABLE {table_name} ({self.newline}{("," + self.newline).join([self.tab + c + " " + self.types[dtypes[c]] for c in list(df.columns)])}{self.newline})"""

    def ingest(
        self, data: List[Dict[str, Any]], table_name: str, replace: bool = True
    ) -> None:
        for dic in data:
            try:
                for key in dic.keys():
                    if isinstance(
                        dic[key], (Dict, dict)
                    ):  # Converts nested dicts to strings
                        dic[key] = json.dumps(dic[key])
            except AttributeError:
                if isinstance(dic, int):
                    for i in range(len(data)):
                        data[i] = {"year": data[i]}
        df = pandas.DataFrame(data)
        create_sql = self.create_sql(table_name, df, replace)
        print(create_sql)
        self.conn.sql(create_sql)
        self.conn.sql(f"INSERT INTO {table_name} SELECT * FROM df")
        self.conn.table(table_name).show(max_rows=100)

    def query(self, table_name: str) -> List[Dict[str, Any]]:
        relation = self.conn.table(table_name)
        relation.show(max_rows=100)
        df: pandas.DataFrame = relation.fetchdf()
        return df.to_dict(orient="records")

    def nba_standard_league(self, teams: List[Dict[str, Any]]) -> None:
        """
        Creates `nba_standard_league` table via Python-object manipulation.
        Less performant.
        """
        filtered_teams = []
        table_name = "nba_standard_league"
        for team in teams:
            leagues: Dict[str, Any] = json.loads(team["leagues"])
            standard_league = leagues.get("standard")
            if standard_league and team.get("nbaFranchise", False) == True:
                team["team_id"] = team.pop("id")
                team["team_name"] = team.pop("name")
                team.pop("leagues")
                conference = standard_league["conference"]
                division = standard_league["division"]
                team["conference"] = conference
                team["division"] = division
                filtered_teams.append(team)
        df = pandas.DataFrame(data=filtered_teams)
        create_sql = self.create_sql(table_name, df)
        print(create_sql)
        self.conn.sql(create_sql)
        self.conn.sql(f"INSERT INTO {table_name} SELECT * FROM df")
        self.conn.table(table_name).show(max_rows=100)

    def games_standard_league(self) -> None:
        """
        Creates `games_standard_league` table via SQL query.
        More performant.
        """
        table_name = "games_standard_league"
        sql = f"""
        CREATE OR REPLACE TABLE {table_name}
        AS
            WITH
            cte AS (
                SELECT
                    *,
                    date::JSON AS date_json,
                    teams::JSON AS teams_json,
                    scores::JSON AS scores_json
                FROM games
            WHERE league = 'standard'
            )
            SELECT
                id AS game_id,
                season,
                date_json.start::DATE AS date,
                teams_json.home.name::STRING AS home_team,
                teams_json.visitors.name::STRING AS away_team,
                scores_json.home.points::INTEGER AS home_score,
                scores_json.visitors.points::INTEGER AS away_score
            FROM cte
        """
        self.conn.sql(sql)
        self.conn.table(table_name).show(max_rows=100)

    def run_sqls(self, filepath: str) -> None:
        """
        Executes SQL script at `filepath`.
        """
        with open(filepath, "r") as f:
            txt = f.read()
            txt = txt.replace("FROM games", "FROM games_standard_league").replace(
                "FROM teams", "FROM nba_standard_league"
            )
            sqls: List[str] = txt.split(";")
            for sql in sqls:
                sql = sql.strip()
                if len(sql) == 0:
                    continue
                print(sql)
                self.conn.sql(sql).show(max_rows=100)
