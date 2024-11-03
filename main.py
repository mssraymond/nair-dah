import argparse

from duck import Duck
from nba import NBA


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--ingest", action="store_true")
    args = parser.parse_args()
    duck = Duck("nba")
    if args.ingest:
        nba = NBA()
        seasons_data = nba.fetch_seasons()
        duck.ingest(data=seasons_data, table_name="seasons")
        duck.truncate(table_name="games")
        for season in seasons_data:
            season = season["year"]
            games_data = nba.fetch_games(season=season)
            duck.ingest(data=games_data, table_name="games", replace=False)
        teams_data = nba.fetch_teams()
        duck.ingest(data=teams_data, table_name="teams")
    duck.query("seasons")
    duck.query("games")
    teams = duck.query("teams")
    duck.nba_standard_league(teams=teams)
    duck.games_standard_league()
    duck.run_sqls(filepath="queries.sql")


if __name__ == "__main__":
    main()
