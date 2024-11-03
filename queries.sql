/* Top 10 highest-scoring games in past decade */
WITH
past_decade AS (
    SELECT
        *,
        home_score + away_score AS total_score
    FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
nba_teams AS (
    SELECT * FROM teams
)
SELECT DISTINCT
    game_id,
    date,
    home_team,
    away_team,
    total_score
FROM past_decade AS x
INNER JOIN nba_teams AS y ON trim(x.home_team, '"') = y.team_name OR trim(x.away_team, '"') = y.team_name
ORDER BY total_score DESC
LIMIT 10
;

/* Win-Loss records */
WITH
past_decade AS (
    SELECT
        *
    FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
home AS (
    SELECT
        trim(home_team, '"') AS team_name,
        count(CASE WHEN home_score > away_score THEN 1 ELSE NULL END) AS wins,
        count(CASE WHEN home_score < away_score THEN 1 ELSE NULL END) AS losses,
        count(CASE WHEN home_score = away_score THEN 1 ELSE NULL END) AS ties
    FROM past_decade
    GROUP BY 1
),
away AS (
    SELECT
        trim(away_team, '"') AS team_name,
        count(CASE WHEN home_score < away_score THEN 1 ELSE NULL END) AS wins,
        count(CASE WHEN home_score > away_score THEN 1 ELSE NULL END) AS losses,
        count(CASE WHEN home_score = away_score THEN 1 ELSE NULL END) AS ties
    FROM past_decade
    GROUP BY 1
),
joined AS (
    SELECT
        x.team_id,
        x.team_name,
        coalesce(y.wins, 0) + coalesce(z.wins, 0) AS total_wins,
        coalesce(y.losses, 0) + coalesce(z.losses, 0) AS total_losses,
        coalesce(y.ties, 0) + coalesce(z.ties, 0) AS total_ties
    FROM teams AS x
    LEFT JOIN home AS y ON x.team_name = y.team_name
    LEFT JOIN away AS z ON x.team_name = z.team_name
)
SELECT * FROM joined
;

/* Average team scores per season */
WITH
past_decade AS (
    SELECT
        *
    FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
home AS (
    SELECT
        home_team AS team_name,
        season,
        count(DISTINCT game_id) AS num_games,
        sum(home_score) AS num_scores
    FROM past_decade
    GROUP BY 1,2
),
away AS (
    SELECT
        away_team AS team_name,
        season,
        count(DISTINCT game_id) AS num_games,
        sum(away_score) AS num_scores
    FROM past_decade
    GROUP BY 1,2
),
unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
),
agg AS (
    SELECT
        team_name,
        season,
        sum(num_games) AS total_games,
        sum(num_scores) AS total_scores
    FROM unioned
    GROUP BY 1,2
),
nba_teams AS (
    SELECT * FROM teams
)
SELECT
    x.team_name,
    season,
    total_scores / total_games AS avg_score
FROM agg AS x
INNER JOIN nba_teams AS y ON trim(x.team_name, '"') = y.team_name
ORDER BY 1,2
;

/* East vs. West Conferences */
WITH
past_decade AS (
    SELECT
        *
    FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
home AS (
    SELECT
        trim(home_team, '"') AS team_name,
        count(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins
    FROM past_decade
    GROUP BY 1
),
away AS (
    SELECT
        trim(away_team, '"') AS team_name,
        count(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS wins
    FROM past_decade
    GROUP BY 1
),
joined AS (
    SELECT
        x.conference,
        sum(coalesce(y.wins, 0) + coalesce(z.wins, 0)) AS total_wins
    FROM teams AS x
    LEFT JOIN home AS y ON x.team_name = y.team_name
    LEFT JOIN away AS z ON x.team_name = z.team_name
    GROUP BY 1
)
SELECT * FROM joined
;

/* Team w/ highest average margin of victory */
WITH
past_decade AS (
    SELECT * FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
home AS (
    SELECT
        home_team AS team_name,
        sum(home_score - away_score) AS margin,
        count(DISTINCT game_id) AS num_games
    FROM past_decade
    GROUP BY 1
),
away AS (
    SELECT
        away_team AS team_name,
        sum(away_score - home_score) AS margin,
        count(DISTINCT game_id) AS num_games
    FROM past_decade
    GROUP BY 1
),
unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
),
agg AS (
    SELECT
        team_name,
        sum(margin) AS total_margin,
        sum(num_games) AS total_games
    FROM unioned
    GROUP BY 1
),
nba_teams AS (
    SELECT * FROM teams
)
SELECT
    x.team_name,
    total_margin / total_games AS avg_margin
FROM agg AS x
INNER JOIN nba_teams AS y ON trim(x.team_name, '"') = y.team_name
ORDER BY avg_margin DESC
;

/* Average Points Scored vs. Average Points Allowed */
WITH
past_decade AS (
    SELECT * FROM games
    WHERE season >= year(CURRENT_DATE()) - 10
),
home AS (
    SELECT
        home_team AS team_name,
        season,
        count(DISTINCT game_id) AS num_games,
        sum(home_score) AS points_scored,
        sum(away_score) AS points_allowed
    FROM past_decade
    GROUP BY 1,2
),
away AS (
    SELECT
        away_team AS team_name,
        season,
        count(DISTINCT game_id) AS num_games,
        sum(away_score) AS points_scored,
        sum(home_score) AS points_allowed
    FROM past_decade
    GROUP BY 1,2
),
unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
),
agg AS (
    SELECT
        team_name,
        season,
        sum(num_games) AS total_games,
        sum(points_scored) AS total_points_scored,
        sum(points_allowed) AS total_points_allowed
    FROM unioned
    GROUP BY 1,2
),
nba_teams AS (
    SELECT * FROM teams
)
SELECT
    y.team_name,
    season,
    total_points_scored / total_games AS avg_points_scored,
    total_points_allowed / total_games AS avg_points_allowed
FROM agg AS x
INNER JOIN nba_teams AS y ON trim(x.team_name, '"') = y.team_name
ORDER BY y.team_name, season
;