-- Query 1: Tracking Players’ State Changes
WITH yesterday AS (
    SELECT player_name, first_active_date, last_active_date, dates_active, date
    FROM players
    WHERE date = DATE '2023-03-09'
),
today AS (
    SELECT 
        player_name,
        MAX(effective_date) AS today_date
    FROM players_scd
    WHERE current_season = 2020
    GROUP BY player_name
)
SELECT 
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.player_name IS NULL THEN 'New'
        WHEN y.last_active_date = t.today_date - INTERVAL '1 day' THEN 'Continued Playing'
        WHEN y.last_active_date < t.today_date - INTERVAL '1 day' THEN 'Returned from retirement'
        WHEN t.today_date IS NULL THEN 'Retired'
        ELSE 'Stayed Retired'
    END AS daily_active_state,
    CASE
        WHEN y.player_name IS NULL THEN 'New'
        WHEN y.last_active_date < t.today_date - INTERVAL '365 day' THEN 'Returned from retirement'
        WHEN t.today_date IS NULL THEN 'Retired'
        ELSE 'Continued Playing'
    END AS yearly_active_state,
    COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
    CASE WHEN t.today_date IS NOT NULL THEN ARRAY[t.today_date] ELSE ARRAY[]::DATE[] END AS dates_active,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;

-- Query 2: Aggregations with GROUPING SETS
WITH events_augmented AS (
    SELECT 
        COALESCE(gd.player_name, 'unknown') AS player_name,
        COALESCE(gd.team_id, 'unknown') AS team_id,
        COALESCE(g.season, 'unknown') AS season,
        gd.pts,
        g.home_team_wins
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT 
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_id) = 0 AND GROUPING(season) = 0 THEN 'player_name__team_id__season'
        WHEN GROUPING(player_name) = 0 THEN 'player_name'
        WHEN GROUPING(team_id) = 0 THEN 'team_id'
        WHEN GROUPING(season) = 0 THEN 'season'
        ELSE 'overall'
    END AS aggregation_level,
    COALESCE(player_name, '(overall)') AS player_name,
    COALESCE(team_id, '(overall)') AS team_id,
    COALESCE(season, '(overall)') AS season,
    SUM(pts) AS total_points, 
    COUNT(1) AS number_of_hits
FROM events_augmented
GROUP BY GROUPING SETS (
    (player_name, team_id, season),
    (player_name),
    (team_id),
    ()
)
ORDER BY total_points DESC;

-- Query 3: Player Scoring the Most Points for a Single Team
SELECT 
    gd.player_name,
    gd.team_id,
    SUM(gd.pts) AS total_points
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
GROUP BY gd.player_name, gd.team_id
ORDER BY total_points DESC
LIMIT 1;

-- Query 4: Player Scoring the Most Points in a Single Season
SELECT 
    gd.player_name,
    g.season,
    SUM(gd.pts) AS total_points
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
GROUP BY gd.player_name, g.season
ORDER BY total_points DESC
LIMIT 1;

-- Query 5: Team with the Most Total Wins
WITH team_wins AS (
    SELECT 
        CASE 
            WHEN g.home_team_wins = 1 THEN g.home_team_id
            ELSE g.away_team_id
        END AS winning_team_id
    FROM games g
)
SELECT 
    winning_team_id AS team_id,
    COUNT(*) AS total_wins
FROM team_wins
GROUP BY winning_team_id
ORDER BY total_wins DESC
LIMIT 1;

-- Query 6: Most Games Won in a 90-game Stretch
WITH team_wins AS (
    SELECT 
        gd.team_id,
        ROW_NUMBER() OVER (PARTITION BY gd.team_id ORDER BY g.game_date) AS game_num,
        CASE 
            WHEN (gd.team_id = g.home_team_id AND g.home_team_wins = 1) OR 
                 (gd.team_id = g.away_team_id AND g.home_team_wins = 0) THEN 1
            ELSE 0
        END AS win
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
team_win_streaks AS (
    SELECT 
        team_id,
        game_num,
        SUM(win) OVER (PARTITION BY team_id ORDER BY game_num ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_90_games
    FROM team_wins
)
SELECT 
    team_id,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM team_win_streaks
GROUP BY team_id
ORDER BY max_wins_in_90_games DESC
LIMIT 1;

-- Query 7: Longest Streak of Games with LeBron James Scoring Over 10 Points
WITH lebron_scores AS (
    SELECT 
        player_name,
        game_id,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY g.game_date ASC) 
        - ROW_NUMBER() OVER (PARTITION BY player_name, CASE WHEN gd.pts > 10 THEN 1 ELSE 0 END ORDER BY g.game_date ASC) AS streak_id,
        CASE 
            WHEN gd.pts > 10 THEN 1 
            ELSE 0 
        END AS scored_over_10
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE player_name = 'LeBron James'
),
scoring_streaks AS (
    SELECT 
        player_name,
        streak_id,
        COUNT(*) AS streak_length
    FROM lebron_scores
    WHERE scored_over_10 = 1
    GROUP BY player_name, streak_id
)
SELECT 
    player_name,
    MAX(streak_length) AS longest_streak
FROM scoring_streaks
GROUP BY player_name;
