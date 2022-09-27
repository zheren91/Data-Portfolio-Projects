-- Query the date, home and away team, their home and away goals
WITH home AS (
  SELECT 
    m.id,
    m.date,
    t.team_long_name AS hometeam,
    m.home_team_goal
  FROM team AS t LEFT JOIN matches AS m
  ON t.team_api_id = m.home_team_api_id
),

away AS (
  SELECT 
    m.id,
    m.date,
    t.team_long_name AS awayteam,
    m.away_team_goal
  FROM team AS t LEFT JOIN matches AS m
  ON t.team_api_id = m.away_team_api_id
)
SELECT 
  home.date,
  home.hometeam,
  away.awayteam,
  home.home_team_goal,
  away.away_team_goal
FROM home 
INNER JOIN away ON home.id = away.id;



-- Query the country, league name, average goals scored, and rank each league according to the most average goals in their league
-- to the most average goals in their league in the 2012/2013 season
SELECT 
    c.name AS country,
    l.name AS league,
    AVG(m.home_team_goal + m.away_team_goal) AS avg_goals,
    RANK() OVER(ORDER BY AVG(m.home_team_goal + m.away_team_goal)DESC) AS league_rank
FROM league AS l LEFT JOIN matches AS m
ON l.country_id = m.country_id
LEFT JOIN country c 
ON l.country_id = c.id
WHERE m.season = '2012/2013'
GROUP BY l.name
ORDER BY league_rank


-- Query the date, home and away team, home and away goal, where Liverpool lost, and rank 
-- the goal difference for the games they've lost for season 2014/2015
-- Liverpool team_api_id is 8650
SELECT * FROM team WHERE team_long_name = 'Liverpool'

-- Set up the home team / away CTE 
WITH home AS (
SELECT
  m.id,
  t.team_long_name,
  CASE WHEN m.home_team_goal > m.away_team_goal THEN 'Liverpool Win'
       WHEN m.home_team_goal < m.away_team_goal THEN 'Liverpool Loss'
       ELSE 'Tie' END AS outcome
FROM matches AS m LEFT JOIN team AS t ON 
m.home_team_api_id = t.team_api_id),

away AS (
SELECT
  m.id,
  t.team_long_name,
  CASE WHEN m.home_team_goal < m.away_team_goal THEN 'Liverpool Win'
       WHEN m.home_team_goal > m.away_team_goal THEN 'Liverpool Loss'
       ELSE 'Tie' END AS outcome
FROM matches AS m LEFT JOIN team AS t ON 
m.away_team_api_id = t.team_api_id)

-- Select columns and and rank the matches by goal difference
SELECT DISTINCT
    m.date,
    home.team_long_name AS home_team,
    away.team_long_name AS away_team,
    m.home_team_goal, m.away_team_goal,
    RANK() OVER(ORDER BY ABS(home_team_goal - away_team_goal) DESC) AS match_rank
FROM matches AS m 
LEFT JOIN home ON m.id = home.id
LEFT JOIN away ON m.id = away.id
WHERE m.season =  '2014/2015'
AND ((home.team_long_name = 'Liverpool' AND home.outcome = 'Liverpool Loss')
OR   (away.team_long_name = 'Liverpool' AND away.outcome = 'Liverpool Loss'));

-- Query the date, season, average home and away goal, and season average home and away goal for Legia Warszawd
-- round down to 2 decimals
SELECT * FROM team
WHERE team_long_name = 'Legia Warszawa';

SELECT
	DATE,
	season,
	home_team_goal,
	away_team_goal,
	CASE WHEN home_team_api_id = 8673 THEN 'home' 
		 ELSE 'away' END AS in_poland,
	ROUND(AVG(home_team_goal) OVER(PARTITION BY season),2) AS season_homeavg,
	ROUND(AVG(away_team_goal) OVER(PARTITION BY season),2) AS season_awayavg
FROM matches
WHERE home_team_api_id = 8673 OR away_team_api_id = 8673
ORDER BY (home_team_goal + away_team_goal) DESC;


-- Query the date, home, away goals, running total and running average of Legia Warszawa in 2012/2013
SELECT 
	DATE,
	home_team_goal,
	away_team_goal,
    SUM(home_team_goal) OVER(ORDER BY DATE 
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    ROUND(AVG(home_team_goal) OVER(ORDER BY DATE 
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) AS running_avg
FROM matches
WHERE 
	home_team_api_id = 8673 
	AND season = '2012/2013';


-- Query the id, country name, season, home and away goals, and aggregate average in each row
SELECT 
	m.id, 
    c.name AS country, 
    m.season,
	m.home_team_goal,
	m.away_team_goal,
	AVG(home_team_goal + away_team_goal) OVER() AS overall_avg
FROM matches AS m
LEFT JOIN country AS c ON m.country_id = c.id

-- Query the average number of goals per much in each country's league,
-- find the difference between the league average and overall average
-- in season 2013/2014 and round to 2 decimals
SELECT
    l.name AS league,
     ROUND(AVG(m.home_team_goal + m.away_team_goal),2) AS avg_goals,
    (SELECT
        ROUND(AVG(home_team_goal+away_team_goal),2)
    FROM matches
    WHERE season = '2013/2014') overall_avg,
    (ROUND(AVG(m.home_team_goal + m.away_team_goal) -
	(SELECT AVG(home_team_goal + away_team_goal)
	 FROM matches
	 WHERE season = '2013/2014' ), 2)) AS avg_difference
FROM matches AS m RIGHT JOIN league AS l
    ON l.country_id = m.country_id
WHERE m.season = '2013/2014'
GROUP BY league;


-- Query the stage, number, average goal, overall average of the goals in 2012/2013 season
SELECT
	s.stage,
	ROUND(s.avg_goals,2) AS avg_goal,
	(SELECT 
	 AVG(home_team_goal+away_team_goal)
	 FROM matches
	 WHERE season = '2012/2013') AS overall_avg
FROM 
	(SELECT
		stage,
		AVG(home_team_goal+away_team_goal) AS avg_goals
	FROM matches
	WHERE season = '2012/2013'
	GROUP BY stage) AS s
WHERE 
	s.avg_goals > (SELECT AVG(home_team_goal+away_team_goal)
					FROM matches
					WHERE season = '2012/2013');


-- Query the highest total number of goals in each season, overall, and during August
SELECT
    season,
    MAX(home_team_goal + away_team_goal) AS max_goals,
    (SELECT MAX(home_team_goal + away_team_goal) FROM matches) AS overall_max_goals,
    (SELECT MAX(home_team_goal + away_team_goal) 
     FROM matches
     WHERE id IN 
                    (SELECT id      
                    FROM matches
                    WHERE EXTRACT(MONTH FROM DATE) = 8)) AS august_max_goals   
FROM matches
GROUP BY season;


-- Find the number of soccar matches played in a European country differ across seasons between 2008-2013
SELECT
	c.name AS country,
	COUNT(CASE WHEN m.season = '2008/2009' THEN m.id ELSE NULL END) AS matches_2008_2009,
	COUNT(CASE WHEN m.season = '2009/2010' THEN m.id ELSE NULL END) AS matches_2009_2010,
	COUNT(CASE WHEN m.season = '2010/2011' THEN m.id ELSE NULL END) AS matches_2010_2011,
	COUNT(CASE WHEN m.season = '2011/2012' THEN m.id ELSE NULL END) AS matches_2011_2012,
	COUNT(CASE WHEN m.season = '2012/2013' THEN m.id ELSE NULL END) AS matches_2012_2013		
FROM country AS c LEFT JOIN matches AS m ON c.id = m.country_id 
GROUP BY country;


-- Find the total number of matches won by the home team in each country during 2012 - 2015
SELECT 
        c.name AS country,
        SUM(CASE WHEN season = '2012/2013' AND 
        home_team_goal > away_team_goal THEN 1 ELSE 0 END) AS matches_2012_2013,
        SUM(CASE WHEN season = '2013/2014' AND 
        home_team_goal > away_team_goal THEN 1 ELSE 0 END) AS matches_2013_2014,
        SUM(CASE WHEN season = '2014/2015' AND 
        home_team_goal > away_team_goal THEN 1 ELSE 0 END) AS matches_2014_2015
FROM country AS c LEFT JOIN matches AS m ON c.id = m.country_id
GROUP BY country;


-- Examine the number of wins, losses, and ties in each country using count
-- Count the home wins, away wins, and ties in each country
SELECT 
    c.name AS country,
	COUNT(CASE WHEN m.home_team_goal > m.away_team_goal THEN m.id 
        END) AS home_wins,
	COUNT(CASE WHEN m.home_team_goal < m.away_team_goal THEN m.id 
        END) AS away_wins,
	COUNT(CASE WHEN m.home_team_goal = m.away_team_goal THEN m.id 
        END) AS TIES
FROM country AS c
LEFT JOIN matches AS m
ON c.id = m.country_id
GROUP BY country;


-- Select the country, date, home, and away goals for countries that scored 10 or more goals total
SELECT
    sub.name AS country,
    `date`,
    home_team_goal,
    away_team_goal
FROM 
    (SELECT 
        c.name,
        m.date,
        m.home_team_goal,
        m.away_team_goal,
        (m.home_team_goal+m.away_team_goal) AS total_goals
    FROM matches AS m 
    LEFT JOIN country AS c ON m.country_id = c.id) AS sub
WHERE total_goals >= 10;


-- Query the team long and short names for teams with more than 7 goals as the home team
SELECT
	team_long_name,
	team_short_name
FROM team
WHERE team_api_id IN
	(SELECT 
		home_team_api_id
	 FROM matches
	 WHERE home_team_goal >= 7);
	 
-- Count the number of matches in each country during 2012 to 2015 match seasons
SELECT 
	c.name AS country,
	COUNT(CASE WHEN m.season = '2012/2013' THEN m.id ELSE NULL END) AS matches_2012_2013,
	COUNT(CASE WHEN m.season = '2013/2014' THEN m.id ELSE NULL END) AS matches_2013_2014,
	COUNT(CASE WHEN m.season = '2014/2015' THEN m.id ELSE NULL END) AS matches_2014_2015
FROM country AS c LEFT JOIN matches AS m ON c.id = m.country_id 
GROUP BY country;
	 






