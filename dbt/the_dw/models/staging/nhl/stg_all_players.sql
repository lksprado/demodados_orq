{{
  config(
    materialized = 'table',
    )
}}

with 
source as (
  select * from {{ source('raw','nhl_raw_all_players')}}
),
regular as (
  select
  (payload ->> 'playerId')::int as player_id,
  (payload ->> 'isActive')::boolean as is_active,
  (payload ->> 'position') as position,
  (payload -> 'birthCity' ->> 'default') as birth_city,
  (payload ->> 'birthDate')::date as birthdate,
  (payload ->> 'birthCountry') as birth_country,
  (payload -> 'birthStateProvince' ->> 'default') as birth_state_province,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'pim')::int as pim,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'goals')::int as goals,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'shots')::int as shots,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'avgToi') as avg_toi,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'points')::int as points,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'assists')::int as assists,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'otGoals')::int as overtime_goals,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'plusMinus')::int as plus_minus,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'gamesPlayed')::int as games_played,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'shootingPctg')::float as shooting_pctg,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'powerPlayGoals')::int as powerplay_goals,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'powerPlayPoints')::int as powerplay_points,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'gameWinningGoals')::int as game_winning_goals,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'shorthandedGoals')::int as shorthanded_goals,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'shorthandedPoints')::int as shorthanded_points,
  (payload -> 'careerTotals' -> 'regularSeason' ->> 'faceoffWinningPctg')::float as faceoff_win_pctg,
  (payload -> 'draftDetails' ->> 'year')::int as draft_year,
  (payload -> 'draftDetails' ->> 'round')::int as draft_round,
  (payload -> 'draftDetails' ->> 'teamAbbrev') as draft_team_id,
  (payload -> 'draftDetails' ->> 'overallPick')::int as draft_overall_pick,
  (payload -> 'draftDetails' ->> 'pickInRound')::int as draft_pick_in_round,
  (payload -> 'fullTeamName' ->> 'default') as team_full_name,
  (payload ->> 'currentTeamId')::int as current_team_id,
  (payload ->> 'shootsCatches') as shoots_catches,
  (payload ->> 'sweaterNumber')::int as sweater_number,
  (payload ->> 'heightInInches')::int as height_inches,
  (payload ->> 'weightInPounds')::int as weight_pounds,
  (payload ->> 'heightInCentimeters')::int as height_centimeters,
  (payload ->> 'weightInKilograms')::int as weight_kilogram,
  'regular' as season_type  
  from source 
  where 1=1
  {# and payload not like 'https://api-web.nhle.com/v1/player/%/landing' #}
  and payload is not null 
  and payload <> 'null'
),
playoffs as (
  select
  (payload ->> 'playerId')::int as player_id,
  (payload ->> 'isActive')::boolean as is_active,
  (payload ->> 'position') as position,
  (payload -> 'birthCity' ->> 'default') as birth_city,
  (payload ->> 'birthDate')::date as birthdate,
  (payload ->> 'birthCountry') as birth_country,
  (payload -> 'birthStateProvince' ->> 'default') as birth_state_province,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'pim')::int as pim,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'goals')::int as goals,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'shots')::int as shots,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'avgToi') as avg_toi,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'points')::int as points,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'assists')::int as assists,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'otGoals')::int as overtime_goals,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'plusMinus')::int as plus_minus,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'gamesPlayed')::int as games_played,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'shootingPctg')::float as shooting_pctg,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'powerPlayGoals')::int as powerplay_goals,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'powerPlayPoints')::int as powerplay_points,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'gameWinningGoals')::int as game_winning_goals,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'shorthandedGoals')::int as shorthanded_goals,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'shorthandedPoints')::int as shorthanded_points,
  (payload -> 'careerTotals' -> 'playoffs' ->> 'faceoffWinningPctg')::float as faceoff_win_pctg,
  (payload -> 'draftDetails' ->> 'year')::int as draft_year,
  (payload -> 'draftDetails' ->> 'round')::int as draft_round,
  (payload -> 'draftDetails' ->> 'teamAbbrev') as draft_team_id,
  (payload -> 'draftDetails' ->> 'overallPick')::int as draft_overall_pick,
  (payload -> 'draftDetails' ->> 'pickInRound')::int as draft_pick_in_round,
  (payload -> 'fullTeamName' ->> 'default') as team_full_name,
  (payload ->> 'currentTeamId')::int as current_team_id,
  (payload ->> 'shootsCatches') as shoots_catches,
  (payload ->> 'sweaterNumber')::int as sweater_number,
  (payload ->> 'heightInInches')::int as height_inches,
  (payload ->> 'weightInPounds')::int as weight_pounds,
  (payload ->> 'heightInCentimeters')::int as height_centimeters,
  (payload ->> 'weightInKilograms')::int as weight_kilogram,
  'playoffs' as season_type  
  from source 
  where 1=1 
  {# and payload not like 'https://api-web.nhle.com/v1/player/%/landing' #}
  and payload is not null 
  and payload <> 'null'
),
union_tbs as (
  select * from regular
  union all
  select * from playoffs
)
select * from union_tbs where player_id is not null order by player_id desc, season_type desc
