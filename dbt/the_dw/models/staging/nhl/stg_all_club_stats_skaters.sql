{{ 
  config(
    materialized = 'table',
  )
}}

with source as (
    select *
    from {{ source('raw', 'nhl_raw_all_club_stats') }}
),

skaters as (
    select
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        'skater' as player_type,
        (p ->> 'playerId')::int as player_id,
        (p -> 'firstName' ->> 'default') as player_first_name,
        (p -> 'lastName' ->> 'default') as player_last_name,
        (p ->> 'goals')::int as goals,
        (p ->> 'shots')::int as shots,
        (p ->> 'points')::int as points,
        (p ->> 'assists')::int as assists,
        (p ->> 'headshot') as player_picture,
        (p ->> 'plusMinus')::int as plus_minus,
        (p ->> 'gamesPlayed')::int as games_played,
        (p ->> 'positionCode') as position,
        (p ->> 'shootingPctg')::float as shooting_pctg,
        (p ->> 'overtimeGoals')::int as overtime_goals,
        (p ->> 'faceoffWinPctg')::float as faceoff_win_pctg,
        (p ->> 'penalyMinutes')::int as pim,        
        (p ->> 'powerPlayGoals')::int as powerplay_goals,
        (p ->> 'avgShiftsPerGame')::float as avg_shifts_per_game,
        (p ->> 'gameWinningGoals')::int as game_winning_goals,
        (p ->> 'shorthandedGoals')::int as shorthanded_goals,
        (p ->> 'avgTimeOnIcePerGame')::float as avg_toi_per_game
    from source,
        jsonb_array_elements(payload -> 'skaters') as p
)
select * from skaters