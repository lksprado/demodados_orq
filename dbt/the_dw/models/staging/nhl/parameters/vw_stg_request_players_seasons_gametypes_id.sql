{{ 
  config(
    materialized = 'view',
    unique_key = ['game_id']
  )
}}

with
skaters_games_details as (
  select 
  distinct
  player_id,
  season_id,
  game_type_id
  from {{ ref('stg_all_games_details_skaters') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_details_skaters') }})
  and game_type_id in (2,3)
),
goalie_games_details as (
  select 
  distinct
  player_id,
  season_id,
  game_type_id
  from {{ ref('stg_all_games_details_goalies') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_details_skaters') }})
  and game_type_id in (2,3)
),
players_gamelog as (
  select 
  distinct
  player_id,
  season_id,
  game_type_id
  from {{ ref('stg_all_player_game_log') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_details_skaters') }})
  and game_type_id in (2,3)
),
final as (
  select * from skaters_games_details
  union
  select * from goalie_games_details
  union
  select * from players_gamelog
)
select * from final