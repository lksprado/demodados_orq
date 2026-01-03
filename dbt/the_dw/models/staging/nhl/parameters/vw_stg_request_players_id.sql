{{ 
  config(
    materialized = 'view'
  )
}}

with
goalies_club_stats as (
  select 
  distinct
  player_id,
  'goalie' as position
  from {{ ref('stg_all_club_stats_goalies') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_club_stats_goalies') }})
  and game_type_id in (2,3)
),
skaters_club_stats as (
  select 
  distinct
  player_id,
  'skater' as position
  from {{ ref('stg_all_club_stats_skaters') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_club_stats_skaters') }})
  and game_type_id in (2,3)
),
goalies_games_details as (
  select
  distinct
  player_id,
  'skater' as position
  from {{ ref('stg_all_games_details_goalies') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_details_goalies') }})
  and game_type_id in (2,3)
), 
skaters_games_details as (
  select
  distinct
  player_id,
  'skater' as position
  from {{ ref('stg_all_games_details_skaters') }}
  where season_id = (select max(season_id) from {{ ref('stg_all_games_details_skaters') }})
  and game_type_id in (2,3)
), 
final as (
  select * from goalies_club_stats
  union
  select * from skaters_club_stats
  union
  select * from goalies_games_details
  union
  select * from skaters_games_details
)
select 
* 
from final