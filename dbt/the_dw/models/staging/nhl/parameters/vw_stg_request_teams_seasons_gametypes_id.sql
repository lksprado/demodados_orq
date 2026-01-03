{{ 
  config(
    materialized = 'view'
  )
}}

with
away as (
  select 
  distinct
  away_team_abbrev as team_id,
  season_id,
  game_type_id
  from {{ ref('stg_all_games_details') }}
),
home as (
  select 
  distinct
  home_team_abbrev as team_id,
  season_id,
  game_type_id
  from {{ ref('stg_all_games_details') }}
),
final as (
  select * from away
  union
  select * from home
)
select 
* 
from final
where season_id = (select max(season_id) from final) and game_type_id in (2,3)
order by 1 asc, 2 desc