{{
  config(
    materialized = 'view',
    unique_key   = 'game_id'
  )
}}

with
-- -----------------------------------------------------
-- Temporada atual
-- -----------------------------------------------------
current_season as (
    select season_id
    from {{ ref('vw_stg_request_seasons_id') }}
    where is_current = true
),

-- -----------------------------------------------------
-- Jogos que já aconteceram (fonte de verdade)
-- -----------------------------------------------------
games_happened as (
    select
        distinct game_id
    from {{ ref('stg_all_games_summary') }}
    where has_happened_by_status = true
      and season_id = (select season_id from current_season)
),
games_details as (
    select
        distinct game_id,
        true as has_games_details
    from {{ ref('stg_all_games_details') }}
),
games_summary_details as (
    select
        distinct game_id,
        true as has_games_summary_details
    from {{ ref('stg_all_games_summary_details') }}
),
play_by_play as (
    select
      game_id,
      true as has_play_by_play
    from {{ ref('stg_all_play_by_play') }}
    group by game_id
),

-- -----------------------------------------------------
-- Consolidação final
-- -----------------------------------------------------
final as (
    select
        gh.game_id,

        -- flags individuais
        coalesce(gd.has_games_details, false) as has_games_details,
        coalesce(gsd.has_games_summary_details, false) as has_games_summary_details,
        coalesce(pbp.has_play_by_play, false) as has_play_by_play,

        -- flag de prontidão geral
        case
            when coalesce(gd.has_games_details, false)
             and coalesce(gsd.has_games_summary_details, false)
             and coalesce(pbp.has_play_by_play, false)
            then true
            else false
        end as is_fully_synced

    from games_happened gh
    left join games_details gd using (game_id)
    left join games_summary_details gsd using (game_id)
    left join play_by_play pbp using (game_id)
)

select *
from final
where is_fully_synced is false