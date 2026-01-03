{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id']
    )
}}

with source as (
    select *
    from {{ source('raw','nhl_raw_all_games_summary_details') }}
),

exploded as (
    select
        split_part(source_filename,'_',2)::int as game_id,
        p ->> 'category'              as metric,
        (p ->> 'awayValue') as away_value,
        (p ->> 'homeValue') as home_value
    from source,
        jsonb_array_elements(payload -> 'teamGameStats') as p
),
wide as (
    select
        game_id,

        max(away_value) filter (where metric = 'sog')::int                    as away_sog,
        max(home_value) filter (where metric = 'sog')::int                    as home_sog,
        
        max(away_value) filter (where metric = 'faceoffWinningPctg')::float                    as away_faceoff_winning_pctg,
        max(home_value) filter (where metric = 'faceoffWinningPctg')::float                    as home_faceoff_winning_pctg,

        max(away_value) filter (where metric = 'powerPlayPctg')::float                    as away_powerplay_pctg,
        max(home_value) filter (where metric = 'powerPlayPctg')::float                    as home_powerplay_pctg,

        max(away_value) filter (where metric = 'pim')::int     as away_pim,
        max(home_value) filter (where metric = 'pim')::int     as home_pim,

        max(away_value) filter (where metric = 'hits')::int                   as away_hits,
        max(home_value) filter (where metric = 'hits')::int                   as home_hits,

        max(away_value) filter (where metric = 'blockedShots')::int           as away_blocked_shots,
        max(home_value) filter (where metric = 'blockedShots')::int           as home_blocked_shots,

        max(away_value) filter (where metric = 'giveaways')::int           as away_giveaways,
        max(home_value) filter (where metric = 'giveaways')::int           as home_giveaways,

        max(away_value) filter (where metric = 'takeaways')::int           as away_takeaways,
        max(home_value) filter (where metric = 'takeaways')::int           as home_takeaways
    from exploded
    where game_id is not null
    group by game_id
    order by game_id desc
)
select * from wide
{% if is_incremental() %}
where game_id >= (select max(game_id) from {{ this }} )
{% endif %}