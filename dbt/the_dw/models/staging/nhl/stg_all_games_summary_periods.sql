{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'period_number']
    )
}}

with source as (
    select *
    from {{ source('raw','nhl_raw_all_games_summary_details') }}
),

exploded_shots as (
    select
        split_part(source_filename,'_',2)::int as game_id,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p -> 'periodDescriptor' ->> 'periodType')  as period_type,
        (p ->> 'away')::int as away_shots,
        (p ->> 'home')::int as home_shots
    from source,
         jsonb_array_elements(payload -> 'shotsByPeriod') as p
),
exploded_goals as (
    select
        split_part(source_filename,'_',2)::int as game_id,
        (p -> 'periodDescriptor' ->> 'number')::int as period_number,
        (p -> 'periodDescriptor' ->> 'periodType')  as period_type,
        (p ->> 'away')::int as away_goals,
        (p ->> 'home')::int as home_goals
    from source,
         jsonb_array_elements(payload -> 'linescore' -> 'byPeriod') as p
),
joins as (
    select
    s.game_id,
    s.period_number,
    s.period_type,
    s.away_shots,
    s.home_shots,
    g.away_goals,
    g.home_goals
    from exploded_shots s 
    join exploded_goals g
    on s.game_id = g.game_id and s.period_number = g.period_number
    order by game_id, period_number
)
select * from joins
{% if is_incremental() %}
where game_id >= (select max(game_id) from {{ this }} )
{% endif %}
