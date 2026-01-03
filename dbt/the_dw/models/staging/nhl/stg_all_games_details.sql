{{
  config(
    materialized = 'incremental',
    unique_key='game_id'
    )
}}

with 
source as (
  select * from {{ source('raw','nhl_raw_all_games_details')}}
),
renamed as (
  select 
  (payload ->> 'id')::int as game_id,
  (payload ->> 'season')::int as season_id,
  (payload -> 'awayTeam' ->> 'id')::int as away_team_id,
  (payload -> 'awayTeam' ->> 'sog')::int as away_team_sog,
  (payload -> 'awayTeam' ->> 'score')::int as away_team_score,
  (payload -> 'awayTeam' ->> 'abbrev') as away_team_abbrev,
  (payload -> 'awayTeam' -> 'placeName' ->> 'default') as away_team_placename,
  (payload -> 'awayTeam' -> 'commonName' ->> 'default') as away_team_commonname,
  (payload ->> 'gameDate')::date as game_date,
  (payload ->> 'gameType')::int as game_type_id,
  case 
    when payload ->> 'gameType' like '1' then 'preseason'
    when payload ->> 'gameType' like '2' then 'regular'
    when payload ->> 'gameType' like '3' then 'playoffs'
  end as game_type_name,
  (payload -> 'homeTeam' ->> 'id')::int as home_team_id,
  (payload -> 'homeTeam' ->> 'sog')::int as home_team_sog,
  (payload -> 'homeTeam' ->> 'score')::int as home_team_score,
  (payload -> 'homeTeam' ->> 'abbrev') as home_team_abbrev,
  (payload -> 'homeTeam' -> 'placeName' ->> 'default') as home_team_placename,
  (payload -> 'homeTeam' -> 'commonName' ->> 'default') as home_team_commonname,
  (payload ->> 'gameState') as game_state,
  (payload ->> 'regPeriods') as reg_periods,
  (payload -> 'gameOutcome' ->> 'lastPeriodType') as last_period_type,
  (payload ->> 'startTimeUTC')::timestamp as game_start_timestamp_utc
  from source
  where (payload ->> 'id') is not null
  order by game_id
  
)

select * from renamed
{% if is_incremental() %}
where game_date >= (
    select max(game_date)
    from {{ this }}
) - interval '3 days'
{% endif %}