{{ 
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'player_id']
  )
}}

with source as (

    select payload
    from {{ source('raw', 'nhl_raw_all_games_details') }}

    {% if is_incremental() %}
    where (payload ->> 'id')::int not in (
        select distinct game_id from {{ this }}
    )
    {% endif %}

),

players as (

    -- AWAY TEAM - GOALIES
    select
        (payload ->> 'id')::int as game_id,
        'away' as team_side,
        'goalie' as player_type,
        (p ->> 'playerId')::int as player_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'starter')::boolean as is_starter,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        split_part((p ->> 'saveShotsAgainst'),'/',1)::int as saves,
        (p ->> 'powerPlayGoalsAgainst')::int as powerplay_goals_against,
        split_part((p ->> 'powerPlayShotsAgainst'),'/',1)::int as powerplay_saves,
        split_part((p ->> 'powerPlayShotsAgainst'),'/',2)::int as powerplay_shots_against,
        (p ->> 'shorthandedGoalsAgainst')::int as shorthanded_goals_against,
        (p ->> 'evenStrengthGoalsAgainst')::int as evenstrenght_goals_against,
        split_part((p ->> 'evenStrengthShotsAgainst'),'/',1)::text as evenstrenght_saves,
        split_part((p ->> 'evenStrengthShotsAgainst'),'/',2)::text as evenstrenght_shots_againast
    from source,
         jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'goalies') as p

    union all

    -- HOME TEAM - GOALIES
    select
        (payload ->> 'id')::int as game_id,
        'home' as team_side,
        'goalie' as player_type,
        (p ->> 'playerId')::int as player_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'starter')::boolean as is_starter,
        (p ->> 'goalsAgainst')::int as goals_against,
        (p ->> 'shotsAgainst')::int as shots_against,
        split_part((p ->> 'saveShotsAgainst'),'/',1)::int as saves,
        (p ->> 'powerPlayGoalsAgainst')::int as powerplay_goals_against,
        split_part((p ->> 'powerPlayShotsAgainst'),'/',1)::int as powerplay_saves,
        split_part((p ->> 'powerPlayShotsAgainst'),'/',2)::int as powerplay_shots_against,
        (p ->> 'shorthandedGoalsAgainst')::int as shorthanded_goals_against,
        (p ->> 'evenStrengthGoalsAgainst')::int as evenstrenght_goals_against,
        split_part((p ->> 'evenStrengthShotsAgainst'),'/',1)::text as evenstrenght_saves,
        split_part((p ->> 'evenStrengthShotsAgainst'),'/',2)::text as evenstrenght_shots_againast
    from source,
         jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'goalies') as p
)
select * from players