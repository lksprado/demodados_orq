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

    -- AWAY TEAM - DEFENSE
    select
        (payload ->> 'id')::int as game_id,
        'away' as team_side,
        'defense' as player_type,
        (p ->> 'playerId')::int as player_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from source,
        jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'defense') as p

    union all

    -- AWAY TEAM - FORWARDS
    select
        (payload ->> 'id')::int,
        'away',
        'forward',
        (p ->> 'playerId')::int,
        (payload ->> 'season')::int,
        (payload ->> 'gameType')::int,
        (p -> 'name' ->> 'default'),
        (p ->> 'position'),
        (p ->> 'sweaterNumber')::int,
        (p ->> 'toi'),
        (p ->> 'goals')::int,
        (p ->> 'assists')::int,
        (p ->> 'points')::int,
        (p ->> 'sog')::int,
        (p ->> 'hits')::int,
        (p ->> 'blockedShots')::int,
        (p ->> 'plusMinus')::int
    from source,
        jsonb_array_elements(payload -> 'playerByGameStats' -> 'awayTeam' -> 'forwards') as p

    union all

    -- HOME TEAM - DEFENSE
    select
        (payload ->> 'id')::int as game_id,
        'home' as team_side,
        'defense' as player_type,
        (p ->> 'playerId')::int as player_id,
        (payload ->> 'season')::int as season_id,
        (payload ->> 'gameType')::int as game_type_id,
        (p -> 'name' ->> 'default') as player_name,
        (p ->> 'position') as position,
        (p ->> 'sweaterNumber')::int as sweater_number,
        (p ->> 'toi') as time_on_ice,
        (p ->> 'goals')::int as goals,
        (p ->> 'assists')::int as assists,
        (p ->> 'points')::int as points,
        (p ->> 'sog')::int as shots_on_goal,
        (p ->> 'hits')::int as hits,
        (p ->> 'blockedShots')::int as blocked_shots,
        (p ->> 'plusMinus')::int as plus_minus
    from source,
        jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'defense') as p

    union all

    -- HOME TEAM - FORWARDS
    select
        (payload ->> 'id')::int,
        'home',
        'forward',
        (p ->> 'playerId')::int,
        (payload ->> 'season')::int,
        (payload ->> 'gameType')::int,
        (p -> 'name' ->> 'default'),
        (p ->> 'position'),
        (p ->> 'sweaterNumber')::int,
        (p ->> 'toi'),
        (p ->> 'goals')::int,
        (p ->> 'assists')::int,
        (p ->> 'points')::int,
        (p ->> 'sog')::int,
        (p ->> 'hits')::int,
        (p ->> 'blockedShots')::int,
        (p ->> 'plusMinus')::int
    from source,
        jsonb_array_elements(payload -> 'playerByGameStats' -> 'homeTeam' -> 'forwards') as p

)
select * from players