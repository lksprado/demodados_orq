{{
  config(
    materialized = 'incremental',
    unique_key = ['game_id', 'event_id']
    )
}}

with 
source as (
  select * from {{ source('raw', 'nhl_raw_all_play_by_play') }}
),
renamed as (
  select
    (payload ->> 'id')::int as game_id,
    (payload ->> 'season')::int as season_id,
    (payload -> 'clock' ->> 'running')::boolean as is_running,
    (payload -> 'clock' ->> 'timeRemaining') as time_remaining,
    (payload -> 'clock' ->> 'inIntermission')::boolean as is_in_intermission,
    (payload -> 'clock' ->> 'secondsRemaining') as seconds_remaining,
    (payload ->> 'otInUse')::boolean as overtime_in_use,
    (payload ->> 'gameDate')::date as game_date,
    (payload ->> 'gameType')::int as game_type_id,
    (payload ->> 'gameState') as game_state,
    (payload ->> 'maxPeriods')::int as max_periods,
    (payload ->> 'regPeriods')::int as reg_periods,
    (payload -> 'gameOutcome' ->> 'lastPeriodType') as outcome_last_period_type,
    (payload ->> 'startTimeUTC')::timestamp as start_time_utc,
    (payload ->> 'shootoutInUse')::boolean as shootout_in_use,
    (payload ->> 'gameScheduleState') as game_schedule_state,
    (p ->> 'eventId')::int as event_id,
    (p ->> 'typeCode')::int as type_code,
    (p ->> 'sortOrder')::int as sort_order,
    (p ->> 'typeDescKey') as type_desc_key,
    (p ->> 'timeInPeriod') as time_in_period,
    (p ->> 'situationCode')::int as situation_code,
    (p -> 'periodDescriptor' ->> 'number')::int as period_number,
    (p -> 'periodDescriptor' ->> 'periodType') as period_type,
    (p ->> 'homeTeamDefendingSide') as home_team_defending_side
  from source,
      jsonb_array_elements(payload -> 'plays') as p
)
select * from renamed
{% if is_incremental() %}
where game_date >= (select max(game_date) from {{ this }} )
{% endif %}
