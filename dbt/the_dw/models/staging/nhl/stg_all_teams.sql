{{
  config(
    materialized = 'table',
    )
}}

with 
source as (
  select * from {{ source('raw','nhl_raw_all_teams_id')}}
),
renamed as (
  select
  (payload ->> 'id')::int as team_id,
  (payload ->> 'triCode')::int as team_code,
  (payload ->> 'fullName')::int as team_fullname,
  (payload ->> 'leagueId')::int as league_id,
  (payload ->> 'rawTricode')::int as team_raw_code,
  (payload ->> 'franchiseId')::int as franchise_id
  from source 
)
select * from source
