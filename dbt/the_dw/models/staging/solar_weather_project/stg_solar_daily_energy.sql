with
source as (select * from {{ source('raw', 'solar_daily_energy') }}),
renamed as (
    select 
    date::date as dt,
    duration::int as duracao_geracao_horas,
    total::float as total_kwh,
    co2:: float,
    max::float as max_kwh
    from source
)
select * from renamed