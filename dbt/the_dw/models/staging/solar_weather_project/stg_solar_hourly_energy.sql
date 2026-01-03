with
source as (select * from {{ source('raw', 'solar_hourly_energy') }}),
renamed as (
    select 
    datetime:: timestamp as dt_hora,
    energy:: float as kwh
    from source
)
select * from renamed