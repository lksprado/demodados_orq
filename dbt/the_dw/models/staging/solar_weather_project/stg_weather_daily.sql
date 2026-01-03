with
source as (select * from {{ source('raw', 'openweather_daily') }}),
renamed as (
    select 
    date:: date as dt,
    cloud_cover_afternoon:: float as nebulosidade_tarde,
    humidity_afternoon:: float as humidade_tarde,
    precipitation_total:: float as precipitacao_total,
    temperature_min:: float as temperatura_min,
    temperature_max:: float as temperatura_max,
    temperature_afternoon:: float as temperatura_tarde,
    temperature_night:: float as temperatura_noite,
    temperature_morning:: float as temperatura_manha,
    pressure_afternoon:: float as pressao_tarde,
    wind_max_speed:: float as velocidade_vento_max,
    wind_max_direction:: float as direcao_vento_max
    from source
)
select * from renamed