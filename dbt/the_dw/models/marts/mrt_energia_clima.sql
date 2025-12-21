with
tab_energia as (
    select
    *
    from {{ ref('stg_solar_daily_energy')}}
),
tab_clima as (
    select
    *
    from {{ ref('stg_weather_daily')}}
),
final as (
    select
    t1.dt,
    t1.duracao_geracao_horas,
    t1.total_kwh,
    t1.max_kwh,
    t2.nebulosidade_tarde,
    t2.humidade_tarde,
    t2.precipitacao_total,
    t2.temperatura_min,
    t2.temperatura_max,
    t2.temperatura_manha,
    t2.temperatura_tarde,
    t2.temperatura_noite,
    t2.pressao_tarde,
    t2.velocidade_vento_max,
    t2.direcao_vento_max
    from tab_energia t1 
    join tab_clima t2 
    on t1.dt = t2.dt 
    where t1.dt > date '2021-09-16'
    )
select * from final