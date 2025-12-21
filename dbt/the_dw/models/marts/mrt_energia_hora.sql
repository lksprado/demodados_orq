with
tab_energia_hora as (
    select
    *
    from {{ ref('stg_solar_hourly_energy')}}
)
select * from tab_energia_hora