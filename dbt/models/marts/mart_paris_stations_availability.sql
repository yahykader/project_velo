with 

paris_stations_availability as (
    select 
        station_fr_id,
        m_bikes_count,
        e_bikes_count,
        bikes_count,
        available_docks_count,
        GCS_loaded_at
    from {{ ref('fct_paris_stations_availability') }}
),

paris_stations_properties as (
    select
        station_fr_id,
        lat,
        lon,
        total_docks_count,
        station_city,
        station_address,
        station_district
    from
        {{ ref('dim_paris_stations') }}
    where 
        is_active = true
),

final as (
    select 
        p.*,
        a.* except(station_fr_id)
    from 
        paris_stations_properties as p
    left join
        paris_stations_availability as a
        on p.station_fr_id = a.station_fr_id
)

select * from final