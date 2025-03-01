with lyon_stations as (

     select * from {{ ref('stg_stations_status_lyon') }}

),

final as (
    select
        station_fr_id,
        station_id,
        m_bikes_count,
        e_bikes_count,
        bikes_count,
        available_docks_count,
        last_reported_at,
        GCS_loaded_at
    from 
        lyon_stations
)

select * from final