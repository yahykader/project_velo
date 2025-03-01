with lille_stations as (

     select * from {{ ref('stg_stations_status_lille') }}

),

final as (
    select
        station_fr_id,
        station_id,
        m_bikes_count,
        null as e_bikes_count,
        m_bikes_count as bikes_count,
        available_docks_count,
        last_reported_at,
        GCS_loaded_at
    from 
        lille_stations
)

select * from final