with 

lille_stations as (

     select * from {{ ref('stg_stations_status_lille') }}

),

final as (
    select
        station_fr_id,
        station_id,
        station_name,
        lat,
        lon,
        address as station_address,
        city as station_city,
        cast(null as string) as station_district,
        -- boolean indicating if the station is currently on service
        station_state = 'EN SERVICE' as is_active,

        -- to get the total of docks we sum the number of bikes with the number of available docks
        available_docks_count + m_bikes_count as total_docks_count
    from 
        lille_stations
    -- here we filter on the last loaded row
    qualify row_number() over(partition by station_id order by GCS_loaded_at desc) =1
)

select 
    * 
from 
    final