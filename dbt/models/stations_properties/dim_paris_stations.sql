with 

paris_stations as (
    select * from {{ ref('stg_stations_status_paris') }}
),

final as (
    select
        station_fr_id,
        station_id,
        name as station_name,
        lat,
        lon,
        cast(null as string) as station_address,
        cast(null as string) station_city,
        cast(null as string) station_district,
        -- boolean indicating if the station is currently on service
        (
            is_installed = true 
            and is_renting = true
            and is_returning = true
        ) as is_active,
        -- to get the total of docks we sum the number of mech
        m_bikes_count + e_bikes_count + available_docks_count as total_docks_count
    from 
        paris_stations
    -- here we filter on the last loaded row
    qualify row_number() over(partition by station_id order by GCS_loaded_at desc) =1
)

select 
    * 
from 
    final