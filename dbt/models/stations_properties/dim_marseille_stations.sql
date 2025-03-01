with 

marseille_stations as (

     select * from {{ ref('stg_stations_status_marseille') }}

),

final as (
    select
        station_fr_id,
        station_id,
        station_name,
        lat,
        lon,
        cast(null as string) as station_address,
        'Marseille' as station_city,
        district as station_district,
        -- boolean indicating if the station is currently working for renting and returning bikes
        (is_installed = true and is_renting = true and is_returning = true) as is_active,
        total_docks_count
    from 
        marseille_stations
    -- here we filter on the last loaded row
    qualify row_number() over(partition by station_id order by GCS_loaded_at desc) =1
)

select 
    * 
from 
    final