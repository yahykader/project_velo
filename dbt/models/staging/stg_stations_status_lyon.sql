{{
 config(
   materialized = 'incremental',
   incremental_strategy = 'insert_overwrite',
   partition_by = {
     'field': 'GCS_loaded_at', 
     'data_type': 'timestamp',
     'granularity': 'day'
   }
 )
}}

with 

final as (
    select
        md5('lyon' || cast(gid as string)) as station_fr_id,
        cast(gid as string) as station_id,
        address,
        address_jcd,
        availability as avaibility,
        availabilitycode as availability_code,
        available_bike_stands as available_docks_count,
        bike_stands as total_docks_count,
        code_insee as insee_code,
        commune as city,
        lat,
        lng as lon,
        bikes as bikes_count,
        electricalBikes as e_bikes_count,
        mechanicalBikes as m_bikes_count,
        last_update as last_reported_at,
        GCS_loaded_at
    from {{source('lyon_source', 'raw_view_lyon')}}
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    -- (uses >= to include records arriving later on the same day as the last run of this model)
    where GCS_loaded_at >= (select coalesce(max(GCS_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select 
    * 
from 
    final