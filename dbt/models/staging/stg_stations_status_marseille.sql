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
        md5('marseille' || cast(station_id as string)) as station_fr_id,
        cast(station_id as string) as station_id,
        nom_division as district,
        name as station_name,
        capacity as total_docks_count,
        num_bikes_available as bikes_count,
        num_bikes_available as m_bikes_count,
        num_docks_available as available_docks_count,
        cast(is_installed as bool) as is_installed,
        cast(is_renting as bool) as is_renting,
        cast(is_returning as bool) as is_returning,
        lon,
        lat,
        last_reported_tr as last_reported_at,
        GCS_loaded_at
    from {{source('marseille_source', 'raw_view_marseille')}}
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