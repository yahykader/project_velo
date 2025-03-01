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
        md5('paris' || cast(station_id as string)) as station_fr_id,
        cast(station_id as string) as station_id,
        mechanical as m_bikes_count,
        ebike as e_bikes_count,
        numDocksAvailable as available_docks_count,
        cast(is_installed as bool) as is_installed,
        cast(is_renting as bool) as is_renting,
        cast(is_returning as bool) as is_returning,
        name,
        lon,
        lat,
        TIMESTAMP_SECONDS(last_reported) as last_reported_at,
        GCS_loaded_at
    from {{source('paris_source', 'raw_view_paris')}}
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