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
        md5('bordeaux' || cast(gid as string)) as station_fr_id,
        lon,
        lat,
        insee as insee_code,
        commune as city,
        cast(gid as string) as station_id,
        nom as station_name,
        etat as state,
        nbplaces as available_docks_count,
        nbvelos as bikes_count,
        nbelec as e_bikes_count,
        nbclassiq as m_bikes_count,
        code_commune as city_code,
        mdate as last_reported_at,
        GCS_loaded_at
    from {{source('bordeaux_source', 'raw_view_bordeaux')}}
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