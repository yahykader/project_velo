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
        md5('lille' || cast(id as string)) as station_fr_id,
        cast(id as string) as station_id,
        nom as station_name,
        adresse as address,
        commune as city,
        etat as station_state,
        nb_places_dispo as available_docks_count,
        nb_velos_dispo as m_bikes_count,
        etat_connexion as connexion_state,
        x as lat,
        y as lon,
        date_modification as last_reported_at,
        GCS_loaded_at
    from {{source('lille_source', 'raw_view_lille')}}
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