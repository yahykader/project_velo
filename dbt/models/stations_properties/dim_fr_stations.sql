with 

fr_stations as (
    select 'paris' as service, * from {{ ref('dim_paris_stations') }}
    union all
    select 'lille' as service, * from {{ ref('dim_lille_stations') }}
    union all
    select 'marseille' as service, * from {{ ref('dim_marseille_stations') }}
    union all
    select 'bordeaux' as service, * from {{ ref('dim_bordeaux_stations') }}
    union all
    select 'lyon' as service, * from {{ ref('dim_lyon_stations') }}
),


final as (
    select
        *
    from fr_stations
)

select 
    * 
from 
    final