select 'paris' as service, * from {{ ref('mart_paris_stations_availability') }}
union all
select 'marseille' as service,* from {{ ref('mart_marseille_stations_availability') }}
union all
select 'bordeaux' as service,* from {{ ref('mart_bordeaux_stations_availability') }}
union all
select 'lille' as service,* from {{ ref('mart_lille_stations_availability') }}
union all
select 'lyon' as service,* from {{ ref('mart_lyon_stations_availability') }}
