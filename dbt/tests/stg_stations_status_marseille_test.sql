{{ config(
    severity='error',
    store_failures=true
) }}

WITH final AS (
    SELECT 
        station_id,
        GCS_loaded_at,
        count(*) as nb_rows
    FROM {{ ref('stg_stations_status_marseille') }}
    GROUP BY 
        1,2
)

SELECT *
FROM final
WHERE nb_rows > 1