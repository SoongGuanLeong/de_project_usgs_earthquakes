{{ config(materialized='table', schema='gold') }}

with source as (
    select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
  distinct_alerts AS (
    SELECT DISTINCT alert
    FROM source
    WHERE
        alert IS NOT NULL      -- Exclude any null alerts from the dimension
        AND TRIM(alert) != ''  -- Exclude empty strings if any exist
  ),
  renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['alert']) }} AS alert_key,
        alert AS alert_name,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
    from distinct_alerts
  )
select * from renamed
union all
SELECT
CAST(-1 AS STRING) AS alert_key,
'unknown' AS alert_name,
cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
order by alert_name
