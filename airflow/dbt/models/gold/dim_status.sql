{{ config(materialized='table', schema='gold') }}

with source as (
    select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
distinct_status AS (
    SELECT DISTINCT status
    FROM source
    WHERE
        status IS NOT NULL      -- Exclude any null status from the dimension
        AND TRIM(status) != ''  -- Exclude empty strings if any exist
  ),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS status_key,
        status AS status_name,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
    from distinct_status
)
select * from renamed
union all
SELECT
CAST(-1 AS STRING) AS status_key,
'unknown' AS status_name,
cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
order by status_name