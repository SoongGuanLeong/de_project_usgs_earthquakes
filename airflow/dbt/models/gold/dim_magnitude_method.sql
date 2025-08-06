{{ config(materialized='table', schema='gold') }}

with source as (
    select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
distinct_magtypes AS (
    SELECT DISTINCT magtype
    FROM source
    WHERE
        magType IS NOT NULL      -- Exclude any null magTypes from the dimension
        AND TRIM(magType) != ''  -- Exclude empty strings if any exist
        AND TRIM(LOWER(magType)) != 'unknown'
),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['magType']) }} AS magnitude_method_key,
        magType AS magnitude_method_name,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
    from distinct_magtypes
)
select * from renamed
union all
SELECT
CAST(-1 AS STRING) AS magnitude_method_key,
'unknown' AS magnitude_method_name,
cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
order by magnitude_method_name