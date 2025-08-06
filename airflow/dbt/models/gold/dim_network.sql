{{ config(materialized='table', schema='gold') }}

with source as (
    select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
distinct_network AS (
    SELECT DISTINCT net
    FROM source
  ),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['net']) }} AS network_key,
        net AS network_name,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
    from distinct_network
)
select * from renamed
union all
SELECT
CAST(-1 AS STRING) AS network_key,
'unknown' AS network_name,
cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
order by network_name