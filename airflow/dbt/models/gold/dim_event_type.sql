{{ config(materialized='table', schema='gold') }}

with source as (
        select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
  distinct_event_types as (
        select distinct event_type
        from source
  ),
  renamed as (
        select {{ dbt_utils.generate_surrogate_key(['event_type']) }} AS event_type_key,
        event_type AS event_type_name,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
        from distinct_event_types
  )
select * from renamed
union all
SELECT
CAST(-1 AS STRING) AS event_type_key,
'unknown' AS event_type_name,
cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
order by event_type_name