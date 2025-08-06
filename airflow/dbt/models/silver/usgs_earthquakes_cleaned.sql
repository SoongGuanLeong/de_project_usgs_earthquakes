{{ config(materialized='view', schema='silver') }}

with source as (
        select * from {{ source('bronze', 'usgs_earthquakes') }}
  ),
  renamed as (
      select
        {# we verified id == concat(net, code) #}
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("id"), api.Column.translate_type("string")) }})) as id,
        cast({{ adapter.quote("mag") }} as numeric) as mag,
        TRIM({{ dbt.safe_cast(adapter.quote("place"), api.Column.translate_type("string")) }}) as place,
        {# TIMESTAMP_MILLIS bigquery platform specific #}
        TIMESTAMP_MILLIS({{ dbt.safe_cast(adapter.quote("time"), api.Column.translate_type("integer")) }}) as time_utc,
        TIMESTAMP_MILLIS({{ dbt.safe_cast(adapter.quote("updated"), api.Column.translate_type("integer")) }}) as updated_utc,
        cast({{ adapter.quote("tz") }} as numeric) / 60.0 as tz_hrs,
        {{ dbt.safe_cast(adapter.quote("url"), api.Column.translate_type("string")) }} as url,
        {{ dbt.safe_cast(adapter.quote("detail"), api.Column.translate_type("string")) }} as detail,
        {{ dbt.safe_cast(adapter.quote("felt"), api.Column.translate_type("integer")) }} as felt,
        cast({{ adapter.quote("cdi") }} as numeric) as cdi,
        cast({{ adapter.quote("mmi") }} as numeric) as mmi,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("alert"), api.Column.translate_type("string")) }})) as alert,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("status"), api.Column.translate_type("string")) }})) as status,
        cast({{ adapter.quote("tsunami") }} as boolean) as has_noaa_tsunami_link,
        {{ dbt.safe_cast(adapter.quote("sig"), api.Column.translate_type("integer")) }} as sig,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("net"), api.Column.translate_type("string")) }})) as net,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("code"), api.Column.translate_type("string")) }})) as code,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("ids"), api.Column.translate_type("string")) }})) as ids,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("sources"), api.Column.translate_type("string")) }})) as sources,
        TRIM(LOWER({{ dbt.safe_cast(adapter.quote("types"), api.Column.translate_type("string")) }})) as types,
        {{ dbt.safe_cast(adapter.quote("nst"), api.Column.translate_type("integer")) }} as nst,
        cast({{ adapter.quote("dmin") }} as numeric) as dmin,
        ABS(cast({{ adapter.quote("rms") }} as numeric)) as rms,
        cast({{ adapter.quote("gap") }} as numeric) as gap,
        CASE 
          WHEN TRIM(LOWER({{ dbt.safe_cast(adapter.quote("magType"), api.Column.translate_type("string")) }}))='mb_lg' THEN 'mlbg'
          ELSE TRIM(LOWER({{ dbt.safe_cast(adapter.quote("magType"), api.Column.translate_type("string")) }}))
        END AS magType,
        CASE
          WHEN TRIM(LOWER({{ dbt.safe_cast(adapter.quote("event_type"), api.Column.translate_type("string")) }}))='quarry' THEN 'quarry blast'
          ELSE TRIM(LOWER({{ dbt.safe_cast(adapter.quote("event_type"), api.Column.translate_type("string")) }}))
        END AS event_type,
        TRIM({{ dbt.safe_cast(adapter.quote("title"), api.Column.translate_type("string")) }}) as title,
        cast({{ adapter.quote("longitude") }} as numeric) as longitude,
        cast({{ adapter.quote("latitude") }} as numeric) as latitude,
        cast({{ adapter.quote("depth_km") }} as numeric) as depth_km,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime

      from source
      WHERE cast({{ adapter.quote("mag") }} as numeric) >= 2.0
  )
  select * from renamed
    