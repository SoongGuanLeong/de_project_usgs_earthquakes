{{ config(materialized='table', schema='gold') }}

with src as (
    select * from {{ ref('usgs_earthquakes_cleaned') }}
  ),
renamed as (
    select
        src.id AS earthquake_id,
        -- Foreign Keys (FKs) to Dimension Tables
        COALESCE(dd.date_key, -1) AS date_key,
        COALESCE(da.alert_key, CAST(-1 AS STRING)) AS alert_key,
        COALESCE(det.event_type_key, CAST(-1 AS STRING)) AS event_type_key,
        COALESCE(dmm.magnitude_method_key, CAST(-1 AS STRING)) AS magnitude_method_key,
        COALESCE(dn.network_key, CAST(-1 AS STRING)) AS network_key,
        COALESCE(ds.status_key, CAST(-1 AS STRING)) AS status_key,
        -- Measures
        src.mag AS magnitude,
        -- Add the new Magnitude Group column using a CASE statement
        CASE
            WHEN src.mag < 3 THEN '0-2.9 (Minor)'
            WHEN src.mag < 4 THEN '3.0-3.9 (Light)'
            WHEN src.mag < 5 THEN '4.0-4.9 (Moderate)'
            WHEN src.mag < 6 THEN '5.0-5.9 (Strong)'
            WHEN src.mag < 7 THEN '6.0-6.9 (Major)'
            WHEN src.mag < 8 THEN '7.0-7.9 (Great)'
            ELSE '8.0+ (Mega)'
        END AS magnitude_group,
        src.latitude,
        src.longitude,
        src.depth_km,
        src.cdi AS community_decimal_intensity,
        src.mmi AS modified_mercalli_intensity,
        src.has_noaa_tsunami_link,
        src.nst AS number_of_stations,
        src.dmin AS horizontal_distance_to_nearest_station_deg,
        src.rms AS rms_travel_time_residual_sec,
        src.gap AS azimuthal_gap_deg,
        -- Other Descriptive Attributes (from your Silver schema, not dimensionalized in this model)
        src.place,
        src.code,
        src.title,
        src.detail,
        src.url,
        src.felt,
        src.sig AS seismic_significance_score,
        src.ids AS related_event_ids_list,
        src.sources AS related_event_sources_list,
        src.types AS related_event_types_list,
        src.tz_hrs AS timezone_offset_hours,
        -- Original Timestamps (kept for granular analysis if needed)
        src.time_utc,
        -- Add the new hour_of_day column
        EXTRACT(HOUR FROM src.time_utc) AS hour_of_day,
        src.updated_utc,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
    from src
    LEFT JOIN {{ ref('dim_alert')}} da
        ON COALESCE(LOWER(TRIM(src.alert)), 'unknown') = LOWER(TRIM(da.alert_name))
    LEFT JOIN {{ ref('dim_event_type')}} det
        ON COALESCE(LOWER(TRIM(src.event_type)), 'unknown') = LOWER(TRIM(det.event_type_name))
    LEFT JOIN {{ ref('dim_magnitude_method')}} dmm
        ON COALESCE(LOWER(TRIM(src.magType)), 'unknown') = LOWER(TRIM(dmm.magnitude_method_name))
    LEFT JOIN {{ ref('dim_network')}} dn
        ON COALESCE(LOWER(TRIM(src.net)), 'unknown') = LOWER(TRIM(dn.network_name))
    LEFT JOIN {{ ref('dim_status')}} ds
        ON COALESCE(LOWER(TRIM(src.status)), 'unknown') = LOWER(TRIM(ds.status_name))
    LEFT JOIN {{ ref('dim_date')}} dd
        ON CAST(FORMAT_DATE('%Y%m%d', CAST(src.time_utc AS DATE)) AS INT64) = dd.date_key
)
select * from renamed