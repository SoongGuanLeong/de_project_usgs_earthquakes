{{ config(materialized='table', schema='gold') }}

with source as (
        {# Generate a continuous sequence of dates #}
        select * from UNNEST(GENERATE_DATE_ARRAY('1990-01-01', '2024-12-31', INTERVAL 1 DAY)) AS date_col
  ),
  renamed as (
        select
        -- Primary Key: YYYYMMDD as an integer
        CAST(FORMAT_DATE('%Y%m%d', date_col) AS integer) AS date_key,

        date_col AS full_date,
        EXTRACT(DAY FROM date_col) AS date_day,
        EXTRACT(MONTH FROM date_col) AS date_month,
        FORMAT_DATE('%B', date_col) AS month_name, -- Full month name (e.g., 'July')
        EXTRACT(YEAR FROM date_col) AS date_year,

        -- Day of Week Attributes
        EXTRACT(DAYOFWEEK FROM date_col) AS day_of_week, -- 1=Sunday, 2=Monday, ..., 7=Saturday
        FORMAT_DATE('%A', date_col) AS day_name, -- Full day name (e.g., 'Monday')

        -- Other Date Attributes
        EXTRACT(DAYOFYEAR FROM date_col) AS day_of_year,
        EXTRACT(WEEK FROM date_col) AS week_of_year, -- Week of the year (1-53)
        EXTRACT(QUARTER FROM date_col) AS quarter_of_year,

        -- Flags
        CASE
            WHEN EXTRACT(DAYOFWEEK FROM date_col) IN (1, 7) THEN TRUE -- 1=Sunday, 7=Saturday
            ELSE FALSE
        END AS is_weekend,
        cast('{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as timestamp) as dwh_creation_datetime
        from source
  )
  select * from renamed
  order by full_date