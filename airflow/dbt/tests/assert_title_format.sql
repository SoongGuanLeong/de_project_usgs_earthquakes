/*
This test selects rows where the 'title' column does not match the concatenation of 'mag' and 'place' columns. If this query returns any rows, the test fails.
*/

with source as (
        select * from {{ ref('usgs_earthquakes_cleaned') }}
  )

select title, mag, place
from source
where
  title != concat('M ', FORMAT('%.1f', mag), ' - ', place)
  and title != concat('M ', FORMAT('%.1f', mag), ' ', INITCAP(event_type), ' - ', place)