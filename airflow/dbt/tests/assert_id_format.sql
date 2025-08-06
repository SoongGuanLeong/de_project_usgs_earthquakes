/*
This test selects rows where the 'id' column does not match the concatenation of 'net' and 'code' columns. If this query returns any rows, the test fails.
*/

with source as (
        select * from {{ ref('usgs_earthquakes_cleaned') }}
  )

select id, net, code
from source
where id != concat(net, code)