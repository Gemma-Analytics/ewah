
-- Use the `ref` function to select from other models

select id, timestamp_sub(dt, interval 2 day) as dt
from {{ ref('my_first_dbt_model') }}
where id = 1
