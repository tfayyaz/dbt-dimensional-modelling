with d_address as (
    select * from {{ ref("dim_address") }}
)

select city_name, count(*) as city_count
from d_address
group by city_name