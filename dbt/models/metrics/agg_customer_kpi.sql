with source as (
    select * from {{ source('silver', 'dim_customers')}}
)

select
    cast(created_at as date) as onboarding_date,
    count(1) as num_new_customers
from source
group by
    cast(created_at as date)
order by
    cast(created_at as date)
