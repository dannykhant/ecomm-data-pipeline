with source as (
    select * from {{ source('gold', 'sales_analytics')}}
)

select
    cast(order_date as date) as order_date,
    sum(item_price) as total_revenue,
    count(distinct order_id) as total_orders
from source
where order_status <> 'cancelled'
group by
    cast(order_date as date)
order by
    cast(order_date as date)
