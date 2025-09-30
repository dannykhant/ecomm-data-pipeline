with source as (
    select * from {{ source('gold', 'sales_analytics')}}
)

select
    cast(order_date as date) as order_date,
    product_name,
    category,
    sum(quantity) as qty_sold
from source
where order_status <> 'cancelled'
group by
    cast(order_date as date),
    product_name,
    category
order by
    cast(order_date as date)
