-- Cancelled orders are omitted so revenue and average order value stay on orders that were not voided.
with orders as (

    select * from {{ ref('stg_orders') }}
    where status != 'cancelled'

)

select
    order_date,
    count(*) as order_count,
    cast(sum(line_amount) as decimal(12, 2)) as total_revenue,
    cast(sum(line_amount) / count(*) as decimal(12, 2)) as avg_order_value
from orders
group by order_date
