with source as (

    select * from {{ source('raw', 'orders') }}

),

cleaned as (

    select
        trim(order_id) as order_id,
        trim(customer_id) as customer_id,
        trim(product_id) as product_id,
        cast(order_date as date) as order_date,
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(12, 2)) as unit_price,
        lower(trim(status)) as status
    from source

)

select
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    status,
    cast(quantity * unit_price as decimal(12, 2)) as line_amount
from cleaned
