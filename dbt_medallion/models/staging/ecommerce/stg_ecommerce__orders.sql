with source as (
    select * from {{ source('ecommerce', 'raw_orders') }}
),

renamed as (
    select
        cast(order_id as integer) as order_id,
        cast(customer_id as integer) as customer_id,
        cast(amount as numeric(12, 2)) as order_amount,
        trim(lower(cast(status as varchar))) as order_status,
        cast(created_at as timestamp_ntz) as created_at_utc,
        cast(coalesce(updated_at, created_at) as timestamp_ntz) as updated_at_utc
    from source
    where order_id is not null
)

select * from renamed
