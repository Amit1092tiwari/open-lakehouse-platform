with orders as (
    select * from {{ ref('stg_ecommerce__orders') }}
),

customers as (
    select * from {{ ref('stg_ecommerce__customers') }}
),

deduplicated_orders as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at_utc desc
        ) as _dedup_rank
    from orders
),

filtered_orders as (
    select * from deduplicated_orders where _dedup_rank = 1
),

enriched as (
    select
        -- Surrogate Hash Key for High-Performance Join Processing
        md5(concat(cast(o.order_id as varchar), '|', cast(o.customer_id as varchar))) as order_hk,
        o.order_id,
        o.customer_id,
        c.email_address as customer_email,
        c.country_code as customer_country,
        o.order_amount,
        o.order_status,
        -- FinOps & Business Rule calculations
        case
            when o.order_status = 'completed' then o.order_amount
            else 0.00
        end as net_realized_revenue,
        case
            when o.order_amount >= 500.00 then 'HIGH_VALUE'
            when o.order_amount >= 100.00 then 'MEDIUM_VALUE'
            else 'STANDARD_VALUE'
        end as order_value_tier,
        o.created_at_utc,
        o.updated_at_utc
    from filtered_orders o
    left join customers c
        on o.customer_id = c.customer_id
)

select * from enriched
