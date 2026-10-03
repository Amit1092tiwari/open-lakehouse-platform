{{
    config(
        materialized='table',
        unique_key='customer_hk',
        tags=['marts', 'core']
    )
}}

with customers as (
    select * from {{ ref('stg_ecommerce__customers') }}
),

orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customer_metrics as (
    select
        customer_id,
        count(order_id) as total_lifetime_orders,
        sum(order_amount) as lifetime_spend,
        sum(net_realized_revenue) as lifetime_realized_revenue,
        min(created_at_utc) as first_order_timestamp,
        max(created_at_utc) as most_recent_order_timestamp
    from orders
    group by customer_id
),

final_dim as (
    select
        md5(cast(c.customer_id as varchar)) as customer_hk,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email_address,
        c.country_code,
        c.signed_up_at_utc,
        coalesce(m.total_lifetime_orders, 0) as total_lifetime_orders,
        coalesce(m.lifetime_spend, 0.00) as lifetime_spend,
        coalesce(m.lifetime_realized_revenue, 0.00) as lifetime_realized_revenue,
        m.first_order_timestamp,
        m.most_recent_order_timestamp,
        case
            when coalesce(m.lifetime_spend, 0.00) >= 1000.00 then 'TIER_PLATINUM'
            when coalesce(m.lifetime_spend, 0.00) >= 300.00 then 'TIER_GOLD'
            else 'TIER_STANDARD'
        end as customer_loyalty_segment,
        current_timestamp() as dbt_loaded_at
    from customers c
    left join customer_metrics m
        on c.customer_id = m.customer_id
)

select * from final_dim
