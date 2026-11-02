{{
    config(
        materialized='incremental',
        unique_key='order_hk',
        incremental_strategy='merge',
        cluster_by=['order_date', 'customer_id'],
        tags=['marts', 'finance', 'ai_context']
    )
}}

with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
),

final_mart as (
    select
        order_hk,
        order_id,
        customer_id,
        cast(created_at_utc as date) as order_date,
        order_amount,
        net_realized_revenue,
        order_status,
        order_value_tier,
        customer_country,
        created_at_utc,
        updated_at_utc,
        current_timestamp() as dbt_loaded_at
    from enriched_orders
    {% if is_incremental() %}
        where updated_at_utc >= (
            select coalesce(dateadd('day', -3, max(updated_at_utc)), '1970-01-01')
            from {{ this }}
        )
    {% endif %}
)

select * from final_mart
