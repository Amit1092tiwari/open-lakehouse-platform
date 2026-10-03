with source as (
    select * from {{ source('ecommerce', 'raw_customers') }}
),

renamed as (
    select
        cast(customer_id as integer) as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(lower(email)) as email_address,
        trim(country_code) as country_code,
        cast(created_at as timestamp_ntz) as signed_up_at_utc
    from source
    where customer_id is not null
)

select * from renamed
