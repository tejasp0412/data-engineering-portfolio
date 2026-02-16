{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_products') }}
),

cleaned as (
    select product_id,

        --Clean Product Category
        coalesce(upper(trim(product_category_name)), 'UNKNOWN') as product_category_name,

        --Dimensions
        coalesce(product_weight_g, 0) as product_weight_grams,
        coalesce(product_length_cm, 0) as product_length_cm,
        coalesce(product_height_cm, 0) as product_height_cm,
        coalesce(product_width_cm, 0) as product_width_cm,

        --Metadata
        current_timestamp as transformed_at

    from source
)

select * from cleaned