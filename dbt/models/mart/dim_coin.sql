-- models/mart/dim_coin.sql
-- SCD Type 1 (overwrite) — ข้อมูล Coin ไม่ค่อยเปลี่ยน

with source as (
    select * from silver.coin_metadata
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['coin_id']) }}  as coin_sk,
        coin_id,
        symbol,
        name,
        categories,
        genesis_date,
        hashing_algorithm,
        current_timestamp   as dbt_updated_at

    from source
    qualify row_number() over (partition by coin_id order by extracted_at desc) = 1
)

select * from final