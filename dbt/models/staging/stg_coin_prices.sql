-- models/staging/stg_coin_prices.sql
with source as (
    -- ระบุ schema ตรงๆ ไม่ผ่าน dbt source()
    select * from silver.ohlcv
),

cleaned as (
    select
        coin_id,
        cast(date as date)              as price_date,
        round(cast(open  as double), 8) as open_price,
        round(cast(high  as double), 8) as high_price,
        round(cast(low   as double), 8) as low_price,
        round(cast(close as double), 8) as close_price,
        source,
        cast(extracted_at as timestamp) as extracted_at,

        case
            when open is null or close is null
            then true else false
        end as has_null_price,

        case
            when high < low
            then true else false
        end as is_invalid_ohlc

    from source
    where coin_id is not null
      and date   is not null
)

select * from cleaned