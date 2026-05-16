-- models/staging/stg_market_index.sql
with source as (
    select * from silver.market_index
),

cleaned as (
    select
        ticker,
        index_name,
        cast(trade_date as date)             as trade_date,
        round(cast(open  as double), 4)      as open_price,
        round(cast(high  as double), 4)      as high_price,
        round(cast(low   as double), 4)      as low_price,
        round(cast(close as double), 4)      as close_price,
        cast(volume as bigint)               as volume,
        extracted_at

    from source
    where trade_date is not null
)

select * from cleaned