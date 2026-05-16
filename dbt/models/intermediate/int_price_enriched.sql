-- models/intermediate/int_price_enriched.sql
-- Join price + sentiment + compute derived metrics

with prices as (
    select * from {{ ref('stg_coin_prices') }}
    where not has_null_price
      and not is_invalid_ohlc
),

final as (
    select
        coin_id,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,

        -- Derived metrics
        round(high_price - low_price, 8)                as daily_range,
        round(
            (close_price - open_price) / open_price * 100,
        4)                                              as daily_return_pct,

        round(close_price - lag(close_price) over (
            partition by coin_id order by price_date
        ), 8)                                           as price_change,

        -- Moving averages
        round(avg(close_price) over (
            partition by coin_id
            order by price_date
            rows between 6 preceding and current row
        ), 8)                                           as ma_7d,

        round(avg(close_price) over (
            partition by coin_id
            order by price_date
            rows between 29 preceding and current row
        ), 8)                                           as ma_30d,

        source,
        extracted_at

    from prices
)

select * from final