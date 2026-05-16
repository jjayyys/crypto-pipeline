-- models/intermediate/int_price_enriched.sql
-- Join price + sentiment + compute derived metrics

with prices as (
    select * from {{ ref('stg_coin_prices') }}
    where not has_null_price
      and not is_invalid_ohlc
),

sentiment as (
    select * from {{ ref('stg_sentiment') }}
),

enriched as (
    select
        p.coin_id,
        p.price_date,
        p.open_price,
        p.high_price,
        p.low_price,
        p.close_price,

        -- Derived metrics
        round(p.high_price - p.low_price, 8)                    as daily_range,
        round((p.close_price - p.open_price) / p.open_price * 100, 4)
                                                                 as daily_return_pct,
        round(p.close_price - lag(p.close_price) over (
            partition by p.coin_id order by p.price_date
        ), 8)                                                    as price_change,

        -- 7-day & 30-day moving average
        round(avg(p.close_price) over (
            partition by p.coin_id
            order by p.price_date
            rows between 6 preceding and current row
        ), 8)                                                    as ma_7d,

        round(avg(p.close_price) over (
            partition by p.coin_id
            order by p.price_date
            rows between 29 preceding and current row
        ), 8)                                                    as ma_30d,

        -- Sentiment
        s.fear_greed_value,
        s.classification        as sentiment_classification,
        s.sentiment_bucket,

        p.source,
        p.extracted_at

    from prices p
    left join sentiment s
        on p.price_date = s.sentiment_date
)

select * from enriched