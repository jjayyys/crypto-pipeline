-- models/mart/fact_price_ohlcv.sql
-- Central Fact Table

with enriched as (
    select * from {{ ref('int_price_enriched') }}
),

dim_coin as (
    select coin_sk, coin_id from {{ ref('dim_coin') }}
),

dim_date as (
    select date_id, date from {{ ref('dim_date') }}
),

dim_sentiment as (
    select sentiment_sk, sentiment_date from {{ ref('dim_sentiment') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['e.coin_id', 'e.price_date']) }}
                                        as price_sk,

        -- Foreign Keys
        dc.coin_sk,
        dd.date_id,
        ds.sentiment_sk,

        -- Degenerate Dimensions
        e.source                        as source_system,

        -- Measures
        e.open_price,
        e.high_price,
        e.low_price,
        e.close_price,
        e.daily_range,
        e.daily_return_pct,
        e.price_change,
        e.ma_7d,
        e.ma_30d,
        e.fear_greed_value,

        -- Metadata
        e.extracted_at

    from enriched e
    left join dim_coin      dc on e.coin_id    = dc.coin_id
    left join dim_date      dd on e.price_date = dd.date
    left join dim_sentiment ds on e.price_date = ds.sentiment_date
)

select * from final