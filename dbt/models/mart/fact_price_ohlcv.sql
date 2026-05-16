-- models/mart/fact_price_ohlcv.sql
with enriched as (
    select * from {{ ref('int_price_enriched') }}
),

dim_coin as (
    select coin_sk, coin_id from {{ ref('dim_coin') }}
),

dim_date as (
    select date_id, date from {{ ref('dim_date') }}
),

final as (
    select
        md5(concat(e.coin_id, '|', cast(e.price_date as varchar)))
                            as price_sk,
        dc.coin_sk,
        dd.date_id,
        e.source            as source_system,
        e.open_price,
        e.high_price,
        e.low_price,
        e.close_price,
        e.daily_range,
        e.daily_return_pct,
        e.price_change,
        e.ma_7d,
        e.ma_30d,
        e.extracted_at

    from enriched e
    left join dim_coin dc on e.coin_id    = dc.coin_id
    left join dim_date dd on e.price_date = dd.date
)

select * from final