-- models/mart/dim_sentiment.sql

with source as (
    select * from {{ ref('stg_sentiment') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sentiment_date']) }} as sentiment_sk,
        sentiment_date,
        fear_greed_value,
        classification,
        sentiment_bucket,
        extracted_at

    from source
)

select * from final