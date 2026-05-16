-- models/staging/stg_sentiment.sql
with source as (
    select * from silver.sentiment
),

cleaned as (
    select
        cast(sentiment_date as date)            as sentiment_date,
        cast(fear_greed_value as integer)       as fear_greed_value,
        trim(classification)                    as classification,

        -- Bucket classification
        case
            when cast(fear_greed_value as integer) between 0  and 24 then 'Extreme Fear'
            when cast(fear_greed_value as integer) between 25 and 44 then 'Fear'
            when cast(fear_greed_value as integer) between 45 and 55 then 'Neutral'
            when cast(fear_greed_value as integer) between 56 and 75 then 'Greed'
            when cast(fear_greed_value as integer) between 76 and 100 then 'Extreme Greed'
            else 'Unknown'
        end as sentiment_bucket,

        extracted_at

    from source
    where sentiment_date is not null
)

select * from cleaned