-- models/mart/dim_date.sql
-- Date dimension generated from price date range

with date_spine as (
    select
        unnest(
            generate_series(
                date '2023-01-01',
                current_date,
                interval '1 day'
            )
        ) as full_date
),

final as (
    select
        cast(strftime(full_date, '%Y%m%d') as integer)  as date_id,
        full_date                                         as date,
        extract('year'  from full_date)                  as year,
        extract('month' from full_date)                  as month,
        extract('day'   from full_date)                  as day,
        extract('quarter' from full_date)                as quarter,
        extract('week'  from full_date)                  as week_of_year,
        strftime(full_date, '%A')                        as day_name,
        strftime(full_date, '%B')                        as month_name,
        case
            when extract('dow' from full_date) in (0, 6)
            then true else false
        end                                              as is_weekend,
        case
            when extract('month' from full_date) in (12, 1, 2) then 'Winter'
            when extract('month' from full_date) in (3, 4, 5)  then 'Spring'
            when extract('month' from full_date) in (6, 7, 8)  then 'Summer'
            else 'Fall'
        end                                              as season

    from date_spine
)

select * from final