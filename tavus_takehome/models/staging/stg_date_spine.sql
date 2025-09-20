-- date spine table
-- This model creates a continuous date spine for date-level joins
-- Generates dates from 2020-01-01 to 2030-12-31 (adjust range as needed)

with date_spine as (
    select 
        generate_series as date_key,
        year(date_key) as year,
        month(date_key) as month,
        day(date_key) as day,
        dayofweek(date_key) as day_of_week,
        dayofyear(date_key) as day_of_year,
        week(date_key) as week_of_year,
        quarter(date_key) as quarter,
        case 
            when dayofweek(date_key) in (1, 7) then true 
            else false 
        end as is_weekend,
        case 
            when dayofweek(date_key) between 2 and 6 then true 
            else false 
        end as is_weekday
    from
        generate_series(
            '2020-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )
)

select * from date_spine