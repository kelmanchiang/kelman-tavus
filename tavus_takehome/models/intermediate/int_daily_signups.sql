{{ config(materialized='table') }}

-- Daily Signups Model
-- This model calculates the number of unique users who signed up each day

with daily_signups as (
    select 
        date(created_at) as signup_date,
        count(distinct user_id) as daily_total_signups,
        count(distinct case when is_demo_booked = true then user_id end) as daily_demo_booked_signups,
        count(distinct case when is_invite_flow = true then user_id end) as daily_invite_flow_signups,
        count(distinct case when is_mic_available = true then user_id end) as daily_mic_available_signups,
        count(distinct case when invited_by is not null then user_id end) as daily_invited_signups -- can use for referral analysis
    from {{ ref('stg_users') }}
    where status = 'ready' 
    group by 1
),


-- Join with date spine to ensure we have all dates, even those with 0 signups
final as (
    select 
        ds.date_key,
        coalesce(ds.daily_total_signups, 0) as daily_total_signups,
        coalesce(ds.daily_demo_booked_signups, 0) as daily_demo_booked_signups,
        coalesce(ds.daily_invite_flow_signups, 0) as daily_invite_flow_signups,
        coalesce(ds.daily_mic_available_signups, 0) as daily_mic_available_signups,
        coalesce(ds.daily_invited_signups, 0) as daily_invited_signups
    from {{ ref('stg_date_spine') }} ds
    left join daily_signups ds
        on date(ds.date_key) = date(ds.signup_date)
    where ds.date_key <= current_date()  -- Only include dates up to today
    -- filter out dates before the first signup
    and ds.date_key >= (select min(date(created_at)) from {{ ref('stg_users') }})
)

select * from final 
order by date_key
