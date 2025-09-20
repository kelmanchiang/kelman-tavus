{{ config(materialized='table') }}

-- Daily Active Users Model
-- This model calculates the number of unique users who had at least 1 conversation each day
-- An active user is defined as a user who had at least 1 conversation on a given day

with daily_conversations as (
    select 
        date(created_at) as conversation_date,
        owner_id as user_id,
        count(distinct conversation_uuid) as conversation_count,
        sum(conversation_length_minutes) as total_conversation_length_minutes
    from {{ ref('stg_conversation') }}
    where is_deleted = false  -- Exclude deleted conversations
    group by 1, 2
),

daily_active_users as (
    select 
        conversation_date as date_key,
        count(distinct user_id) as daily_active_users,
        count(distinct case when total_conversation_length_minutes>=1 then user_id end) as daily_engaged_users,
        sum(total_conversation_length_minutes) as daily_total_conversation_length_minutes,
        daily_total_conversation_length_minutes / daily_active_users as daily_average_conversation_length_minutes
    from daily_conversations
    group by 1
),

-- Join with date spine to ensure we have all dates, even those with 0 active users
final as (
    select 
        ds.date_key,
        coalesce(dau.daily_active_users, 0) as daily_active_users,
        coalesce(dau.daily_engaged_users, 0) as daily_engaged_users,
        coalesce(dau.daily_total_conversation_length_minutes, 0) as daily_total_conversation_length_minutes,
        coalesce(dau.daily_average_conversation_length_minutes, 0) as daily_average_conversation_length_minutes
    from {{ ref('stg_date_spine') }} ds
    left join daily_active_users dau
        on date(ds.date_key) = date(dau.date_key)
    where ds.date_key <= current_date()  -- Only include dates up to today
     and ds.date_key >= (select min(date(date_key)) from daily_active_users)
)

select * from final
order by date_key
