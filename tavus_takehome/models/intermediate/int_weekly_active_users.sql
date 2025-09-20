{{ config(materialized='table') }}

-- Weekly Active Users Model
-- This model calculates the number of unique users who had at least 1 conversation in each week
-- An active user is defined as a user who had at least 1 conversation in a given week

with weekly_conversations as (
    select 
        date_trunc('week', created_at) as conversation_week,
        owner_id as user_id,
        count(distinct conversation_uuid) as conversation_count,
        sum(conversation_length_minutes) as total_conversation_length_minutes
    from {{ ref('stg_conversation') }}
    where is_deleted = false  -- Exclude deleted conversations
    group by 1, 2
),

weekly_active_users as (
    select 
        conversation_week as week_key,
        count(distinct user_id) as weekly_active_users,
        count(distinct case when total_conversation_length_minutes >= 1 then user_id end) as weekly_engaged_users
    from weekly_conversations
    group by 1
),

-- Join with date spine to ensure we have all weeks, even those with 0 active users
final as (
    select 
        ds.week_key as week_start_date,
        coalesce(wau.weekly_active_users, 0) as weekly_active_users,
        coalesce(wau.weekly_engaged_users, 0) as weekly_engaged_users
    from (select distinct date_trunc('week', created_at) as week_key from {{ ref('stg_conversation') }}) ds
    left join weekly_active_users wau
        on ds.week_key = wau.week_key
    where ds.week_key <= current_date()  -- Only include dates up to today
      and ds.week_key >= (select min(week_key) from weekly_active_users)
)

select * from final
order by week_start_date
