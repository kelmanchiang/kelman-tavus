{{ config(materialized='table') }}

with accounts as (
    select
        billing_account_id,
        date_diff('day', created_at, current_date()) as account_age,
        seat_quantity
    from {{ ref('stg_billing_accounts') }}
    where not is_internal
    and status = 'active'
)

, users_by_account as (
    select
        billing_account_id_dummy as billing_account_id,
        count(distinct case when date_diff('day', created_at, current_date()) <= 30 then user_id end) as user_signups_30d,
        count(distinct case when date_diff('day', created_at, current_date()) <= 60 then user_id end) as user_signups_60d,
        count(distinct case when date_diff('day', created_at, current_date()) <= 90 then user_id end) as user_signups_90d

        -- no meaningful data for these metrics
        -- count(distinct case when date_diff('day', created_at, current_date()) <= 30 and is_demo_booked = true then user_id end) as demo_booked_user_signups_30d,
        -- count(distinct case when date_diff('day', created_at, current_date()) <= 30 and invited_by is not null then user_id end) as invited_user_signups_30d
    from {{ ref('stg_users') }}
    group by 1
)

, user_conversations as (
    select
        u.billing_account_id_dummy as billing_account_id,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 30 then conversation_uuid end) as conversation_count_30d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 30 then conversation_length_minutes end) as conversation_length_minutes_30d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 30 and conversation_length_minutes >= 1 then conversation_uuid end) as engaged_conversation_count_30d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 30 and conversation_length_minutes >= 1 then conversation_length_minutes end) as engaged_conversation_length_minutes_30d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 30 then c.owner_id end) as user_count_30d,
        conversation_length_minutes_30d / user_count_30d as average_conversation_length_minutes_30d,

        count(distinct case when date_diff('day', c.created_at, current_date()) <= 60 then conversation_uuid end) as conversation_count_60d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 60 then conversation_length_minutes end) as conversation_length_minutes_60d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 60 and conversation_length_minutes >= 1 then conversation_uuid end) as engaged_conversation_count_60d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 60 and conversation_length_minutes >= 1 then conversation_length_minutes end) as engaged_conversation_length_minutes_60d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 60 then c.owner_id end) as user_count_60d,
        conversation_length_minutes_60d / user_count_60d as average_conversation_length_minutes_60d,


        count(distinct case when date_diff('day', c.created_at, current_date()) <= 90 then conversation_uuid end) as conversation_count_90d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 90 then conversation_length_minutes end) as conversation_length_minutes_90d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 90 and conversation_length_minutes >= 1 then conversation_uuid end) as engaged_conversation_count_90d,
        sum(case when date_diff('day', c.created_at, current_date()) <= 90 and conversation_length_minutes >= 1 then conversation_length_minutes end) as engaged_conversation_length_minutes_90d,
        count(distinct case when date_diff('day', c.created_at, current_date()) <= 90 then c.owner_id end) as user_count_90d,
        conversation_length_minutes_90d / user_count_90d as average_conversation_length_minutes_90d,

    from {{ ref('stg_conversation') }} c
    left join {{ ref('stg_users') }} u
        on u.user_id = c.owner_id
    where c.is_deleted = false
    group by 1
)

select
    a.billing_account_id,
    a.account_age,
    a.seat_quantity,
    coalesce(u.user_signups_30d, 0) as user_signups_30d,
    coalesce(u.user_signups_60d, 0) as user_signups_60d,
    coalesce(u.user_signups_90d, 0) as user_signups_90d,

    coalesce(uc.conversation_count_30d, 0) as conversation_count_30d,
    coalesce(uc.conversation_length_minutes_30d, 0) as conversation_length_minutes_30d,
    coalesce(uc.engaged_conversation_count_30d, 0) as engaged_conversation_count_30d,
    coalesce(uc.engaged_conversation_length_minutes_30d, 0) as engaged_conversation_length_minutes_30d,
    coalesce(uc.average_conversation_length_minutes_30d, 0) as average_conversation_length_minutes_30d,

    coalesce(uc.conversation_count_60d, 0) as conversation_count_60d,
    coalesce(uc.conversation_length_minutes_60d, 0) as conversation_length_minutes_60d,
    coalesce(uc.engaged_conversation_count_60d, 0) as engaged_conversation_count_60d,
    coalesce(uc.engaged_conversation_length_minutes_60d, 0) as engaged_conversation_length_minutes_60d,
    coalesce(uc.average_conversation_length_minutes_60d, 0) as average_conversation_length_minutes_60d,

    coalesce(uc.conversation_count_90d, 0) as conversation_count_90d,
    coalesce(uc.conversation_length_minutes_90d, 0) as conversation_length_minutes_90d,
    coalesce(uc.engaged_conversation_count_90d, 0) as engaged_conversation_count_90d,
    coalesce(uc.engaged_conversation_length_minutes_90d, 0) as engaged_conversation_length_minutes_90d,
    coalesce(uc.average_conversation_length_minutes_90d, 0) as average_conversation_length_minutes_90d,
    
from accounts a
left join users_by_account u
    on a.billing_account_id = u.billing_account_id
left join user_conversations uc
    on a.billing_account_id = uc.billing_account_id


