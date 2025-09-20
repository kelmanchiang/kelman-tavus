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
        count(distinct user_id) as total_user_count
        -- count(distinct case when status = 'ready' then user_id end) as ready_user_count,
        -- count(distinct case when is_demo_booked = true then user_id end) as demo_booked_user_count,
        -- count(distinct case when invited_by is not null then user_id end) as invited_user_count
    from {{ ref('stg_users') }}
    group by 1
)

, user_conversations as (
    select
        u.billing_account_id_dummy as billing_account_id,
        count(distinct conversation_uuid) as total_conversation_count,
        sum(conversation_length_minutes) as total_conversation_length_minutes,
        count(distinct case when conversation_length_minutes >= 1 then conversation_uuid end) as engaged_conversation_count,
        sum(case when conversation_length_minutes >= 1 then conversation_length_minutes end) as engaged_conversation_length_minutes,

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
    coalesce(u.total_user_count, 0) as total_user_count,
    -- u.ready_user_count,
    -- u.demo_booked_user_count,
    -- u.invited_user_count,
    coalesce(uc.total_conversation_count, 0) as total_conversation_count,
    coalesce(uc.total_conversation_length_minutes, 0) as total_conversation_length_minutes,
    coalesce(uc.engaged_conversation_count, 0) as engaged_conversation_count,
    coalesce(uc.engaged_conversation_length_minutes, 0) as engaged_conversation_length_minutes,
    case 
        when u.total_user_count is null or u.total_user_count = 0 then 0
        else coalesce(uc.total_conversation_length_minutes, 0) / u.total_user_count
    end as average_conversation_length_minutes
from accounts a
left join users_by_account u
    on a.billing_account_id = u.billing_account_id
left join user_conversations uc
    on a.billing_account_id = uc.billing_account_id
