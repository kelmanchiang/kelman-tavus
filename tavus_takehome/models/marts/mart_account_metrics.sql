with lifetime_metrics as (
    select
        billing_account_id,
        account_age,
        seat_quantity,
        total_user_count,
        -- ready_user_count,
        -- demo_booked_user_count,
        -- invited_user_count,
        total_conversation_count,
        total_conversation_length_minutes,
        engaged_conversation_count,
        engaged_conversation_length_minutes,
        total_user_count / seat_quantity as seat_utilization_percentage
    from {{ ref('int_billing_accounts_metrics_lifetime') }}
)

, l30d_60d_90d_metrics as (
    select
        billing_account_id,
        user_signups_30d,
        user_signups_60d,
        user_signups_90d,
        -- demo_booked_user_signups_30d,
        -- invited_user_signups_30d,
        conversation_count_30d,
        conversation_length_minutes_30d,
        engaged_conversation_count_30d,
        engaged_conversation_length_minutes_30d,
        conversation_count_60d,
        conversation_length_minutes_60d,
        engaged_conversation_count_60d,
        engaged_conversation_length_minutes_60d,
        conversation_count_90d,
        conversation_length_minutes_90d,
        engaged_conversation_count_90d,
        engaged_conversation_length_minutes_90d
    from {{ ref('int_billing_accounts_metrics_L30_60_90D') }}
)

select 
    lifetime_metrics.*,
    l30d_60d_90d_metrics.*
from lifetime_metrics
left join l30d_60d_90d_metrics
    on lifetime_metrics.billing_account_id = l30d_60d_90d_metrics.billing_account_id