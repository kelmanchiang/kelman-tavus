{{ config(materialized='view') }}

with source_data as (

    select
        id as billing_account_id,
        company_name,
        external_billing_account_id,
        seat_quantity,
        subscription_type,
        uuid as billing_account_uuid,
        requires_consent,
        subscription_id,
        plan_id,
        status_updated_at,
        status,
        created_at,
        updated_at,
        is_internal,
        scheduled_cancellation_date,
        raw_recording_allowed

    from {{ ref('billing_accounts') }}

)

select * from source_data
