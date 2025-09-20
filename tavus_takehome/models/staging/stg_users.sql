{{ config(materialized='view') }}

with source_data as (

    select
        "ID" as user_id,
        "UID" as uid,
        "FIRST_NAME" as first_name,
        "LAST_NAME" as last_name,
        "EMAIL" as email,
        "MOBILE" as mobile,
        "STATUS" as status,
        "CREATED_AT" as raw_created_at,
        strptime(raw_created_at, '%m/%d/%Y %H:%M:%S') as created_at,
        "UPDATED_AT" as raw_updated_at,
        strptime(raw_updated_at, '%m/%d/%Y %H:%M:%S') as updated_at,
        "ROLE" as role,
        "INVITED_BY" as invited_by,
        "BILLING_ACCOUNT_ID" as billing_account_id,
        "MARKETING_COMMUNICATION" as marketing_communication,
        "IS_DEMO_BOOKED" as is_demo_booked,
        "SIGNUP_TYPE" as signup_type,
        "IS_MIC_AVAILABLE" as is_mic_available,
        "STEPS" as steps,
        "UUID" as uuid,
        "TRAINING_PERMISSION_VIDEO" as training_permission_video,
        "TERMS_AND_CONDITION" as terms_and_condition,
        "IS_INVITE_FLOW" as is_invite_flow,
        ("BILLING_ACCOUNT_ID" % 10) as billing_account_id_dummy

    from {{ ref('users') }}

)

select * from source_data
