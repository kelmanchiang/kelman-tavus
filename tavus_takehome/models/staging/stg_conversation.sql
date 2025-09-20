{{ config(materialized='view') }}

with source_data as (

    select
        id as conversation_id,
        uuid as conversation_uuid,
        status,
        replica_uuid,
        persona_uuid,
        context_override,
        owner_id,
        webhook_url,
        is_deleted,
        created_at,
        updated_at,
        datediff('minute', created_at, updated_at) as conversation_length_minutes

    from {{ ref('conversation') }}

)

select * from source_data
