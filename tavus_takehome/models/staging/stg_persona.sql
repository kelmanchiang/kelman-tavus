{{ config(materialized='view') }}

with source_data as (

    select
        id as persona_id,
        uuid as persona_uuid,
        name as persona_name,
        owner_id,
        system_prompt,
        context,
        layers,
        is_deleted,
        created_at,
        updated_at,
        default_replica_id,
        pipeline_mode,
        objectives_id,
        guardrails_id,
        greeting,
        is_draft,
        session_id,
        public_id

    from {{ ref('persona') }}

)

select * from source_data
