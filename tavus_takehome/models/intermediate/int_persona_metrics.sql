{{ config(materialized='table') }}

with persona_metrics as (
    select
        persona_id,
        length(context) as context_length,
        length(system_prompt) as system_prompt_length,
        -- len(json_keys(layers)) as layers_key_count,
        date_diff('day', created_at, updated_at) as days_since_last_update,
        date_diff('day', created_at, current_date()) as days_since_creation
    from {{ ref('stg_persona') }}
    where is_deleted = false
    and not is_draft
)

select * from persona_metrics