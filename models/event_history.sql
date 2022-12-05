
{{ config(
    materialized='incremental',
    indexes=[
        {'columns': ['emitted_at','ownerId'], 'type': 'btree'}
    ]  
    ) }}


select
    snapshot_id,
    emitted_at,
    operation_type,
    globalId_key,
    commitMetadata ->> 'author' as author,
    (commitMetadata ->> 'commitDateInstant')::timestamp as changed_at,
    globalId ->>'cdoId' as event_id,
    globalId ->>'entity' as entity_class,
    version,
    state ->> 'id' as id,
    state ->> 'name' as name,
    state ->> 'status' as status,
    state ->> 'ownerId' as ownerId,
    state ->> 'playbook' as playbook,
    state ->> 'priority' as priority,
    (state ->> 'createdAt')::timestamp as createdAt,
    state ->> 'monitorId' as monitorId,
    state ->> 'description' as description,
    (state ->> 'sequenceNumber')::int as sequenceNumber,
    state ->> 'variables' as variables,
    changedProperties
    from (
    select
        _airbyte_emitted_at as emitted_at,
        _airbyte_data ->> 'type' as operation_type,
        _airbyte_data ->> '_id' as snapshot_id,
        _airbyte_data ->> 'globalId_key' as globalId_key,
        (_airbyte_data ->> 'version_aibyte_transform')::int as version,
        _airbyte_data -> 'globalId' as globalId,
        _airbyte_data -> 'commitMetadata' as commitMetadata,
        _airbyte_data -> 'state' as state,
        _airbyte_data -> 'changedProperties' as changedProperties
from
    {% if env_var('DBT_ENVIRONMENT','dev') == prod %}
      test.jv_snapshots
    {% else %}
            test._airbyte_raw_jv_snapshots
    {% endif %}
   where _airbyte_data @> '{"globalId": {"entity": "so.flawless.apigateway.event.EventEntity"}}'

{% if is_incremental() %}
   and  _airbyte_emitted_at > (select coalesce(max(emitted_at),now() - interval '5' minute) from public.event_history)
{% endif %}
) data