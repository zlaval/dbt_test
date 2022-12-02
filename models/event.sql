{{ 
    config(
        materialized='view',
    ) 
}} 

select 
    distinct on (id) id,
    version,
    name,
    status,
    ownerId,
    playbook,
    priority,
    createdAt,
    monitorId,
    description,
    sequenceNumber,
    variables
from {{ ref('event_history') }}
order by id, emitted_at desc
