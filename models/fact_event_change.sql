
{{ config(materialized='table') }}

with data_source as (

    select
    base._id as snapshot_id,
    base.type as operation_type,
    base.version as event_version,
    base.globalid_key as snapshot_group_key,
    cm.id as snapshot_commit_id,
    cm.author as change_author,
    cm.commitdateinstant as snapshot_crated_at,
    gi.entity as entity_class,
    gi.cdoid as event_id,
    st.createdat as event_created_at,
    st.sequencenumber as event_sequence_number,
    st.monitorid as event_monitor_id,
    st.name as event_name,
    st.description as event_description,
    st.ownerid as event_owner_id,
    st.status as event_status,
    st.playbook as event_playbook,
    var.*
             from test.jv_snapshots base
                     inner join test.jv_snapshots_commitmetadata cm
                                 on base._airbyte_jv_snapshots_hashid = cm._airbyte_jv_snapshots_hashid
                     inner join test.jv_snapshots_globalid gi
                                 on base._airbyte_jv_snapshots_hashid = gi._airbyte_jv_snapshots_hashid
                     inner join test.jv_snapshots_state st
                                on base._airbyte_jv_snapshots_hashid = st._airbyte_jv_snapshots_hashid
                     left join test.jv_snapshots_state_variables var
                                 on st._airbyte_state_hashid = var._airbyte_state_hashid
            where gi.entity = 'so.flawless.apigateway.event.EventEntity'

)

select * from data_source
