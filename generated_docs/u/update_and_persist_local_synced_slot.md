# update_and_persist_local_synced_slot

## Location
[src/backend/replication/logical/slotsync.c:545-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L545-L608)

## Overview
Updates local synchronized replication slot with remote slot data and persists it to disk if the slot has reached a consistent state and caught up with the primary server.

## Definition

```c
static bool
update_and_persist_local_synced_slot(RemoteSlot *remote_slot, Oid remote_dbid)
```
## Detailed Description
This function is responsible for the final stage of replication slot synchronization in PostgreSQL's logical replication. It first calls  to update the local slot with remote slot information, then performs critical validation checks before persisting the slot data.

The function implements two key safety checks:
1. **Catch-up validation**: Ensures the remote slot has caught up with the locally reserved position to prevent data consistency issues
2. **Consistent snapshot validation**: Verifies that a consistent snapshot can be built from the restart LSN to avoid potential data loss during WAL decoding

If both conditions are met, the slot is persisted using  and marked as sync-ready (RS_PERSISTENT state).

## Parameters / Member Variables
- `*remote_slot`: Pointer to RemoteSlot structure containing the remote replication slot data to synchronize
- `remote_dbid`: Object identifier (Oid) of the remote database associated with the slot
## Dependencies
- Functions called/Symbols referenced:
  -  - Updates local slot with remote data and performs validation
  -  - Persists the replication slot data to disk
  -  - Structure representing remote replication slot data
  -  - Structure representing local replication slot
- Called from:
  -  - Main slot synchronization function (called at lines 702 and 772)

## Notes and Other Information
- Returns  if slot is successfully marked as RS_PERSISTENT (sync-ready),  otherwise
- The function will not persist slots that haven't caught up or lack consistent snapshots, preventing data loss scenarios
- Logs important status messages including sync-ready notifications and consistency warnings
- Part of PostgreSQL's logical replication slot synchronization mechanism between primary and standby servers
- The slot persistence only occurs after all safety validations pass, ensuring data integrity

## Simplified Source

```c
static bool update_and_persist_local_synced_slot(RemoteSlot *remote_slot, Oid remote_dbid)
{
    ReplicationSlot *slot = MyReplicationSlot;
    bool found_consistent_snapshot = false;
    bool remote_slot_precedes = false;

    // Update local slot with remote slot data
    update_local_synced_slot(remote_slot, remote_dbid,
                            &found_consistent_snapshot,
                            &remote_slot_precedes);

    // Check if remote server has caught up to local position
    if (remote_slot_precedes)
    {
        // Remote slot hasn't caught up yet - keep slot for next cycle
        return false;
    }

    // Check if slot can reach consistent point from restart_lsn
    if (!found_consistent_snapshot)
    {
        ereport(LOG,
                errmsg("could not synchronize replication slot \"%s\"",
                       remote_slot->name),
                errdetail("Synchronization could lead to data loss, because the "
                         "standby could not build a consistent snapshot to decode "
                         "WALs at LSN %X/%X.", LSN_FORMAT_ARGS(slot->data.restart_lsn)));
        return false;
    }

    // All checks passed - persist the slot
    ReplicationSlotPersist();

    ereport(LOG,
            errmsg("newly created replication slot \"%s\" is sync-ready now",
                   remote_slot->name));

    return true;
}
```