# synchronize_one_slot

## Location
[src/backend/replication/logical/slotsync.c:609-790](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/slotsync.c#L609-L790)

## Overview
Synchronizes a single replication slot with remote slot data from the primary server, creating new slots or updating existing ones as needed for PostgreSQL's logical replication.

## Definition

```c
static bool
synchronize_one_slot(RemoteSlot *remote_slot, Oid remote_dbid)
```
## Detailed Description
This function is the core logic for synchronizing individual replication slots in PostgreSQL's slot synchronization mechanism. It handles both creating new synchronized slots and updating existing ones based on data received from the primary server.

The function implements a comprehensive synchronization workflow:

1. **Pre-sync validation**: Verifies that required WAL data has been received and flushed locally before attempting synchronization
2. **Slot existence check**: Searches for existing slots with the same name and handles conflicts
3. **State management**: Manages slot states (temporary, persistent, invalidated) appropriately
4. **Creation path**: For new slots, creates temporary slots with proper metadata and transaction ID management
5. **Update path**: For existing slots, updates metadata and handles invalidation states
6. **Persistence**: Calls helper functions to persist slots once they reach sync-ready state

The function ensures data consistency by validating LSN positions and managing proper locking to prevent race conditions during slot operations.

## Parameters / Member Variables
- `*remote_slot`: Pointer to RemoteSlot structure containing slot data from the primary server to synchronize locally
- `remote_dbid`: Object identifier (Oid) of the remote database associated with the replication slot
## Dependencies
- Functions called/Symbols referenced:
  -  - Gets the latest flushed WAL position on standby
  -  - Searches for existing slot by name
  - / - Slot locking mechanisms
  -  - Creates new replication slots
  -  - Updates and persists temporary slots
  -  - Updates existing persistent slots
  - / - Slot persistence operations
  -  - Reserves WAL for slot restart LSN
  -  - Gets transaction ID for catalog_xmin
  - Various slot state constants (RS_TEMPORARY, RS_INVAL_NONE, etc.)
- Called from:
  -  context (referenced at line 911)

## Notes and Other Information
- Returns  if the local slot was updated,  otherwise
- Creates slots as temporary (RS_TEMPORARY) initially, upgrading to persistent once sync-ready
- Handles invalidated slots by preserving invalidation state and skipping sync operations
- Implements extensive error checking for LSN consistency and slot state validation
- Manages complex locking protocols to prevent race conditions with slot invalidation
- Part of PostgreSQL's logical replication slot synchronization between primary and standby servers
- Ensures WAL availability before synchronization to prevent data loss scenarios

## Simplified Source

```c
static bool synchronize_one_slot(RemoteSlot *remote_slot, Oid remote_dbid)
{
    ReplicationSlot *slot;
    XLogRecPtr latestFlushPtr;
    bool slot_updated = false;

    // Ensure required WAL is available locally before syncing
    latestFlushPtr = GetStandbyFlushRecPtr(NULL);
    if (remote_slot->confirmed_lsn > latestFlushPtr)
    {
        ereport(AmLogicalSlotSyncWorkerProcess() ? LOG : ERROR,
                errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("skipping slot synchronization because the received slot sync"
                       " LSN %X/%X for slot \"%s\" is ahead of the standby position %X/%X",
                       LSN_FORMAT_ARGS(remote_slot->confirmed_lsn),
                       remote_slot->name,
                       LSN_FORMAT_ARGS(latestFlushPtr)));
        return false;
    }

    // Check if slot already exists
    if ((slot = SearchNamedReplicationSlot(remote_slot->name, true)))
    {
        // Verify it's a synced slot, not user-created
        SpinLockAcquire(&slot->mutex);
        bool synced = slot->data.synced;
        SpinLockRelease(&slot->mutex);

        if (!synced)
            ereport(ERROR, /* error for user-created slot conflict */);

        // Acquire slot to prevent invalidation race
        ReplicationSlotAcquire(remote_slot->name, true);

        // Handle invalidation state updates
        if (slot->data.invalidated == RS_INVAL_NONE &&
            remote_slot->invalidated != RS_INVAL_NONE)
        {
            SpinLockAcquire(&slot->mutex);
            slot->data.invalidated = remote_slot->invalidated;
            SpinLockRelease(&slot->mutex);
            ReplicationSlotMarkDirty();
            ReplicationSlotSave();
            slot_updated = true;
        }

        // Skip sync if slot is invalidated
        if (slot->data.invalidated != RS_INVAL_NONE)
        {
            ReplicationSlotRelease();
            return slot_updated;
        }

        // Update slot based on current state
        if (slot->data.persistency == RS_TEMPORARY)
        {
            // Try to make temporary slot sync-ready
            slot_updated = update_and_persist_local_synced_slot(remote_slot, remote_dbid);
        }
        else
        {
            // Update persistent slot
            slot_updated = update_local_synced_slot(remote_slot, remote_dbid, NULL, NULL);
        }
    }
    else
    {
        // Create new slot if remote slot is valid
        if (remote_slot->invalidated != RS_INVAL_NONE)
            return false;

        // Create temporary slot
        ReplicationSlotCreate(remote_slot->name, true, RS_TEMPORARY,
                             remote_slot->two_phase, remote_slot->failover, true);

        slot = MyReplicationSlot;

        // Set slot metadata
        NameData plugin_name;
        namestrcpy(&plugin_name, remote_slot->plugin);
        SpinLockAcquire(&slot->mutex);
        slot->data.database = remote_dbid;
        slot->data.plugin = plugin_name;
        SpinLockRelease(&slot->mutex);

        // Reserve WAL and set transaction IDs
        reserve_wal_for_local_slot(remote_slot->restart_lsn);

        LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
        TransactionId xmin_horizon = GetOldestSafeDecodingTransactionId(true);
        SpinLockAcquire(&slot->mutex);
        slot->effective_catalog_xmin = xmin_horizon;
        slot->data.catalog_xmin = xmin_horizon;
        SpinLockRelease(&slot->mutex);
        ReplicationSlotsComputeRequiredXmin(true);
        LWLockRelease(ProcArrayLock);

        // Try to persist the new slot
        update_and_persist_local_synced_slot(remote_slot, remote_dbid);
        slot_updated = true;
    }

    ReplicationSlotRelease();
    return slot_updated;
}
```