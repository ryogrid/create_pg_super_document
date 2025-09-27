# CheckPointReplicationSlots

## Location
[src/backend/replication/slot.c:1835-1891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1835-L1891)

## Overview
Flushes all replication slots to disk during checkpoint operations, with special handling for logical slots during shutdown to preserve confirmed_flush LSN progress.

## Definition

```c
struct dirent *replication_de;
```
## Detailed Description
This function performs a checkpoint operation for all replication slots by flushing their state to persistent storage. It serves two main purposes:
1. Regular checkpoint flushing of dirty replication slots to ensure data durability
2. Special shutdown handling for logical slots to prevent unnecessary retreat of the confirmed_flush LSN after restart

The function iterates through all replication slots in the shared memory array and saves each active slot to disk. During shutdown checkpoints, it performs additional logic for logical slots: if the confirmed_flush LSN has advanced since the last save, it marks the slot as dirty to force a flush, preventing LSN retreat on restart.

## Parameters
- : Boolean flag indicating whether this is a shutdown checkpoint, which triggers special handling for logical slots to preserve confirmed_flush LSN progress

## Dependencies
- Functions called/Symbols referenced:
  -  (with DEBUG1 level)
  -  (with ReplicationSlotAllocationLock and LW_SHARED)
  - 
  -  (on slot mutex)
  - 
  - 
- Called from:
  -  (src/backend/access/transam/xlog.c:7317)
  -  (src/backend/access/transam/xlog.c:7507)
  -  (src/backend/access/transam/xlog.c:7788)

## Notes and Other Information
- Acquires ReplicationSlotAllocationLock in shared mode to prevent slot creation/deletion during the checkpoint
- Uses ReplicationSlotCtl global structure to access the slot array
- Constructs slot paths using the format "pg_replslot/[slot_name]"
- For logical slots during shutdown, checks if confirmed_flush LSN has advanced since last save to avoid unnecessary LSN retreat
- Error handling is delegated to SaveSlotToPath function with LOG error level
- The function is designed to be non-blocking for slot iteration and acquisition operations

## Simplified Source

```c
// Simplified version of CheckPointReplicationSlots
void CheckPointReplicationSlots(bool is_shutdown) {
    int i;

    // Log the checkpoint operation
    elog(DEBUG1, "performing replication slot checkpoint");

    // Acquire lock to prevent slot creation/deletion during checkpoint
    LWLockAcquire(ReplicationSlotAllocationLock, LW_SHARED);

    // Iterate through all replication slots
    for (i = 0; i < max_replication_slots; i++) {
        ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];
        char path[MAXPGPATH];

        // Skip unused slots
        if (!slot->in_use)
            continue;

        // Build the slot directory path
        sprintf(path, "pg_replslot/%s", NameStr(slot->data.name));

        // Special handling for logical slots during shutdown
        if (is_shutdown && SlotIsLogical(slot)) {
            SpinLockAcquire(&slot->mutex);

            // Mark slot as dirty if confirmed_flush LSN has advanced
            if (slot->data.invalidated == RS_INVAL_NONE &&
                slot->data.confirmed_flush > slot->last_saved_confirmed_flush) {
                slot->just_dirtied = true;
                slot->dirty = true;
            }

            SpinLockRelease(&slot->mutex);
        }

        // Save the slot to disk
        SaveSlotToPath(slot, path, LOG);
    }

    // Release the allocation lock
    LWLockRelease(ReplicationSlotAllocationLock);
}
```

Key simplifications made:
- Consolidated variable declarations for clarity
- Removed detailed comments that repeat the code logic
- Simplified variable names (s -> slot) for better readability
- Focused on the main execution path and core algorithm
- Preserved all essential logic including locking, iteration, and special shutdown handling