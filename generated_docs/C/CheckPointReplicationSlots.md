# CheckPointReplicationSlots

## Location
[src/backend/replication/slot.c:1835-1891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/slot.c#L1835-L1891)

## Overview
Flushes all replication slots to disk during checkpoint operations, with special handling for logical slots during shutdown to preserve confirmed_flush LSN progress.

## Definition


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