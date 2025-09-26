# CheckPointRelationMap

## Location
[src/backend/utils/cache/relmapper.c:611-624](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L611-L624)

## Overview
Ensures that any relation map updates logged before a checkpoint are securely flushed to disk during checkpoint processing.

## Definition

```c
void
CheckPointRelationMap(void)
```
## Detailed Description
The CheckPointRelationMap function is called during PostgreSQL's checkpoint process to ensure data durability for relation mapping changes. It implements a simple but effective synchronization mechanism to guarantee that any relation map updates that were WAL-logged before the start of the checkpoint are securely flushed to disk.

The function uses a straightforward approach: it acquires and immediately releases the RelationMappingLock in shared mode. This seemingly simple operation has important implications:

1. **Synchronization**: It waits for any currently running map update operations to complete, since those operations hold RelationMappingLock in exclusive mode
2. **Flush Guarantee**: Any completed map update will have called write_relmap_file(), which performs an fsync operation before releasing the RelationMappingLock
3. **Checkpoint Consistency**: This ensures that map changes visible at checkpoint time are durably persisted and won't need to be replayed during recovery

The design prioritizes correctness and simplicity over performance, as relation map updates are relatively infrequent operations and checkpoint consistency is critical for database integrity.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md) (acquiring RelationMappingLock in LW_SHARED mode)
  - [LWLockRelease](../L/LWLockRelease.md) (releasing RelationMappingLock)
  - LW_SHARED (lock mode constant)
- Synchronizes with:
  - [write_relmap_file](../w/write_relmap_file.md) (ensures any ongoing map file writes complete with fsync)
  - [perform_relmap_update](../p/perform_relmap_update.md) (waits for any ongoing map updates to finish)
- Called from (representative examples):
  - [CheckPointGuts](CheckPointGuts.md) (in src/backend/access/transam/xlog.c)

## Notes and Other Information
- This function is part of PostgreSQL's checkpoint infrastructure that ensures crash recovery consistency
- The shared lock acquisition pattern ensures serialization with exclusive map update operations without blocking concurrent reads
- The function relies on write_relmap_file()'s guarantee that it performs fsync before releasing RelationMappingLock
- Though simple, this design is sufficient because relation mapping updates are infrequent and the checkpoint process can afford the brief synchronization delay
- The function is called during the checkpoint process alongside other critical system state persistence operations
- This mechanism ensures that any relation mapping changes that are visible at checkpoint time will survive a crash and won't require replay from WAL