# RelationCreateStorage

## Location
[src/backend/catalog/storage.c:121-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L121-L185)

## Overview
RelationCreateStorage creates the physical disk storage for a relation, handling WAL logging and transaction-aware cleanup based on relation persistence characteristics.

## Definition
```c
SMgrRelation RelationCreateStorage(RelFileLocator rlocator, char relpersistence, bool register_delete)
```

## Detailed Description
RelationCreateStorage is responsible for creating the underlying disk file storage for database relations. It only creates the main fork initially, with additional forks created lazily by modules that need them. The function is fully transactional - creation is WAL-logged for permanent relations, and storage will be automatically destroyed if the transaction aborts (unless register_delete is false). The function handles different persistence types: temporary relations use process-specific storage, unlogged relations skip WAL logging, and permanent relations are fully logged and may be queued for post-commit synchronization when WAL is disabled.

## Parameters / Member Variables
- `rlocator`: RelFileLocator structure specifying the tablespace, database, relation OIDs and fork number for the storage location
- `relpersistence`: Character indicating relation persistence (RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED, or RELPERSISTENCE_PERMANENT)
- `register_delete`: Boolean flag controlling whether storage should be deleted on transaction abort (true) or preserved (false)

## Dependencies
- Functions called/Symbols referenced:
  - [smgropen](../s/smgropen.md)
  - [smgrcreate](../s/smgrcreate.md)
  - [log_smgrcreate](../l/log_smgrcreate.md)
  - [AddPendingSync](../A/AddPendingSync.md)
  - ProcNumberForTempRelations
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - XLogIsNeeded
  - [IsInParallelMode](../I/IsInParallelMode.md)
- Called from (representative examples):
  - [heap_create](../h/heap_create.md)
  - [RelationSetNewRelfilenumber](RelationSetNewRelfilenumber.md)
  - [heapam_relation_set_new_filelocator](../h/heapam_relation_set_new_filelocator.md)
  - [index_copy_data](../i/index_copy_data.md)

## Notes and Other Information
- Function asserts that it is not called in parallel mode due to pendingSyncHash updates
- For permanent relations when XLogIsNeeded() returns false, the relation is added to pending sync queue for later fsync
- Temporary relations use process-specific numbering and do not require WAL logging
- When register_delete is true, a PendingRelDelete entry is created in TopMemoryContext to track cleanup on abort
- Only creates the MAIN_FORKNUM initially; other forks (FSM, VM, init) are created on-demand

## Simplified Source

```c
SMgrRelation
RelationCreateStorage(RelFileLocator rlocator, char relpersistence,
                     bool register_delete)
{
    SMgrRelation srel;
    ProcNumber procNumber;
    bool needs_wal;

    Assert(!IsInParallelMode());

    // Determine WAL logging and process numbering based on persistence
    switch (relpersistence) {
        case RELPERSISTENCE_TEMP:
            procNumber = ProcNumberForTempRelations();
            needs_wal = false;
            break;
        case RELPERSISTENCE_UNLOGGED:
            procNumber = INVALID_PROC_NUMBER;
            needs_wal = false;
            break;
        case RELPERSISTENCE_PERMANENT:
            procNumber = INVALID_PROC_NUMBER;
            needs_wal = true;
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c", relpersistence);
    }

    // Create the storage manager relation and main fork
    srel = smgropen(rlocator, procNumber);
    smgrcreate(srel, MAIN_FORKNUM, false);

    // Log creation for permanent relations
    if (needs_wal)
        log_smgrcreate(&srel->smgr_rlocator.locator, MAIN_FORKNUM);

    // Register for deletion on abort if requested
    if (register_delete) {
        PendingRelDelete *pending;

        pending = (PendingRelDelete *)
            MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDelete));
        pending->rlocator = rlocator;
        pending->procNumber = procNumber;
        pending->atCommit = false;  // delete on abort
        pending->nestLevel = GetCurrentTransactionNestLevel();
        pending->next = pendingDeletes;
        pendingDeletes = pending;
    }

    // Add to pending sync queue if permanent relation and WAL not needed
    if (relpersistence == RELPERSISTENCE_PERMANENT && !XLogIsNeeded()) {
        AddPendingSync(&rlocator);
    }

    return srel;
}
```