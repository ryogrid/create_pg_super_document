# ProcArrayGetReplicationSlotXmin

## Location
[src/backend/storage/ipc/procarray.c:3967-3989](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L3967-L3989)

## Overview
Retrieves the current replication slot xmin limits to determine what data can be safely removed by vacuum and other cleanup operations.

## Definition
```c
void ProcArrayGetReplicationSlotXmin(TransactionId *xmin, TransactionId *catalog_xmin)
```

## Detailed Description
This function reads the current minimum transaction ID limits that are enforced by replication slots. It provides a thread-safe way for vacuum operations and other cleanup processes to query what data must be preserved for replication slot clients. The function returns both regular table data xmin and catalog data xmin limits.

The function uses shared locking to ensure consistent reads of the xmin values while allowing concurrent access from multiple processes that need to check these limits.

## Parameters / Member Variables
- `xmin`: Output parameter that receives the minimum transaction ID for regular table data that must be preserved (can be NULL if not needed)
- `catalog_xmin`: Output parameter that receives the minimum transaction ID for catalog data that must be preserved (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (for process array synchronization with shared lock)
- Called from (representative examples):
  - [logical_begin_heap_rewrite](../l/logical_begin_heap_rewrite.md) (in rewriteheap.c)

## Notes and Other Information
- Uses shared locking (LW_SHARED) to allow concurrent reads without blocking other readers
- Both output parameters are optional (can be NULL) allowing callers to retrieve only the xmin values they need
- Provides the counterpart to ProcArraySetReplicationSlotXmin for reading the limits
- Critical for vacuum operations to determine the oldest data that must be preserved
- Used during table rewrites and other operations that need to understand replication slot requirements

## Simplified Source

```c
void ProcArrayGetReplicationSlotXmin(TransactionId *xmin, TransactionId *catalog_xmin) {
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    // Return regular data xmin limit if requested
    if (xmin != NULL)
        *xmin = procArray->replication_slot_xmin;

    // Return catalog data xmin limit if requested
    if (catalog_xmin != NULL)
        *catalog_xmin = procArray->replication_slot_catalog_xmin;

    LWLockRelease(ProcArrayLock);
}
```