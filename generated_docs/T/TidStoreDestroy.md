# TidStoreDestroy

## Location
src/backend/access/common/tidstore.c: 328 - 355

## Overview
Destroys a TidStore and frees all associated memory, including the underlying radix tree and memory contexts.

## Definition
```c
void TidStoreDestroy(TidStore *ts)
```

## Detailed Description
TidStoreDestroy completely destroys a TidStore object and releases all memory associated with it. The function handles both shared and local TidStores appropriately. For shared TidStores, it calls shared_ts_free() to destroy the shared radix tree and dsa_detach() to detach from the DSA area. For local TidStores, it calls local_ts_free() to destroy the local radix tree. Finally, it deletes the radix tree memory context and frees the TidStore structure itself. This function should only be called by the backend that created the TidStore, and other backends should use TidStoreDetach() instead.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to destroy

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - shared_ts_free (radix tree generated function)
  - dsa_detach 
  - local_ts_free (radix tree generated function)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [dead_items_reset](../d/dead_items_reset.md) (in vacuumlazy.c)
  - [parallel_vacuum_end](../p/parallel_vacuum_end.md) (in vacuumparallel.c)
  - [parallel_vacuum_reset_dead_items](../p/parallel_vacuum_reset_dead_items.md) (in vacuumparallel.c)
  - [test_destroy](../t/test_destroy.md) (in test_tidstore.c)

## Notes and Other Information
- The caller must ensure no other backend will attempt to access the TidStore before calling this function
- Other backends must explicitly call TidStoreDetach() to free backend-local memory
- The backend calling TidStoreDestroy() must NOT call TidStoreDetach()
- Handles both shared and local TidStore cleanup appropriately
- Frees the radix tree memory context and the TidStore structure itself
- Used primarily in vacuum operations and parallel processing cleanup