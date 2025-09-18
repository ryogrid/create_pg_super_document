# systable_recheck_tuple

## Location
src/backend/access/index/genam.c: 564 - 597

## Overview
systable_recheck_tuple is a specialized function that rechecks the visibility of a previously fetched tuple to determine if it would still be visible to a catalog scan started at the current time.

## Definition
```c
bool systable_recheck_tuple(SysScanDesc sysscan, HeapTuple tup)
```

## Detailed Description
This function provides a mechanism to verify whether a tuple that was previously retrieved from a system catalog scan would still be visible if the scan were started fresh at the current moment. This is particularly useful for testing whether an object was deleted while the caller was waiting to acquire a lock on it, helping to detect concurrent modifications to system catalogs.

The function works by obtaining a fresh catalog snapshot and using it to test tuple visibility via table_tuple_satisfies_snapshot(). It operates only with MVCC snapshots and does not handle non-MVCC scan snapshots, as no current caller requires that functionality. The function includes an assertion to verify that the passed tuple matches the most recently fetched tuple from the scan's slot, providing a cross-check that the caller is working with the correct tuple.

## Parameters / Member Variables
- : A SysScanDesc structure containing the scan state, including the heap relation and tuple slot
- : The HeapTuple to recheck for visibility (should match the most recently fetched tuple)

## Dependencies
- Functions called/Symbols referenced:
  - ExecFetchSlotHeapTuple (for tuple validation)
  - GetCatalogSnapshot (to obtain fresh snapshot)
  - RelationGetRelid (to get relation OID)
  - table_tuple_satisfies_snapshot (for visibility testing)
  - HandleConcurrentAbort (concurrent abort handling)
- Called from (representative examples):
  - findDependentObjects
  - shdepDropOwned
  - IndexScanIsValid

## Notes and Other Information
- Returns true if the tuple is still visible, false if it has been deleted or is otherwise not visible
- The tuple parameter serves as a cross-check but is not strictly necessary for the operation
- The function assumes that low-level visibility checking functions do not acquire snapshots themselves
- Specifically designed for use in dependency tracking and object management scenarios where concurrent modifications need to be detected
- Part of PostgreSQL's concurrency control system for maintaining consistency during catalog operations
- Only supports MVCC snapshot visibility checking, not other snapshot types