# IndexOnlyNext

## Location
[src/backend/executor/nodeIndexonlyscan.c:61-267](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeIndexonlyscan.c#L61-L267)

## Overview
Retrieves the next tuple from an index-only scan operation, attempting to avoid heap access when possible by utilizing the visibility map to determine tuple visibility.

## Definition

```c
static TupleTableSlot *
IndexOnlyNext(IndexOnlyScanState *node)
```
## Detailed Description
The IndexOnlyNext function is the core tuple retrieval mechanism for index-only scans in PostgreSQL. It implements an optimization where data can be returned directly from the index without accessing the heap table, provided that all tuples on the relevant heap page are visible to all transactions (as indicated by the visibility map).

The function first initializes or reuses an index scan descriptor, then enters a loop to fetch TuDs from the index. For each TID, it checks the visibility map to determine if a heap access is necessary. If the visibility map indicates that all tuples on the page are visible, the function can return data directly from the index. Otherwise, it performs a heap fetch to verify tuple visibility.

The function also handles lossy index scans by rechecking index qualifiers when necessary, and maintains proper predicate locking for serializable isolation levels.

## Parameters / Member Variables
- : IndexOnlyScanState containing scan state information including scan descriptors, relation information, scan keys, and slot references

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionCombine: Combines plan and execution scan directions
  - [index_beginscan](../i/index_beginscan.md): Initiates a new index scan operation
  - [index_rescan](../i/index_rescan.md): Restarts an index scan with new parameters  
  - [index_getnext_tid](../i/index_getnext_tid.md): Retrieves the next TID from the index scan
  - VM_ALL_VISIBLE: Checks if all tuples on a heap page are visible
  - [index_fetch_heap](../i/index_fetch_heap.md): Fetches the actual heap tuple for visibility checking
  - [StoreIndexTuple](../S/StoreIndexTuple.md): Stores index tuple data into the scan slot
  - [ExecQualAndReset](../E/ExecQualAndReset.md): Rechecks index qualifiers for lossy scans
  - [PredicateLockPage](../P/PredicateLockPage.md): Acquires predicate locks for serializable isolation
- Called from (representative examples):
  - [ExecIndexOnlyScan](../E/ExecIndexOnlyScan.md): Main execution function for index-only scan nodes

## Notes and Other Information
- The function implements a sophisticated visibility checking mechanism using the visibility map to avoid unnecessary heap accesses
- Includes detailed memory ordering considerations to handle concurrent inserts and deletes safely
- Only supports MVCC snapshots and will error on non-MVCC snapshot types
- Does not support rechecking ORDER BY distances for lossy scans
- Maintains buffer pins on heap pages across calls for potential reuse
- Returns an empty slot when the scan is exhausted