# heapam_tuple_delete

## Location
[src/backend/access/heap/heapam_handler.c:301-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L301-L314)

## Overview
Provides the heap table access method interface for deleting tuples, serving as a wrapper around the core `heap_delete` function with additional considerations for index tuple cleanup.

## Definition
```c
static TM_Result heapam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
```

## Detailed Description
This function implements the tuple deletion interface for PostgreSQL's heap table access method. It serves as a thin wrapper around `heap_delete`, maintaining the abstraction layer between the table access method API and the specific heap implementation.

The function includes commentary noting that index tuple deletion is typically handled during vacuum operations, but acknowledges that if the storage layer cleans dead tuples automatically, index tuple deletion should be coordinated at that time. Currently, it delegates all deletion logic to the core `heap_delete` function.

## Parameters / Member Variables
- `relation`: The heap relation from which to delete the tuple
- `tid`: ItemPointer (TID) identifying the specific tuple to delete
- `cid`: CommandId for the current command, used for visibility and concurrency control
- `snapshot`: Snapshot for visibility checks during deletion
- `crosscheck`: Additional snapshot for cross-transaction visibility validation
- `wait`: Boolean indicating whether to wait if the tuple is locked by another transaction
- `tmfd`: TM_FailureData structure to receive detailed failure information if deletion fails
- `changingPart`: Boolean indicating whether this deletion is part of a partition change operation

## Dependencies
- Functions called/Symbols referenced:
  - [heap_delete](heap_delete.md)
  - CommandId (type)
  - TM_FailureData (type)
  - TM_Result (type)
- Called from (representative examples):
  - Used through table access method interface (no direct callers found in indexed code)

## Notes and Other Information
- This is a static function within heapam_handler.c, part of the heap table access method implementation
- Returns TM_Result indicating success, failure, or specific conditions (e.g., tuple already deleted, tuple locked)
- The function includes a design note about index tuple cleanup being deferred to vacuum operations
- Part of PostgreSQL's pluggable table access method architecture, providing heap-specific deletion semantics
- The `changingPart` parameter is used for partition-wise operations where tuple deletion is part of moving data between partitions