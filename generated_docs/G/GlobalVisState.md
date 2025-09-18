# GlobalVisState

## Location
[src/backend/storage/ipc/procarray.c:167-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L167-L178)

## Overview
GlobalVisState is a structure that maintains transaction visibility boundaries to efficiently determine whether deleted tuples can be safely removed without violating MVCC semantics.

## Definition


## Detailed Description
GlobalVisState implements an optimization for MVCC tuple visibility testing by maintaining two transaction ID boundaries instead of computing precise visibility information on every check. This approach significantly improves performance, particularly in vacuum and pruning operations, by avoiding the overhead of repeatedly checking the global transaction state.

The structure works by establishing a range of uncertainty between two boundaries. Transactions outside this range can be quickly classified as either definitely visible or definitely removable, while transactions within the range require more expensive precise computation only when necessary.

PostgreSQL maintains four different instances of this structure optimized for different relation types: GlobalVisSharedRels (for shared catalog tables), GlobalVisCatalogRels (for database-specific catalog tables), GlobalVisDataRels (for regular user tables), and GlobalVisTempRels (for temporary tables). Each has different visibility requirements based on the scope of access to those relations.

## Parameters / Member Variables
- : Transaction IDs greater than or equal to this value are considered to be running by some backend, meaning tuples deleted by these transactions must be preserved
- : Transaction IDs less than this value are not considered to be running by any backend, meaning tuples deleted by these transactions can be safely removed

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId

- Called from (representative examples):
  - [GlobalVisTestFor](GlobalVisTestFor.md)
  - [GlobalVisTestShouldUpdate](GlobalVisTestShouldUpdate.md)
  - [GlobalVisTestIsRemovableFullXid](GlobalVisTestIsRemovableFullXid.md)
  - [GlobalVisTestIsRemovableXid](GlobalVisTestIsRemovableXid.md)
  - [HeapTupleIsSurelyDead](../H/HeapTupleIsSurelyDead.md)
  - [heap_page_prune_opt](../h/heap_page_prune_opt.md)
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md)

## Notes and Other Information
- The boundaries use FullTransactionId instead of TransactionId to prevent wraparound issues during long-running operations
- When testing XIDs between maybe_needed and definitely_needed, the boundaries can be recomputed using ComputeXidHorizons() for more accurate results
- Rate limiting prevents excessive recomputation of boundaries in short succession via GlobalVisTestShouldUpdate()
- Each relation type has its own instance optimized for different visibility scopes (shared relations, catalog relations, data relations, temporary relations)
- This optimization is critical for vacuum performance as it avoids expensive snapshot checks for every tuple visibility decision
- The structure is designed to be conservative: it may preserve tuples longer than strictly necessary but will never incorrectly remove visible tuples