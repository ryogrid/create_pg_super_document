# table_tuple_insert_speculative

## Location
[src/include/access/tableam.h:1422-1435](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1422-L1435)

## Overview
This function performs a speculative tuple insertion that can be backed out without aborting the entire transaction, primarily used to implement INSERT .. ON CONFLICT functionality by providing short-duration value locks.

## Definition
```c
static inline void
table_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
                               CommandId cid, int options,
                               struct BulkInsertStateData *bistate,
                               uint32 specToken)
```

## Detailed Description
The `table_tuple_insert_speculative` function enables speculative tuple insertion, a key mechanism for implementing PostgreSQL's INSERT .. ON CONFLICT (UPSERT) functionality. Unlike regular insertions, speculative insertions create tuples that can be rolled back without aborting the entire transaction.

Speculative insertions behave as "value locks" of short duration, allowing other sessions to detect conflicts and wait for the speculative insertion to be either confirmed (converted to a regular tuple) or aborted (as if it never existed). This mechanism is essential for handling concurrent INSERT .. ON CONFLICT operations correctly.

The process requires two phases:
1. **Speculative insertion**: This function inserts the tuple in a speculative state
2. **Completion**: The insertion must be finalized using `table_tuple_complete_speculative()` with a success/failure indication

The `specToken` parameter serves as a unique identifier for the speculative insertion, allowing the system to track and manage the speculative state throughout the operation.

During the speculative phase, other transactions can detect the existence of the speculative tuple and wait for its resolution, enabling proper conflict detection and resolution for unique constraint violations.

## Parameters / Member Variables
- `rel`: The relation (table) into which the tuple will be speculatively inserted
- `slot`: TupleTableSlot containing the tuple data to be inserted
- `cid`: CommandId for the current command, used for visibility and MVCC purposes
- `options`: Bitmask of insertion options (TABLE_INSERT_* flags)
- `bistate`: BulkInsertStateData structure for bulk insertion optimization (can be NULL)
- `specToken`: Unique token identifying this speculative insertion operation

## Dependencies
- Functions called/Symbols referenced:
  - `rel->rd_tableam->tuple_insert_speculative` (access method-specific implementation)
  - `CommandId` (type)
  - `[BulkInsertStateData](../B/BulkInsertStateData.md)` (structure type)
- Called from (representative examples):
  - `[ExecInsert](../E/ExecInsert.md)` (src/backend/executor/nodeModifyTable.c:1118)

## Notes and Other Information
- Essential component of PostgreSQL's INSERT .. ON CONFLICT (UPSERT) implementation
- Must always be paired with a call to `table_tuple_complete_speculative()` to finalize the operation
- Provides atomicity for conflict detection without requiring full transaction rollback
- The specToken parameter is crucial for tracking and managing speculative state
- Other transactions can wait on speculative insertions, enabling proper concurrency control
- Speculative tuples are visible to the inserting transaction but handled specially by other transactions
- Part of the table access method abstraction supporting pluggable storage engines
- Enables efficient implementation of UPSERT operations in high-concurrency scenarios

## Simplified Source

```c
static inline void table_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
                                                  CommandId cid, int options,
                                                  struct BulkInsertStateData *bistate,
                                                  uint32 specToken)
{
    // Delegate to storage-specific implementation
    rel->rd_tableam->tuple_insert_speculative(rel, slot, cid, options,
                                             bistate, specToken);
}
```