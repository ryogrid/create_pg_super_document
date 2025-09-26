# table_tuple_complete_speculative

## Location
[src/include/access/tableam.h:1436-1457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1436-L1457)

## Overview
This function completes a speculative insertion started in the same transaction by either confirming the tuple as fully inserted or removing it entirely based on the success outcome.

## Definition
```c
static inline void
table_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
                                 uint32 specToken, bool succeeded)
```

## Detailed Description
The `table_tuple_complete_speculative` function serves as the completion mechanism for speculative tuple insertions in PostgreSQL's INSERT .. ON CONFLICT implementation. It must be called to finalize any speculative insertion started with `table_tuple_insert_speculative()`.

The function operates on two possible outcomes:

**Success Path (succeeded = true)**: The speculative tuple is converted into a regular, fully committed tuple. This happens when no conflicts are detected during the INSERT .. ON CONFLICT operation, and the tuple can be safely inserted according to all constraints.

**Failure Path (succeeded = false)**: The speculative tuple is completely removed as if it never existed. This occurs when conflicts are detected (such as unique constraint violations) and the INSERT .. ON CONFLICT operation determines that the tuple should not be inserted.

The `specToken` parameter must match the token used in the corresponding `table_tuple_insert_speculative()` call, ensuring that the completion operation acts on the correct speculative insertion. This token-based approach allows the system to safely handle multiple concurrent speculative operations within the same transaction.

The completion of a speculative insertion releases any locks or waiting states that other transactions may have entered while waiting for the speculative insertion's resolution.

## Parameters / Member Variables
- `rel`: The relation (table) containing the speculative tuple
- `slot`: TupleTableSlot containing the speculative tuple to be completed
- `specToken`: Unique token identifying the speculative insertion operation (must match the one used in table_tuple_insert_speculative)
- `succeeded`: Boolean indicating whether to commit (true) or abort (false) the speculative insertion

## Dependencies
- Functions called/Symbols referenced:
  - `rel->rd_tableam->tuple_complete_speculative` (access method-specific implementation)
- Called from (representative examples):
  - `[ExecInsert](../E/ExecInsert.md)` (src/backend/executor/nodeModifyTable.c:1132)

## Notes and Other Information
- Essential completion function for PostgreSQL's INSERT .. ON CONFLICT (UPSERT) mechanism
- Must be called exactly once for every `table_tuple_insert_speculative()` call
- The specToken parameter provides safety against completing the wrong speculative operation
- Resolves any waiting states that other concurrent transactions may have entered
- Part of the table access method abstraction supporting pluggable storage engines
- Critical for maintaining ACID properties during conflict resolution in UPSERT operations
- The succeeded parameter determines whether the operation results in an insert or a no-op
- Failure to call this function after a speculative insertion can leave the system in an inconsistent state