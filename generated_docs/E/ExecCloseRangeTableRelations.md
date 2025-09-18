# ExecCloseRangeTableRelations

## Location
src/backend/executor/execMain.c: 1576 - 1596

## Overview
ExecCloseRangeTableRelations closes all relations that were opened by ExecGetRangeTableRelation() during query execution, while preserving any locks held on those relations.

## Definition
```c
void ExecCloseRangeTableRelations(EState *estate)
```

## Detailed Description
ExecCloseRangeTableRelations is a straightforward cleanup function that systematically closes all relations stored in the executor's range table relations array. The function iterates through the `es_relations` array up to `es_range_table_size`, closing each non-NULL relation descriptor using `table_close` with NoLock.

This function is specifically designed to close relations that were opened by ExecGetRangeTableRelation() during query execution setup. Importantly, it does not release any locks that might be held on these relations, preserving the locking semantics established during query execution. This design allows for proper resource cleanup while maintaining transaction-level locking consistency.

The function works in coordination with ExecCloseResultRelations to ensure complete cleanup of all opened relations during executor shutdown.

## Parameters / Member Variables
- `estate`: Pointer to the EState containing the range table relations array (`es_relations`) and the size of this array (`es_range_table_size`) that need to be closed

## Dependencies
- Functions called/Symbols referenced:
  - table_close (closes individual relation descriptors with NoLock parameter)
- Called from:
  - ExecEndPlan (main execution cleanup sequence)
  - CopyFrom (COPY command cleanup)
  - ResetPerTupleExprContext (expression context reset cleanup)

## Notes and Other Information
- This function complements ExecCloseResultRelations in the executor cleanup sequence
- Uses NoLock parameter with table_close, indicating that lock management is handled at a higher level
- Simple array iteration ensures all opened range table relations are properly closed
- Critical for preventing relation descriptor leaks in queries that access multiple tables
- The function's design assumes that `es_relations` array slots are either valid relation descriptors or NULL
- Part of the standard PostgreSQL executor resource cleanup protocol
- Lock preservation is important for maintaining proper transaction isolation and consistency