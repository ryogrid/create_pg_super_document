# CopyMultiInsertBufferFlush

## Location
[src/backend/commands/copyfrom.c:304-477](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L304-L477)

## Overview
Writes the tuples stored in a CopyMultiInsertBuffer out to the target table, handling both regular tables and foreign data wrapper (FDW) tables with different insertion strategies.

## Definition
```c
static inline void CopyMultiInsertBufferFlush(CopyMultiInsertInfo *miinfo,
                                             CopyMultiInsertBuffer *buffer, 
                                             int64 *processed)
```

## Detailed Description
This function is the core implementation for flushing buffered tuples to their destination table. It handles two distinct paths:

1. **Foreign Data Wrapper (FDW) Tables**: Uses batch insert functionality through the FDW's ExecForeignBatchInsert routine, respecting the configured batch size and handling partial inserts.

2. **Regular Tables**: Uses PostgreSQL's table_multi_insert function for efficient bulk insertion, followed by index updates and trigger execution for each inserted tuple.

The function also manages memory contexts, progress reporting, error context information, and cleanup of tuple slots after insertion. For regular tables, it handles index maintenance and AFTER ROW INSERT trigger execution on a per-tuple basis.

## Parameters / Member Variables
- `miinfo`: Pointer to CopyMultiInsertInfo containing overall copy operation state and configuration
- `buffer`: Pointer to CopyMultiInsertBuffer containing the tuples to flush and table-specific information
- `processed`: Pointer to counter tracking total number of tuples processed, updated by this function

## Dependencies
- Functions called/Symbols referenced:
  - CopyFromState, EState, CommandId (state management types)
  - [ExecARInsertTriggers](../E/ExecARInsertTriggers.md) (trigger execution)
  - [pgstat_progress_update_param](../p/pgstat_progress_update_param.md), PROGRESS_COPY_TUPLES_PROCESSED (progress reporting)
  - ExecClearTuple (slot cleanup)
  - GetPerTupleMemoryContext (memory management)
  - table_multi_insert (bulk insertion for regular tables)
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md) (index maintenance)
  - [list_free](../l/list_free.md) (memory cleanup)
- Called from (representative examples):
  - [CopyMultiInsertInfoFlush](CopyMultiInsertInfoFlush.md) (at src/backend/commands/copyfrom.c:529)

## Notes and Other Information
The function switches memory contexts to GetPerTupleMemoryContext before calling table_multi_insert to prevent memory leaks. For FDW tables, it suppresses detailed error context information (relname_only mode) to avoid confusion when batch operations fail. The function ensures proper cleanup by clearing all tuple slots regardless of the insertion path taken.