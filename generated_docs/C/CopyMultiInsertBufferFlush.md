# CopyMultiInsertBufferFlush

## Location
src/backend/commands/copyfrom.c: 304 - 477

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
  - ExecARInsertTriggers (trigger execution)
  - pgstat_progress_update_param, PROGRESS_COPY_TUPLES_PROCESSED (progress reporting)
  - ExecClearTuple (slot cleanup)
  - GetPerTupleMemoryContext (memory management)
  - table_multi_insert (bulk insertion for regular tables)
  - ExecInsertIndexTuples (index maintenance)
  - list_free (memory cleanup)
- Called from (representative examples):
  - CopyMultiInsertInfoFlush (at src/backend/commands/copyfrom.c:529)

## Notes and Other Information
The function switches memory contexts to GetPerTupleMemoryContext before calling table_multi_insert to prevent memory leaks. For FDW tables, it suppresses detailed error context information (relname_only mode) to avoid confusion when batch operations fail. The function ensures proper cleanup by clearing all tuple slots regardless of the insertion path taken.