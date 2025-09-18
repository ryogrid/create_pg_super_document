# ExecFindRowMark

## Location
src/backend/executor/execMain.c: 2379 - 2401

## Overview
Retrieves the ExecRowMark structure associated with a given range table index, providing access to row locking information for a specific relation in the query.

## Definition
ExecRowMark *ExecFindRowMark(EState *estate, Index rti, bool missing_ok)

## Detailed Description
ExecFindRowMark searches for the ExecRowMark structure corresponding to a specific range table index (rti) within the execution state. The ExecRowMark structure contains information about row locking requirements for a particular relation in the query. The function performs bounds checking to ensure the range table index is valid and within the allocated array size. If the requested ExecRowMark is not found, the function's behavior depends on the missing_ok parameter - it either returns NULL or throws an error.

## Parameters / Member Variables
- `estate`: Execution state containing the es_rowmarks array and range table size information
- `rti`: Range table index (1-based) identifying the specific relation whose ExecRowMark is requested
- `missing_ok`: Boolean flag controlling error handling when the ExecRowMark is not found (true = return NULL, false = throw error)

## Dependencies
- Functions called/Symbols referenced:
  - ExecRowMark (structure type)
  - elog (for error reporting)
- Called from (representative examples):
  - ExecInitLockRows
  - ExecInitModifyTable

## Notes and Other Information
This function is part of PostgreSQL's row locking infrastructure, used during query execution to manage concurrent access to rows. The range table index is 1-based in PostgreSQL's range table system, but the es_rowmarks array is 0-based, hence the `rti - 1` indexing. The function includes safety checks to prevent array bounds violations when accessing the es_rowmarks array.