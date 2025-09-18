# tfuncFetchRows

## Location
src/backend/executor/nodeTableFuncscan.c: 268 - 339

## Overview
This static function reads rows from a TableFunc producer by initializing the table function, evaluating the document expression, and loading all resulting rows into a tuplestore.

## Definition
```c
static void tfuncFetchRows(TableFuncScanState *tstate, ExprContext *econtext)
```

## Detailed Description
tfuncFetchRows is responsible for fetching all rows from a table function (such as XMLTABLE or JSON_TABLE) and storing them in a tuplestore for subsequent retrieval. The function operates in multiple phases: first it creates a tuplestore in per-query memory, then switches to per-table context for the actual data processing. It evaluates the document expression and if the result is not NULL, initializes the table function and loads all rows. The function includes proper exception handling to ensure cleanup of opaque state if errors occur during processing.

## Parameters / Member Variables
- `tstate`: TableFuncScanState pointer containing the scan state and configuration
- `econtext`: ExprContext pointer for expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - tuplestore_begin_heap
  - ExecEvalExpr
  - [tfuncInitialize](tfuncInitialize.md)
  - [tfuncLoadRows](tfuncLoadRows.md)
  - PG_TRY/PG_CATCH/PG_RE_THROW/PG_END_TRY
  - [MemoryContextReset](../M/MemoryContextReset.md)
- Called from (representative examples):
  - [TableFuncNext](../T/TableFuncNext.md)

## Notes and Other Information
- Uses per-table memory context to manage potentially large memory allocations
- Implements proper exception handling with PG_TRY/PG_CATCH blocks
- Handles NULL document expressions by returning empty results
- Essential for XMLTABLE and JSON_TABLE functionality in lateral joins
- Manages opaque state lifecycle including cleanup on errors
- Initializes ordinality counter for row numbering