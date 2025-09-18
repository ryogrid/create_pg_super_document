# tfuncLoadRows

## Location
src/backend/executor/nodeTableFuncscan.c: 435 - 524

## Overview
This static function loads all rows from a TableFunc table builder into a tuplestore by iterating through each row and column to populate tuple values with proper handling of ordinality columns, default expressions, and NOT NULL constraints.

## Definition
```c
static void tfuncLoadRows(TableFuncScanState *tstate, ExprContext *econtext)
```

## Detailed Description
tfuncLoadRows is responsible for the actual data extraction phase of table function execution. It continuously fetches rows from the table builder until no more rows are available, processing each column value according to its type and constraints. The function handles ordinality columns by automatically incrementing a counter, retrieves regular column values through the routine's GetValue method, and applies default expressions when values are null. It also enforces NOT NULL constraints and manages memory efficiently by using per-tuple context that gets reset after each row. All processed tuples are stored in the tuplestore for later retrieval.

## Parameters / Member Variables
- `tstate`: TableFuncScanState pointer containing the scan state, tuplestore, and table function configuration
- `econtext`: ExprContext pointer providing the evaluation context for default expressions

## Dependencies
- Functions called/Symbols referenced:
  - list_head
  - ExecClearTuple
  - ExecEvalExpr
  - bms_is_member
  - lnext
  - tuplestore_putvalues
  - MemoryContextReset
- Called from (representative examples):
  - tfuncFetchRows

## Notes and Other Information
- Implements the core row processing loop for table functions
- Handles special ordinality column processing with automatic incrementing
- Supports default expressions for columns when values are missing
- Enforces NOT NULL constraints with appropriate error reporting
- Uses per-tuple memory context for efficient memory management
- Integrates with CHECK_FOR_INTERRUPTS() for query cancellation support
- Essential component of XMLTABLE and JSON_TABLE data extraction pipeline