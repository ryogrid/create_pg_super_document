# FunctionScanPerFuncState

## Location
[src/backend/executor/nodeFunctionscan.c:35-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeFunctionscan.c#L35-L43)

## Overview
FunctionScanPerFuncState is a structure that holds runtime data for each function being scanned in PostgreSQL's function scan executor node, managing the state and result data for individual functions during query execution.

## Definition

```c
typedef struct FunctionScanPerFuncState
{
	SetExprState *setexpr;		/* state of the expression being evaluated */
	TupleDesc	tupdesc;		/* desc of the function result type */
	int			colcount;		/* expected number of result columns */
	Tuplestorestate *tstore;	/* holds the function result set */
	int64		rowcount;		/* # of rows in result set, -1 if not known */
	TupleTableSlot *func_slot;	/* function result slot (or NULL) */
} FunctionScanPerFuncState;
```
## Detailed Description
FunctionScanPerFuncState is a per-function state structure used within PostgreSQL's function scan execution framework. It encapsulates all the necessary runtime information needed to execute and manage the results of a single function call during query processing. This structure is particularly important for handling set-returning functions (SRFs) and managing their result sets efficiently.

The structure maintains both the execution state of the function expression and the storage mechanism for its results. It supports functions that return multiple rows by using a tuple store to cache results, and tracks metadata such as the expected column count and total row count when available.

## Parameters / Member Variables
- `*setexpr`: Pointer to SetExprState containing the state of the expression being evaluated, managing the function's execution context
- `tupdesc`: TupleDesc describing the function's result type structure, defining the schema of returned tuples
- `colcount`: Integer representing the expected number of result columns from the function
- `*tstore`: Pointer to Tuplestorestate that holds the complete function result set for set-returning functions
- `rowcount`: 64-bit integer tracking the number of rows in the result set, or -1 if the count is not known
- `*func_slot`: Pointer to TupleTableSlot for holding individual function result tuples, may be NULL
## Dependencies
- Functions called/Symbols referenced:
  - [SetExprState](../S/SetExprState.md)
  - Tuplestorestate
- Called from (representative examples):
  - [FunctionNext](FunctionNext.md)
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md)
  - [ExecEndFunctionScan](../E/ExecEndFunctionScan.md)
  - [ExecReScanFunctionScan](../E/ExecReScanFunctionScan.md)
  - [FunctionScanState](FunctionScanState.md)

## Notes and Other Information
- This structure is defined in src/backend/executor/nodeFunctionscan.c at lines 35-43
- Part of PostgreSQL's executor framework for handling function scans in query execution
- Designed to efficiently handle both scalar functions and set-returning functions (SRFs)
- The rowcount field being -1 indicates that the total number of rows is unknown until the function completes execution
- Used as a component within the larger FunctionScanState structure for managing multiple functions in a single scan operation
- Critical for memory management and result caching in function scan operations