# postquel_get_single_result

## Location
[src/backend/executor/functions.c:986-1028](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L986-L1028)

## Overview
Extracts the SQL function's return value from a single result row, handling both scalar functions and individual rows from set-returning functions.

## Definition

```c
static Datum
postquel_get_single_result(TupleTableSlot *slot,
						   FunctionCallInfo fcinfo,
						   SQLFunctionCachePtr fcache,
						   MemoryContext resultcontext)
```
## Detailed Description
postquel_get_single_result is responsible for extracting and properly formatting the return value from a SQL function's execution result. It handles two distinct return types: composite/tuple returns where the entire row is returned as a single Datum, and scalar returns where only the first column value is extracted. The function ensures proper memory management by allocating results in the specified result context rather than the query's temporary context, and performs necessary data copying for pass-by-reference types to ensure the data remains valid after the slot is cleared.

## Parameters / Member Variables
- : TupleTableSlot containing the result row from query execution
- : FunctionCallInfo structure where the isnull flag will be set
- : SQLFunctionCache containing function metadata including return type information
- : MemoryContext where the result should be allocated for proper lifetime management

## Dependencies
- Functions called/Symbols referenced:
  - [ExecFetchSlotHeapTupleDatum](../E/ExecFetchSlotHeapTupleDatum.md)
  - slot_getattr
  - [datumCopy](../d/datumCopy.md)
- Called from (representative examples):
  - [fmgr_sql](../f/fmgr_sql.md) (multiple locations)

## Notes and Other Information
- Switches to resultcontext to ensure proper memory allocation lifetime for return values
- For tuple-returning functions, uses ExecFetchSlotHeapTupleDatum to serialize the entire row
- For scalar functions, extracts the first column value using slot_getattr
- Performs datumCopy for non-null pass-by-reference values to ensure data persistence
- Used both for single-value scalar functions and for processing individual rows in set-returning functions
- Properly handles null values by setting fcinfo->isnull appropriately