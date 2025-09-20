# FunctionCall2Coll

## Location
[src/backend/utils/fmgr/fmgr.c:1149-1170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1149-L1170)

## Overview
FunctionCall2Coll is a utility function that invokes a previously-looked-up PostgreSQL function with two parameters and an explicit collation setting.

## Definition

```c
Datum
FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2)
```
## Detailed Description
This function is part of PostgreSQL's function manager (fmgr) system that provides a high-level interface for calling database functions. FunctionCall2Coll handles the case where a function needs to be called with exactly two arguments and a specific collation context. The function sets up the necessary function call information structure, populates both arguments, invokes the target function, and performs error checking to ensure the result is not NULL.

The function creates a local FunctionCallInfoData structure with space for 2 arguments, initializes it with the provided function info and collation, sets both argument values and their null indicators to false, then calls the actual function through FunctionCallInvoke.

## Parameters / Member Variables
- : Pointer to FmgrInfo structure containing the previously-looked-up function information
- : OID of the collation to be used during function execution
- : The first Datum argument to pass to the function
- : The second Datum argument to pass to the function

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for creating local FunctionCallInfoData)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - elog (for error reporting)
- Called from (representative examples):
  - [brin_inclusion_add_value](../b/brin_inclusion_add_value.md)
  - [brin_inclusion_consistent](../b/brin_inclusion_consistent.md)
  - [brin_minmax_add_value](../b/brin_minmax_add_value.md)
  - [brin_minmax_consistent](../b/brin_minmax_consistent.md)
  - [_bt_compare](../b/_bt_compare.md)
  - [genericPickSplit](../g/genericPickSplit.md)
  - [gistMakeUnionItVec](../g/gistMakeUnionItVec.md)
  - [array_position_common](../a/array_position_common.md)
  - [range_cmp_bounds](../r/range_cmp_bounds.md)
  - [OidFunctionCall2Coll](../O/OidFunctionCall2Coll.md)

## Notes and Other Information
- This function explicitly checks for NULL results and throws an ERROR if the called function returns NULL
- Part of a family of FunctionCallNColl functions (0-4 parameters) that provide collation-aware function calling interfaces
- The collation parameter allows for locale-sensitive operations in functions that support collation
- Extensively used throughout PostgreSQL for comparison functions, BRIN index operations, B-tree operations, GiST operations, range type operations, and various data type operations
- Critical for operations that require comparing two values with collation awareness
- Located in src/backend/utils/fmgr/fmgr.c:1149-1170