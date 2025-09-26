# FormIndexDatum

## Location
[src/backend/catalog/index.c:2702-2780](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2702-L2780)

## Overview
FormIndexDatum constructs the values and isnull arrays needed for creating an index tuple from a heap tuple, handling both regular columns and index expressions.

## Definition

```c
void
FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   EState *estate,
			   Datum *values,
			   bool *isnull)
```
## Detailed Description
FormIndexDatum is a core function responsible for extracting and preparing data values from a heap tuple that will be used to construct an index tuple. The function processes each index column, handling three distinct cases: system attributes (negative column numbers), regular table columns, and computed expressions. For regular columns, it directly extracts values from the heap tuple. For index expressions, it evaluates them using the provided execution state. The function manages expression state initialization on first use for performance optimization and ensures proper context setup for expression evaluation. This function prepares the input arrays for index_form_tuple() but doesn't call it directly, allowing index access methods to potentially modify the data before storage.

## Parameters / Member Variables
- : IndexInfo structure containing index metadata and configuration
- : TupleTableSlot containing the heap tuple to extract index values from
- : Executor state required for evaluating index expressions (can be NULL if no expressions)
- : Output array to store the extracted Datum values for each index column
- : Output array to store null indicators corresponding to each index column

## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfo](../I/IndexInfo.md) (structure type)
  - [ExecPrepareExprList](../E/ExecPrepareExprList.md) (function)
  - GetPerTupleExprContext (function)
  - [list_head](../l/list_head.md) (function)
  - [slot_getsysattr](../s/slot_getsysattr.md) (function)
  - [slot_getattr](../s/slot_getattr.md) (function)
  - [ExecEvalExprSwitchContext](../E/ExecEvalExprSwitchContext.md) (function)
  - [lnext](../l/lnext.md) (function)
- Called from (representative examples):
  - [heapam_index_build_range_scan](../h/heapam_index_build_range_scan.md)
  - [CatalogIndexInsert](../C/CatalogIndexInsert.md)
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md)
  - [ExecCheckIndexConstraints](../E/ExecCheckIndexConstraints.md)

## Notes and Other Information
- Expression evaluation state is lazily initialized on first use to avoid overhead when not needed
- The estate parameter must have its ecxt_scantuple pointing to the same slot being processed
- System attributes (like ctid, oid) are handled via slot_getsysattr with negative column numbers
- The function validates that the number of expressions matches expectations and will error on mismatch
- Used extensively throughout the system for index maintenance, constraint checking, and analysis operations
- Does not actually create the index tuple - only prepares the data arrays for the access method