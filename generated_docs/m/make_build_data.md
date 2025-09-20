# make_build_data

## Location
[src/backend/statistics/extended_stats.c:2452-2617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2452-L2617)

## Overview
Creates and populates a StatsBuildData structure containing evaluated expression values and column data for building extended statistics.

## Definition

```c
static StatsBuildData *
make_build_data(Relation rel, StatExtEntry *stat, int numrows, HeapTuple *rows,
				VacAttrStats **stats, int stattarget)
```
## Detailed Description
This function prepares data needed for building extended statistics by evaluating expressions and extracting column values from a sample of table rows. It creates a comprehensive data structure that holds both regular column values and computed expression results, which are then used to build various types of extended statistics (functional dependencies, N-distinct, MCV lists, etc.).

The function allocates a single memory chunk containing arrays for attribute numbers, statistics metadata, and data values/nulls for both columns and expressions. For regular columns, it extracts values directly from the heap tuples. For expressions, it sets up an executor state and evaluates each expression against every sample row, storing the results in the same format as column data.

The resulting StatsBuildData structure provides a uniform interface for accessing both column and expression data during statistics computation, abstracting away the differences between simple column references and complex expressions.

## Parameters / Member Variables
- : Relation for which statistics are being built
- : StatExtEntry containing information about the extended statistics object (columns, expressions, types)
- : Number of sample rows to process
- : Array of HeapTuple pointers containing the sample data
- : Array of VacAttrStats for the columns being analyzed
- : Statistics target controlling the level of detail in statistics

## Dependencies
- Functions called/Symbols referenced:
  - [bms_num_members](../b/bms_num_members.md), bms_next_member, examine_expression, heap_getattr
  - [CreateExecutorState](../C/CreateExecutorState.md), GetPerTupleExprContext, MakeSingleTupleTableSlot
  - [ExecPrepareExprList](../E/ExecPrepareExprList.md), ResetExprContext, ExecStoreHeapTuple, ExecEvalExpr
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md), FreeExecutorState
- Called from (representative examples):
  - [BuildRelationExtStatistics](../B/BuildRelationExtStatistics.md)

## Notes and Other Information
- Allocates all memory in a single chunk for efficient cleanup
- Uses PostgreSQL's expression evaluation infrastructure for computing expression values
- Handles memory management carefully to avoid leaks during expression evaluation
- The resulting data structure is used by various extended statistics building functions
- Critical for extended statistics that involve expressions, not just simple column combinations
- Expression evaluation is performed in a per-tuple context that is reset for each row to prevent memory accumulation