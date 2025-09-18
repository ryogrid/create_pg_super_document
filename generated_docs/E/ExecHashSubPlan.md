# ExecHashSubPlan

## Location
src/backend/executor/nodeSubplan.c: 101 - 222

## Overview
ExecHashSubPlan executes subselect queries by storing the result in an in-memory hash table and performing hash-based lookups for efficient IN/EXISTS/ANY/ALL subquery evaluation.

## Definition
```c
static Datum ExecHashSubPlan(SubPlanState *node, ExprContext *econtext, bool *isNull)
```

## Detailed Description
ExecHashSubPlan implements hash-based subplan execution for PostgreSQL's executor. It builds and maintains in-memory hash tables to efficiently evaluate subqueries, particularly those used in IN, EXISTS, ANY, and ALL operations. The function handles both exact matches (for non-null values) and partial matches (for nullable comparisons), implementing SQL's three-valued logic.

The function operates in two phases: first building the hash table from subquery results (via buildSubPlanHash), then probing the hash table with left-hand-side expressions. It maintains separate hash tables for regular rows and rows containing nulls, ensuring correct NULL handling according to SQL semantics.

Key features include:
- Efficient hash-based lookup avoiding full subquery re-execution
- Proper three-valued logic handling (TRUE/FALSE/UNKNOWN)
- Separate handling of null and non-null values
- Automatic hash table rebuilding when parameters change

## Parameters / Member Variables
- `node`: SubPlanState containing the execution state, hash tables, and projection information
- `econtext`: ExprContext providing the evaluation context for left-hand-side expressions
- `isNull`: Pointer to boolean flag set to true when the result should be UNKNOWN (NULL in three-valued logic)

## Dependencies
- Functions called/Symbols referenced:
  - [buildSubPlanHash](../b/buildSubPlanHash.md) (builds the hash table from subquery results)
  - ExecProject (projects left-hand-side expressions into a tuple)
  - FindTupleHashEntry (searches for exact matches in hash table)
  - [findPartialMatch](../f/findPartialMatch.md) (searches for partial matches with nulls)
  - ExecClearTuple (clears projected tuple to prevent memory leaks)
  - [slotNoNulls](../s/slotNoNulls.md)/slotAllNulls (check tuple null status)
  - [BoolGetDatum](../B/BoolGetDatum.md) (converts boolean to Datum)
- Called from (representative examples):
  - [ExecSubPlan](ExecSubPlan.md) (in nodeSubplan.c:87)

## Notes and Other Information
- Does not support direct correlation variables (parParam must be NIL)
- Rebuilds hash table when chgParam indicates parameter changes
- Implements SQL's three-valued logic correctly for NULL comparisons
- Uses separate hash tables for rows with and without nulls for efficiency
- Explicitly clears projected tuples to prevent double-free situations in per-tuple contexts
- Returns FALSE for empty subplans, TRUE for exact matches, and UNKNOWN for ambiguous cases
- The combining operators are assumed to never yield NULL when both inputs are non-null