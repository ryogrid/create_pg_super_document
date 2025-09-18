# buildSubPlanHash

## Location
src/backend/executor/nodeSubplan.c: 504 - 674

## Overview
buildSubPlanHash builds hash tables from subplan output for efficient subquery evaluation, creating separate hash tables for non-null and partially-null rows to support proper SQL NULL semantics.

## Definition
```c
static void buildSubPlanHash(SubPlanState *node, ExprContext *econtext)
```

## Detailed Description
buildSubPlanHash constructs in-memory hash tables by scanning the subplan's output and storing tuples for efficient lookup during subquery evaluation. This function is specifically designed for ANY_SUBLINK operations and implements a sophisticated NULL handling strategy.

The function creates up to two separate hash tables:
1. **Main hash table (hashtable)**: Stores tuples with no NULL values for exact matching
2. **Null hash table (hashnulls)**: Stores tuples containing NULL values for partial matching when unknownEqFalse is false

Key operations include:
- Calculating optimal bucket counts based on estimated plan rows
- Resetting existing hash tables or creating new ones
- Scanning the subplan and projecting tuples into the appropriate hash table
- Managing memory contexts to prevent leaks
- Handling parameter passing between subplan output and hash table storage

The separation of null and non-null tuples optimizes performance by allowing exact hash lookups for the common case (no nulls) while still supporting correct three-valued logic for NULL comparisons.

## Parameters / Member Variables
- `node`: SubPlanState containing hash table structures, projection info, and execution state
- `econtext`: ExprContext providing the evaluation context and memory management

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets hash table memory context)
  - [clamp_cardinality_to_long](../c/clamp_cardinality_to_long.md) (converts plan rows estimate to bucket count)
  - BuildTupleHashTableExt (creates new hash tables with specified parameters)
  - ResetTupleHashTable (clears existing hash table contents)
  - [ExecReScan](../E/ExecReScan.md) (resets subplan to beginning)
  - ExecProcNode (fetches tuples from subplan)
  - ExecProject (projects subplan output into hash table format)
  - LookupTupleHashEntry (inserts tuples into hash table)
  - slot_getattr (extracts values from tuple slots)
  - [slotNoNulls](../s/slotNoNulls.md) (checks if tuple contains any nulls)
  - ResetExprContext (cleans up expression evaluation context)
  - ExecClearTuple (clears tuple slot to prevent double-free)
- Called from (representative examples):
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md) (in nodeSubplan.c:118)

## Notes and Other Information
- Only supports ANY_SUBLINK subplan types (enforced by assertion)
- Creates smaller hash table for nulls (1/16 the size of main table, minimum 1 bucket)
- For single-column subplans with nulls, uses only 1 bucket since there can be only one distinct null entry
- Switches to per-query memory context during subplan scanning for proper memory management  
- Handles duplicate elimination automatically through hash table insertion
- Sets havehashrows/havenullrows flags to indicate which tables contain data
- Clears projection slot to prevent double-free issues when subplan context is reset
- The unknownEqFalse flag determines whether null-containing tuples need to be stored
- Uses innerecontext for parameter value extraction to isolate memory usage per tuple