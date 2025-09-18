# ExecReScanAgg

## Location
[src/backend/executor/nodeAgg.c:4364-4510](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L4364-L4510)

## Overview
ExecReScanAgg resets an aggregate node to its initial state for rescanning, handling both hash-based and sort-based aggregate strategies.

## Definition
```c
void ExecReScanAgg(AggState *node)
```

## Detailed Description
This function performs a comprehensive reset of an aggregate execution node to enable rescanning from the beginning. It handles different aggregate strategies (AGG_HASHED, AGG_MIXED, and sort-based) with optimized logic. For hash aggregates, it can either reset the hash table iterator (if no parameters changed and no spilling occurred) or rebuild the entire hash table. For sort-based aggregates, it resets per-group state and reinitializes to phase 1. The function also manages tuplesort cleanup, expression context reset, and proper handling of grouped tuple state.

## Parameters / Member Variables
- `node`: Pointer to the AggState structure containing the aggregate execution state to be reset

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState
  - [bms_overlap](../b/bms_overlap.md)
  - ResetTupleHashIterator
  - [select_current_set](../s/select_current_set.md)
  - tuplesort_end
  - ReScanExprContext
  - [heap_freetuple](../h/heap_freetuple.md)
  - ExecClearTuple
  - MemSet
  - [hashagg_reset_spill_state](../h/hashagg_reset_spill_state.md)
  - [build_hash_tables](../b/build_hash_tables.md)
  - [hashagg_recompile_expressions](../h/hashagg_recompile_expressions.md)
  - [initialize_phase](../i/initialize_phase.md)
  - [ExecReScan](ExecReScan.md)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (src/backend/executor/execAmi.c:273)

## Notes and Other Information
- Optimizes hash aggregate rescans by avoiding hash table rebuilds when parameters haven't changed and no spilling occurred
- Properly handles multiple grouping sets by resetting each set's context and state
- Manages memory efficiently by cleaning up tuplesorts and resetting spill state
- Distinguishes between different aggregate strategies (hashed vs. sort-based) for appropriate cleanup
- Part of PostgreSQL's executor rescan protocol, allowing plan nodes to be executed multiple times
- Critical for proper functioning of nested loop joins and other operators that require multiple scans of their inputs