# ExecReScanSetOp

## Location
src/backend/executor/nodeSetOp.c: 594 - 649

## Overview
ExecReScanSetOp resets a SetOp execution node to restart processing, handling both hashed and non-hashed set operation strategies while preserving optimization opportunities when parameters haven't changed.

## Definition
```c
void ExecReScanSetOp(SetOpState *node)
```

## Detailed Description
ExecReScanSetOp prepares a SetOp execution node for re-execution by resetting its internal state and data structures. The function handles two distinct execution strategies: hashed and non-hashed set operations. For hashed operations, it includes optimizations to avoid unnecessary work - if the hash table hasn't been built yet or if subplan parameters haven't changed, it can take shortcuts like simply resetting the hash iterator rather than rebuilding the entire table. The function clears the result tuple slot, resets counters, frees any cached first tuple of groups, and resets or rebuilds hashtables as needed. It also recursively rescans the outer subplan unless parameter changes indicate it will be rescanned automatically.

## Parameters / Member Variables
- `node`: A pointer to the SetOpState structure representing the set operation execution state that needs to be reset for re-execution

## Dependencies
- Functions called/Symbols referenced:
  - outerPlanState (macro to access outer plan state)
  - ExecClearTuple (clears the result tuple slot)
  - ResetTupleHashIterator (resets hash table iterator for re-iteration)
  - [heap_freetuple](../h/heap_freetuple.md) (frees cached tuple memory)
  - [MemoryContextReset](../M/MemoryContextReset.md) (resets hashtable memory context)
  - ResetTupleHashTable (clears and reinitializes hash table)
  - [ExecReScan](ExecReScan.md) (recursively rescans the outer subplan)
- Called from (representative examples):
  - [ExecReScan](ExecReScan.md) (from src/backend/executor/execAmi.c:289)

## Notes and Other Information
- Handles both SETOP_HASHED and non-hashed execution strategies differently
- Includes performance optimizations to avoid unnecessary hash table rebuilds when subplan parameters haven't changed
- Uses the table_filled flag to track whether the hash table has been populated
- Follows PostgreSQL's parameter change propagation mechanism through chgParam
- The function is part of the executor's rescan framework for set operations like UNION, INTERSECT, and EXCEPT
- Declared in src/include/executor/nodeSetOp.h and defined in src/backend/executor/nodeSetOp.c:594-649