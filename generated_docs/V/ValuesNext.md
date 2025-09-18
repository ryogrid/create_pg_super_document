# ValuesNext

## Location
[src/backend/executor/nodeValuesscan.c:47-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeValuesscan.c#L47-L179)

## Overview
ValuesNext is a static function that serves as the core workhorse for ExecValuesScan, responsible for iterating through VALUES clause rows and materializing the next tuple in a VALUES scan operation.

## Definition
```c
static TupleTableSlot *ValuesNext(ValuesScanState *node)
```

## Detailed Description
ValuesNext implements the core scanning logic for VALUES clauses in PostgreSQL. It handles bidirectional scanning (forward and backward) through an array of expression lists representing VALUES rows. For each row, it evaluates the expressions in the current row context, builds a virtual tuple with the computed values, and returns it in a TupleTableSlot.

The function employs a careful memory management strategy by using the per-tuple memory context for expression evaluation and optionally building expression state on-demand for rows that don't have pre-built expression states. This approach helps control memory growth when processing long VALUES lists.

The function also handles expanded datums by forcing them to read-only state to prevent issues with multiple references in the execution plan.

## Parameters / Member Variables
- `node`: ValuesScanState containing the scan state, current position index, expression lists for each VALUES row, and associated execution context

## Dependencies
- Functions called/Symbols referenced:
  - ScanDirectionIsForward
  - ExecClearTuple
  - ReScanExprContext
  - [ExecInitExprList](../E/ExecInitExprList.md)
  - ExecEvalExpr
  - MakeExpandedObjectReadOnly
  - [ExecStoreVirtualTuple](../E/ExecStoreVirtualTuple.md)
- Called from:
  - [ExecValuesScan](../E/ExecValuesScan.md)

## Notes and Other Information
- The function maintains a curr_idx to track the current position in the VALUES array
- Expression evaluation is performed in the per-tuple memory context to facilitate cleanup between rows
- For efficiency, expression states may be built on-demand rather than at initialization for certain rows
- The function properly handles both forward and backward scanning directions
- Virtual tuples are used to avoid unnecessary tuple materialization overhead