# ExecQualAndReset

## Location
[src/include/executor/executor.h:441-504](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/executor.h#L441-L504)

## Overview
ExecQualAndReset evaluates a qualification expression and immediately resets the per-tuple memory context, combining expression evaluation with memory management for optimal performance in tuple-processing loops.

## Definition

```c
typedef TupleTableSlot *(*ExecScanAccessMtd) (ScanState *node);
```
## Detailed Description
ExecQualAndReset is a convenience function that combines two frequently paired operations: evaluating a qualification expression via ExecQual and resetting the per-tuple memory context. This function is particularly useful in executor nodes that process many tuples in tight loops, as it ensures memory allocated during expression evaluation is cleaned up immediately after each tuple evaluation.

The function first calls ExecQual to evaluate the boolean expression, then immediately resets the ecxt_per_tuple_memory context to free any temporary memory allocated during expression evaluation. This pattern prevents memory bloat in long-running queries that process many tuples, as temporary allocations (such as intermediate string operations, function call results, etc.) are cleaned up after each tuple rather than accumulating throughout the query execution.

The inline implementation inlines ResetExprContext functionality directly to avoid header ordering issues while maintaining optimal performance.

## Parameters / Member Variables
- : ExprState pointer containing the compiled expression to evaluate; if NULL, ExecQual returns true
- : ExprContext providing execution context and containing the per-tuple memory context to be reset

## Dependencies
- Functions called/Symbols referenced:
  - [ExecQual](ExecQual.md) (for expression evaluation)
  - [MemoryContextReset](../M/MemoryContextReset.md) (for memory context cleanup)
- Called from (representative examples):
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (heap scan filtering)
  - [IndexNext](../I/IndexNext.md) (index scan filtering)
  - [ExecGroup](ExecGroup.md) (grouping qualification)
  - [ExecLimit](ExecLimit.md) (limit condition checking)
  - [TupleHashTableMatch](../T/TupleHashTableMatch.md) (hash table matching)

## Notes and Other Information
- This inline function optimizes the common pattern of evaluate-and-reset that occurs frequently in executor nodes
- Essential for preventing memory leaks in long-running queries that process many tuples
- The per-tuple memory context reset only affects temporary allocations made during expression evaluation
- Particularly important for expressions involving string operations, function calls, or other operations that allocate temporary memory
- Used extensively in scan nodes and other iterative processing contexts where memory efficiency is crucial