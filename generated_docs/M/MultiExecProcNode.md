# MultiExecProcNode

## Location
[src/backend/executor/execProcnode.c:502-556](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execProcnode.c#L502-L556)

## Overview
MultiExecProcNode executes plan nodes that return complex data structures (like hash tables or bitmaps) rather than individual tuples.

## Definition

```c
Node *
MultiExecProcNode(PlanState *node)
```
## Detailed Description
MultiExecProcNode serves as the execution dispatcher for plan nodes that produce bulk data structures rather than streaming individual tuples. Unlike the regular ExecProcNode which returns TupleTableSlot pointers for tuple-at-a-time processing, MultiExecProcNode returns Node pointers representing complex data structures such as hash tables, bitmaps, or other specialized formats.

The function handles a limited set of node types that support multi-execution semantics:
- HashState: Produces hash tables for hash joins
- BitmapIndexScanState: Produces bitmaps from index scans
- BitmapAndState: Combines multiple bitmaps using AND logic
- BitmapOrState: Combines multiple bitmaps using OR logic

Key differences from ExecProcNode include:
- No automatic instrumentation wrapper (each node type must handle its own instrumentation)
- Returns Node* instead of TupleTableSlot*
- Supports parameter change detection and automatic rescanning
- Stack depth checking and interrupt handling for safety

The function performs parameter change detection via chgParam and automatically triggers rescanning when parameters have changed, ensuring that multi-exec nodes remain consistent with their execution environment.

## Parameters / Member Variables
- : The PlanState node to execute in multi-exec mode

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md) (stack overflow prevention)
  - CHECK_FOR_INTERRUPTS (interrupt handling macro)
  - [ExecReScan](../E/ExecReScan.md) (parameter change handling)
  - nodeTag (node type identification)
  - [MultiExecHash](MultiExecHash.md), MultiExecBitmapIndexScan, MultiExecBitmapAnd, MultiExecBitmapOr (type-specific execution functions)
- Called from (representative examples):
  - [MultiExecBitmapAnd](MultiExecBitmapAnd.md) (for child bitmap operations)
  - [BitmapHeapNext](../B/BitmapHeapNext.md) (for bitmap heap scan setup)
  - [MultiExecBitmapOr](MultiExecBitmapOr.md) (for child bitmap operations)
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md) (for hash table construction)

## Notes and Other Information
- Returns a Node pointer rather than TupleTableSlot like regular execution functions
- Only supports specific node types that can produce bulk data structures
- Each supported node type must provide its own instrumentation since automatic timing is not feasible
- Handles parameter changes by automatically triggering node rescanning
- Stack depth and interrupt checking provide safety during potentially long-running bulk operations
- Used primarily for hash table construction and bitmap index operations