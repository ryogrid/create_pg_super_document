# ExecEndNode

## Location
src/backend/executor/execProcnode.c: 557 - 766

## Overview
ExecEndNode is the centralized cleanup function that recursively terminates all nodes in a PostgreSQL query plan tree, ensuring proper resource deallocation and preventing memory leaks after query execution.

## Definition
```c
void ExecEndNode(PlanState *node)
```

## Detailed Description
ExecEndNode implements a comprehensive node cleanup mechanism using a dispatch pattern based on node types. It performs several key operations:

1. **Null Safety**: Handles null node pointers gracefully by returning early
2. **Stack Overflow Protection**: Validates stack depth using check_stack_depth() to prevent crashes during deep recursion
3. **Parameter Cleanup**: Frees the chgParam bitmapset that tracks changed parameters
4. **Type-Based Dispatch**: Uses a large switch statement to call the appropriate cleanup function for each node type
5. **Comprehensive Coverage**: Handles all PostgreSQL plan node types including control nodes (Result, Append), scan nodes (SeqScan, IndexScan), join nodes (NestLoop, HashJoin), and materialization nodes (Sort, Hash, Agg)

The function is designed to be called only after query execution is complete, as it renders the plan tree unusable for further processing. It follows PostgreSQL's naming convention where each node type has a corresponding ExecEnd* function (e.g., ExecEndSeqScan, ExecEndHashJoin).

## Parameters / Member Variables
- `node`: PlanState pointer to the root of the plan tree to be cleaned up; can be NULL for safe termination

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth (stack overflow protection)
  - [bms_free](../b/bms_free.md) (bitmap set memory deallocation)
  - nodeTag (node type identification)
  - [ExecEndResult](ExecEndResult.md), ExecEndProjectSet, ExecEndModifyTable (control node cleanup)
  - [ExecEndSeqScan](ExecEndSeqScan.md), ExecEndIndexScan, ExecEndBitmapHeapScan (scan node cleanup)
  - [ExecEndNestLoop](ExecEndNestLoop.md), ExecEndMergeJoin, ExecEndHashJoin (join node cleanup)
  - [ExecEndSort](ExecEndSort.md), ExecEndHash, ExecEndAgg, ExecEndWindowAgg (materialization node cleanup)
- Called from (representative examples):
  - [ExecEndPlan](ExecEndPlan.md) (main plan termination)
  - [EvalPlanQualEnd](EvalPlanQualEnd.md) (EPQ cleanup)
  - Various ExecEnd* functions (recursive cleanup)

## Notes and Other Information
- Acts as the central dispatcher for all plan node cleanup operations in PostgreSQL
- Essential for preventing memory leaks in query execution
- Must be called after query execution completion
- Includes special handling for nodes that require no cleanup (ValuesScanState, NamedTuplestoreScanState, WorkTableScanState)
- Uses recursive pattern where many node-specific ExecEnd* functions call ExecEndNode on their child nodes
- Critical for parallel query cleanup through specialized functions like ExecEndGather and ExecEndGatherMerge