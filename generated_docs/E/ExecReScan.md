# ExecReScan

## Location
[src/backend/executor/execAmi.c:76-325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAmi.c#L76-L325)

## Overview
ExecReScan is the central function that resets a plan node so that its output can be re-scanned, handling parameter changes and dispatching to node-type-specific rescan functions.

## Definition

```c
void
ExecReScan(PlanState *node)
```
## Detailed Description
ExecReScan serves as the main dispatcher function for rescanning plan nodes in PostgreSQL's executor. When a plan node needs to be reset to start producing output from the beginning again, this function handles the common setup tasks and then delegates to specialized rescan functions based on the node type.

The function performs several key operations:
1. **Timing instrumentation**: Updates execution timing statistics if instrumentation is enabled
2. **Parameter propagation**: If parameters have changed (indicated by node->chgParam), it propagates these changes to InitPlans, SubPlans, and child nodes
3. **Expression context reset**: Calls ReScanExprContext to reset expression evaluation contexts
4. **Node-specific processing**: Dispatches to the appropriate node-type-specific rescan function based on the node's type tag
5. **Cleanup**: Frees the changed parameter set after processing

The function supports a comprehensive set of PostgreSQL plan node types, from basic scan nodes (SeqScan, IndexScan) to complex join and aggregation nodes (HashJoin, WindowAgg), ensuring that any plan tree can be properly reset for re-execution.

## Parameters / Member Variables
- : Pointer to the PlanState node to be rescanned. Contains execution state information including timing instrumentation, parameter change indicators, and node-type-specific data.

## Dependencies
- Functions called/Symbols referenced:
  - [InstrEndLoop](../I/InstrEndLoop.md) (timing instrumentation)
  - [UpdateChangedParamSet](../U/UpdateChangedParamSet.md) (parameter propagation)
  - [ExecReScanSetParamPlan](ExecReScanSetParamPlan.md) (InitPlan parameter handling)
  - ReScanExprContext (expression context reset)
  - nodeTag (node type identification)
  - [bms_free](../b/bms_free.md) (memory cleanup)
  - [Node](../N/Node.md)-specific rescan functions (ExecReScanResult, ExecReScanSeqScan, etc.)
- Called from (representative examples):
  - [ExecutorRewind](ExecutorRewind.md) (main executor rewind)
  - [ExecReScanNestLoop](ExecReScanNestLoop.md) (nested loop joins)
  - ExecReScanMergeJoin (merge joins)
  - [ExecReScanHashJoin](ExecReScanHashJoin.md) (hash joins)

## Notes and Other Information
- The function includes extensive parameter change handling logic that ensures InitPlans can depend on sibling InitPlans that appear earlier in the list
- Parameter propagation follows a specific order: InitPlans are processed first, then SubPlans, then child nodes (outer and inner)
- The comprehensive switch statement covers all supported PostgreSQL plan node types, with an error case for unrecognized node types
- Memory management is handled by freeing the chgParam bitmapset after processing
- This function is critical for implementing features like nested loops, subqueries, and other constructs that require multiple passes over the same data